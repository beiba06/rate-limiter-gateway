package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
)

type Route struct {
	Prefix  string `yaml:"prefix"`
	Backend string `yaml:"backend"`
}

type Config struct {
	Listen      string           `yaml:"listen"`
	AdminListen string           `yaml:"admin_listen"`
	RedisAddr   string           `yaml:"redis_addr"`
	Algorithm   string           `yaml:"algorithm"`
	Routes      []Route          `yaml:"routes"`
	Keys        map[string]Limit `yaml:"keys"`
}

func loadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if _, ok := scripts[cfg.Algorithm]; !ok {
		return nil, fmt.Errorf("unknown algorithm %q", cfg.Algorithm)
	}
	for key, lim := range cfg.Keys {
		if lim.Limit <= 0 || lim.Window < time.Millisecond {
			return nil, fmt.Errorf("key %q: limit must be > 0 and window >= 1ms", key)
		}
	}
	return &cfg, nil
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	router, err := newRouter(cfg.Routes)
	if err != nil {
		log.Fatal(err)
	}

	// Short timeouts and no retries: a stalled Redis must fail open quickly
	// rather than queue every request behind it.
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		MaxRetries:   -1,
	})
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	limiter := NewLimiter(rdb, cfg.Algorithm)
	go limiter.Sync(ctx, time.Second)

	gateway := &http.Server{
		Addr:              cfg.Listen,
		Handler:           withMetrics(cfg.Keys, limiter, withRateLimit(cfg.Keys, limiter, router)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	admin := &http.Server{
		Addr:              cfg.AdminListen,
		Handler:           adminHandler(limiter),
		ReadHeaderTimeout: 5 * time.Second,
	}

	for _, srv := range []*http.Server{gateway, admin} {
		go func() {
			log.Printf("listening on %s", srv.Addr)
			if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				log.Fatal(err)
			}
		}()
	}

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gateway.Shutdown(shutdown)
	admin.Shutdown(shutdown)
}
