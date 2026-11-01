package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_requests_total",
		Help: "Requests by API key, active algorithm and response status.",
	}, []string{"key", "algorithm", "status"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_request_duration_seconds",
		Help:    "End-to-end request latency through the gateway.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"algorithm"})

	limiterDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_ratelimit_check_duration_seconds",
		Help:    "Latency of the Redis rate-limit check alone.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
	}, []string{"algorithm"})

	redisErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_redis_errors_total",
		Help: "Rate-limit checks that failed and were allowed through (fail open).",
	})
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(status int) {
	s.status = status
	s.ResponseWriter.WriteHeader(status)
}

// Unwrap lets http.ResponseController reach Flush on the real writer, which
// ReverseProxy needs for streaming responses.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func withMetrics(keys map[string]Limit, limiter *Limiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		algorithm := limiter.Algorithm()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)

		// Only configured keys become label values. The header is attacker
		// controlled, and every new label value is a new Prometheus time series.
		key := r.Header.Get("X-API-Key")
		if _, ok := keys[key]; !ok {
			key = "unknown"
		}
		requestsTotal.WithLabelValues(key, algorithm, strconv.Itoa(recorder.status)).Inc()
		requestDuration.WithLabelValues(algorithm).Observe(time.Since(start).Seconds())
	})
}
