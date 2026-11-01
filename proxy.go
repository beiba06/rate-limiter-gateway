package main

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"
)

// The default transport keeps only 2 idle connections per host, which churns TCP
// connections under load and risks exhausting ephemeral ports.
var backendTransport = &http.Transport{
	MaxIdleConns:        1024,
	MaxIdleConnsPerHost: 512,
	IdleConnTimeout:     90 * time.Second,
}

func newRouter(routes []Route) (http.Handler, error) {
	mux := http.NewServeMux()
	for _, route := range routes {
		target, err := url.Parse(route.Backend)
		if err != nil {
			return nil, fmt.Errorf("route %s: %w", route.Prefix, err)
		}
		mux.Handle(route.Prefix, &httputil.ReverseProxy{
			Transport: backendTransport,
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(target)
				r.SetXForwarded()
			},
		})
	}
	return mux, nil
}

// withRateLimit rejects requests over their key's quota. If Redis is unreachable
// it fails open: losing rate limiting is better than losing the whole API.
func withRateLimit(keys map[string]Limit, limiter *Limiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		limit, ok := keys[key]
		if !ok {
			http.Error(w, "missing or unknown API key", http.StatusUnauthorized)
			return
		}
		decision, err := limiter.Allow(r.Context(), key, limit)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		if !decision.Allowed {
			w.Header().Set("Retry-After", retryAfterSeconds(decision.RetryAfter))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// retryAfterSeconds rounds up: Retry-After is whole seconds, and rounding down
// would invite a retry that is guaranteed to be rejected.
func retryAfterSeconds(d time.Duration) string {
	return strconv.FormatInt(int64(math.Ceil(d.Seconds())), 10)
}
