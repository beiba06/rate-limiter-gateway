package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var algorithms = []string{"token_bucket", "sliding_window", "fixed_window"}

// clock drives miniredis time. Both are needed: SetTime moves what the scripts
// read from TIME, FastForward moves key expiry.
type clock struct {
	server *miniredis.Miniredis
	now    time.Time
}

func (c *clock) advance(d time.Duration) {
	c.now = c.now.Add(d)
	c.server.SetTime(c.now)
	c.server.FastForward(d)
}

// setup starts a fake Redis with its clock frozen on a 10s window boundary.
func setup(t *testing.T) (*redis.Client, *clock) {
	t.Helper()
	server := miniredis.RunT(t)
	c := &clock{server: server, now: time.Unix(1_700_000_000, 0)}
	server.SetTime(c.now)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return rdb, c
}

func admitted(t *testing.T, l *Limiter, key string, limit Limit, n int) int {
	t.Helper()
	count := 0
	for range n {
		d, err := l.Allow(context.Background(), key, limit)
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			count++
		}
	}
	return count
}

func TestAllowsUpToLimitThenRejects(t *testing.T) {
	for _, algorithm := range algorithms {
		t.Run(algorithm, func(t *testing.T) {
			rdb, _ := setup(t)
			l := NewLimiter(rdb, algorithm)
			limit := Limit{Limit: 5, Window: 10 * time.Second}

			// The clock is frozen, so this also proves same-millisecond requests
			// are each counted.
			if got := admitted(t, l, "user", limit, 5); got != 5 {
				t.Fatalf("admitted %d of the first 5", got)
			}
			d, err := l.Allow(context.Background(), "user", limit)
			if err != nil {
				t.Fatal(err)
			}
			if d.Allowed {
				t.Fatal("request over the limit was allowed")
			}
			if d.RetryAfter <= 0 || d.RetryAfter > limit.Window {
				t.Fatalf("RetryAfter = %v, want within (0, %v]", d.RetryAfter, limit.Window)
			}
		})
	}
}

func TestRetryAfterIsSufficient(t *testing.T) {
	for _, algorithm := range algorithms {
		t.Run(algorithm, func(t *testing.T) {
			rdb, c := setup(t)
			l := NewLimiter(rdb, algorithm)
			limit := Limit{Limit: 5, Window: 10 * time.Second}

			admitted(t, l, "user", limit, 5)
			d, _ := l.Allow(context.Background(), "user", limit)
			c.advance(d.RetryAfter)
			if got := admitted(t, l, "user", limit, 1); got != 1 {
				t.Fatalf("still rejected after waiting RetryAfter = %v", d.RetryAfter)
			}
		})
	}
}

func TestKeysAreIsolated(t *testing.T) {
	for _, algorithm := range algorithms {
		t.Run(algorithm, func(t *testing.T) {
			rdb, _ := setup(t)
			l := NewLimiter(rdb, algorithm)
			limit := Limit{Limit: 3, Window: 10 * time.Second}

			admitted(t, l, "a", limit, 10)
			if got := admitted(t, l, "b", limit, 3); got != 3 {
				t.Fatalf("key b admitted %d of 3 after key a was exhausted", got)
			}
		})
	}
}

func TestConcurrentRequestsCannotOvershoot(t *testing.T) {
	for _, algorithm := range algorithms {
		t.Run(algorithm, func(t *testing.T) {
			rdb, _ := setup(t)
			l := NewLimiter(rdb, algorithm)
			limit := Limit{Limit: 50, Window: 10 * time.Second}

			var allowed atomic.Int64
			var wg sync.WaitGroup
			for range 200 {
				wg.Go(func() {
					d, err := l.Allow(context.Background(), "user", limit)
					if err != nil {
						t.Error(err)
					}
					if d.Allowed {
						allowed.Add(1)
					}
				})
			}
			wg.Wait()
			if allowed.Load() != limit.Limit {
				t.Fatalf("admitted %d, want exactly %d", allowed.Load(), limit.Limit)
			}
		})
	}
}

// The trade-off from the README, as an executable claim: send the full quota in
// the last 100ms of a window and again in the first 100ms of the next one.
func TestWindowBoundaryBurst(t *testing.T) {
	limit := Limit{Limit: 100, Window: 10 * time.Second}
	want := map[string]int{
		"fixed_window":   200, // Double the quota inside 200ms.
		"sliding_window": 100,
		"token_bucket":   102, // The quota, plus 200ms of refill at 10 tokens/s.
	}

	for _, algorithm := range algorithms {
		t.Run(algorithm, func(t *testing.T) {
			rdb, c := setup(t)
			l := NewLimiter(rdb, algorithm)

			c.advance(limit.Window - 100*time.Millisecond)
			got := admitted(t, l, "user", limit, 100)
			c.advance(200 * time.Millisecond)
			got += admitted(t, l, "user", limit, 100)
			if got != want[algorithm] {
				t.Fatalf("admitted %d around the boundary, want %d", got, want[algorithm])
			}
		})
	}
}

func TestTokenBucketRefillIsCappedAtCapacity(t *testing.T) {
	rdb, c := setup(t)
	l := NewLimiter(rdb, "token_bucket")
	limit := Limit{Limit: 5, Window: 10 * time.Second}

	// Stay inside the key's TTL: expired state is a fresh bucket and would hide a
	// missing cap. 4 tokens left + 2.5 refilled would be 6.5 without one.
	admitted(t, l, "user", limit, 1)
	c.advance(limit.Window / 2)
	if got := admitted(t, l, "user", limit, 20); got != 5 {
		t.Fatalf("admitted %d after idling, want capacity 5", got)
	}
}

func TestSlidingWindowMemoryIsBoundedByLimit(t *testing.T) {
	rdb, c := setup(t)
	l := NewLimiter(rdb, "sliding_window")
	limit := Limit{Limit: 5, Window: 10 * time.Second}

	admitted(t, l, "user", limit, 500)
	entries, err := c.server.ZMembers("ratelimit:sliding_window:user")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("stored %d entries, want 5: rejected requests must not be logged", len(entries))
	}
}

func TestStateExpiresWhenIdle(t *testing.T) {
	for _, algorithm := range algorithms {
		t.Run(algorithm, func(t *testing.T) {
			rdb, c := setup(t)
			l := NewLimiter(rdb, algorithm)
			limit := Limit{Limit: 5, Window: 10 * time.Second}

			admitted(t, l, "user", limit, 1)
			c.advance(limit.Window)
			if key := "ratelimit:" + algorithm + ":user"; c.server.Exists(key) {
				t.Fatalf("%s outlived its window", key)
			}
		})
	}
}

func TestAlgorithmSwitchReachesOtherInstances(t *testing.T) {
	rdb, _ := setup(t)
	ctx := context.Background()
	a, b := NewLimiter(rdb, "token_bucket"), NewLimiter(rdb, "token_bucket")

	if err := a.SetAlgorithm(ctx, "fixed_window"); err != nil {
		t.Fatal(err)
	}
	if err := b.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if b.Algorithm() != "fixed_window" {
		t.Fatalf("instance b is on %s", b.Algorithm())
	}

	// A restart with a different configured default must not undo a live switch.
	c := NewLimiter(rdb, "sliding_window")
	if err := c.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Algorithm() != "fixed_window" {
		t.Fatalf("restarted instance is on %s", c.Algorithm())
	}
}

func TestRedisRestartKeepsLiveAlgorithm(t *testing.T) {
	rdb, c := setup(t)
	ctx := context.Background()
	l := NewLimiter(rdb, "token_bucket")
	if err := l.SetAlgorithm(ctx, "fixed_window"); err != nil {
		t.Fatal(err)
	}

	c.server.FlushAll()
	if err := l.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := rdb.Get(ctx, algorithmKey).Result(); got != "fixed_window" {
		t.Fatalf("Redis was re-seeded with %q, want the live algorithm", got)
	}
}

func TestFailOpenIsFast(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := newRedisClient(server.Addr())
	t.Cleanup(func() { rdb.Close() })
	server.Close()

	l := NewLimiter(rdb, "token_bucket")
	start := time.Now()
	_, err := l.Allow(context.Background(), "user", Limit{Limit: 1, Window: time.Second})
	if err == nil {
		t.Fatal("expected an error with Redis down")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("failing open took %v; every request pays this during an outage", elapsed)
	}
}

func TestSetAlgorithmRejectsUnknownName(t *testing.T) {
	rdb, _ := setup(t)
	l := NewLimiter(rdb, "token_bucket")
	if err := l.SetAlgorithm(context.Background(), "leaky_bucket"); err == nil {
		t.Fatal("expected an error")
	}
	if l.Algorithm() != "token_bucket" {
		t.Fatalf("algorithm changed to %s", l.Algorithm())
	}
}

func gateway(t *testing.T, l *Limiter, keys map[string]Limit) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Write([]byte("backend saw " + r.URL.Path))
	}))
	t.Cleanup(backend.Close)

	router, err := newRouter([]Route{{Prefix: "/api/", Backend: backend.URL}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(withMetrics(keys, l, withRateLimit(keys, l, router)))
	t.Cleanup(server.Close)
	return server, &hits
}

func request(t *testing.T, url, key string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestGatewayProxiesThenRejectsWithRetryAfter(t *testing.T) {
	rdb, _ := setup(t)
	keys := map[string]Limit{"user1": {Limit: 5, Window: 10 * time.Second}}
	server, hits := gateway(t, NewLimiter(rdb, "token_bucket"), keys)

	for range 5 {
		if resp := request(t, server.URL+"/api/hello", "user1"); resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	resp := request(t, server.URL+"/api/hello", "user1")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want 2", got)
	}
	if hits.Load() != 5 {
		t.Fatalf("backend received %d requests, want 5", hits.Load())
	}
}

func TestGatewayRejectsMissingAndUnknownKeys(t *testing.T) {
	rdb, _ := setup(t)
	keys := map[string]Limit{"user1": {Limit: 5, Window: time.Second}}
	server, hits := gateway(t, NewLimiter(rdb, "token_bucket"), keys)

	for _, key := range []string{"", "not-a-real-key"} {
		if resp := request(t, server.URL+"/api/hello", key); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("key %q: status %d, want 401", key, resp.StatusCode)
		}
	}
	if hits.Load() != 0 {
		t.Fatal("unauthenticated request reached the backend")
	}
}

func TestGatewayFailsOpenWhenRedisIsDown(t *testing.T) {
	rdb, c := setup(t)
	keys := map[string]Limit{"user1": {Limit: 1, Window: time.Minute}}
	server, hits := gateway(t, NewLimiter(rdb, "token_bucket"), keys)
	c.server.Close()

	for range 3 {
		if resp := request(t, server.URL+"/api/hello", "user1"); resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d with Redis down, want 200", resp.StatusCode)
		}
	}
	if hits.Load() != 3 {
		t.Fatalf("backend received %d requests, want 3", hits.Load())
	}
}

func TestRouterSendsPrefixToItsBackend(t *testing.T) {
	serve := func(name string) *httptest.Server {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(name))
		}))
		t.Cleanup(s.Close)
		return s
	}
	users, orders := serve("users"), serve("orders")

	router, err := newRouter([]Route{
		{Prefix: "/users/", Backend: users.URL},
		{Prefix: "/orders/", Backend: orders.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{"/users/1": "users", "/orders/1": "orders"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Body.String() != want {
			t.Fatalf("%s reached %q, want %q", path, w.Body.String(), want)
		}
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nothing", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("unrouted path returned %d", w.Code)
	}
}

func TestAdminAlgorithmEndpoint(t *testing.T) {
	rdb, _ := setup(t)
	l := NewLimiter(rdb, "token_bucket")
	admin := httptest.NewServer(adminHandler(l))
	t.Cleanup(admin.Close)

	post := func(body string) int {
		resp, err := http.Post(admin.URL+"/admin/algorithm", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := post(`{"algorithm":"sliding_window"}`); got != http.StatusOK {
		t.Fatalf("valid switch returned %d", got)
	}
	if l.Algorithm() != "sliding_window" {
		t.Fatalf("algorithm is %s", l.Algorithm())
	}
	if got := post(`{"algorithm":"leaky_bucket"}`); got != http.StatusBadRequest {
		t.Fatalf("unknown algorithm returned %d", got)
	}
	if got := post(`not json`); got != http.StatusBadRequest {
		t.Fatalf("malformed body returned %d", got)
	}
}

func TestMetricsCollapseUnknownKeysIntoOneLabel(t *testing.T) {
	rdb, _ := setup(t)
	keys := map[string]Limit{"user1": {Limit: 5, Window: time.Second}}
	server, _ := gateway(t, NewLimiter(rdb, "token_bucket"), keys)
	admin := httptest.NewServer(adminHandler(NewLimiter(rdb, "token_bucket")))
	t.Cleanup(admin.Close)

	request(t, server.URL+"/api/hello", "attacker-controlled-value")
	body, err := io.ReadAll(request(t, admin.URL+"/metrics", "").Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "attacker-controlled-value") {
		t.Fatal("raw X-API-Key value leaked into metric labels")
	}
	if !strings.Contains(string(body), `key="unknown"`) {
		t.Fatal(`expected a key="unknown" series`)
	}
}
