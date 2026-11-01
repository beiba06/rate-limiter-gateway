# Distributed Rate Limiter & API Gateway

A standalone API gateway in Go that sits in front of any backend, enforcing per-user rate limits with three algorithms (token bucket, sliding window, fixed window), request routing, and real-time metrics.

## Architecture

```
Client Request
    │
    ▼
API Gateway (Go, net/http)
    ├── 1. Extract API key / user ID
    ├── 2. Check rate limit ──────► Redis (atomic INCR + TTL)
    │      token bucket / sliding window / fixed window
    ├── 3. Allow → proxy to backend
    │   or 429 + Retry-After header
    ├── 4. Log metrics ──────► Prometheus /metrics
    └── 5. Reverse proxy ──────► Backend service(s)
```

**Core principle: minimum code, maximum depth.** Implement all three rate-limiting algorithms, benchmark them against each other, and explain the trade-offs. No framework — stdlib `net/http` only.

## Milestones

- [ ] **M1: Gateway core** — Go reverse proxy with config-based routing to backend services. A request hits the gateway, gets proxied, response comes back.
- [ ] **M2: Rate limiting** — Token bucket algorithm in Redis (atomic Lua script). Returns 429 with Retry-After when limit exceeded. Per-key limits configurable via YAML.
- [ ] **M3: Three algorithms** — Add sliding window log and fixed window counter alongside token bucket. A `/admin/algorithm` endpoint switches between them live. Benchmark script compares all three under load.
- [ ] **M4: Observability** — Prometheus metrics (request count, latency histogram, rejection rate per key). Grafana dashboard JSON included.
- [ ] **M5: Ship** — Docker Compose (gateway + Redis + dummy backend), Dockerfile, deploy to Fly.io. Load test with k6 proving it handles 1000+ req/s.

## File Structure

```
rate-limiter-gateway/
├── main.go              # Entry point, config loading, server start         (~40 lines)
├── proxy.go             # Reverse proxy + middleware chain                  (~50 lines)
├── ratelimit.go         # Three algorithms: token bucket, sliding, fixed    (~120 lines)
├── ratelimit_test.go    # Unit tests for all three algorithms               (~80 lines)
├── metrics.go           # Prometheus middleware                             (~30 lines)
├── config.yaml          # Routes, rate limits, algorithm selection
├── bench/
│   ├── k6-load-test.js  # Load test script
│   └── results.md       # Benchmark results: 3 algorithms compared
├── docker-compose.yml   # Gateway + Redis + dummy backend
├── Dockerfile           # Multi-stage Go build
└── README.md
```

**~320 lines of Go + tests. No framework, no ORM, stdlib only.**

## Tech Choices

| Choice | Why | Not |
|--------|-----|-----|
| **Go stdlib `net/http`** | Reverse proxy built-in (`httputil.ReverseProxy`), goroutines handle concurrency natively | Gin/Echo (unnecessary for a proxy) |
| **Redis** | Atomic counters via Lua scripts, shared state across gateway instances, sub-ms latency | In-memory (doesn't scale horizontally) |
| **Lua scripts in Redis** | Atomic check-and-increment — no race conditions between read and write | Multiple Redis commands (race condition window) |
| **Prometheus** | Industry standard, Go client library is 5 lines | Custom metrics (nobody knows your format) |
| **k6** | Load testing in JS, beautiful output, supports scenarios | wrk (less flexible), ab (too basic) |

## Running Locally

```bash
# Start Redis + dummy backend
docker compose up -d redis backend

# Run gateway
go run . --config config.yaml

# Test it
curl -H "X-API-Key: user1" http://localhost:8080/api/hello
# After N requests:
# HTTP 429 Too Many Requests
# Retry-After: 2
```

## Algorithms Compared

| Algorithm | Pros | Cons | Best For |
|-----------|------|------|----------|
| **Token bucket** | Smooth, allows bursts up to bucket size | Slightly more complex state | General-purpose APIs |
| **Sliding window log** | Most accurate, no boundary issues | Stores every timestamp (memory) | Low-volume, high-accuracy |
| **Fixed window counter** | Simplest, least memory | Boundary spike (2x burst at window edge) | High-volume, approximate |

M3 benchmarks all three under identical load and documents the results in `bench/results.md`.
