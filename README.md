# Distributed Rate Limiter & API Gateway

A standalone API gateway in Go that sits in front of any backend, enforcing per-user rate limits with three algorithms (token bucket, sliding window, fixed window), request routing, and real-time metrics.

## Architecture

```
Client Request
    │
    ▼
API Gateway (Go, net/http)
    ├── 1. Extract X-API-Key (missing or unknown → 401)
    ├── 2. Check rate limit ──────► Redis (atomic Lua script)
    │      token bucket / sliding window / fixed window
    ├── 3. Allow → proxy to backend
    │   or 429 + Retry-After header
    ├── 4. Log metrics ──────► Prometheus /metrics
    └── 5. Reverse proxy ──────► Backend service(s)
```

**Core principle: minimum code, maximum depth.** Implement all three rate-limiting algorithms, benchmark them against each other, and explain the trade-offs. No web framework: `net/http` plus the Redis client, the Prometheus client and a YAML parser.

## Results

Measured with k6 on 4 vCPUs shared with the load generator. Full method and caveats are in [`bench/results.md`](bench/results.md).

- **Throughput:** 7,000 req/s with a p99 under 2.6 ms and no errors, on all three algorithms. Saturation starts around 9,000 req/s on this machine.
- **Overhead:** about 0.13 ms added at the median at 3,000 req/s, including the Redis check.
- **Reliability:** 2.8M requests through the gateway with 0 errors. In a manual check, two gateway instances sharing Redis admitted exactly the quota between them.

| | Token bucket | Sliding window log | Fixed window |
|---|---|---|---|
| Admitted in a boundary burst (quota 100) | 106 | **100** | 200 |
| Redis memory for one key at 3,000 req/s | 97 B | about 2.4 MB | **62 B** |
| Median latency at 3,000 req/s | 0.22 ms | 0.25 ms | 0.22 ms |

Latency does not separate the algorithms. Accuracy and memory do.

## Milestones

- [x] **M1: Gateway core** — Go reverse proxy with config-based routing to backend services. A request hits the gateway, gets proxied, response comes back.
- [x] **M2: Rate limiting** — Token bucket algorithm in Redis (atomic Lua script). Returns 429 with Retry-After when limit exceeded. Per-key limits configurable via YAML.
- [x] **M3: Three algorithms** — Add sliding window log and fixed window counter alongside token bucket. A `/admin/algorithm` endpoint switches between them live. Benchmark script compares all three under load.
- [x] **M4: Observability** — Prometheus metrics (request count, latency histogram, rejection rate per key). Grafana dashboard JSON included.
- [ ] **M5: Ship** — Docker Compose (gateway + Redis + dummy backend), Dockerfile and a k6 load test exceeding 1000 req/s are done. Deploying to Fly.io is not.

## File Structure

```
rate-limiter-gateway/
├── main.go                # Entry point, config loading, Redis client, server start
├── proxy.go               # Reverse proxy, rate-limit middleware, admin endpoints
├── ratelimit.go           # Three algorithms as Lua scripts, shared algorithm state
├── ratelimit_test.go      # Unit and HTTP tests
├── metrics.go             # Prometheus metrics and middleware
├── config.yaml            # Routes, keys and limits for `go run`
├── config.docker.yaml     # The same, with Compose hostnames and benchmark keys
├── bench/
│   ├── k6-load-test.js    # Constant-arrival-rate load test
│   ├── boundary-test.js   # Burst around a window boundary
│   ├── run.sh             # Reproduces results.md
│   ├── results.md         # Benchmark results: 3 algorithms compared
│   └── raw-results.txt    # Unprocessed output behind results.md
├── grafana/               # Dashboard JSON and provisioning
├── prometheus.yml         # Scrape config
├── docker-compose.yml     # Gateway, Redis, backend, Prometheus, Grafana
├── Dockerfile             # Multi-stage build onto scratch
└── README.md
```

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
curl -i -H "X-API-Key: user1" http://localhost:8080/api/hello
# user1 is limited to 5 requests per 10s. After that:
# HTTP/1.1 429 Too Many Requests
# Retry-After: 2

# Switch algorithm live, on every gateway instance
curl -X POST localhost:9090/admin/algorithm -d '{"algorithm":"sliding_window"}'

# Unit tests need no Redis or Docker
go test ./...
```

To run the whole stack instead, including Prometheus and the Grafana dashboard:

```bash
docker compose up -d --build
```

| Port | Service |
|------|---------|
| 8080 | Gateway |
| 9090 | Gateway admin: `/metrics`, `/admin/algorithm` |
| 9091 | Prometheus |
| 3000 | Grafana (dashboard: Rate Limiter Gateway) |

## Algorithms Compared

| Algorithm | Pros | Cons | Best For |
|-----------|------|------|----------|
| **Token bucket** | Smooth, allows bursts up to bucket size | Slightly more complex state | General-purpose APIs |
| **Sliding window log** | Most accurate, no boundary issues | Stores every timestamp (memory) | Low-volume, high-accuracy |
| **Fixed window counter** | Simplest, least memory | Boundary spike (2x burst at window edge) | High-volume, approximate |

M3 benchmarks all three under identical load and documents the results in [`bench/results.md`](bench/results.md).

## Design Decisions

- **Redis is the clock.** Every script reads `TIME` from Redis, so clock skew between gateway instances cannot change a limit.
- **The active algorithm lives in Redis.** Each instance polls it once a second. An in-process variable would only switch the one instance that received the admin request.
- **Unknown API keys get a 401, not a default quota.** Otherwise rotating `X-API-Key` would mint fresh quota on every request. It also keeps Prometheus cardinality bounded, because only configured keys become label values.
- **Fail open when Redis is down, with a bounded delay.** Losing rate limiting is better than losing the API. Failed checks are counted in `gateway_redis_errors_total`, shown on the dashboard. The Redis client's defaults had to be tightened: with `DialerRetries` left at 5, each request paid 430ms during an outage. It now pays about 2ms if Redis is down, and about 105ms if Redis accepts connections but never answers.
- **Sliding window members embed a counter.** Two requests in the same millisecond would otherwise share a sorted-set member, and the second would silently not count. Rejected requests are not logged, so memory is bounded by the limit.
- **Fixed window needs one key, not one per window.** `PEXPIREAT` expires the counter exactly on the boundary. No key has to be derived inside the script, which keeps it compatible with Redis Cluster's rule that scripts declare their keys.
- **Admin and metrics are on a separate port.** `/admin/algorithm` has no authentication of its own, and metric labels contain API keys, so neither is reachable through the public port.
- **The backend transport keeps a large idle pool.** Go's default of 2 idle connections per host churns TCP connections under load.

## Known Limitations

- Limits are not enforced during a Redis outage, by design. Redis has no replica here.
- Switching algorithm gives every key a fresh allowance, because each algorithm keeps separate state.
- API keys are stored in plaintext config and appear in metric labels. A production version would hash them and give each key a display name.
- The scripts follow Redis Cluster's key rules, but this has only been run against a single Redis node.
