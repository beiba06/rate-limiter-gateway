# Benchmark Results

All three algorithms ran through the same gateway and Redis under identical load. `bench/run.sh` reproduces this, and the raw output is in [`raw-results.txt`](raw-results.txt).

**Latency does not separate the algorithms. Accuracy and memory do.**

| | Token bucket | Sliding window log | Fixed window |
|---|---|---|---|
| Admitted in a boundary burst (quota 100) | 106 | **100** | 200 |
| Redis memory for one key at 3,000 req/s | 97 B | about 2.4 MB | **62 B** |
| Median latency at 3,000 req/s | 0.22 ms | 0.25 ms | 0.22 ms |

## Setup

- **Host:** Apple M4, running a Colima VM with 4 vCPUs and 6 GB of memory.
- **Stack:** k6 2.3.0, Go 1.27.3, Redis 8.10, with Prometheus and Grafana running alongside. Everything shares the same VM.
- **Backend:** `traefik/whoami`, which responds almost instantly, so measured latency is mostly gateway and Redis.
- **Load:** k6 `constant-arrival-rate`, which keeps sending at the target rate even if responses slow down.

Read the numbers with that in mind. Throughput is understated, because k6 competes with the gateway for CPU. Latency is flattering, because every hop stays inside one VM with no real network.

## Allowed Requests at 3,000 req/s

Median of 3 trials of 30 seconds each, rotating the order so no algorithm always ran first. Ranges are shown where they matter.

| | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|
| No gateway (direct to backend) | 0.09 | 0.16 | 0.49 |
| Token bucket | 0.22 | 0.49 | 1.14 (1.06–1.71) |
| Sliding window log | 0.25 (0.22–0.25) | 1.06 (0.44–1.17) | 2.15 (1.00–2.49) |
| Fixed window | 0.22 | 0.51 | 1.25 (0.92–1.30) |

The gateway adds about 0.13 ms at the median, including the Redis check.

Sliding window's p95 was about twice the others in 2 of 3 trials. That could be real, since it is the only algorithm maintaining a large sorted set, but it is a weak signal: the third trial matched the others. Mean Redis check time was 80–103 µs, and the order of the algorithms changed between full runs, so no difference is claimed there.

## Rejected Requests at 3,000 req/s

Quota of 100 req/s, so about 97% of requests are rejected. Median of 3 trials of 30 seconds each.

| | p50 (ms) | p95 (ms) | p99 (ms) | Admitted (ideal 3,000) |
|---|---|---|---|---|
| Token bucket | 0.12 | 0.22 | 0.71 | 3,087–3,099 |
| Sliding window log | 0.12 | 0.23 | 0.64 | 3,000 in every trial |
| Fixed window | 0.12 | 0.24 | 0.74 | 3,100 in every trial |

A rejected request costs about half as much as an allowed one because it never reaches the backend. Under this load the gateway protected the backend from about 87,000 of 90,000 requests.

Only sliding window hit the ideal count exactly. Token bucket starts with a full bucket of 100, and fixed window touched 31 windows during the 30-second run.

## Window Boundary Burst

Quota of 100 per 5 seconds. The test sends the full quota 300 ms before a window boundary and again 50 ms after it, all within about 355 ms. All three trials gave identical results.

| | Before boundary | After boundary | Total admitted |
|---|---|---|---|
| Token bucket | 100 | 6 | 106 |
| Sliding window log | 100 | 0 | **100** |
| Fixed window | 100 | 100 | **200** |

This is fixed window's known weakness, measured: twice the quota in about a third of a second, against a limit meant to spread it across 5 seconds. Token bucket's extra 6 is its refill rate of 20 tokens/s over the 300 ms between bursts.

## Memory per Key

`MEMORY USAGE ... SAMPLES 0` on the key under load, at 3,000 req/s with a 10-second window.

| | Size | Grows with |
|---|---|---|
| Token bucket | 97 B | Constant |
| Fixed window | 62 B | Constant |
| Sliding window log | 2.38–2.52 MB | Requests admitted in the window, about 90 B each |

For one busy key, sliding window uses about 25,000 times the memory of token bucket. Its accuracy is real, but so is the cost, which grows with both traffic and window length.

## Capacity

One 20-second run per algorithm at each rate.

| Rate (req/s) | p50 (ms) | p99 (ms) | Dropped | Errors |
|---|---|---|---|---|
| 5,000 | 0.19 | 1.25–1.56 | 0 | 0 |
| 7,000 | 0.20 | 2.10–2.59 | 0 | 0 |
| 9,000 | 0.27–0.29 | 7.33–26.27 | 0–1,170 (up to 0.65%) | 0 |

All three algorithms ran cleanly through 7,000 req/s and began to saturate at 9,000. That is the limit of this shared VM, which is not necessarily the limit of the gateway. "Dropped" means k6 could not start a request on schedule.

Across the whole run, 2,878,502 requests went through the gateway with 0 errors. There were 0 Redis errors, so fail-open never inflated these results.

## Noise

Medians were stable between full runs of this suite. Tails were not: brief stalls landed on random runs, and in one earlier run they pushed the no-gateway baseline to a p99 of 5.98 ms. Earlier runs also dropped requests at loads this run handled cleanly, at worst 0.87% in one 3,000 req/s run. Because of this, small p99 differences between algorithms should be read as noise.
