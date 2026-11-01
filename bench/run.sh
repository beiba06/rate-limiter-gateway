#!/usr/bin/env bash
# Reproduces bench/results.md. Needs the Compose stack running: docker compose up -d
# Usage: RATE=3000 DURATION=30s TRIALS=3 bench/run.sh
set -euo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-3000}
DURATION=${DURATION:-30s}
TRIALS=${TRIALS:-3}
ALGORITHMS=(token_bucket sliding_window fixed_window)

k6() { docker compose --profile bench run --rm -T "$@"; }
redis() { docker compose exec -T redis redis-cli "$@"; }
use() { curl -fsS -o /dev/null -X POST localhost:9090/admin/algorithm -d "{\"algorithm\":\"$1\"}"; }
load() { # label, target, api key
  k6 -e LABEL="$1" -e TARGET="$2" -e API_KEY="$3" -e RATE="$RATE" -e DURATION="$DURATION" \
    k6 run --quiet /bench/k6-load-test.js
}
# Fixed window state can expire between the end of a run and the reading, so touch it first.
memory() {
  curl -fsS -o /dev/null -H 'X-API-Key: bench_open' localhost:8080/api/hello
  echo "{\"label\":\"memory:$1\",\"bytes\":$(redis memory usage "ratelimit:$1:bench_open" samples 0)}"
}
rotated() { for offset in 0 1 2; do echo "${ALGORITHMS[($1 + offset) % 3]}"; done; }

# Fresh counters, so the metrics printed at the end cover exactly this run.
docker compose restart gateway
sleep 2

echo "# Environment: $(docker info --format '{{.NCPU}} vCPU, {{.MemTotal}} bytes') (k6, gateway, Redis and backend share it)"

echo "# Allowed path: $RATE req/s for $DURATION, $TRIALS trials. Order rotates so no algorithm always runs first."
for ((trial = 0; trial < TRIALS; trial++)); do
  load baseline_no_gateway http://backend:80/api/hello none
  sleep 5
  for algorithm in $(rotated "$trial"); do
    use "$algorithm"
    load "allowed:$algorithm" http://gateway:8080/api/hello bench_open
    memory "$algorithm"
    sleep 5
  done
done

echo "# Rejected path: quota 100 req/s under $RATE req/s of load"
for ((trial = 0; trial < TRIALS; trial++)); do
  for algorithm in $(rotated "$trial"); do
    use "$algorithm"
    load "rejected:$algorithm" http://gateway:8080/api/hello bench_hot
    sleep 5
  done
done

echo "# Boundary burst: quota 100 per 5s, two full-quota bursts around the boundary"
for algorithm in "${ALGORITHMS[@]}"; do
  use "$algorithm"
  for ((trial = 0; trial < TRIALS; trial++)); do
    k6 -e LABEL="$algorithm" k6 run --quiet /bench/boundary-test.js 2>&1 | grep -o '{.*}' | sed 's/\\"/"/g'
    sleep 6
  done
done

echo "# Capacity sweep"
for rate in 5000 7000 9000; do
  for algorithm in "${ALGORITHMS[@]}"; do
    use "$algorithm"
    RATE=$rate DURATION=20s load "sweep:$algorithm" http://gateway:8080/api/hello bench_open
    sleep 5
  done
done

echo "# Gateway counters for the whole run"
curl -fsS localhost:9090/metrics | grep -E '^gateway_(redis_errors_total|ratelimit_check_duration_seconds_(sum|count))'
use token_bucket
