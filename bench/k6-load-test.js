// Constant-arrival-rate load: requests keep arriving at RATE even if responses slow
// down, so a saturated system shows up as latency and dropped iterations rather than
// as a quietly lower request rate.
import http from 'k6/http';
import { Counter } from 'k6/metrics';

const TARGET = __ENV.TARGET || 'http://gateway:8080/api/hello';
const API_KEY = __ENV.API_KEY || 'bench_open';
const RATE = Number(__ENV.RATE || 1000);

const allowed = new Counter('allowed');
const rejected = new Counter('rejected');
const errors = new Counter('errors');

// A 429 is the correct answer from a rate limiter, not a failed request.
http.setResponseCallback(http.expectedStatuses(200, 429));

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    load: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
    },
  },
};

export default function () {
  const res = http.get(TARGET, { headers: { 'X-API-Key': API_KEY } });
  if (res.status === 200) allowed.add(1);
  else if (res.status === 429) rejected.add(1);
  else errors.add(1);
}

export function handleSummary(data) {
  const value = (name, stat) => data.metrics[name]?.values[stat] ?? 0;
  const ms = (stat) => Number(value('http_req_duration', stat).toFixed(2));
  const summary = {
    label: __ENV.LABEL || TARGET,
    target_rate: RATE,
    achieved_rate: Math.round(value('http_reqs', 'rate')),
    p50_ms: ms('med'),
    p95_ms: ms('p(95)'),
    p99_ms: ms('p(99)'),
    max_ms: ms('max'),
    allowed: value('allowed', 'count'),
    rejected: value('rejected', 'count'),
    errors: value('errors', 'count'),
    dropped: value('dropped_iterations', 'count'),
  };
  return { stdout: JSON.stringify(summary) + '\n' };
}
