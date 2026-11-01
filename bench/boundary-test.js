// Sends the full quota just before a window boundary and again just after it, then
// reports how many requests each algorithm admitted across the two bursts.
// Windows are aligned to the epoch, so the boundary is computable from the clock.
import http from 'k6/http';
import { sleep } from 'k6';

const TARGET = __ENV.TARGET || 'http://gateway:8080/api/hello';
const API_KEY = 'bench_edge';
const LIMIT = 100; // Must match bench_edge in config.docker.yaml.
const WINDOW_MS = 5000;

export const options = {
  discardResponseBodies: true,
  batch: LIMIT,
  batchPerHost: LIMIT,
  vus: 1,
  iterations: 1,
};

const sleepUntil = (ms) => sleep(Math.max(0, ms - Date.now()) / 1000);

function burst() {
  const request = ['GET', TARGET, null, { headers: { 'X-API-Key': API_KEY } }];
  const responses = http.batch(Array(LIMIT).fill(request));
  return responses.filter((res) => res.status === 200).length;
}

let result;

export default function () {
  // Skip one whole window so the burst starts from empty state.
  const boundary = (Math.floor(Date.now() / WINDOW_MS) + 2) * WINDOW_MS;

  // A burst takes ~100ms. Start early enough that the first one cannot spill over.
  sleepUntil(boundary - 300);
  const before = burst();
  const firstDone = Date.now();

  sleepUntil(boundary + 50);
  const after = burst();

  result = {
    label: __ENV.LABEL || '',
    quota: LIMIT,
    before_boundary: before,
    after_boundary: after,
    total_admitted: before + after,
    first_burst_end_vs_boundary_ms: firstDone - boundary,
    elapsed_ms: Date.now() - (boundary - 300),
  };
  console.log(JSON.stringify(result));
}

export function handleSummary() {
  return {};
}
