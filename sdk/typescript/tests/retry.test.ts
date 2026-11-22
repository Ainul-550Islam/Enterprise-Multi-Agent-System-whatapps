import assert from "node:assert/strict";
import { test } from "node:test";

import { backoffWithJitter, isRetryableStatus, waitBeforeRetry } from "../src/retry.js";

test("only transient statuses are retryable", () => {
  for (const status of [429, 500, 502, 503, 504]) assert.equal(isRetryableStatus(status), true);
  for (const status of [200, 400, 401, 403, 404, 409]) assert.equal(isRetryableStatus(status), false);
});

test("retry-after overrides exponential delay", () => {
  assert.equal(backoffWithJitter(5, 0.25, { maxDelayMs: 5000 }), 250);
});

test("exponential delay is bounded and jittered", () => {
  for (const attempt of [1, 2, 8, 20]) {
    const delay = backoffWithJitter(attempt, null, { maxDelayMs: 2000 });
    assert.ok(delay >= 0 && delay <= 2000, `attempt ${attempt}: ${delay}`);
  }
});

test("waitBeforeRetry uses the injected clock", async () => {
  const sleeps: number[] = [];
  await waitBeforeRetry(1, 1, { sleep: async (ms) => { sleeps.push(ms); } });
  assert.deepEqual(sleeps, [1000]);
});
