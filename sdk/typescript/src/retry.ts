/** Bounded retry helpers for transient HTTP failures. */

export interface RetryOptions {
  attempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  jitterRatio?: number;
  sleep?: (milliseconds: number) => Promise<void>;
}

const DEFAULT_OPTIONS = {
  attempts: 3,
  baseDelayMs: 50,
  maxDelayMs: 2000,
  jitterRatio: 0.25,
};

/** Retryable status codes: rate limits and transient server failures. */
export function isRetryableStatus(status: number): boolean {
  return status === 429 || status === 500 || status === 502 || status === 503 || status === 504;
}

/** Jittered exponential delay; a numeric Retry-After header wins. */
export function backoffWithJitter(
  attempt: number,
  retryAfterSeconds: number | null = null,
  options: RetryOptions = {},
): number {
  const config = { ...DEFAULT_OPTIONS, ...options };
  if (retryAfterSeconds !== null && Number.isFinite(retryAfterSeconds)) {
    return Math.min(Math.max(retryAfterSeconds * 1000, 0), config.maxDelayMs);
  }
  const exponential = Math.min(
    config.baseDelayMs * 2 ** Math.max(attempt - 1, 0),
    config.maxDelayMs,
  );
  const spread = exponential * config.jitterRatio;
  return Math.min(
    Math.max(0, exponential + (Math.random() * 2 - 1) * spread),
    config.maxDelayMs,
  );
}

/** Waits according to the retry policy. */
export async function waitBeforeRetry(
  attempt: number,
  retryAfterSeconds: number | null,
  options: RetryOptions = {},
): Promise<void> {
  const config = { ...DEFAULT_OPTIONS, ...options };
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  await sleep(backoffWithJitter(attempt, retryAfterSeconds, config));
}
