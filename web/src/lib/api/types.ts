/**
 * Envelope + error types taken from the generated OpenAPI schema.
 * Hand-written field lists are forbidden — `schema.d.ts` is the source.
 */
import type { components } from "./schema";

export type StableApiErrorBody = components["schemas"]["StableApiError"];
export type EnvelopeMeta = components["schemas"]["EnvelopeMeta"];

export interface ApiEnvelope<T> {
  data?: T | null;
  error?: StableApiErrorBody | null;
  meta: EnvelopeMeta;
}

export class StableApiError extends Error {
  readonly code: string;
  readonly requestId?: string;
  readonly details?: Record<string, unknown>;

  constructor(body: StableApiErrorBody) {
    super(body.message);
    this.name = "StableApiError";
    this.code = body.code;
    this.requestId = body.request_id;
    this.details = body.details as Record<string, unknown> | undefined;
  }
}

export function unwrapEnvelope<T>(envelope: ApiEnvelope<T>, status: number): T {
  if (envelope.error && envelope.error.code) {
    throw new StableApiError(envelope.error);
  }
  if (status >= 400) {
    throw new StableApiError({
      code: `HTTP_${status}`,
      message: envelope.error?.message ?? `request failed with HTTP ${status}`,
    });
  }
  return envelope.data as T;
}
