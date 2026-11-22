/**
 * mas-sdk — TypeScript client for the Enterprise MAS API.
 *
 * Design rules mirrored from the `mas` CLI:
 * - the scope guard fails locally (before any network call) when a scoped
 *   route is used without tenant+organization;
 * - every mutation carries an idempotency key (generated when the caller
 *   does not supply one);
 * - responses are unwrapped from the stable envelope
 *   (`{ data | error, meta }`) into typed results — callers branch on
 *   `ok`, never on message text.
 */

import { isRetryableStatus, waitBeforeRetry, type RetryOptions } from "./retry.js";

export * from "./errors.js";
export * from "./resources/index.js";
export { backoffWithJitter, isRetryableStatus } from "./retry.js";

/** Scope headers the API requires together. */
export interface Scope {
  tenantId: string;
  organizationId: string;
}

/** Connection configuration. */
export interface MasClientConfig {
  baseUrl: string;
  token: string;
  scope?: Scope;
  correlationId?: string;
  fetchImpl?: typeof fetch;
  /** HTTP timeout in milliseconds (default 10000). */
  timeoutMs?: number;
  /** Transient-failure retry policy; defaults to three attempts. */
  retry?: RetryOptions;
  /** Raise typed errors instead of returning an error result. */
  raiseOnError?: boolean;
}

/** A decoded API envelope result. */
export type MasResult<T> = { ok: true; data: T } | { ok: false; code: string; message: string };

import { MasScopeError } from "./errors.js";

/** Backwards-compatible alias for the original SDK error name. */
export class MissingScopeError extends MasScopeError {}

function uuid(): string {
  return globalThis.crypto.randomUUID();
}

/** The MAS API client. */
export class MasClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly scope?: Scope;
  private readonly correlationId: string;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;
  private readonly retry: RetryOptions;
  private readonly raiseOnError: boolean;

  constructor(config: MasClientConfig) {
    this.baseUrl = config.baseUrl.replace(/\/+$/, "");
    this.token = config.token;
    this.scope = config.scope;
    this.correlationId = config.correlationId ?? uuid();
    this.fetchImpl = config.fetchImpl ?? fetch;
    this.timeoutMs = config.timeoutMs ?? 10_000;
    this.retry = config.retry ?? {};
    this.raiseOnError = config.raiseOnError ?? false;
  }

  /** Resolves the scope or fails locally, before the network. */
  requireScope(): Scope {
    if (!this.scope) {
      throw new MissingScopeError();
    }
    return this.scope;
  }

  /** Submits an execution and returns its id. */
  async submitExecution(input: {
    projectId: string;
    input: unknown;
    agentId?: string;
    workflowId?: string;
    idempotencyKey?: string;
  }): Promise<MasResult<{ id: string; status: string }>> {
    this.requireScope();
    if (Boolean(input.agentId) === Boolean(input.workflowId)) {
      throw new TypeError("provide exactly one of agentId or workflowId");
    }
    const body: Record<string, unknown> = { input: input.input };
    if (input.agentId) body.agent_id = input.agentId;
    if (input.workflowId) body.workflow_id = input.workflowId;
    return this.request(
      "POST",
      `/v1/projects/${encodeURIComponent(input.projectId)}/executions`,
      body,
      input.idempotencyKey ?? uuid(),
    );
  }

  /** Reads one execution. */
  async getExecution(executionId: string): Promise<MasResult<{ status: string }>> {
    this.requireScope();
    return this.request("GET", `/v1/executions/${encodeURIComponent(executionId)}`);
  }

  /** Sends a signal verb (start|pause|resume|cancel) to an execution. */
  async signalExecution(executionId: string, signal: "start" | "pause" | "resume" | "cancel") {
    this.requireScope();
    return this.request("POST", `/v1/executions/${encodeURIComponent(executionId)}/signal`, { signal });
  }

  /** Raw request through the envelope decoder, with bounded transient retries. */
  async request<T>(method: string, path: string, body?: unknown, idempotencyKey?: string): Promise<MasResult<T>> {
    const headers: Record<string, string> = {
      authorization: `Bearer ${this.token}`,
      "x-correlation-id": this.correlationId,
    };
    if (this.scope) {
      headers["x-tenant-id"] = this.scope.tenantId;
      headers["x-organization-id"] = this.scope.organizationId;
    }
    if (body !== undefined) {
      headers["content-type"] = "application/json";
    }
    if (idempotencyKey) {
      headers["x-idempotency-key"] = idempotencyKey;
    }

    const attempts = this.retry.attempts ?? 3;
    let lastStatus = 0;
    let lastStatusText = "";
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(this.timeoutMs),
      });
      lastStatus = response.status;
      lastStatusText = response.statusText;
      const envelope = (await response.json()) as {
        data?: T;
        error?: { code?: string; message?: string };
        meta?: { request_id?: string };
        retry_after?: number;
      };
      const failed = !response.ok || Boolean(envelope.error);
      if (!failed || !isRetryableStatus(response.status) || attempt === attempts) {
        if (failed && this.raiseOnError) {
          const { MasApiError, MasRateLimited } = await import("./errors.js");
          if (response.status === 429) {
            throw new MasRateLimited(envelope.retry_after ?? null, envelope.error?.message ?? lastStatusText);
          }
          throw new MasApiError(
            response.status,
            envelope.error?.code ?? `HTTP_${response.status}`,
            envelope.error?.message ?? lastStatusText,
            envelope.meta?.request_id,
          );
        }
        if (failed) {
          return {
            ok: false,
            code: envelope.error?.code ?? `HTTP_${response.status}`,
            message: envelope.error?.message ?? lastStatusText,
          };
        }
        return { ok: true, data: envelope.data as T };
      }
      const retryAfter = response.headers?.get?.("retry-after");
      const seconds = retryAfter === null || retryAfter === undefined
        ? (typeof envelope.retry_after === "number" ? envelope.retry_after : null)
        : Number(retryAfter);
      await waitBeforeRetry(attempt, Number.isFinite(seconds) ? seconds : null, this.retry);
    }
    return { ok: false, code: `HTTP_${lastStatus}`, message: lastStatusText };
  }
}
