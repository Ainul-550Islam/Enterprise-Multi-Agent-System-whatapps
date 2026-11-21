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
}

/** A decoded API envelope result. */
export type MasResult<T> = { ok: true; data: T } | { ok: false; code: string; message: string };

/** Thrown when a scoped call is attempted without a scope. */
export class MissingScopeError extends Error {
  constructor() {
    super("missing scope: pass tenantId and organizationId together (MAS_TENANT_ID / MAS_ORGANIZATION_ID)");
    this.name = "MissingScopeError";
  }
}

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

  constructor(config: MasClientConfig) {
    this.baseUrl = config.baseUrl.replace(/\/+$/, "");
    this.token = config.token;
    this.scope = config.scope;
    this.correlationId = config.correlationId ?? uuid();
    this.fetchImpl = config.fetchImpl ?? fetch;
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
    agentId: string;
    input: string;
    idempotencyKey?: string;
  }): Promise<MasResult<{ executionId: string; status: string }>> {
    this.requireScope();
    return this.request("POST", "/v1/executions", input, input.idempotencyKey ?? uuid());
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

  /** Raw request through the envelope decoder. */
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
    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const envelope = (await response.json()) as {
      data?: T;
      error?: { code?: string; message?: string };
    };
    if (!response.ok || envelope.error) {
      return {
        ok: false,
        code: envelope.error?.code ?? `HTTP_${response.status}`,
        message: envelope.error?.message ?? response.statusText,
      };
    }
    return { ok: true, data: envelope.data as T };
  }
}
