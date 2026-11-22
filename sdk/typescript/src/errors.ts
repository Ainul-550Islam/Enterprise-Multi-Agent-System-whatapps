/** Typed MAS SDK errors. */

/** A non-success API envelope, raised when `raiseOnError` is enabled. */
export class MasApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly requestId?: string,
  ) {
    super(`${status} ${code}: ${message}`);
    this.name = "MasApiError";
  }
}

/** HTTP 429 after the retry budget was exhausted. */
export class MasRateLimited extends MasApiError {
  constructor(public readonly retryAfter: number | null, message = "rate limited") {
    super(429, "RATE_LIMITED", message);
    this.name = "MasRateLimited";
  }
}

/** A scoped route was used without a resolved tenant and organization. */
export class MasScopeError extends Error {
  constructor() {
    super(
      "missing scope: pass tenantId and organizationId together (MAS_TENANT_ID / MAS_ORGANIZATION_ID)",
    );
    this.name = "MasScopeError";
  }
}
