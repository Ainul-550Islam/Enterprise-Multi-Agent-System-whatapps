/**
 * ApiClient — stable import path for dashboard code that talks to the BFF.
 *
 * The implementation lives in `lib/api/client.ts`. This module re-exports it
 * so callers can `import { api, ApiClient } from "@/lib/api"` without
 * reaching past the BFF (the MAS bearer token never enters the browser).
 */

export { api, masFetch, type MasRequestInit } from "@/lib/api/client";
export { StableApiError, unwrapEnvelope, type ApiEnvelope } from "@/lib/api/types";

import { api } from "@/lib/api/client";

/** Named alias of the same-origin BFF client. */
export const ApiClient = api;
