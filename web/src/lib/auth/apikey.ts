/**
 * Self-host API-key login.
 *
 * The raw key is posted to the BFF (`/api/auth/apikey`) which stores it in an
 * httpOnly session cookie. This module never writes the key to localStorage,
 * sessionStorage, or any other browser-visible store.
 */
import { readCsrfCookie } from "@/lib/csrf";

export class MissingApiKeyError extends Error {
  constructor() {
    super("API key is required");
    this.name = "MissingApiKeyError";
  }
}

export class ApiKeyLoginError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "ApiKeyLoginError";
    this.code = code;
  }
}

export interface ApiKeyLoginResult {
  subject: string;
  tenantId?: string;
  organizationId?: string;
}

/** Posts the API key to the BFF. The key is not retained by the caller. */
export async function resolveApiKey(
  apiKey: string,
  fetchImpl: typeof fetch = fetch,
): Promise<ApiKeyLoginResult> {
  const trimmed = apiKey.trim();
  if (!trimmed) {
    throw new MissingApiKeyError();
  }
  const response = await fetchImpl("/api/auth/apikey", {
    method: "POST",
    credentials: "same-origin",
    headers: {
      "content-type": "application/json",
      "x-csrf-token": readCsrfCookie(),
    },
    body: JSON.stringify({ apiKey: trimmed }),
  });
  const document = (await response.json().catch(() => ({}))) as {
    subject?: string;
    tenantId?: string;
    organizationId?: string;
    error?: { code?: string; message?: string };
  };
  if (!response.ok) {
    throw new ApiKeyLoginError(
      document.error?.code ?? `HTTP_${response.status}`,
      document.error?.message ?? "API key login failed",
    );
  }
  return {
    subject: document.subject ?? "apikey",
    tenantId: document.tenantId,
    organizationId: document.organizationId,
  };
}

/** Alias kept for existing call sites. */
export const loginWithApiKey = resolveApiKey;

export async function logout(fetchImpl: typeof fetch = fetch): Promise<void> {
  await fetchImpl("/api/auth/logout", {
    method: "POST",
    credentials: "same-origin",
    headers: { "x-csrf-token": readCsrfCookie() },
  });
}
