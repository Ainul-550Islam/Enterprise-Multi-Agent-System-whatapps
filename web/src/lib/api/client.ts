/**
 * Browser API client. Talks only to the same-origin BFF (`/api/mas/...`).
 * The MAS bearer token never enters JavaScript.
 */
import { readCsrfCookie } from "@/lib/csrf";

import { unwrapEnvelope, StableApiError, type ApiEnvelope } from "./types";

export interface MasRequestInit {
  method?: string;
  body?: unknown;
  query?: Record<string, string | number | undefined>;
  headers?: Record<string, string>;
  signal?: AbortSignal;
}

function withQuery(path: string, query?: Record<string, string | number | undefined>): string {
  if (!query) {
    return path;
  }
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === "") {
      continue;
    }
    params.set(key, String(value));
  }
  const encoded = params.toString();
  return encoded ? `${path}?${encoded}` : path;
}

export async function masFetch<T>(path: string, init: MasRequestInit = {}): Promise<T> {
  const method = (init.method ?? "GET").toUpperCase();
  const headers: Record<string, string> = {
    accept: "application/json",
    "x-csrf-token": readCsrfCookie(),
    ...(init.headers ?? {}),
  };
  let body: string | undefined;
  if (init.body !== undefined) {
    headers["content-type"] = "application/json";
    body = JSON.stringify(init.body);
  }
  const response = await fetch(`/api/mas${withQuery(path, init.query)}`, {
    method,
    credentials: "same-origin",
    headers,
    body,
    signal: init.signal,
  });
  if (response.status === 401) {
    if (typeof window !== "undefined" && !path.startsWith("/auth")) {
      window.location.href = `/login?next=${encodeURIComponent(window.location.pathname)}`;
    }
    throw new StableApiError({ code: "UNAUTHENTICATED", message: "authentication required" });
  }
  const envelope = (await response.json().catch(() => ({
    error: { code: `HTTP_${response.status}`, message: response.statusText },
    meta: {
      request_id: "",
      correlation_id: "",
      api_version: "v1",
      responded_at: new Date().toISOString(),
    },
  }))) as ApiEnvelope<T>;
  return unwrapEnvelope(envelope, response.status);
}

export const api = {
  get: <T>(path: string, query?: MasRequestInit["query"]) => masFetch<T>(path, { query }),
  post: <T>(path: string, body?: unknown) => masFetch<T>(path, { method: "POST", body }),
  put: <T>(path: string, body?: unknown) => masFetch<T>(path, { method: "PUT", body }),
  patch: <T>(path: string, body?: unknown) => masFetch<T>(path, { method: "PATCH", body }),
  delete: <T>(path: string, body?: unknown) => masFetch<T>(path, { method: "DELETE", body }),
};
