import { createApiClient, type ApiClient } from "openapi-fetch"
import type { ApiEnvelope, StableApiError } from "@/lib/api/types"

const BASE_URL = process.env.MAS_API_URL || "http://127.0.0.1:8080"

// Create typed fetch client from OpenAPI spec
export const api: ApiClient = createApiClient({
  baseUrl: BASE_URL,
  // Transform response to return ApiEnvelope
  transformResponse: (response) => {
    const data = response.text()
    // Parse and extract the envelope
    const json = JSON.parse(data)
    return json as ApiEnvelope
  },
  // Handle errors consistently
  async onError({ error, request, options }) {
    const status = error.status
    // Map StableApiError from response body
    if (error.status >= 500) {
      throw new Error(`Server error: ${error.status}`)
    }
    // Extract error message from API envelope
    const errorBody = error?.message || "API request failed"
    throw new Error(`API error ${status}: ${errorBody}`)
  },
} as const)

// Type-safe fetch helper with authentication headers
export async function fetchApi(
  endpoint: string,
  init: RequestInit = {},
  options: { withAuth?: boolean } = { withAuth: true }
): Promise<ApiEnvelope> {
  const headers: HeadersInit = {
    "Content-Type": "application/json",
    ...(options.withAuth && {
      "Authorization": `Bearer ${typeof window !== "undefined" ? sessionStorage.getItem("access_token") : ""}`,
    }),
    "X-Request-ID": crypto.randomUUID(),
    ...headers,
  }

  const response = await fetch(`${BASE_URL}${endpoint}`, {
    ...init,
    headers,
    credentials: "include", // BFF: httpOnly cookie
  })

  const data = await response.json()

  if (!response.ok) {
    const stableError: StableApiError = data.error || {
      code: String(response.status),
      message: data.message || "API request failed",
    }
    throw stableError
  }

  return data
}

// Type-safe wrapper for common operations
export async function get<T>(endpoint: string, options?: { withAuth?: boolean }): Promise<T> {
  return (await api(endpoint, { method: "GET" })).data as T
}

export async function post<T>(endpoint: string, body: unknown, options?: { withAuth?: boolean }): Promise<T> {
  return (await api(endpoint, {
    method: "POST",
    body: JSON.stringify(body),
    ...(options?.withAuth && { headers: { "Authorization": `Bearer ${sessionStorage.getItem("access_token")}` } }),
  })).data as T
}

export async function patch<T>(endpoint: string, body: unknown, options?: { withAuth?: boolean }): Promise<T> {
  return (await api(endpoint, {
    method: "PATCH",
    body: JSON.stringify(body),
    ...(options?.withAuth && { headers: { "Authorization": `Bearer ${sessionStorage.getItem("access_token")}` } }),
  })).data as T
}

export async function del<T>(endpoint: string, options?: { withAuth?: boolean }): Promise<T> {
  return (await api(endpoint, { method: "DELETE" })).data as T
}
