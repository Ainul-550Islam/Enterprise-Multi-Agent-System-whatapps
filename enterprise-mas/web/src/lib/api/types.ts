import type { ApiEnvelope, StableApiError } from "@/lib/api/types"

// Authentication types
export type AuthStatus = "unauthenticated" | "authenticated" | "loading"

// Session types
export interface SessionData {
  access_token: string
  refresh_token?: string
  expires_at?: number
  user: UserInfo
}

export interface UserInfo {
  id: string
  email: string
  name?: string
  tenant_id?: string
  project_id?: string
  roles: string[]
}

// API error types
export interface StableApiError {
  code: string
  message: string
  type?: "VALIDATION" | "AUTHENTICATION" | "AUTHORIZATION" | "NOT_FOUND" | "UNKNOWN"
}

export interface ValidationError {
  field: string
  message: string
  issues: string[]
}

// OIDC types
export interface OIDCCallbackResult {
  success: boolean
  code?: string
  error?: string
  error_description?: string
}

export interface LoginCredentials {
  method: "api-key" | "oidc"
  apiKey?: string
  oidcCode?: string
  redirectUrl?: string
}

// API response types (from OpenAPI)
export interface ApiEnvelope<T = any> {
  data: T
  error?: StableApiError
  meta?: {
    requestId: string
    timestamp: string
    path: string
  }
}

// API key login result
export interface ApiKeyLoginResult {
  success: boolean
  session?: SessionData
  error?: StableApiError
}

// OIDC login result
export interface OIDCLoginResult {
  success: boolean
  session?: SessionData
  error?: StableApiError
  redirectUrl?: string
}
