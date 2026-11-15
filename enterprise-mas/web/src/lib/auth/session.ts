import { persistSession, getSession, removeSession } from "@/lib/api/session-storage"

// Session persistence using httpOnly-cookie-set via next/headers
// In production, the BFF sets cookies; client-side we just track the session state

export interface SessionInfo {
  /** True if user is authenticated */
  authenticated: boolean
  /** User information */
  user: import("@/lib/api/types").UserInfo | null
  /** Token expiration timestamp */
  expiresAt: number | null
  /** Loaded from cookies or session storage */
  loadedFrom: "cookie" | "storage" | "none"
}

/**
 * Parse the session from cookies (BFF pattern).
 * The BFF sets httpOnly cookies; this reads them on the server or client.
 */
export async function getServerSession(
  cookies: import("next").NextCookies
): Promise<SessionInfo> {
  const accessToken = cookies.get("access_token") ?? ""
  const refreshToken = cookies.get("refresh_token") ?? ""
  const expiresAt = cookies.get("expires_at") ? Number(cookies.get("expires_at")) : null

  let user: import("@/lib/api/types").UserInfo | null = null

  if (accessToken) {
    try {
      // Verify the token and get user info
      // In production, this would call the OIDC introspection endpoint
      // or decode the JWT to extract user claims
      user = {
        id: "user_123",
        email: "user@example.com",
        name: "Test User",
        tenant_id: "tenant_1",
        project_id: "project_1",
        roles: ["user"],
      }
    } catch {
      // Token invalid
    }
  }

  const from: SessionInfo["loadedFrom"] = accessToken && refreshToken ? "cookie" : "none"

  return {
    authenticated: !!accessToken,
    user,
    expiresAt,
    loadedFrom: from,
  }
}

/**
 * Get client-side session from sessionStorage.
 * Used during navigation when no cookies are available.
 */
export async function getClientSession(): Promise<SessionInfo> {
  const accessToken = sessionStorage.getItem("access_token") ?? ""
  const refreshToken = sessionStorage.getItem("refresh_token") ?? ""
  const expiresAt = sessionStorage.getItem("expires_at") ? Number(sessionStorage.getItem("expires_at")) : null

  let user: import("@/lib/api/types").UserInfo | null = null

  if (accessToken) {
    try {
      user = {
        id: "user_123",
        email: "user@example.com",
        name: "Test User",
        tenant_id: "tenant_1",
        project_id: "project_1",
        roles: ["user"],
      }
    } catch {
      // Token invalid
    }
  }

  const from: SessionInfo["loadedFrom"] = accessToken && refreshToken ? "storage" : "none"

  return {
    authenticated: !!accessToken,
    user,
    expiresAt,
    loadedFrom: from,
  }
}

/**
 * Clear the session (logout).
 */
export async function clearSession(): Promise<void> {
  // Clear httpOnly cookies via next/headers in server components
  // Clear client-side storage
  sessionStorage.removeItem("access_token")
  sessionStorage.removeItem("refresh_token")
  sessionStorage.removeItem("expires_at")
}
/** Logout the user by clearing all authentication cookies and storage. */
export async function logout(): Promise<void> {
  await clearSession()
}

// Rotate the refresh token
export async function refreshTokens(): Promise<SessionInfo> {
  // In production, call the token refresh endpoint
  // For now, simulate a new session
  return getClientSession()
}
