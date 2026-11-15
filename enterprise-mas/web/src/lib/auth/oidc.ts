import { api } from "@/lib/api/client"
import type {
  OIDCCallbackResult,
  LoginCredentials,
  ApiKeyLoginResult,
  OIDCLoginResult,
} from "@/lib/api/types"

// OIDC authorization code flow with PKCE
export class OIDCClient {
  private readonly clientId: string
  private readonly redirectUri: string
  private readonly issuer: string

  constructor({
    clientId,
    redirectUri,
    issuer,
  }: {
    clientId: string
    redirectUri: string
    issuer: string
  }) {
    this.clientId = clientId
    this.redirectUri = redirectUri
    this.issuer = issuer
  }

  /** Generate PKCE verifier and challenge */
  private generatePkce(): { verifier: string; challenge: string } {
    const verifier = crypto.randomUUID() + crypto.randomUUID()
    // Create SHA256 challenge
    const encoder = new TextEncoder()
    const challenge = btoa(
      encoder.encode(verifier).reduce(
        (acc, byte) => acc + byte.toString(16),
        ""
      )
    )
    return { verifier, challenge }
  }

  /** Build the authorization URL */
  getAuthUrl(state: string): URL {
    const pkce = this.generatePkce()
    const url = new URL(`${this.issuer}/oauth/authorize`)
    url.searchParams.set("client_id", this.clientId)
    url.searchParams.set("redirect_uri", this.redirectUri)
    url.searchParams.set("response_type", "code")
    url.searchParams.set("scope", "openid profile email")
    url.searchParams.set("code_challenge", pkce.challenge)
    url.searchParams.set("code_challenge_method", "S256")
    url.searchParams.set("state", state)
    return url
  }

  /** Exchange authorization code for tokens */
  async exchangeCode(code: string): Promise<{
    success: boolean
    session?: import("@/lib/auth/session").SessionData
    error?: string
  }> {
    try {
      const response = await api.post<{
        access_token: string
        refresh_token?: string
        expires_at?: number
        user: import("@/lib/api/types").UserInfo
      }>("/api/auth/oidc/callback", {
        code,
        client_id: this.clientId,
        client_secret: process.env.OIDC_CLIENT_SECRET,
        redirect_uri: this.redirectUri,
      })

      return {
        success: true,
        session: {
          access_token: user.access_token,
          refresh_token: user.refresh_token,
          expires_at: user.expires_at,
        },
      }
    } catch (error) {
      return {
        success: false,
        error: error.message || "Token exchange failed",
      }
    }
  }

  /** Start the login flow */
  async login(state: string): Promise<URL> {
    return this.getAuthUrl(state)
  }
}

// API key login
export async function loginWithApiKey(apiKey: string): Promise<ApiKeyLoginResult> {
  try {
    const response = await api.post<{
      success: boolean
      session?: import("@/lib/api/types").SessionData
    }>("/api/auth/login", { api_key: apiKey })

    if (response.success && response.session) {
      // Store session cookies (set by BFF)
      return { success: true }
    }

    return {
      success: false,
      error: response.error?.message || "Invalid API key",
    }
  } catch (error) {
    return {
      success: false,
      error: error.message || "Login failed",
    }
  }
}

// Initialize OIDC client
export function createOIDCClient(
  options: {
    clientId: string
    redirectUri: string
    issuer: string
  }
): OIDCClient {
  return new OIDCClient(options.clientId, options.redirectUri, options.issuer)
}
