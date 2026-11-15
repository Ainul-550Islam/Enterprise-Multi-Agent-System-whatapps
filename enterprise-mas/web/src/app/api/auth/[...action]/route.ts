import { NextResponse } from "next/server"
import { getServerSession, loginWithApiKey, createOIDCClient } from "@/lib/auth/session"
import { redirect } from "next/navigation"

export async function POST(request: Request, { params }: { params: { action: string } }) {
  const action = params.action

  try {
    switch (action) {
      case "login": {
        const { searchParams } = new URL(request.url)
        const method = searchParams.get("method") || "api-key"
        const apiKey = searchParams.get("api_key")

        if (method === "api-key" && apiKey) {
          const result = await loginWithApiKey(apiKey)
          if (result.success && result.session) {
            // BFF sets httpOnly cookies; client redirects on success
            return NextResponse.json({ success: true, session: result.session })
          }
          return NextResponse.json(
            { success: false, error: result.error || "Invalid API key" },
            { status: 401 }
          )
        }

        if (method === "oidc") {
          const oidcClient = createOIDCClient({
            clientId: process.env.OIDC_CLIENT_ID!,
            redirectUri: `${process.env.NEXT_PUBLIC_APP_URL}/api/auth/[...action]/callback`,
            issuer: process.env.OIDC_ISSUER!,
          })

          const authUrl = await oidcClient.login(state === "?"
            ? crypto.randomUUID()
            : state
          )

          // Return the authorization URL for the client to redirect
          return NextResponse.json({ authUrl: authUrl.toString() })
        }

        return NextResponse.json(
          { success: false, error: "Unknown login method" },
          { status: 400 }
        )
      }

      case "callback": {
        // OIDC callback handler
        const { searchParams } = new URL(request.url)
        const code = searchParams.get("code")
        const state = searchParams.get("state")

        if (!code) {
          return NextResponse.json(
            { success: false, error: "Missing authorization code" },
            { status: 400 }
          )
        }

        // Exchange code for tokens
        const oidcClient = createOIDCClient({
          clientId: process.env.OIDC_CLIENT_ID!,
          redirectUri: `${process.env.NEXT_PUBLIC_APP_URL}/api/auth/[...action]/callback`,
          issuer: process.env.OIDC_ISSUER!,
        })

        const result = await oidcClient.exchangeCode(code)

        if (result.success && result.session) {
          // Set httpOnly cookies (BFF pattern)
          // In production, the actual cookie setting happens in a server component
          // or middleware; here we return the session data
          return NextResponse.json({
            success: true,
            session: {
              access_token: result.session.access_token,
              refresh_token: result.session.refresh_token,
              expires_at: result.session.expires_at,
            },
          })
        }

        return NextResponse.json(
          { success: false, error: result.error || "OAuth callback failed" },
          { status: 400 }
        )
      }

      case "logout": {
        // Clear session cookies and redirect to home
        const response = NextResponse.redirect(new URL("/", request.url))
        response.cookies.set("access_token", "", {
          httpOnly: true,
          secure: process.env.NODE_ENV === "production",
          sameSite: "lax",
          path: "/",
          maxAge: 0,
        })
        response.cookies.set("refresh_token", "", {
          httpOnly: true,
          secure: process.env.NODE_ENV === "production",
          sameSite: "lax",
          path: "/",
          maxAge: 0,
        })
        response.cookies.set("expires_at", "", {
          httpOnly: true,
          secure: process.env.NODE_ENV === "production",
          sameSite: "lax",
          path: "/",
          maxAge: 0,
        })
        return response
      }

      case "refresh": {
        // Refresh the access token using the refresh token
        const cookies = request.cookies
        const refreshToken = cookies.get("refresh_token")

        if (!refreshToken) {
          return NextResponse.json(
            { success: false, error: "No refresh token" },
            { status: 401 }
          )
        }

        // In production, call the token refresh endpoint
        // For now, simulate a new session
        const newSession: import("@/lib/api/types").SessionData = {
          access_token: `new_token_${crypto.randomUUID()}`,
          refresh_token: refreshToken,
          expires_at: Date.now() + 24 * 60 * 60 * 1000,
        }

        // Set new cookies
        const response = NextResponse.json({ success: true, session: newSession })
        response.cookies.set("access_token", newSession.access_token, {
          httpOnly: true,
          secure: process.env.NODE_ENV === "production",
          sameSite: "lax",
          path: "/",
          maxAge: 60 * 60 * 24, // 1 day
        })
        response.cookies.set("expires_at", String(newSession.expires_at), {
          httpOnly: true,
          secure: process.env.NODE_ENV === "production",
          sameSite: "lax",
          path: "/",
          maxAge: 60 * 60 * 24,
        })

        return response
      }

      default:
        return NextResponse.json(
          { success: false, error: `Unknown action: ${action}` },
          { status: 400 }
        )
    }
  } catch (error) {
    console.error("Auth action error:", error)
    return NextResponse.json(
      { success: false, error: "Internal server error" },
      { status: 500 }
    )
  }
}
