import { NextResponse } from "next/server"
import { createOIDCClient } from "@/lib/auth/oidc"
import { verifyCsrfToken } from "@/lib/security"

export async function GET(request: Request) {
  const oidcClient = createOIDCClient({
    clientId: process.env.OIDC_CLIENT_ID!,
    redirectUri: process.env.NEXT_PUBLIC_APP_URL + "/api/auth/[...action]/callback",
    issuer: process.env.OIDC_ISSUER!,
  })

  const { tokens, state } = await oidcClient.callback(request)
  
  if (!tokens.id_token) {
    return NextResponse.json(
      { error: "Authentication failed" },
      { status: 400 }
    )
  }

  // Decode the ID token to get user info
  const payload = oidcClient.decoded_id_token(tokens.id_token)
  
  // Create session
  const response = NextResponse.redirect(new URL("/(dashboard)/overview", request.url))
  
  // Set auth cookie
  response.cookies.set("next-auth", JSON.stringify({
    user: {
      id: payload.sub,
      email: payload.email,
      name: payload.name,
    },
    authenticated: true,
  }), {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/",
  })
  
  return response
}
