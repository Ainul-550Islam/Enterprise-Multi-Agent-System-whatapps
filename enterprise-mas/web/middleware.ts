import { NextResponse } from "next/server"
import type { NextConfig } from "next"

const nextConfig: NextConfig = {
  /* config */
}

const basePath = ""

export const config = {
  matcher: [
    /*
     * Match all request paths except for the ones starting with:
     * - _next/static (static assets)
     * - _next/image (image optimization)
     * - favicon.ico (favicon)
     * - public (public assets)
     */
    "/((?!_next/static|_next/image|favicon.ico|public).*)",
  ],
}

export async function middleware(request) {
  const path = request.nextUrl.pathname

  // Skip auth pages
  if (path.startsWith("/(auth)") || path === "/") {
    return NextResponse.next()
  }

  // Protect dashboard routes
  if (path.startsWith("/(dashboard)")) {
    const authCookie = request.cookies.get("next-auth")
    
    if (!authCookie || !authCookie.value) {
      const loginUrl = new URL("/(auth)/login", request.url)
      loginUrl.searchParams.set("callbackUrl", request.url)
      return NextResponse.redirect(loginUrl)
    }

    try {
      const user = JSON.parse(authCookie.value)
      if (!user?.authenticated) {
        const loginUrl = new URL("/(auth)/login", request.url)
        loginUrl.searchParams.set("callbackUrl", request.url)
        return NextResponse.redirect(loginUrl)
      }
    } catch {
      const loginUrl = new URL("/(auth)/login", request.url)
      loginUrl.searchParams.set("callbackUrl", request.url)
      return NextResponse.redirect(loginUrl)
    }
  }

  return NextResponse.next()
}
