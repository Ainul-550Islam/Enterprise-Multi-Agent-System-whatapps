import { NextResponse } from "next/server"
import { useAuth } from "@/lib/auth/session"

export default function LogoutPage() {
  const { clearAuth } = useAuth()

  return (
    <div className="min-h-screen flex items-center justify-center bg-background p-6">
      <div className="w-full max-w-md bg-card rounded-lg p-8 shadow-lg text-center">
        <h2 className="text-2xl font-bold mb-4">Logged Out</h2>
        <p className="text-muted-foreground mb-6">
          You have been successfully logged out.
        </p>
        <button
          onClick={async () => {
            clearAuth()
            const response = NextResponse.redirect(new URL("/", window.location.href))
            response.cookies.set("next-auth", "", {
              httpOnly: true,
              secure: process.env.NODE_ENV === "production",
              sameSite: "lax",
              path: "/",
              maxAge: 0,
            })
            window.location.href = "/"
          }}
          className="btn-primary"
        >
          Continue to Home
        </button>
      </div>
    </div>
  )
}

export async function GET(request: Request) {
  const { clearAuth } = useAuth()
  
  clearAuth()
  
  const response = NextResponse.redirect(new URL("/", request.url))
  response.cookies.set("next-auth", "", {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/",
    maxAge: 0,
  })
  
  return response
}