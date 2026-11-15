import { useState, useEffect, useRouter } from "react"
import { useAuth } from "@/lib/auth/session"
import { loginWithApiKey } from "@/lib/auth/session"
import { createOIDCClient } from "@/lib/auth/oidc"
import { generateCsrfToken } from "@/lib/security"

export default function LoginPage() {
  const { setAuth, auth } = useAuth()
  const router = useRouter()
  const [method, setMethod] = useState<"api-key" | "oidc">("api-key")
  const [apiKey, setApiKey] = useState("")
  const [csrfToken, setCsrfToken] = useState(generateCsrfToken())

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setCsrfToken(generateCsrfToken())

    if (method === "api-key") {
      const result = await loginWithApiKey(apiKey)
      if (result.success) {
        setAuth({
          authenticated: true,
          user: result.session?.user ?? null,
        })
        router.push("/(dashboard)/overview")
      } else {
        alert(result.error || "Login failed")
      }
    } else if (method === "oidc") {
      const oidcClient = createOIDCClient({
        clientId: process.env.OIDC_CLIENT_ID!,
        redirectUri: process.env.NEXT_PUBLIC_APP_URL + "/api/auth/[...action]/callback",
        issuer: process.env.OIDC_ISSUER!,
      })

      const authUrl = await oidcClient.login(state)
      router.push(authUrl.toString())
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-background p-6">
      <div className="w-full max-w-md space-y-6 bg-card rounded-lg p-8 shadow-lg">
        <h2 className="text-2xl font-bold mb-4">Sign In</h2>

        <form
          onSubmit={handleSubmit}
          className="space-y-4"
          noValidate
        >
          <div>
            <label htmlFor="method" className="block text-sm font-medium mb-1">
              Login method
            </label>
            <div className="flex space-x-2">
              <label
                className="w-1/2 rounded border-2 px-3 py-2 cursor-pointer"
                onClick={() => setMethod("api-key")}
              >
                API Key
              </label>
              <label
                className="w-1/2 rounded border-2 px-3 py-2 cursor-pointer"
                onClick={() => setMethod("oidc")}
              >
                OIDC
              </label>
            </div>
          </div>

          {method === "api-key" && (
            <div>
              <label htmlFor="api-key" className="block text-sm mb-1">
                API Key
              </label>
              <input
                id="api-key"
                type="password"
                className="w-full rounded border p-2 focus:outline-none focus:ring-2 focus:ring-primary"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder="sk-..."
                required
                aria-describedby="api-key-help"
              />
              <p className="text-xs text-muted-foreground mt-1" id="api-key-help">
                Use your API key
              </p>
            </div>
          )}

          {method === "oidc" && (
            <>
              <p className="text-sm text-muted-foreground">
                Sign in with your identity provider
              </p>
              <button
                onClick={() => handleSubmit(e as React.FormEvent)}
                className="w-full btn-primary mt-2"
              >
                Continue with OIDC
              </button>
              <p className="text-xs text-muted-foreground">
                You will be redirected to your identity provider
              </p>
            </>
          )}
        </form>
      </div>

      {auth.authenticated && router.push("/(dashboard)/overview")}
    </div>
  )
}
