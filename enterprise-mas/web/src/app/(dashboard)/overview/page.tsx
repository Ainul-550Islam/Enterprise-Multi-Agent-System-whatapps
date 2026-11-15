import { useEffect } from "react"
import { useSearchParams, useRouter } from "next/navigation"
import { useAuth } from "@/lib/auth/session"

export default function OverviewPage() {
  const { user } = useAuth()
  const searchParams = useSearchParams()
  const router = useRouter()

  const tier = searchParams.get("tier") || "lite"

  useEffect(() => {
    if (!user) {
      router.push("/(auth)/login")
    }
  }, [user, router])

  return (
    <div className="min-h-screen bg-sidebar overflow-y-auto">
      <nav className="bg-sidebar border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-bold">Dashboard</h1>
          <button
            onClick={() => router.push("?tier=lite")}
            className={tier === "lite" ? "btn-primary" : "btn-outline"}
          >
            LITE
          </button>
          <button
            onClick={() => router.push("?tier=full")}
            className={tier === "full" ? "btn-primary" : "btn-outline"}
          >
            FULL
          </button>
        </div>
      </nav>

      <main className="p-6 pt-8">
        {tier === "lite" && (
          <div className="max-w-2xl mx-auto">
            <h2 className="text-2xl font-bold mb-4">Lite Tier Overview</h2>
            <p className="text-muted-foreground">
              Welcome to the Lite tier. Upgrade to FULL for complete features.
            </p>
            <ul className="list-disc pl-5 space-y-2">
              <li>Basic analytics</li>
              <li>Limited messaging</li>
              <li>Single workspace</li>
            </ul>
          </div>
        )}

        {tier === "full" && (
          <div className="max-w-3xl mx-auto">
            <h2 className="text-2xl font-bold mb-4">Full Tier Overview</h2>
            <p className="text-muted-foreground">
              Full access to all features and capabilities.
            </p>
            <ul className="list-disc pl-5 space-y-2">
              <li>Advanced analytics</li>
              <li>Unlimited messaging</li>
              <li>Multiple workspaces</li>
              <li>Custom branding</li>
              <li>API access</li>
            </ul>
          </div>
        )}
      </main>
    </div>
  )
}
