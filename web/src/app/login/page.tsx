"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { Suspense, useState, type FormEvent } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ErrorState } from "@/components/ui/error-state";
import { loginWithApiKey } from "@/lib/auth/apikey";
import { useSession } from "@/lib/auth/SessionProvider";

function LoginForm() {
  const router = useRouter();
  const params = useSearchParams();
  const session = useSession();
  const next = params.get("next") || "/overview";
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const submitted = String(new FormData(event.currentTarget).get("apiKey") ?? "");
    setBusy(true);
    setError(null);
    try {
      await loginWithApiKey(submitted);
      router.replace(next);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto flex min-h-screen max-w-md flex-col justify-center p-6">
      <Card title="Sign in">
        <p className="mas-muted mb-4">
          Self-hosted operators sign in with an API key. The key is stored in an httpOnly cookie
          on this origin and is never written to localStorage.
        </p>
        {error ? <ErrorState error={error} /> : null}
        <form onSubmit={(event) => void onSubmit(event)}>
          <Input
            label="API key"
            name="apiKey"
            type="password"
            autoComplete="off"
            required
          />
          <Button type="submit" busy={busy}>
            Continue with API key
          </Button>
        </form>
        {session.oidc ? (
          <p className="mt-6">
            <a className="mas-btn mas-btn-primary inline-block" href={`/api/auth/oidc?next=${encodeURIComponent(next)}`}>
              Continue with SSO
            </a>
          </p>
        ) : (
          <p className="mas-muted mt-4">SSO is hidden until OIDC_ISSUER is configured on the server.</p>
        )}
      </Card>
    </div>
  );
}

export default function LoginPage() {
  return (
    <Suspense>
      <LoginForm />
    </Suspense>
  );
}
