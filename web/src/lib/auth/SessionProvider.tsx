"use client";

import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

export interface SessionView {
  authenticated: boolean;
  kind?: "apikey" | "oidc";
  subject?: string;
  scope?: {
    tenantId?: string;
    organizationId?: string;
    projectId?: string;
  };
  oidc?: boolean;
}

const SessionContext = createContext<SessionView>({ authenticated: false });

export function useSession(): SessionView {
  return useContext(SessionContext);
}

export function SessionProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<SessionView>({ authenticated: false });
  useEffect(() => {
    let cancelled = false;
    void fetch("/api/auth/session", { credentials: "same-origin" })
      .then((response) => response.json() as Promise<SessionView>)
      .then((view) => {
        if (!cancelled) {
          setSession(view);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSession({ authenticated: false });
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);
  const value = useMemo(() => session, [session]);
  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}
