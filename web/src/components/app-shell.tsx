"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState, type ReactNode } from "react";

import { logout } from "@/lib/auth/apikey";
import { useSession } from "@/lib/auth/SessionProvider";
import { api } from "@/lib/api/client";
import { readCsrfCookie } from "@/lib/csrf";
import { Button } from "@/components/ui/button";

const NAV: ReadonlyArray<{ href: string; label: string }> = [
  { href: "/overview", label: "Overview" },
  { href: "/agents", label: "Agents" },
  { href: "/executions", label: "Executions" },
  { href: "/workflows", label: "Workflows" },
  { href: "/approvals", label: "Approvals" },
  { href: "/tools", label: "Tools" },
  { href: "/connectors", label: "Connectors" },
  { href: "/webhooks", label: "Webhooks" },
  { href: "/api-keys", label: "API keys" },
  { href: "/billing", label: "Billing" },
  { href: "/usage", label: "Usage" },
  { href: "/policies", label: "Policies" },
  { href: "/audit", label: "Audit" },
  { href: "/whatsapp", label: "WhatsApp" },
  { href: "/whatsapp/inbox", label: "Inbox" },
  { href: "/settings", label: "Settings" },
];

interface Named {
  id: string;
  name: string;
}

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const session = useSession();
  const [orgs, setOrgs] = useState<Named[]>([]);
  const [tenants, setTenants] = useState<Named[]>([]);
  const [orgId, setOrgId] = useState(session.scope?.organizationId ?? "");
  const [tenantId, setTenantId] = useState(session.scope?.tenantId ?? "");

  useEffect(() => {
    void api.get<Named[]>("/v1/organizations").then(setOrgs).catch(() => setOrgs([]));
    void api.get<Named[]>("/v1/tenants").then(setTenants).catch(() => setTenants([]));
  }, []);

  useEffect(() => {
    setOrgId(session.scope?.organizationId ?? "");
    setTenantId(session.scope?.tenantId ?? "");
  }, [session.scope?.organizationId, session.scope?.tenantId]);

  async function persistScope(next: { organizationId?: string; tenantId?: string }) {
    await fetch("/api/auth/scope", {
      method: "POST",
      credentials: "same-origin",
      headers: { "content-type": "application/json", "x-csrf-token": readCsrfCookie() },
      body: JSON.stringify(next),
    });
  }

  return (
    <div className="mas-shell">
      <nav className="mas-nav" aria-label="Primary">
        <p className="mb-3 px-2 text-xs uppercase tracking-wide text-muted">Enterprise MAS</p>
        {NAV.map((item) => (
          <Link
            key={item.href}
            href={item.href}
            aria-current={pathname === item.href || pathname.startsWith(`${item.href}/`) ? "page" : undefined}
          >
            {item.label}
          </Link>
        ))}
      </nav>
      <div>
        <header className="flex flex-wrap items-center gap-3 border-b border-line px-6 py-3">
          <label className="mas-muted text-xs">
            Organization
            <select
              className="ml-2 rounded border border-line bg-panel px-2 py-1 text-ink"
              value={orgId}
              onChange={(event) => {
                const value = event.target.value;
                setOrgId(value);
                void persistScope({ organizationId: value, tenantId });
              }}
            >
              <option value="">All</option>
              {orgs.map((org) => (
                <option key={org.id} value={org.id}>
                  {org.name}
                </option>
              ))}
            </select>
          </label>
          <label className="mas-muted text-xs">
            Tenant
            <select
              className="ml-2 rounded border border-line bg-panel px-2 py-1 text-ink"
              value={tenantId}
              onChange={(event) => {
                const value = event.target.value;
                setTenantId(value);
                void persistScope({ organizationId: orgId, tenantId: value });
              }}
            >
              <option value="">All</option>
              {tenants.map((tenant) => (
                <option key={tenant.id} value={tenant.id}>
                  {tenant.name}
                </option>
              ))}
            </select>
          </label>
          <div className="ml-auto flex items-center gap-3">
            <span className="text-sm text-muted">{session.subject ?? "signed in"}</span>
            <Button
              tone="ghost"
              onClick={() => {
                void logout().then(() => router.push("/login"));
              }}
            >
              Sign out
            </Button>
          </div>
        </header>
        <main className="mas-main">{children}</main>
      </div>
    </div>
  );
}
