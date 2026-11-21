"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";
import { useSession } from "@/lib/auth/SessionProvider";

interface User extends Record<string, unknown> {
  id: string;
  email?: string;
}

export default function SettingsPage() {
  const session = useSession();
  const users = useMas<User[]>("/v1/users");
  const [email, setEmail] = useState("");
  const [role, setRole] = useState("viewer");
  const [userId, setUserId] = useState("");
  const [tenantId, setTenantId] = useState(session.scope?.tenantId ?? "");
  const [error, setError] = useState<string | null>(null);

  async function invite() {
    setError(null);
    try {
      await api.post("/v1/memberships", { user_id: userId, tenant_id: tenantId, role });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  async function createUser() {
    setError(null);
    try {
      await api.post("/v1/users", { email });
      users.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  return (
    <div>
      <h1>Settings</h1>
      <Card title="Session">
        <p>
          Signed in as <strong>{session.subject ?? "unknown"}</strong> via {session.kind ?? "session"}.
        </p>
        <p className="mas-muted">
          API keys and OIDC tokens are stored in an httpOnly cookie. They are not shown here and cannot be copied
          from the browser.
        </p>
      </Card>
      {error ? <ErrorState error={error} /> : null}
      <Card title="Members" loading={users.loading}>
        {users.error ? <ErrorState error={users.error} /> : null}
        <DataTable
          columns={[
            { key: "email", header: "Email" },
            { key: "id", header: "Id" },
          ]}
          rows={users.data ?? []}
          emptyLabel="No users visible in this scope."
          rowKey={(row) => String(row.id)}
        />
      </Card>
      <Card title="Invite">
        <Input label="User id" value={userId} onChange={(event) => setUserId(event.target.value)} />
        <Input label="Tenant id" value={tenantId} onChange={(event) => setTenantId(event.target.value)} />
        <Input label="Role" value={role} onChange={(event) => setRole(event.target.value)} />
        <Button onClick={() => void invite()} disabled={!userId || !tenantId}>
          Send invite
        </Button>
      </Card>
      <Card title="Create user">
        <Input label="Email" type="email" value={email} onChange={(event) => setEmail(event.target.value)} />
        <Button onClick={() => void createUser()} disabled={!email}>
          Create
        </Button>
      </Card>
    </div>
  );
}
