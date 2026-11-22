"use client";

import { useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTableWithCursor } from "@/components/ui/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Toast } from "@/components/ui/toast";
import { useMasMutation } from "@/lib/api/useMas";
import { useMas } from "@/lib/api/useMas";

interface User extends Record<string, unknown> {
  id: string;
  email: string;
  display_name?: string;
  status: string;
}

const ROLES = ["viewer", "operator", "developer", "admin", "owner"].map((value) => ({ value, label: value }));

/** Invites use the documented membership contract; users come from /v1/users. */
export default function TeamPage() {
  const users = useMas<User[]>("/v1/users");
  const invitation = useMasMutation<unknown>("/v1/memberships");
  const [userId, setUserId] = useState("");
  const [role, setRole] = useState("viewer");
  const [notice, setNotice] = useState<string | null>(null);

  async function invite() {
    const result = await invitation.mutate(
      "POST",
      "/v1/memberships",
      { user_id: userId, role },
      { refresh: true },
    );
    if (result !== null) {
      setNotice(`Invited ${userId} as ${role}`);
      setUserId("");
    }
  }

  return (
    <div>
      <h1>Team</h1>
      {users.error || invitation.error ? <ErrorState error={(users.error ?? invitation.error)!} /> : null}
      <Toast message={notice} onDone={() => setNotice(null)} />
      <Card title="Invite member" loading={false}>
        <div className="flex flex-wrap gap-2">
          <Input label="User id" value={userId} onChange={(event) => setUserId(event.target.value)} />
          <Select label="Role" value={role} options={ROLES} onChange={(event) => setRole(event.target.value)} />
          <Button disabled={!userId || invitation.pending} onClick={() => void invite()}>Invite</Button>
        </div>
      </Card>
      <Card title="Users" loading={users.loading}>
        <DataTableWithCursor
          columns={[
            { key: "email", header: "Email" },
            { key: "display_name", header: "Name" },
            { key: "status", header: "Status", render: (row) => <Badge tone={row.status === "active" ? "ok" : "warn"}>{String(row.status)}</Badge> },
          ]}
          rows={users.data ?? []}
          rowKey={(row) => row.id}
          emptyLabel="No users yet."
        />
      </Card>
    </div>
  );
}
