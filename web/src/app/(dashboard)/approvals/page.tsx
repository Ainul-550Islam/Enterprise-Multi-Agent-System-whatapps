"use client";

import { Badge, toneForStatus } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface Approval extends Record<string, unknown> {
  id: string;
  status?: string;
  kind?: string;
  summary?: string;
}

export default function ApprovalsPage() {
  const approvals = useMas<Approval[]>("/v1/approvals");

  async function decide(id: string, decision: "approve" | "deny") {
    await api.post(`/v1/approvals/${encodeURIComponent(id)}/${decision}`, {});
    approvals.reload();
  }

  return (
    <div>
      <h1>Approvals</h1>
      {approvals.error ? <ErrorState error={approvals.error} /> : null}
      <Card title="Pending" loading={approvals.loading}>
        <DataTable
          columns={[
            { key: "kind", header: "Kind" },
            { key: "summary", header: "Summary" },
            {
              key: "status",
              header: "Status",
              render: (row) => <Badge tone={toneForStatus(String(row.status ?? "pending"))}>{String(row.status ?? "pending")}</Badge>,
            },
            {
              key: "actions",
              header: "",
              render: (row) => (
                <span className="flex gap-2">
                  <Button onClick={() => void decide(String(row.id), "approve")}>Approve</Button>
                  <Button tone="danger" onClick={() => void decide(String(row.id), "deny")}>
                    Deny
                  </Button>
                </span>
              ),
            },
          ]}
          rows={approvals.data ?? []}
          emptyLabel="No pending approvals."
          rowKey={(row) => String(row.id)}
        />
      </Card>
    </div>
  );
}
