"use client";

import { Badge, toneForStatus } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface Tool extends Record<string, unknown> {
  id: string;
  name: string;
  kind: string;
  enabled?: boolean;
  status?: string;
}

export default function ToolsPage() {
  const tools = useMas<Tool[]>("/v1/tools");

  async function toggle(tool: Tool, enabled: boolean) {
    await api.patch(`/v1/tools/${encodeURIComponent(tool.id)}`, { enabled });
    tools.reload();
  }

  return (
    <div>
      <h1>Tools</h1>
      {tools.error ? <ErrorState error={tools.error} /> : null}
      <Card title="Registry" loading={tools.loading}>
        <DataTable
          columns={[
            { key: "name", header: "Name" },
            { key: "kind", header: "Kind" },
            {
              key: "enabled",
              header: "Enabled",
              render: (row) => (
                <Badge tone={row.enabled === false ? "danger" : "ok"}>
                  {row.enabled === false ? "disabled" : "enabled"}
                </Badge>
              ),
            },
            {
              key: "status",
              header: "Status",
              render: (row) => <Badge tone={toneForStatus(String(row.status ?? ""))}>{String(row.status ?? "")}</Badge>,
            },
            {
              key: "actions",
              header: "",
              render: (row) => (
                <Button tone="ghost" onClick={() => void toggle(row, row.enabled === false)}>
                  {row.enabled === false ? "Enable" : "Disable"}
                </Button>
              ),
            },
          ]}
          rows={tools.data ?? []}
          emptyLabel="No tools registered."
          rowKey={(row) => String(row.id)}
        />
      </Card>
    </div>
  );
}
