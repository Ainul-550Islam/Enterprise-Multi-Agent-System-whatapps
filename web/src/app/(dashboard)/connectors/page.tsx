"use client";

import { Badge, toneForStatus } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface Connector extends Record<string, unknown> {
  id: string;
  name: string;
  provider: string;
  status?: string;
}

export default function ConnectorsPage() {
  const connectors = useMas<Connector[]>("/v1/connectors");

  async function probe(id: string) {
    await api.post(`/v1/connectors/${encodeURIComponent(id)}/probe`, {});
    connectors.reload();
  }

  async function disable(id: string) {
    await api.post(`/v1/connectors/${encodeURIComponent(id)}/disable`, {});
    connectors.reload();
  }

  async function authorize(id: string) {
    const result = await api.post<{ authorization_url?: string }>(
      `/v1/connectors/${encodeURIComponent(id)}/oauth/authorize`,
      {},
    );
    if (result.authorization_url) {
      window.location.href = result.authorization_url;
    }
  }

  return (
    <div>
      <h1>Connectors</h1>
      {connectors.error ? <ErrorState error={connectors.error} /> : null}
      <Card title="Connectors" loading={connectors.loading}>
        <DataTable
          columns={[
            { key: "name", header: "Name" },
            { key: "provider", header: "Provider" },
            {
              key: "status",
              header: "Status",
              render: (row) => <Badge tone={toneForStatus(String(row.status ?? ""))}>{String(row.status ?? "")}</Badge>,
            },
            {
              key: "actions",
              header: "",
              render: (row) => (
                <span className="flex flex-wrap gap-2">
                  <Button tone="ghost" onClick={() => void probe(String(row.id))}>
                    Probe
                  </Button>
                  <Button tone="ghost" onClick={() => void authorize(String(row.id))}>
                    OAuth
                  </Button>
                  <Button tone="danger" onClick={() => void disable(String(row.id))}>
                    Disable
                  </Button>
                </span>
              ),
            },
          ]}
          rows={connectors.data ?? []}
          emptyLabel="No connectors registered."
          rowKey={(row) => String(row.id)}
        />
      </Card>
    </div>
  );
}
