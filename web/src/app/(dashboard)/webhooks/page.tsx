"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface Webhook extends Record<string, unknown> {
  id: string;
  url: string;
  description?: string;
}

interface Delivery extends Record<string, unknown> {
  id: string;
  status?: string;
  attempt?: number;
}

export default function WebhooksPage() {
  const webhooks = useMas<Webhook[]>("/v1/webhooks");
  const [selected, setSelected] = useState<string | null>(null);
  const deliveries = useMas<Delivery[]>(selected ? `/v1/webhooks/${encodeURIComponent(selected)}/deliveries` : null);

  async function replay(deliveryId: string) {
    if (!selected) {
      return;
    }
    await api.post(`/v1/webhooks/${encodeURIComponent(selected)}/deliveries/${encodeURIComponent(deliveryId)}/replay`, {});
    deliveries.reload();
  }

  async function enable(id: string, on: boolean) {
    await api.post(`/v1/webhooks/${encodeURIComponent(id)}/${on ? "enable" : "disable"}`, {});
    webhooks.reload();
  }

  return (
    <div>
      <h1>Webhooks</h1>
      {webhooks.error ? <ErrorState error={webhooks.error} /> : null}
      <Card title="Endpoints" loading={webhooks.loading}>
        <DataTable
          columns={[
            { key: "url", header: "URL" },
            { key: "description", header: "Description" },
            {
              key: "actions",
              header: "",
              render: (row) => (
                <span className="flex gap-2">
                  <Button tone="ghost" onClick={() => setSelected(String(row.id))}>
                    Deliveries
                  </Button>
                  <Button tone="ghost" onClick={() => void enable(String(row.id), true)}>
                    Enable
                  </Button>
                  <Button tone="danger" onClick={() => void enable(String(row.id), false)}>
                    Disable
                  </Button>
                </span>
              ),
            },
          ]}
          rows={webhooks.data ?? []}
          emptyLabel="No webhook endpoints."
          rowKey={(row) => String(row.id)}
        />
      </Card>
      <Card title={selected ? `Deliveries for ${selected}` : "Deliveries"} loading={deliveries.loading}>
        {!selected ? (
          <p className="mas-muted">Select an endpoint to inspect deliveries.</p>
        ) : (
          <DataTable
            columns={[
              { key: "id", header: "Delivery" },
              { key: "status", header: "Status" },
              { key: "attempt", header: "Attempt" },
              {
                key: "replay",
                header: "",
                render: (row) => (
                  <Button tone="ghost" onClick={() => void replay(String(row.id))}>
                    Replay
                  </Button>
                ),
              },
            ]}
            rows={deliveries.data ?? []}
            emptyLabel="No deliveries recorded."
            rowKey={(row) => String(row.id)}
          />
        )}
      </Card>
    </div>
  );
}
