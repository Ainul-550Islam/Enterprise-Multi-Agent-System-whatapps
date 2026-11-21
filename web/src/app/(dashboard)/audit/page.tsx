"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { ErrorState } from "@/components/ui/error-state";
import { masFetch } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface AuditEvent extends Record<string, unknown> {
  id: string;
  action?: string;
  actor?: string;
  created_at?: string;
}

interface AuditPage {
  items?: AuditEvent[];
  next_cursor?: string | null;
}

function asEvents(data: AuditEvent[] | AuditPage | null): { rows: AuditEvent[]; cursor?: string | null } {
  if (!data) {
    return { rows: [] };
  }
  if (Array.isArray(data)) {
    return { rows: data };
  }
  return { rows: data.items ?? [], cursor: data.next_cursor };
}

export default function AuditPage() {
  const first = useMas<AuditEvent[] | AuditPage>("/v1/audit-events", { limit: 50 });
  const [extra, setExtra] = useState<AuditEvent[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const parsed = asEvents(first.data);
  const rows = [...parsed.rows, ...extra];

  async function loadMore() {
    const next = cursor ?? parsed.cursor;
    if (!next) {
      return;
    }
    const page = await masFetch<AuditEvent[] | AuditPage>("/v1/audit-events", { query: { limit: 50, cursor: next } });
    const more = asEvents(page);
    setExtra((current) => [...current, ...more.rows]);
    setCursor(more.cursor ?? null);
  }

  return (
    <div>
      <h1>Audit</h1>
      <p className="mas-muted">Events are redacted by the API. This page never asks for raw secrets.</p>
      {first.error ? <ErrorState error={first.error} /> : null}
      <Card title="Events" loading={first.loading}>
        <DataTable
          columns={[
            { key: "created_at", header: "When" },
            { key: "actor", header: "Actor" },
            { key: "action", header: "Action" },
            { key: "id", header: "Id" },
          ]}
          rows={rows}
          emptyLabel="No audit events."
          rowKey={(row) => String(row.id)}
        />
        <Button tone="ghost" onClick={() => void loadMore()} disabled={!(cursor ?? parsed.cursor)}>
          Load next page
        </Button>
      </Card>
    </div>
  );
}
