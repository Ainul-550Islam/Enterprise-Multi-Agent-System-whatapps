"use client";

import { useState } from "react";

import { Badge, toneForStatus } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { Dialog } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface AgentRow extends Record<string, unknown> {
  id: string;
  name: string;
  kind: string;
  status: string;
  slug: string;
  current_version?: number;
}

export default function AgentsPage() {
  const agents = useMas<AgentRow[]>("/v1/agents");
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [projectId, setProjectId] = useState("");
  const [filter, setFilter] = useState("");
  const [busy, setBusy] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);

  const rows = (agents.data ?? []).filter((row) => {
    if (!filter) {
      return true;
    }
    const haystack = `${row.name} ${row.kind} ${row.status}`.toLowerCase();
    return haystack.includes(filter.toLowerCase());
  });

  async function createAgent() {
    setBusy(true);
    setFormError(null);
    try {
      await api.post("/v1/agents", { name, project_id: projectId });
      setOpen(false);
      setName("");
      agents.reload();
    } catch (error) {
      setFormError(error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <h1 className="m-0">Agents</h1>
        <Input label="Filter" value={filter} onChange={(event) => setFilter(event.target.value)} />
        <Button onClick={() => setOpen(true)}>Register agent</Button>
      </div>
      {agents.error ? <ErrorState error={agents.error} /> : null}
      <Card title={`Registered agents (${rows.length})`} loading={agents.loading}>
        <DataTable
          columns={[
            { key: "name", header: "Name" },
            { key: "kind", header: "Kind" },
            {
              key: "status",
              header: "Status",
              render: (row) => <Badge tone={toneForStatus(String(row.status))}>{String(row.status)}</Badge>,
            },
            { key: "current_version", header: "Version" },
            {
              key: "id",
              header: "",
              render: (row) => <a href={`/agents/${encodeURIComponent(String(row.id))}`}>Open</a>,
            },
          ]}
          rows={rows}
          emptyLabel="No agents registered yet."
          rowKey={(row) => String(row.id)}
        />
      </Card>
      <Dialog
        title="Register agent"
        open={open}
        onClose={() => setOpen(false)}
        footer={
          <>
            <Button tone="ghost" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button onClick={() => void createAgent()} busy={busy} disabled={!name || !projectId}>
              Create
            </Button>
          </>
        }
      >
        {formError ? <ErrorState error={formError} /> : null}
        <Input label="Name" value={name} onChange={(event) => setName(event.target.value)} required />
        <Input
          label="Project id"
          value={projectId}
          onChange={(event) => setProjectId(event.target.value)}
          required
        />
      </Dialog>
    </div>
  );
}
