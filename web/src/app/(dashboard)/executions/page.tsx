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

interface ExecutionRow extends Record<string, unknown> {
  id: string;
  status: string;
  agent_id?: string;
  created_at?: string;
}

export default function ExecutionsPage() {
  const executions = useMas<ExecutionRow[]>("/v1/executions");
  const [status, setStatus] = useState("");
  const [open, setOpen] = useState(false);
  const [projectId, setProjectId] = useState("");
  const [agentId, setAgentId] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const rows = (executions.data ?? []).filter((row) => (status ? String(row.status) === status : true));

  async function start() {
    setBusy(true);
    setError(null);
    try {
      await api.post(`/v1/projects/${encodeURIComponent(projectId)}/executions`, {
        agent_id: agentId,
      });
      setOpen(false);
      executions.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <h1 className="m-0">Executions</h1>
        <label className="mas-field m-0">
          Status
          <select value={status} onChange={(event) => setStatus(event.target.value)}>
            <option value="">Any</option>
            <option value="queued">queued</option>
            <option value="running">running</option>
            <option value="paused">paused</option>
            <option value="completed">completed</option>
            <option value="failed">failed</option>
            <option value="cancelled">cancelled</option>
          </select>
        </label>
        <Button onClick={() => setOpen(true)}>Start execution</Button>
        <Button tone="ghost" onClick={executions.reload} busy={executions.loading}>
          Refresh
        </Button>
      </div>
      {executions.error ? <ErrorState error={executions.error} /> : null}
      <Card title="All executions" loading={executions.loading}>
        <DataTable
          columns={[
            { key: "id", header: "ID" },
            { key: "agent_id", header: "Agent" },
            {
              key: "status",
              header: "Status",
              render: (row) => <Badge tone={toneForStatus(String(row.status))}>{String(row.status)}</Badge>,
            },
            { key: "created_at", header: "Created" },
            {
              key: "open",
              header: "",
              render: (row) => <a href={`/executions/${encodeURIComponent(String(row.id))}`}>Details</a>,
            },
          ]}
          rows={rows}
          emptyLabel="No executions yet."
          rowKey={(row) => String(row.id)}
        />
      </Card>
      <Dialog
        title="Start execution"
        open={open}
        onClose={() => setOpen(false)}
        footer={
          <>
            <Button tone="ghost" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button onClick={() => void start()} busy={busy} disabled={!projectId || !agentId}>
              Start
            </Button>
          </>
        }
      >
        {error ? <ErrorState error={error} /> : null}
        <Input label="Project id" value={projectId} onChange={(event) => setProjectId(event.target.value)} />
        <Input label="Agent id" value={agentId} onChange={(event) => setAgentId(event.target.value)} />
      </Dialog>
    </div>
  );
}
