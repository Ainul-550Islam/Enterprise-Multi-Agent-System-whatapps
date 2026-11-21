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

interface WorkflowRow extends Record<string, unknown> {
  id: string;
  name: string;
  status?: string;
}

export default function WorkflowsPage() {
  const workflows = useMas<WorkflowRow[]>("/v1/workflows");
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [projectId, setProjectId] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function create() {
    setBusy(true);
    setError(null);
    try {
      await api.post("/v1/workflows", { name, project_id: projectId });
      setOpen(false);
      workflows.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <h1 className="m-0">Workflows</h1>
        <Button onClick={() => setOpen(true)}>Create</Button>
      </div>
      {workflows.error ? <ErrorState error={workflows.error} /> : null}
      <Card title="Workflows" loading={workflows.loading}>
        <DataTable
          columns={[
            { key: "name", header: "Name" },
            {
              key: "status",
              header: "Status",
              render: (row) => <Badge tone={toneForStatus(String(row.status ?? "draft"))}>{String(row.status ?? "draft")}</Badge>,
            },
            {
              key: "id",
              header: "",
              render: (row) => <a href={`/workflows/${encodeURIComponent(String(row.id))}`}>Editor</a>,
            },
          ]}
          rows={workflows.data ?? []}
          emptyLabel="No workflows yet."
          rowKey={(row) => String(row.id)}
        />
      </Card>
      <Dialog
        title="Create workflow"
        open={open}
        onClose={() => setOpen(false)}
        footer={
          <>
            <Button tone="ghost" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button onClick={() => void create()} busy={busy} disabled={!name || !projectId}>
              Create
            </Button>
          </>
        }
      >
        {error ? <ErrorState error={error} /> : null}
        <Input label="Name" value={name} onChange={(event) => setName(event.target.value)} />
        <Input label="Project id" value={projectId} onChange={(event) => setProjectId(event.target.value)} />
      </Dialog>
    </div>
  );
}
