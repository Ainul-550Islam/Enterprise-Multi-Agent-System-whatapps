"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { Dialog } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { ErrorState } from "@/components/ui/error-state";
import { JsonViewer } from "@/components/ui/json-viewer";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface Policy extends Record<string, unknown> {
  id: string;
  name: string;
}

export default function PoliciesPage() {
  const policies = useMas<Policy[]>("/v1/policies");
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [definition, setDefinition] = useState("{}");
  const [evaluateId, setEvaluateId] = useState<string | null>(null);
  const [evaluateInput, setEvaluateInput] = useState("{}");
  const [evaluateResult, setEvaluateResult] = useState<unknown>(null);
  const [error, setError] = useState<string | null>(null);

  async function create() {
    setError(null);
    try {
      await api.post("/v1/policies", { name, definition: JSON.parse(definition) as unknown });
      setOpen(false);
      policies.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  async function dryRun() {
    if (!evaluateId) {
      return;
    }
    setError(null);
    try {
      const result = await api.post(`/v1/policies/${encodeURIComponent(evaluateId)}/evaluate`, {
        input: JSON.parse(evaluateInput) as unknown,
      });
      setEvaluateResult(result);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <h1 className="m-0">Policies</h1>
        <Button onClick={() => setOpen(true)}>Create</Button>
      </div>
      {policies.error ? <ErrorState error={policies.error} /> : null}
      {error ? <ErrorState error={error} /> : null}
      <Card title="Policies" loading={policies.loading}>
        <DataTable
          columns={[
            { key: "name", header: "Name" },
            {
              key: "id",
              header: "",
              render: (row) => (
                <Button tone="ghost" onClick={() => setEvaluateId(String(row.id))}>
                  Dry-run
                </Button>
              ),
            },
          ]}
          rows={policies.data ?? []}
          emptyLabel="No policies."
          rowKey={(row) => String(row.id)}
        />
      </Card>
      {evaluateId ? (
        <Card title={`Evaluate ${evaluateId}`}>
          <label className="mas-field">
            Input JSON
            <textarea rows={6} value={evaluateInput} onChange={(event) => setEvaluateInput(event.target.value)} />
          </label>
          <Button onClick={() => void dryRun()}>Evaluate</Button>
          {evaluateResult ? <JsonViewer value={evaluateResult} /> : null}
        </Card>
      ) : null}
      <Dialog
        title="Create policy"
        open={open}
        onClose={() => setOpen(false)}
        footer={
          <>
            <Button tone="ghost" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button onClick={() => void create()} disabled={!name}>
              Create
            </Button>
          </>
        }
      >
        <Input label="Name" value={name} onChange={(event) => setName(event.target.value)} />
        <label className="mas-field">
          Definition JSON
          <textarea rows={6} value={definition} onChange={(event) => setDefinition(event.target.value)} />
        </label>
      </Dialog>
    </div>
  );
}
