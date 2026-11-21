"use client";

import { useState } from "react";

import { Badge, toneForStatus } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { ErrorState } from "@/components/ui/error-state";
import { Input } from "@/components/ui/input";
import { JsonViewer } from "@/components/ui/json-viewer";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface Agent {
  id: string;
  name: string;
  status: string;
  kind: string;
  current_version?: number;
}

interface Version extends Record<string, unknown> {
  id: string;
  version_number: number;
  deployment_status: string;
  configuration_checksum: string;
  published_at: string;
}

export default function AgentDetailPage({ params }: { params: { id: string } }) {
  const agentId = decodeURIComponent(params.id);
  const agent = useMas<Agent>(`/v1/agents/${encodeURIComponent(agentId)}`);
  const versions = useMas<Version[]>(`/v1/agents/${encodeURIComponent(agentId)}/versions`);
  const [checksum, setChecksum] = useState("");
  const [snapshot, setSnapshot] = useState("{}");
  const [grants, setGrants] = useState("[]");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function publish() {
    setBusy(true);
    setError(null);
    try {
      await api.post(`/v1/agents/${encodeURIComponent(agentId)}/versions`, {
        configuration_checksum: checksum,
        configuration_snapshot: JSON.parse(snapshot) as unknown,
      });
      versions.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function rollback(version: number) {
    setBusy(true);
    setError(null);
    try {
      await api.post(`/v1/agents/${encodeURIComponent(agentId)}/versions/${version}/rollback`, {
        target_version_number: version,
      });
      versions.reload();
      agent.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function savePermissions() {
    setBusy(true);
    setError(null);
    try {
      await api.put(`/v1/agents/${encodeURIComponent(agentId)}/tool-permissions`, {
        grants: JSON.parse(grants) as unknown,
      });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h1>{agent.data?.name ?? "Agent"}</h1>
      {agent.error ? <ErrorState error={agent.error} /> : null}
      {error ? <ErrorState error={error} /> : null}
      <Card title="Profile" loading={agent.loading}>
        {agent.data ? (
          <p>
            <Badge tone={toneForStatus(agent.data.status)}>{agent.data.status}</Badge> · {agent.data.kind} ·
            version {agent.data.current_version ?? "none"}
          </p>
        ) : null}
      </Card>
      <Card title="Versions" loading={versions.loading}>
        <DataTable
          columns={[
            { key: "version_number", header: "Version" },
            {
              key: "deployment_status",
              header: "Status",
              render: (row) => (
                <Badge tone={toneForStatus(String(row.deployment_status))}>{String(row.deployment_status)}</Badge>
              ),
            },
            { key: "configuration_checksum", header: "Checksum" },
            { key: "published_at", header: "Published" },
            {
              key: "rollback",
              header: "",
              render: (row) => (
                <Button tone="ghost" onClick={() => void rollback(Number(row.version_number))} busy={busy}>
                  Rollback
                </Button>
              ),
            },
          ]}
          rows={versions.data ?? []}
          emptyLabel="No published versions."
          rowKey={(row) => String(row.id)}
        />
        <div className="mt-4 grid gap-3">
          <Input label="Configuration checksum" value={checksum} onChange={(event) => setChecksum(event.target.value)} />
          <label className="mas-field">
            Configuration snapshot (JSON)
            <textarea rows={6} value={snapshot} onChange={(event) => setSnapshot(event.target.value)} />
          </label>
          <Button onClick={() => void publish()} busy={busy} disabled={!checksum}>
            Publish version
          </Button>
        </div>
      </Card>
      <Card title="Tool permissions">
        <label className="mas-field">
          Grants JSON
          <textarea rows={6} value={grants} onChange={(event) => setGrants(event.target.value)} />
        </label>
        <Button onClick={() => void savePermissions()} busy={busy}>
          Replace grants
        </Button>
        {agent.data ? <JsonViewer value={agent.data} /> : null}
      </Card>
    </div>
  );
}
