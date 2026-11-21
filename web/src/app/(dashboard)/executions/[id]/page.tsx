"use client";

import { useEffect, useState } from "react";

import { Badge, toneForStatus } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { ErrorState } from "@/components/ui/error-state";
import { JsonViewer } from "@/components/ui/json-viewer";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";
import { subscribeStream, type StreamFrame } from "@/lib/sse";

interface Execution {
  id: string;
  status: string;
  agent_id?: string;
  failure?: string;
}

interface Step extends Record<string, unknown> {
  id: string;
  sequence: number;
  kind: string;
  status: string;
  latency_ms?: number;
  error_code?: string;
}

export default function ExecutionDetailPage({ params }: { params: { id: string } }) {
  const executionId = decodeURIComponent(params.id);
  const detail = useMas<Execution>(`/v1/executions/${encodeURIComponent(executionId)}`);
  const steps = useMas<Step[]>(`/v1/executions/${encodeURIComponent(executionId)}/steps`);
  const [signalError, setSignalError] = useState<string | null>(null);
  const [frames, setFrames] = useState<StreamFrame[]>([]);

  useEffect(() => {
    const stop = subscribeStream({
      onFrame: (frame) => {
        if (!frame.data.includes(executionId) && frame.event !== "execution") {
          return;
        }
        setFrames((current) => [...current.slice(-49), frame]);
        detail.reload();
        steps.reload();
      },
    });
    return stop;
    // subscribe once per execution
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [executionId]);

  async function send(path: "start" | "pause" | "resume" | "cancel") {
    setSignalError(null);
    try {
      await api.post(`/v1/executions/${encodeURIComponent(executionId)}/${path}`, {});
      detail.reload();
    } catch (error) {
      setSignalError(error instanceof Error ? error.message : String(error));
    }
  }

  return (
    <div>
      <h1>Execution {executionId}</h1>
      {detail.error ? <ErrorState error={detail.error} /> : null}
      {signalError ? <ErrorState error={signalError} /> : null}
      <Card title="State" loading={detail.loading}>
        <p>
          Status:{" "}
          <Badge tone={toneForStatus(detail.data?.status ?? "unknown")}>{detail.data?.status ?? "unknown"}</Badge>
        </p>
        {detail.data?.failure ? <ErrorState error={detail.data.failure} /> : null}
        <div className="flex flex-wrap gap-2">
          <Button onClick={() => void send("start")}>Start</Button>
          <Button tone="ghost" onClick={() => void send("pause")}>
            Pause
          </Button>
          <Button tone="ghost" onClick={() => void send("resume")}>
            Resume
          </Button>
          <Button tone="danger" onClick={() => void send("cancel")}>
            Cancel
          </Button>
        </div>
      </Card>
      <Card title="Steps" loading={steps.loading}>
        <DataTable
          columns={[
            { key: "sequence", header: "#" },
            { key: "kind", header: "Kind" },
            {
              key: "status",
              header: "Status",
              render: (row) => <Badge tone={toneForStatus(String(row.status))}>{String(row.status)}</Badge>,
            },
            { key: "latency_ms", header: "Latency ms" },
            { key: "error_code", header: "Error" },
          ]}
          rows={steps.data ?? []}
          emptyLabel="No steps recorded yet."
          rowKey={(row) => String(row.id)}
        />
      </Card>
      <Card title="Live stream">
        {frames.length === 0 ? (
          <p className="mas-muted">Waiting for SSE frames on /v1/stream…</p>
        ) : (
          <JsonViewer value={frames} />
        )}
      </Card>
    </div>
  );
}
