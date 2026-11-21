"use client";

import { useCallback, useEffect, useState } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  addEdge,
  useEdgesState,
  useNodesState,
  type Connection,
  type Edge,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface Workflow {
  id: string;
  name: string;
  graph?: { nodes?: Node[]; edges?: Edge[] };
}

interface Version extends Record<string, unknown> {
  version: number;
  published_at?: string;
}

export default function WorkflowEditorPage({ params }: { params: { id: string } }) {
  const workflowId = decodeURIComponent(params.id);
  const workflow = useMas<Workflow>(`/v1/workflows/${encodeURIComponent(workflowId)}`);
  const versions = useMas<Version[]>(`/v1/workflows/${encodeURIComponent(workflowId)}/versions`);
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    const graph = workflow.data?.graph;
    if (graph?.nodes) {
      setNodes(graph.nodes);
    }
    if (graph?.edges) {
      setEdges(graph.edges);
    }
  }, [workflow.data, setNodes, setEdges]);

  const onConnect = useCallback(
    (connection: Connection) => setEdges((current) => addEdge(connection, current)),
    [setEdges],
  );

  async function saveGraph() {
    setBusy(true);
    setError(null);
    try {
      await api.put(`/v1/workflows/${encodeURIComponent(workflowId)}/graph`, { nodes, edges });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function publish() {
    setBusy(true);
    setError(null);
    try {
      await api.post(`/v1/workflows/${encodeURIComponent(workflowId)}/publish`, {});
      versions.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function disable() {
    setBusy(true);
    setError(null);
    try {
      await api.post(`/v1/workflows/${encodeURIComponent(workflowId)}/disable`, {});
      workflow.reload();
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
      await api.post(`/v1/workflows/${encodeURIComponent(workflowId)}/versions/${version}/rollback`, {});
      workflow.reload();
      versions.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h1>{workflow.data?.name ?? "Workflow"}</h1>
      {workflow.error ? <ErrorState error={workflow.error} /> : null}
      {error ? <ErrorState error={error} /> : null}
      <Card title="Graph">
        <div className="h-[480px] rounded border border-line">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            fitView
          >
            <MiniMap />
            <Controls />
            <Background />
          </ReactFlow>
        </div>
        <div className="mt-3 flex flex-wrap gap-2">
          <Button
            tone="ghost"
            onClick={() =>
              setNodes((current) => [
                ...current,
                {
                  id: `node-${current.length + 1}`,
                  position: { x: 80 + current.length * 40, y: 80 },
                  data: { label: `Step ${current.length + 1}` },
                },
              ])
            }
          >
            Add node
          </Button>
          <Button onClick={() => void saveGraph()} busy={busy}>
            Save graph
          </Button>
          <Button onClick={() => void publish()} busy={busy}>
            Publish
          </Button>
          <Button tone="danger" onClick={() => void disable()} busy={busy}>
            Disable
          </Button>
        </div>
      </Card>
      <Card title="Versions" loading={versions.loading}>
        <DataTable
          columns={[
            { key: "version", header: "Version" },
            { key: "published_at", header: "Published" },
            {
              key: "rollback",
              header: "",
              render: (row) => (
                <Button tone="ghost" onClick={() => void rollback(Number(row.version))} busy={busy}>
                  Rollback
                </Button>
              ),
            },
          ]}
          rows={versions.data ?? []}
          emptyLabel="No versions published."
          rowKey={(row, index) => String(row.version ?? index)}
        />
      </Card>
    </div>
  );
}
