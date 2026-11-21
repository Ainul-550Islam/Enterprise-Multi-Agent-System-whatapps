"use client";

import { useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { Dialog } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface ApiKeyRow extends Record<string, unknown> {
  id: string;
  name: string;
  prefix: string;
  revoked: boolean;
  created_at: string;
  last_used_at?: string;
}

interface CreatedKey {
  metadata: ApiKeyRow;
  key: string;
}

export default function ApiKeysPage() {
  const keys = useMas<ApiKeyRow[]>("/v1/api-keys");
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [scopes, setScopes] = useState("agent:read");
  const [secret, setSecret] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function create() {
    setBusy(true);
    setError(null);
    try {
      const created = await api.post<CreatedKey>("/v1/api-keys", {
        name,
        scopes: scopes.split(",").map((scope) => scope.trim()).filter(Boolean),
      });
      setSecret(created.key);
      keys.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function rotate(id: string) {
    setBusy(true);
    setError(null);
    try {
      const created = await api.post<CreatedKey>(`/v1/api-keys/${encodeURIComponent(id)}/rotate`, {});
      setSecret(created.key);
      setOpen(true);
      keys.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function revoke(id: string) {
    setBusy(true);
    setError(null);
    try {
      await api.post(`/v1/api-keys/${encodeURIComponent(id)}/revoke`, {});
      keys.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <h1 className="m-0">API keys</h1>
        <Button
          onClick={() => {
            setSecret(null);
            setOpen(true);
          }}
        >
          Create key
        </Button>
      </div>
      {keys.error ? <ErrorState error={keys.error} /> : null}
      {error ? <ErrorState error={error} /> : null}
      <Card title="Keys (metadata only)" loading={keys.loading}>
        <DataTable
          columns={[
            { key: "name", header: "Name" },
            { key: "prefix", header: "Prefix" },
            {
              key: "revoked",
              header: "State",
              render: (row) =>
                row.revoked ? <Badge tone="danger">revoked</Badge> : <Badge tone="ok">active</Badge>,
            },
            { key: "last_used_at", header: "Last used" },
            {
              key: "actions",
              header: "",
              render: (row) => (
                <span className="flex gap-2">
                  <Button tone="ghost" onClick={() => void rotate(String(row.id))} busy={busy}>
                    Rotate
                  </Button>
                  <Button tone="danger" onClick={() => void revoke(String(row.id))} busy={busy} disabled={Boolean(row.revoked)}>
                    Revoke
                  </Button>
                </span>
              ),
            },
          ]}
          rows={keys.data ?? []}
          emptyLabel="No API keys issued."
          rowKey={(row) => String(row.id)}
        />
      </Card>
      <Dialog
        title={secret ? "Copy this secret now" : "Create API key"}
        open={open}
        onClose={() => {
          setOpen(false);
          setSecret(null);
        }}
        footer={
          secret ? (
            <Button
              onClick={() => {
                setOpen(false);
                setSecret(null);
              }}
            >
              I have copied it
            </Button>
          ) : (
            <>
              <Button tone="ghost" onClick={() => setOpen(false)}>
                Cancel
              </Button>
              <Button onClick={() => void create()} busy={busy} disabled={!name}>
                Create
              </Button>
            </>
          )
        }
      >
        {secret ? (
          <div>
            <p className="mas-muted">This value is returned once. It is not stored by the dashboard.</p>
            <Input label="Secret" readOnly value={secret} />
          </div>
        ) : (
          <>
            <Input label="Name" value={name} onChange={(event) => setName(event.target.value)} />
            <Input
              label="Scopes (comma-separated resource:action)"
              value={scopes}
              onChange={(event) => setScopes(event.target.value)}
            />
          </>
        )}
      </Dialog>
    </div>
  );
}
