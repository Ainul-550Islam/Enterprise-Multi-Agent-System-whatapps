"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface Channel extends Record<string, unknown> {
  id: string;
  name?: string;
  phone_number?: string;
}

interface Template extends Record<string, unknown> {
  name: string;
  language?: string;
  status?: string;
}

export default function WhatsappPage() {
  const channels = useMas<Channel[]>("/v1/whatsapp/channels");
  const health = useMas<Record<string, unknown>>("/v1/platforms/whatsapp/health");
  const [channelId, setChannelId] = useState("");
  const templates = useMas<Template[]>(
    channelId ? "/v1/whatsapp/templates" : null,
    channelId ? { channel_id: channelId } : undefined,
  );
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);

  async function register() {
    setError(null);
    try {
      await api.post("/v1/whatsapp/channels", { name });
      channels.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  async function syncTemplates() {
    setError(null);
    try {
      await api.post("/v1/whatsapp/templates", { channel_id: channelId });
      templates.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  return (
    <div>
      <h1>WhatsApp setup</h1>
      {error ? <ErrorState error={error} /> : null}
      <Card title="Platform health" loading={health.loading}>
        {health.error ? <ErrorState error={health.error} /> : null}
        <p>{health.data ? JSON.stringify(health.data) : "No health payload."}</p>
      </Card>
      <Card title="1. Register a channel">
        <Input label="Display name" value={name} onChange={(event) => setName(event.target.value)} />
        <Button onClick={() => void register()} disabled={!name}>
          Register channel
        </Button>
      </Card>
      <Card title="2. Channels" loading={channels.loading}>
        <DataTable
          columns={[
            { key: "name", header: "Name" },
            { key: "phone_number", header: "Number" },
            { key: "id", header: "Id" },
          ]}
          rows={channels.data ?? []}
          emptyLabel="No WhatsApp channels."
          rowKey={(row) => String(row.id)}
        />
      </Card>
      <Card title="3. Templates">
        <Input label="Channel id" value={channelId} onChange={(event) => setChannelId(event.target.value)} />
        <Button tone="ghost" onClick={() => void syncTemplates()} disabled={!channelId}>
          Sync from provider
        </Button>
        <DataTable
          columns={[
            { key: "name", header: "Template" },
            { key: "language", header: "Language" },
            { key: "status", header: "Status" },
          ]}
          rows={templates.data ?? []}
          emptyLabel="Select a channel to load templates."
          rowKey={(row, index) => `${row.name}-${index}`}
        />
      </Card>
    </div>
  );
}
