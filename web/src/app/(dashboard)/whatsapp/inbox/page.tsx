"use client";

import { useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ErrorState } from "@/components/ui/error-state";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";
import { subscribeStream } from "@/lib/sse";

interface Conversation extends Record<string, unknown> {
  id: string;
  contact?: string;
  last_message?: string;
}

interface Message extends Record<string, unknown> {
  id: string;
  direction?: string;
  body?: string;
  created_at?: string;
}

export default function WhatsappInboxPage() {
  const conversations = useMas<Conversation[]>("/v1/whatsapp/conversations");
  const stats = useMas<Record<string, number>>("/v1/whatsapp/stats");
  const [active, setActive] = useState<string | null>(null);
  const messages = useMas<Message[]>(
    active ? `/v1/whatsapp/conversations/${encodeURIComponent(active)}/messages` : null,
  );
  const [reply, setReply] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    return subscribeStream({
      onFrame: () => {
        conversations.reload();
        messages.reload();
      },
    });
    // one stream for the inbox tab
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function send() {
    if (!active || !reply.trim()) {
      return;
    }
    setError(null);
    try {
      await api.post(`/v1/whatsapp/conversations/${encodeURIComponent(active)}/messages`, { body: reply });
      setReply("");
      messages.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  return (
    <div>
      <h1>WhatsApp inbox</h1>
      {error ? <ErrorState error={error} /> : null}
      <Card title="Counters" loading={stats.loading}>
        <p className="mas-muted">{stats.data ? JSON.stringify(stats.data) : "No counters."}</p>
      </Card>
      <div className="grid gap-4 lg:grid-cols-[280px_1fr]">
        <Card title="Conversations" loading={conversations.loading}>
          {(conversations.data ?? []).length === 0 ? (
            <p className="mas-muted">No conversations.</p>
          ) : (
            <ul className="m-0 list-none p-0">
              {(conversations.data ?? []).map((conversation) => (
                <li key={conversation.id}>
                  <button
                    type="button"
                    className="mas-btn mb-2 w-full text-left"
                    onClick={() => setActive(conversation.id)}
                  >
                    <strong>{conversation.contact ?? conversation.id}</strong>
                    <div className="mas-muted">{conversation.last_message}</div>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </Card>
        <Card title={active ? `Conversation ${active}` : "Select a conversation"}>
          {(messages.data ?? []).map((message) => (
            <div key={message.id} className="mb-3 rounded border border-line p-2">
              <div className="mas-muted text-xs">
                {message.direction} · {message.created_at}
              </div>
              <div>{message.body}</div>
            </div>
          ))}
          {active ? (
            <>
              <Input label="Human reply" value={reply} onChange={(event) => setReply(event.target.value)} />
              <Button onClick={() => void send()} disabled={!reply.trim()}>
                Take over and send
              </Button>
            </>
          ) : (
            <p className="mas-muted">Pick a conversation to reply as a human.</p>
          )}
        </Card>
      </div>
    </div>
  );
}
