"use client";

import { useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTableWithCursor } from "@/components/ui/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { Input } from "@/components/ui/input";
import { Toast } from "@/components/ui/toast";
import { useMasMutation } from "@/lib/api/useMas";
import { useMas } from "@/lib/api/useMas";

interface Notification extends Record<string, unknown> {
  id: string;
  title?: string;
  body?: string;
  read_at?: string | null;
  archived_at?: string | null;
}

/** Per-recipient notification feed with real read/archive transitions. */
export default function NotificationsPage() {
  const [recipient, setRecipient] = useState("");
  const feed = useMas<{ items: Notification[]; next_cursor?: string }>(
    recipient ? `/v1/notifications/recipients/${encodeURIComponent(recipient)}` : null,
  );
  const mutation = useMasMutation<unknown>("/v1/notifications");
  const [notice, setNotice] = useState<string | null>(null);

  async function transition(row: Notification, action: "read" | "archive") {
    const target = `/v1/notifications/recipients/${encodeURIComponent(recipient)}/${encodeURIComponent(row.id)}/${action}`;
    const result = await mutation.mutate("POST", target, undefined, { refresh: true });
    if (result === null) setNotice("Notification update refused");
  }

  return (
    <div>
      <h1>Notifications</h1>
      {feed.error || mutation.error ? <ErrorState error={(feed.error ?? mutation.error)!} /> : null}
      <Toast message={notice} onDone={() => setNotice(null)} />
      <Card title="Notification feed" loading={feed.loading}>
        <Input label="Recipient user id" value={recipient} onChange={(event) => setRecipient(event.target.value)} />
        <DataTableWithCursor
          columns={[
            { key: "title", header: "Subject", value: (row) => String(row.title ?? row.subject ?? row.body ?? "") },
            { key: "read_at", header: "Read", render: (row) => <Badge tone={row.read_at ? "ok" : "warn"}>{row.read_at ? "read" : "unread"}</Badge> },
            { key: "archived_at", header: "Archived", render: (row) => <Badge tone={row.archived_at ? "warn" : "ok"}>{row.archived_at ? "archived" : "inbox"}</Badge> },
            { key: "actions", header: "", render: (row) => (
              <span className="flex gap-2">
                <Button tone="ghost" disabled={Boolean(row.read_at)} onClick={() => void transition(row, "read")}>Mark read</Button>
                <Button tone="ghost" disabled={Boolean(row.archived_at)} onClick={() => void transition(row, "archive")}>Archive</Button>
              </span>
            ) },
          ]}
          rows={feed.data?.items ?? []}
          rowKey={(row) => row.id}
          emptyLabel={recipient ? "No notifications for this recipient." : "Enter a recipient id."}
        />
      </Card>
    </div>
  );
}
