"use client";

import { useState } from "react";

import { Badge, toneForStatus } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Dialog } from "@/components/ui/dialog";
import { EmptyState } from "@/components/ui/empty";
import { ErrorState } from "@/components/ui/error-state";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { DataTable } from "@/components/ui/table";
import { Toast } from "@/components/ui/toast";
import { useMas } from "@/lib/api/useMas";
import { api } from "@/lib/api/client";
import { useSession } from "@/lib/auth/SessionProvider";

interface Schedule extends Record<string, unknown> {
  id: string;
  name: string;
  status: string;
  next_run_at?: string | null;
}

interface RegisterScheduleRequest {
  project_id?: string;
  name: string;
  kind: "interval" | "one_time";
  every_seconds?: number;
  run_at?: string;
  task_template: Record<string, unknown>;
}

const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

export default function SchedulesPage() {
  const session = useSession();
  const schedules = useMas<Schedule[]>("/v1/schedules");
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [projectId, setProjectId] = useState(session.scope?.projectId ?? "");
  const [kind, setKind] = useState<"interval" | "one_time">("interval");
  const [everySeconds, setEverySeconds] = useState("3600");
  const [runAt, setRunAt] = useState("");
  const [taskTemplate, setTaskTemplate] = useState("{}");
  const [busy, setBusy] = useState(false);
  const [busySchedule, setBusySchedule] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  async function createSchedule() {
    setError(null);
    let parsed: unknown;
    try {
      parsed = JSON.parse(taskTemplate) as unknown;
    } catch {
      setError("Task template must be valid JSON.");
      return;
    }
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      setError("Task template must be a JSON object.");
      return;
    }
    if (!name.trim()) {
      setError("Name is required.");
      return;
    }

    const request: RegisterScheduleRequest = {
      ...(projectId.trim() ? { project_id: projectId.trim() } : {}),
      name: name.trim(),
      kind,
      task_template: parsed as Record<string, unknown>,
    };
    if (kind === "interval") {
      const seconds = Number(everySeconds);
      if (!Number.isInteger(seconds) || seconds < 10 || seconds > 31_536_000) {
        setError("Interval must be a whole number from 10 to 31,536,000 seconds.");
        return;
      }
      request.every_seconds = seconds;
    } else {
      if (!RFC3339.test(runAt) || !Number.isFinite(Date.parse(runAt)) || Date.parse(runAt) <= Date.now()) {
        setError("Enter a future RFC 3339 timestamp, including its timezone.");
        return;
      }
      request.run_at = runAt;
    }

    setBusy(true);
    try {
      await api.post<Schedule>("/v1/schedules", request);
      setOpen(false);
      setName("");
      setNotice("Schedule created.");
      schedules.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function transition(schedule: Schedule, action: "pause" | "resume" | "disable") {
    setError(null);
    setBusySchedule(schedule.id);
    try {
      await api.post(`/v1/schedules/${encodeURIComponent(schedule.id)}/${action}`);
      setNotice(`Schedule ${action}d.`);
      schedules.reload();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusySchedule(null);
    }
  }

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <h1 className="m-0">Schedules</h1>
        <Button onClick={() => { setError(null); setOpen(true); }}>New schedule</Button>
        <Button tone="ghost" onClick={schedules.reload} busy={schedules.loading}>Refresh</Button>
      </div>
      {schedules.error ? <ErrorState error={schedules.error} /> : null}
      {error ? <ErrorState error={error} /> : null}
      <Toast message={notice} onDone={() => setNotice(null)} />
      <Card title="Schedules" loading={schedules.loading}>
        {schedules.error?.code.includes("NOT_PROVISIONED") ? (
          <EmptyState title="Schedules are not provisioned" detail="Configure the schedule plane to use this page." />
        ) : null}
        <DataTable
          columns={[
            { key: "name", header: "Name" },
            { key: "status", header: "Status", render: (row) => <Badge tone={toneForStatus(row.status)}>{row.status}</Badge> },
            { key: "next_run_at", header: "Next run", render: (row) => row.next_run_at ?? "—" },
            { key: "actions", header: "Actions", render: (row) => (
              <span className="flex gap-2">
                {row.status === "active" ? <Button tone="ghost" busy={busySchedule === row.id} onClick={() => void transition(row, "pause")}>Pause</Button> : null}
                {row.status === "paused" ? <Button tone="ghost" busy={busySchedule === row.id} onClick={() => void transition(row, "resume")}>Resume</Button> : null}
                {row.status !== "disabled" && row.status !== "completed" ? <Button tone="danger" busy={busySchedule === row.id} onClick={() => void transition(row, "disable")}>Disable</Button> : null}
              </span>
            ) },
          ]}
          rows={schedules.data ?? []}
          emptyLabel="No schedules yet."
          rowKey={(row) => row.id}
        />
      </Card>
      <Dialog
        title="New schedule"
        open={open}
        onClose={() => { if (!busy) setOpen(false); }}
        footer={<>
          <Button tone="ghost" onClick={() => setOpen(false)} disabled={busy}>Cancel</Button>
          <Button onClick={() => void createSchedule()} busy={busy}>Create schedule</Button>
        </>}
      >
        {error ? <ErrorState error={error} /> : null}
        <Input label="Name" value={name} onChange={(event) => setName(event.target.value)} />
        <Input label="Project id (optional)" value={projectId} onChange={(event) => setProjectId(event.target.value)} />
        <Select label="Kind" value={kind} options={[{ value: "interval", label: "Interval" }, { value: "one_time", label: "One time" }]} onChange={(event) => setKind(event.target.value as "interval" | "one_time")} />
        {kind === "interval" ? (
          <Input label="Every (seconds)" type="number" min={10} max={31_536_000} value={everySeconds} onChange={(event) => setEverySeconds(event.target.value)} />
        ) : (
          <Input label="Run at (RFC 3339)" placeholder="2027-01-01T12:00:00Z" value={runAt} onChange={(event) => setRunAt(event.target.value)} />
        )}
        <Textarea label="Task template (JSON object)" rows={7} value={taskTemplate} onChange={(event) => setTaskTemplate(event.target.value)} />
      </Dialog>
    </div>
  );
}
