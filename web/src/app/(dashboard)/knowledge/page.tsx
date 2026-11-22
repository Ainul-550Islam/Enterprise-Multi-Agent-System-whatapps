"use client";

import { useEffect, useState } from "react";

import { Badge, toneForStatus } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { Dialog } from "@/components/ui/dialog";
import { EmptyState } from "@/components/ui/empty";
import { ErrorState } from "@/components/ui/error-state";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Toast } from "@/components/ui/toast";
import { api, masFetch } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";
import { StableApiError } from "@/lib/api/types";
import { useSession } from "@/lib/auth/SessionProvider";

interface Source extends Record<string, unknown> {
  id: string;
  project_id: string;
  name: string;
  kind: string;
  status: string;
  config: Record<string, unknown>;
  has_credential: boolean;
  last_synced_at?: string | null;
  last_failure_code?: string | null;
  updated_at: string;
}

interface Document extends Record<string, unknown> {
  id: string;
  source_id: string;
  title: string;
  mime_type: string;
  size_bytes: number;
  status: string;
  chunk_count: number;
  failure_code?: string | null;
  created_at: string;
  updated_at: string;
}

interface SyncResult {
  source_id: string;
  job_id?: string | null;
  status: string;
}

interface CreateSourceRequest {
  project_id: string;
  name: string;
  kind: "upload" | "object_storage" | "web";
  config: Record<string, unknown>;
  credential_id?: string;
}

interface UploadRequest {
  source_id: string;
  title: string;
  mime_type: string;
  size_bytes: number;
  content_sha256: string;
  access_policy: Record<string, unknown>;
  metadata: Record<string, unknown>;
}

const MAX_UPLOAD_BYTES = 10 * 1024 * 1024;

async function sha256(file: File): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", await file.arrayBuffer());
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function apiErrorMessage(error: unknown): string {
  if (error instanceof StableApiError) return `${error.code}: ${error.message}`;
  return error instanceof Error ? error.message : String(error);
}

export default function KnowledgePage() {
  const session = useSession();
  const [projectId, setProjectId] = useState(session.scope?.projectId ?? "");
  const sources = useMas<Source[]>(projectId.trim() ? "/v1/data-sources" : null, projectId.trim() ? { project_id: projectId.trim() } : undefined);
  const [selected, setSelected] = useState<Source | null>(null);
  const documents = useMas<Document[]>(selected ? `/v1/data-sources/${encodeURIComponent(selected.id)}/documents` : null);
  const [addOpen, setAddOpen] = useState(false);
  const [sourceName, setSourceName] = useState("");
  const [kind, setKind] = useState<CreateSourceRequest["kind"]>("web");
  const [configText, setConfigText] = useState("{}");
  const [credentialId, setCredentialId] = useState("");
  const [deleteTarget, setDeleteTarget] = useState<Document | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [operationError, setOperationError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [sync, setSync] = useState<SyncResult | null>(null);

  useEffect(() => {
    if (!projectId && session.scope?.projectId) setProjectId(session.scope.projectId);
  }, [projectId, session.scope?.projectId]);

  async function addSource() {
    setOperationError(null);
    if (!projectId.trim() || !sourceName.trim()) {
      setOperationError("Project id and source name are required.");
      return;
    }
    let config: unknown;
    try {
      config = JSON.parse(configText) as unknown;
    } catch {
      setOperationError("Source config must be valid JSON.");
      return;
    }
    if (config === null || typeof config !== "object" || Array.isArray(config)) {
      setOperationError("Source config must be a JSON object.");
      return;
    }
    const body: CreateSourceRequest = {
      project_id: projectId.trim(),
      name: sourceName.trim(),
      kind,
      config: config as Record<string, unknown>,
      ...(credentialId.trim() ? { credential_id: credentialId.trim() } : {}),
    };
    setBusy(true);
    try {
      await api.post("/v1/data-sources", body);
      setAddOpen(false);
      setSourceName("");
      setConfigText("{}");
      setCredentialId("");
      setNotice("Data source created.");
      sources.reload();
    } catch (caught) {
      setOperationError(apiErrorMessage(caught));
    } finally {
      setBusy(false);
    }
  }

  async function syncSource(source: Source) {
    setOperationError(null);
    setSync(null);
    setBusy(true);
    try {
      const result = await api.post<SyncResult>(`/v1/data-sources/${encodeURIComponent(source.id)}/sync`);
      setSync(result);
      setNotice(`Sync ${result.status}${result.job_id ? ` · job ${result.job_id}` : ""}`);
      sources.reload();
      if (selected?.id === source.id) documents.reload();
    } catch (caught) {
      setOperationError(apiErrorMessage(caught));
    } finally {
      setBusy(false);
    }
  }

  async function uploadDocument() {
    setOperationError(null);
    if (!selected || !file) {
      setOperationError("Select a source and a file to upload.");
      return;
    }
    if (file.size > MAX_UPLOAD_BYTES) {
      setOperationError("Files must be 10 MB or smaller.");
      return;
    }
    setBusy(true);
    try {
      const request: UploadRequest = {
        source_id: selected.id,
        title: file.name,
        mime_type: file.type || "application/octet-stream",
        size_bytes: file.size,
        content_sha256: await sha256(file),
        access_policy: {},
        metadata: {},
      };
      const formData = new FormData();
      formData.set("request", JSON.stringify(request));
      formData.set("content", file, file.name);
      await masFetch<Document>("/v1/documents/upload", { method: "POST", formData });
      setFile(null);
      const picker = document.getElementById("knowledge-upload") as HTMLInputElement | null;
      if (picker) picker.value = "";
      setNotice("Document uploaded.");
      documents.reload();
    } catch (caught) {
      setOperationError(apiErrorMessage(caught));
    } finally {
      setBusy(false);
    }
  }

  async function deleteDocument() {
    if (!deleteTarget) return;
    setBusy(true);
    setOperationError(null);
    try {
      await api.delete(`/v1/documents/${encodeURIComponent(deleteTarget.id)}`);
      setDeleteTarget(null);
      setNotice("Document deleted.");
      documents.reload();
    } catch (caught) {
      setOperationError(apiErrorMessage(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h1>Knowledge / Data Hub</h1>
      <div className="mb-4 flex flex-wrap items-end gap-3">
        <Input label="Project id" value={projectId} onChange={(event) => { setProjectId(event.target.value); setSelected(null); }} />
        <Button onClick={() => { setOperationError(null); setAddOpen(true); }} disabled={!projectId.trim()}>Add source</Button>
        <Button tone="ghost" onClick={sources.reload} busy={sources.loading} disabled={!projectId.trim()}>Refresh</Button>
      </div>
      {operationError ? <ErrorState error={operationError} /> : null}
      <Toast message={notice} onDone={() => setNotice(null)} />
      {!projectId.trim() ? <EmptyState title="Choose a project" detail="Enter a project id to load its data sources." /> : null}
      {sources.error ? <ErrorState error={sources.error} /> : null}
      <Card title="Data sources" loading={sources.loading}>
        {sources.error?.code.includes("NOT_PROVISIONED") ? <EmptyState title="Knowledge plane is not provisioned" detail="Configure the Data Hub bridge and database to manage sources." /> : null}
        <DataTable
          columns={[
            { key: "name", header: "Name", render: (row) => <button type="button" className="text-left underline" onClick={() => setSelected(row)}>{row.name}</button> },
            { key: "kind", header: "Kind" },
            { key: "status", header: "Status", render: (row) => <Badge tone={toneForStatus(row.status)}>{row.status}</Badge> },
            { key: "has_credential", header: "Credential", render: (row) => row.has_credential ? "linked" : "none" },
            { key: "last_synced_at", header: "Last synced", render: (row) => row.last_synced_at ?? "—" },
            { key: "last_failure_code", header: "Failure", render: (row) => row.last_failure_code ?? "—" },
            { key: "actions", header: "Actions", render: (row) => <Button tone="ghost" busy={busy} onClick={() => void syncSource(row)}>Sync now</Button> },
          ]}
          rows={sources.data ?? []}
          emptyLabel={projectId.trim() ? "No data sources for this project." : "Enter a project id to load sources."}
          rowKey={(row) => row.id}
        />
      </Card>
      {sync ? <p role="status">Sync status: {sync.status}{sync.job_id ? ` · Job ${sync.job_id}` : ""}</p> : null}

      {selected ? (
        <Card title={`Documents · ${selected.name}`} loading={documents.loading}>
          {documents.error ? <ErrorState error={documents.error} /> : null}
          <DataTable
            columns={[
              { key: "title", header: "Title" },
              { key: "mime_type", header: "MIME" },
              { key: "size_bytes", header: "Size", render: (row) => `${row.size_bytes} bytes` },
              { key: "status", header: "Status", render: (row) => <Badge tone={toneForStatus(row.status)}>{row.status}</Badge> },
              { key: "chunk_count", header: "Chunks" },
              { key: "failure_code", header: "Failure", render: (row) => row.failure_code ?? "—" },
              { key: "actions", header: "Actions", render: (row) => <Button tone="danger" onClick={() => setDeleteTarget(row)}>Delete</Button> },
            ]}
            rows={documents.data ?? []}
            emptyLabel="No documents for this source."
            rowKey={(row) => row.id}
          />
          <div className="mt-4 flex flex-wrap items-end gap-3">
            <label className="mas-field">
              Upload document (max 10 MB)
              <input id="knowledge-upload" type="file" onChange={(event) => setFile(event.target.files?.[0] ?? null)} />
            </label>
            <Button onClick={() => void uploadDocument()} disabled={!file} busy={busy}>Upload</Button>
          </div>
        </Card>
      ) : null}

      <Dialog title="Add data source" open={addOpen} onClose={() => { if (!busy) setAddOpen(false); }} footer={<>
        <Button tone="ghost" onClick={() => setAddOpen(false)} disabled={busy}>Cancel</Button>
        <Button onClick={() => void addSource()} busy={busy}>Create source</Button>
      </>}>
        {operationError ? <ErrorState error={operationError} /> : null}
        <Input label="Name" value={sourceName} onChange={(event) => setSourceName(event.target.value)} />
        <Select label="Kind" value={kind} options={[
          { value: "web", label: "Web" },
          { value: "object_storage", label: "Object storage" },
          { value: "upload", label: "Upload" },
        ]} onChange={(event) => setKind(event.target.value as CreateSourceRequest["kind"])} />
        <Textarea label="Config (JSON object)" rows={6} value={configText} onChange={(event) => setConfigText(event.target.value)} />
        <Input label="Credential id (optional)" value={credentialId} onChange={(event) => setCredentialId(event.target.value)} />
      </Dialog>
      <Dialog title={`Delete ${deleteTarget?.title ?? "document"}?`} open={Boolean(deleteTarget)} onClose={() => { if (!busy) setDeleteTarget(null); }} footer={<>
        <Button tone="ghost" onClick={() => setDeleteTarget(null)} disabled={busy}>Cancel</Button>
        <Button tone="danger" onClick={() => void deleteDocument()} busy={busy}>Confirm delete</Button>
      </>}>
        <p>This removes the document from the selected source.</p>
      </Dialog>
    </div>
  );
}
