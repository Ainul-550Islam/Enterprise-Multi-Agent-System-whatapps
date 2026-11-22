// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  useMas: vi.fn(),
  post: vi.fn(),
  delete: vi.fn(),
  masFetch: vi.fn(),
}));
vi.mock("@/lib/api/useMas", () => ({ useMas: mocks.useMas }));
vi.mock("@/lib/api/client", () => ({ api: { post: mocks.post, delete: mocks.delete }, masFetch: mocks.masFetch }));
vi.mock("@/lib/auth/SessionProvider", () => ({ useSession: () => ({ scope: { projectId: "project-1" } }) }));

import KnowledgePage from "@/app/(dashboard)/knowledge/page";
import { StableApiError } from "@/lib/api/types";

afterEach(cleanup);
beforeEach(() => {
  mocks.useMas.mockImplementation((path: string | null) => ({
    data: path?.includes("/documents") ? [] : [], error: null, loading: false, reload: vi.fn(),
  }));
  mocks.post.mockReset().mockResolvedValue({ source_id: "source-1", status: "accepted", job_id: "job-1" });
  mocks.delete.mockReset().mockResolvedValue({ deleted: true });
  mocks.masFetch.mockReset().mockResolvedValue({ id: "doc-1" });
});

describe("KnowledgePage", () => {
  it("renders source loading", () => {
    mocks.useMas.mockReturnValue({ data: null, error: null, loading: true, reload: vi.fn() });
    render(<KnowledgePage />);
    expect(screen.getByText("Loading…")).toBeInTheDocument();
  });

  it("requires a project before querying and renders empty sources", () => {
    render(<KnowledgePage />);
    expect(mocks.useMas).toHaveBeenCalledWith("/v1/data-sources", { project_id: "project-1" });
    expect(screen.getByText("No data sources for this project.")).toBeInTheDocument();
  });

  it("shows an explicit not-provisioned error", () => {
    mocks.useMas.mockReturnValue({
      data: null,
      error: new StableApiError({ code: "DATA_HUB_NOT_PROVISIONED", message: "data hub unavailable" }),
      loading: false,
      reload: vi.fn(),
    });
    render(<KnowledgePage />);
    expect(screen.getByText("Knowledge plane is not provisioned")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("DATA_HUB_NOT_PROVISIONED");
  });

  it("creates a source with the selected project and object config", async () => {
    render(<KnowledgePage />);
    fireEvent.click(screen.getByRole("button", { name: "Add source" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "Handbook" } });
    fireEvent.change(screen.getByLabelText("Kind"), { target: { value: "web" } });
    fireEvent.change(screen.getByLabelText("Config (JSON object)"), { target: { value: "{\"url\":\"https://example.test/docs\"}" } });
    fireEvent.change(screen.getByLabelText("Credential id (optional)"), { target: { value: "cred-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Create source" }));
    await waitFor(() => expect(mocks.post).toHaveBeenCalledWith("/v1/data-sources", {
      project_id: "project-1",
      name: "Handbook",
      kind: "web",
      config: { url: "https://example.test/docs" },
      credential_id: "cred-1",
    }));
    expect(await screen.findByText("Data source created.")).toBeInTheDocument();
  });

  it("syncs a real source and displays the returned job", async () => {
    mocks.useMas.mockReturnValue({
      data: [{ id: "source-1", project_id: "project-1", name: "Handbook", kind: "web", status: "active", config: {}, has_credential: false, updated_at: "now" }],
      error: null, loading: false, reload: vi.fn(),
    });
    render(<KnowledgePage />);
    fireEvent.click(screen.getByRole("button", { name: "Sync now" }));
    await waitFor(() => expect(mocks.post).toHaveBeenCalledWith("/v1/data-sources/source-1/sync"));
    expect(await screen.findByText("Sync status: accepted · Job job-1")).toBeInTheDocument();
  });

  it("uploads exactly request and content multipart fields with the digest", async () => {
    vi.spyOn(crypto.subtle, "digest").mockResolvedValue(new Uint8Array(32).buffer);
    mocks.useMas.mockImplementation((path: string | null) => ({
      data: path?.includes("/documents") ? [] : [{ id: "source-1", project_id: "project-1", name: "Handbook", kind: "upload", status: "active", config: {}, has_credential: false, updated_at: "now" }],
      error: null, loading: false, reload: vi.fn(),
    }));
    render(<KnowledgePage />);
    fireEvent.click(screen.getByRole("button", { name: "Handbook" }));
    const file = new File(["hello"], "hello.txt", { type: "text/plain" });
    Object.defineProperty(file, "arrayBuffer", { value: async () => new TextEncoder().encode("hello").buffer });
    fireEvent.change(screen.getByLabelText("Upload document (max 10 MB)"), { target: { files: [file] } });
    fireEvent.click(screen.getByRole("button", { name: "Upload" }));
    await waitFor(() => expect(mocks.masFetch).toHaveBeenCalledTimes(1));
    const [path, options] = mocks.masFetch.mock.calls[0] as [string, { method: string; formData: FormData }];
    expect(path).toBe("/v1/documents/upload");
    expect(options.method).toBe("POST");
    expect([...options.formData.keys()].sort()).toEqual(["content", "request"]);
    const request = JSON.parse(String(options.formData.get("request"))) as Record<string, unknown>;
    expect(request).toMatchObject({ source_id: "source-1", title: "hello.txt", mime_type: "text/plain", size_bytes: 5, content_sha256: "0".repeat(64), access_policy: {}, metadata: {} });
    expect(options.formData.get("content")).toBeInstanceOf(File);
  });
});
