// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";

import { masFetch } from "@/lib/api/client";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("masFetch multipart requests", () => {
  it("sends FormData without Content-Type, retains CSRF, and unwraps the envelope", async () => {
    document.cookie = "mas_csrf=form-token; path=/";
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(
        JSON.stringify({
          data: { id: "doc-1" },
          error: null,
          meta: { request_id: "req-1", correlation_id: "corr-1", api_version: "v1", responded_at: "2026-11-21T00:00:00Z" },
        }),
        { status: 201, headers: { "content-type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const formData = new FormData();
    formData.set("request", JSON.stringify({ source_id: "source-1" }));
    formData.set("content", new File(["hello"], "hello.txt", { type: "text/plain" }));

    await expect(masFetch<{ id: string }>("/v1/documents/upload", { method: "POST", formData })).resolves.toEqual({ id: "doc-1" });
    const [, init] = fetchMock.mock.calls[0] ?? [];
    const headers = new Headers(init?.headers);
    expect(init?.body).toBe(formData);
    expect(headers.get("content-type")).toBeNull();
    expect(headers.get("x-csrf-token")).toBe("form-token");
  });

  it("preserves the 401 redirect behavior", async () => {
    const location = { pathname: "/knowledge", href: "" };
    vi.stubGlobal("window", { location });
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(
          JSON.stringify({
            data: null,
            error: { code: "UNAUTHENTICATED", message: "authentication required" },
            meta: { request_id: "req-1", correlation_id: "corr-1", api_version: "v1", responded_at: "2026-11-21T00:00:00Z" },
          }),
          { status: 401 },
        ),
      ),
    );

    await expect(masFetch("/v1/documents/upload", { method: "POST", formData: new FormData() })).rejects.toMatchObject({
      code: "UNAUTHENTICATED",
    });
    expect(location.href).toBe("/login?next=%2Fknowledge");
  });
});
