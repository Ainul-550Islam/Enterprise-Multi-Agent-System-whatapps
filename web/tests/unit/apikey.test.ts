import { describe, expect, it, vi } from "vitest";

import { ApiKeyLoginError, loginWithApiKey, MissingApiKeyError } from "@/lib/auth/apikey";

describe("loginWithApiKey", () => {
  it("rejects missing key", async () => {
    const fetchImpl = vi.fn();
    await expect(loginWithApiKey("", fetchImpl as unknown as typeof fetch)).rejects.toBeInstanceOf(
      MissingApiKeyError,
    );
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("rejects a blank key before any network call", async () => {
    const fetchImpl = vi.fn();
    await expect(loginWithApiKey("  ", fetchImpl as unknown as typeof fetch)).rejects.toBeInstanceOf(
      MissingApiKeyError,
    );
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("posts the key to the BFF and does not persist it on the result", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ subject: "api-key:1", tenantId: "t1" }),
    });
    const result = await loginWithApiKey("mas_live_secret", fetchImpl as unknown as typeof fetch);
    expect(result.subject).toBe("api-key:1");
    expect(JSON.stringify(result).includes("mas_live_secret")).toBe(false);
    const [url, init] = fetchImpl.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/api/auth/apikey");
    expect(init.credentials).toBe("same-origin");
    expect(String(init.body)).toContain("mas_live_secret");
  });

  it("surfaces a typed failure from the BFF", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: false,
      json: async () => ({ error: { code: "UNAUTHENTICATED", message: "authentication required" } }),
    });
    await expect(loginWithApiKey("bad", fetchImpl as unknown as typeof fetch)).rejects.toMatchObject({
      name: "ApiKeyLoginError",
      code: "UNAUTHENTICATED",
    } satisfies Partial<ApiKeyLoginError>);
  });
});
