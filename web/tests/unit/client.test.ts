import { describe, expect, it } from "vitest";

import { StableApiError, unwrapEnvelope, type ApiEnvelope } from "@/lib/api/types";

function envelope<T>(data: T | null, error: ApiEnvelope<T>["error"] = null): ApiEnvelope<T> {
  return {
    data,
    error,
    meta: {
      request_id: "req-1",
      correlation_id: "corr-1",
      api_version: "v1",
      responded_at: "2026-11-15T00:00:00Z",
    },
  };
}

describe("ApiEnvelope unwrap", () => {
  it("returns data on success", () => {
    const data = unwrapEnvelope(envelope([{ id: "a1" }]), 200);
    expect(data).toEqual([{ id: "a1" }]);
  });

  it("throws StableApiError with the documented code", () => {
    expect(() =>
      unwrapEnvelope(
        envelope(null, { code: "UNAUTHENTICATED", message: "authentication required", request_id: "req-1" }),
        401,
      ),
    ).toThrow(StableApiError);
    try {
      unwrapEnvelope(envelope(null, { code: "RESOURCE_NOT_FOUND", message: "not found" }), 404);
    } catch (error) {
      expect(error).toBeInstanceOf(StableApiError);
      expect((error as StableApiError).code).toBe("RESOURCE_NOT_FOUND");
      expect((error as StableApiError).message).toBe("not found");
    }
  });
});
