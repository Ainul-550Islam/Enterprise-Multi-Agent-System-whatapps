import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  csrfMatches,
  sealSession,
  SessionError,
  unsealSession,
  type SessionRecord,
} from "@/lib/auth/session";

const record: SessionRecord = {
  kind: "apikey",
  token: "mas_live_secret",
  csrf: "csrf-token-value",
  subject: "api-key:11111111-1111-1111-1111-111111111111",
  scope: { tenantId: "t1" },
  expiresAt: Date.now() + 60_000,
};

describe("session seal", () => {
  const previous = process.env.SESSION_SECRET;

  beforeEach(() => {
    process.env.SESSION_SECRET = "unit-test-session-secret-must-be-32-chars";
  });

  afterEach(() => {
    process.env.SESSION_SECRET = previous;
  });

  it("round-trips a record without exposing the token in the ciphertext as plaintext", () => {
    const sealed = sealSession(record);
    expect(sealed.includes("mas_live_secret")).toBe(false);
    const opened = unsealSession(sealed);
    expect(opened.token).toBe(record.token);
    expect(opened.subject).toBe(record.subject);
    expect(opened.csrf).toBe(record.csrf);
  });

  it("refuses tampered ciphertext", () => {
    const sealed = sealSession(record);
    const mutated = `${sealed.slice(0, -2)}aa`;
    expect(() => unsealSession(mutated)).toThrow(SessionError);
  });

  it("refuses a missing cookie", () => {
    expect(() => unsealSession(undefined)).toThrow(/missing session/);
  });

  it("compares CSRF tokens in constant time", () => {
    expect(csrfMatches("abc", "abc")).toBe(true);
    expect(csrfMatches("abc", "abd")).toBe(false);
    expect(csrfMatches("abc", null)).toBe(false);
  });
});
