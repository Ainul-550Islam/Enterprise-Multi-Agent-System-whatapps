/**
 * Server-only encrypted session. The access token (API key or OIDC access
 * token) never leaves this module toward the browser.
 */
import { createCipheriv, createDecipheriv, createHash, randomBytes, timingSafeEqual } from "node:crypto";

import { CSRF_COOKIE, SESSION_COOKIE, SESSION_MAX_AGE_SECONDS } from "./cookies";

export type AuthKind = "apikey" | "oidc";

export interface SessionScope {
  tenantId?: string;
  organizationId?: string;
  projectId?: string;
}

export interface SessionRecord {
  kind: AuthKind;
  token: string;
  csrf: string;
  subject: string;
  scope: SessionScope;
  expiresAt: number;
}

export class SessionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SessionError";
  }
}

function secretKey(): Buffer {
  const secret = process.env.SESSION_SECRET;
  if (!secret || secret.length < 32) {
    throw new SessionError("SESSION_SECRET must be at least 32 characters");
  }
  return createHash("sha256").update(secret).digest();
}

/** Seals a session record into an authenticated ciphertext. */
export function sealSession(record: SessionRecord): string {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", secretKey(), iv);
  const plaintext = Buffer.from(JSON.stringify(record), "utf8");
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, ciphertext]).toString("base64url");
}

/** Opens a sealed session, refusing tampered or expired payloads. */
export function unsealSession(token: string | undefined | null): SessionRecord {
  if (!token) {
    throw new SessionError("missing session");
  }
  let raw: Buffer;
  try {
    raw = Buffer.from(token, "base64url");
  } catch {
    throw new SessionError("malformed session");
  }
  if (raw.length < 29) {
    throw new SessionError("malformed session");
  }
  const iv = raw.subarray(0, 12);
  const tag = raw.subarray(12, 28);
  const ciphertext = raw.subarray(28);
  try {
    const decipher = createDecipheriv("aes-256-gcm", secretKey(), iv);
    decipher.setAuthTag(tag);
    const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    const record = JSON.parse(plaintext.toString("utf8")) as SessionRecord;
    if (!record || typeof record.token !== "string" || typeof record.csrf !== "string") {
      throw new SessionError("malformed session");
    }
    if (record.expiresAt <= Date.now()) {
      throw new SessionError("session expired");
    }
    return record;
  } catch (error) {
    if (error instanceof SessionError) {
      throw error;
    }
    throw new SessionError("invalid session");
  }
}

export function newCsrfToken(): string {
  return randomBytes(32).toString("base64url");
}

export function csrfMatches(expected: string, provided: string | null | undefined): boolean {
  if (!provided) {
    return false;
  }
  const a = Buffer.from(expected);
  const b = Buffer.from(provided);
  if (a.length !== b.length) {
    return false;
  }
  return timingSafeEqual(a, b);
}

export function sessionCookieOptions(secure: boolean) {
  return {
    httpOnly: true as const,
    sameSite: "lax" as const,
    secure,
    path: "/",
    maxAge: SESSION_MAX_AGE_SECONDS,
  };
}

export function csrfCookieOptions(secure: boolean) {
  return {
    httpOnly: false as const,
    sameSite: "lax" as const,
    secure,
    path: "/",
    maxAge: SESSION_MAX_AGE_SECONDS,
  };
}

export function isSecureRequest(request: Request): boolean {
  const proto = request.headers.get("x-forwarded-proto");
  if (proto) {
    return proto.split(",")[0]?.trim() === "https";
  }
  return new URL(request.url).protocol === "https:";
}

export { SESSION_COOKIE, CSRF_COOKIE };
