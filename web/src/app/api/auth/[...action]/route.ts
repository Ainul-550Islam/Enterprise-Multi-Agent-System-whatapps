import { cookies } from "next/headers";
import { NextResponse } from "next/server";
import { randomBytes } from "node:crypto";

import { CSRF_COOKIE, OIDC_STATE_COOKIE, SESSION_COOKIE, SESSION_MAX_AGE_SECONDS } from "@/lib/auth/cookies";
import {
  csrfCookieOptions,
  csrfMatches,
  isSecureRequest,
  newCsrfToken,
  sealSession,
  sessionCookieOptions,
  unsealSession,
  type SessionRecord,
} from "@/lib/auth/session";
import {
  authorizationUrl,
  discover,
  exchangeCode,
  generatePkce,
  oidcConfigured,
  readOidcConfig,
  type OidcStartState,
} from "@/lib/auth/oidc";
import { masApiUrl } from "@/lib/mas-url";
import { assertSameOrigin, requestOrigin } from "@/lib/origin";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function jsonError(status: number, code: string, message: string): NextResponse {
  return NextResponse.json(
    {
      data: null,
      error: { code, message },
      meta: {
        request_id: "",
        correlation_id: "",
        api_version: "v1",
        responded_at: new Date().toISOString(),
      },
    },
    { status },
  );
}

function actionOf(params: { action: string[] }): string {
  return params.action.join("/");
}

function attachSession(response: NextResponse, request: Request, record: SessionRecord): NextResponse {
  const secure = isSecureRequest(request);
  response.cookies.set(SESSION_COOKIE, sealSession(record), sessionCookieOptions(secure));
  response.cookies.set(CSRF_COOKIE, record.csrf, csrfCookieOptions(secure));
  return response;
}

function clearSession(response: NextResponse, request: Request): NextResponse {
  const secure = isSecureRequest(request);
  response.cookies.set(SESSION_COOKIE, "", { ...sessionCookieOptions(secure), maxAge: 0 });
  response.cookies.set(CSRF_COOKIE, "", { ...csrfCookieOptions(secure), maxAge: 0 });
  response.cookies.set(OIDC_STATE_COOKIE, "", { ...sessionCookieOptions(secure), maxAge: 0 });
  return response;
}

async function callAuthMe(token: string): Promise<{
  subject: string;
  tenantId?: string;
  organizationId?: string;
}> {
  const response = await fetch(`${masApiUrl()}/v1/auth/me`, {
    headers: {
      authorization: `Bearer ${token}`,
      accept: "application/json",
    },
    signal: AbortSignal.timeout(8_000),
  });
  const envelope = (await response.json().catch(() => ({}))) as {
    data?: { subject?: string; tenant_id?: string; organization_id?: string };
    error?: { code?: string; message?: string };
  };
  if (!response.ok || envelope.error) {
    const error = new Error(envelope.error?.message ?? "authentication failed") as Error & {
      code?: string;
      status?: number;
    };
    error.code = envelope.error?.code ?? "UNAUTHENTICATED";
    error.status = response.status;
    throw error;
  }
  return {
    subject: envelope.data?.subject ?? "unknown",
    tenantId: envelope.data?.tenant_id,
    organizationId: envelope.data?.organization_id,
  };
}

export async function GET(request: Request, context: { params: { action: string[] } }) {
  const action = actionOf(context.params);
  if (action === "session") {
    try {
      const raw = cookies().get(SESSION_COOKIE)?.value;
      const session = unsealSession(raw);
      return NextResponse.json({
        authenticated: true,
        kind: session.kind,
        subject: session.subject,
        scope: session.scope,
        oidc: oidcConfigured(),
      });
    } catch {
      return NextResponse.json({ authenticated: false, oidc: oidcConfigured() });
    }
  }
  if (action === "oidc" || action === "login") {
    try {
      const origin = process.env.MAS_WEB_ORIGIN ?? requestOrigin(request);
      const config = readOidcConfig(origin);
      const discovery = await discover(config.issuer);
      const pkce = generatePkce();
      const start: OidcStartState = {
        state: randomBytes(16).toString("base64url"),
        nonce: randomBytes(16).toString("base64url"),
        verifier: pkce.verifier,
        redirect: new URL(request.url).searchParams.get("next") ?? "/overview",
      };
      const location = authorizationUrl(discovery, config, {
        state: start.state,
        nonce: start.nonce,
        challenge: pkce.challenge,
      });
      const response = NextResponse.redirect(location);
      const secure = isSecureRequest(request);
      response.cookies.set(OIDC_STATE_COOKIE, sealSession({
        kind: "oidc",
        token: JSON.stringify(start),
        csrf: newCsrfToken(),
        subject: "oidc-pending",
        scope: {},
        expiresAt: Date.now() + 10 * 60 * 1000,
      }), { ...sessionCookieOptions(secure), maxAge: 600 });
      return response;
    } catch (error) {
      return jsonError(400, "OIDC_NOT_CONFIGURED", error instanceof Error ? error.message : "OIDC failed");
    }
  }
  if (action === "callback") {
    const url = new URL(request.url);
    const code = url.searchParams.get("code");
    const state = url.searchParams.get("state");
    if (!code || !state) {
      return jsonError(400, "OIDC_CALLBACK", "missing code or state");
    }
    try {
      const pending = unsealSession(cookies().get(OIDC_STATE_COOKIE)?.value);
      const start = JSON.parse(pending.token) as OidcStartState;
      if (start.state !== state) {
        return jsonError(400, "OIDC_STATE", "state mismatch");
      }
      const origin = process.env.MAS_WEB_ORIGIN ?? requestOrigin(request);
      const config = readOidcConfig(origin);
      const discovery = await discover(config.issuer);
      const tokens = await exchangeCode({ discovery, config, code, verifier: start.verifier });
      const me = await callAuthMe(tokens.accessToken).catch(() => ({
        subject: tokens.subject,
        tenantId: undefined,
        organizationId: undefined,
      }));
      const record: SessionRecord = {
        kind: "oidc",
        token: tokens.accessToken,
        csrf: newCsrfToken(),
        subject: me.subject,
        scope: { tenantId: me.tenantId, organizationId: me.organizationId },
        expiresAt: Date.now() + SESSION_MAX_AGE_SECONDS * 1000,
      };
      const redirect = NextResponse.redirect(new URL(start.redirect || "/overview", origin));
      redirect.cookies.set(OIDC_STATE_COOKIE, "", { ...sessionCookieOptions(isSecureRequest(request)), maxAge: 0 });
      return attachSession(redirect, request, record);
    } catch (error) {
      return jsonError(401, "OIDC_CALLBACK", error instanceof Error ? error.message : "OIDC callback failed");
    }
  }
  return jsonError(404, "NOT_FOUND", `unknown auth action ${action}`);
}

export async function POST(request: Request, context: { params: { action: string[] } }) {
  const action = actionOf(context.params);
  try {
    assertSameOrigin(request);
  } catch (error) {
    return jsonError(403, "CSRF", error instanceof Error ? error.message : "origin refused");
  }
  if (action === "apikey") {
    const body = (await request.json().catch(() => ({}))) as { apiKey?: string };
    const apiKey = body.apiKey?.trim();
    if (!apiKey) {
      return jsonError(400, "VALIDATION_FAILED", "apiKey is required");
    }
    try {
      const me = await callAuthMe(apiKey);
      const record: SessionRecord = {
        kind: "apikey",
        token: apiKey,
        csrf: newCsrfToken(),
        subject: me.subject,
        scope: { tenantId: me.tenantId, organizationId: me.organizationId },
        expiresAt: Date.now() + SESSION_MAX_AGE_SECONDS * 1000,
      };
      const response = NextResponse.json({
        subject: me.subject,
        tenantId: me.tenantId,
        organizationId: me.organizationId,
      });
      return attachSession(response, request, record);
    } catch (error) {
      const err = error as Error & { code?: string; status?: number };
      return jsonError(err.status ?? 401, err.code ?? "UNAUTHENTICATED", err.message);
    }
  }
  if (action === "logout") {
    const sessionCookie = cookies().get(SESSION_COOKIE)?.value;
    if (sessionCookie) {
      try {
        const session = unsealSession(sessionCookie);
        const header = request.headers.get("x-csrf-token");
        if (!csrfMatches(session.csrf, header)) {
          return jsonError(403, "CSRF", "csrf token mismatch");
        }
      } catch {
        // already unauthenticated
      }
    }
    return clearSession(NextResponse.json({ ok: true }), request);
  }
  if (action === "scope") {
    const sessionCookie = cookies().get(SESSION_COOKIE)?.value;
    try {
      const session = unsealSession(sessionCookie);
      const header = request.headers.get("x-csrf-token");
      if (!csrfMatches(session.csrf, header)) {
        return jsonError(403, "CSRF", "csrf token mismatch");
      }
      const body = (await request.json().catch(() => ({}))) as {
        tenantId?: string;
        organizationId?: string;
        projectId?: string;
      };
      const next: SessionRecord = {
        ...session,
        scope: {
          tenantId: body.tenantId ?? session.scope.tenantId,
          organizationId: body.organizationId ?? session.scope.organizationId,
          projectId: body.projectId ?? session.scope.projectId,
        },
      };
      const response = NextResponse.json({ scope: next.scope });
      return attachSession(response, request, next);
    } catch {
      return jsonError(401, "UNAUTHENTICATED", "authentication required");
    }
  }
  return jsonError(404, "NOT_FOUND", `unknown auth action ${action}`);
}
