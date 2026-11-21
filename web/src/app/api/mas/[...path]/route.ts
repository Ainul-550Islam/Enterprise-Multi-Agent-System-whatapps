import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import { CSRF_COOKIE, SESSION_COOKIE } from "@/lib/auth/cookies";
import { csrfMatches, unsealSession } from "@/lib/auth/session";
import { assertProxiedPath, masApiUrl } from "@/lib/mas-url";
import { assertSameOrigin } from "@/lib/origin";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const MUTATING = new Set(["POST", "PUT", "PATCH", "DELETE"]);

function envelope(status: number, code: string, message: string): NextResponse {
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

async function proxy(request: Request, pathParts: string[]): Promise<Response> {
  let path: string;
  try {
    path = assertProxiedPath(`/${pathParts.join("/")}`);
  } catch (error) {
    return envelope(400, "PATH_REFUSED", error instanceof Error ? error.message : "path refused");
  }
  if (MUTATING.has(request.method)) {
    try {
      assertSameOrigin(request);
    } catch (error) {
      return envelope(403, "CSRF", error instanceof Error ? error.message : "origin refused");
    }
  }
  let session;
  try {
    session = unsealSession(cookies().get(SESSION_COOKIE)?.value);
  } catch {
    return envelope(401, "UNAUTHENTICATED", "authentication required");
  }
  if (MUTATING.has(request.method)) {
    const header = request.headers.get("x-csrf-token");
    const cookie = cookies().get(CSRF_COOKIE)?.value;
    if (!csrfMatches(session.csrf, header) || (cookie && !csrfMatches(session.csrf, cookie))) {
      return envelope(403, "CSRF", "csrf token mismatch");
    }
  }
  const incoming = new URL(request.url);
  const target = new URL(`${masApiUrl()}${path}${incoming.search}`);
  const headers = new Headers();
  headers.set("authorization", `Bearer ${session.token}`);
  headers.set("accept", request.headers.get("accept") ?? "application/json");
  const correlation = request.headers.get("x-correlation-id");
  if (correlation) {
    headers.set("x-correlation-id", correlation);
  }
  if (session.scope.tenantId) {
    headers.set("x-tenant-id", session.scope.tenantId);
  }
  if (session.scope.organizationId) {
    headers.set("x-organization-id", session.scope.organizationId);
  }
  const lastEvent = request.headers.get("last-event-id");
  if (lastEvent) {
    headers.set("Last-Event-Id", lastEvent);
  }
  const contentType = request.headers.get("content-type");
  if (contentType) {
    headers.set("content-type", contentType);
  }
  const body =
    request.method === "GET" || request.method === "HEAD" ? undefined : await request.arrayBuffer();
  const upstream = await fetch(target, {
    method: request.method,
    headers,
    body: body && body.byteLength > 0 ? body : undefined,
    redirect: "manual",
  });
  const responseHeaders = new Headers();
  const pass = ["content-type", "cache-control", "x-correlation-id", "x-request-id"];
  for (const name of pass) {
    const value = upstream.headers.get(name);
    if (value) {
      responseHeaders.set(name, value);
    }
  }
  if ((upstream.headers.get("content-type") ?? "").includes("text/event-stream")) {
    responseHeaders.set("cache-control", "no-cache");
    return new Response(upstream.body, {
      status: upstream.status,
      headers: responseHeaders,
    });
  }
  return new Response(upstream.body, { status: upstream.status, headers: responseHeaders });
}

type Ctx = { params: { path: string[] } };

export const GET = (request: Request, context: Ctx) => proxy(request, context.params.path);
export const POST = (request: Request, context: Ctx) => proxy(request, context.params.path);
export const PUT = (request: Request, context: Ctx) => proxy(request, context.params.path);
export const PATCH = (request: Request, context: Ctx) => proxy(request, context.params.path);
export const DELETE = (request: Request, context: Ctx) => proxy(request, context.params.path);
