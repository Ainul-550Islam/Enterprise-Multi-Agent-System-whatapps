/** Same-origin check for mutating BFF routes. */
export function assertSameOrigin(request: Request): void {
  const origin = request.headers.get("origin");
  if (!origin) {
    return;
  }
  const host = request.headers.get("x-forwarded-host") ?? request.headers.get("host");
  if (!host) {
    throw new Error("missing host");
  }
  let parsed: URL;
  try {
    parsed = new URL(origin);
  } catch {
    throw new Error("invalid origin");
  }
  if (parsed.host !== host) {
    throw new Error("cross-origin request refused");
  }
}

export function requestOrigin(request: Request): string {
  const proto =
    request.headers.get("x-forwarded-proto")?.split(",")[0]?.trim() ??
    new URL(request.url).protocol.replace(":", "");
  const host = request.headers.get("x-forwarded-host") ?? request.headers.get("host");
  if (!host) {
    return process.env.MAS_WEB_ORIGIN ?? new URL(request.url).origin;
  }
  return `${proto}://${host}`;
}
