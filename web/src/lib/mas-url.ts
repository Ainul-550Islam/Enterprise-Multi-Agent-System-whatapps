/** Operator-configured MAS API origin. Never derived from request input. */
export function masApiUrl(): string {
  const raw = process.env.MAS_API_URL ?? process.env.MAS_API_BASE_URL ?? "http://127.0.0.1:8080";
  return raw.replace(/\/+$/, "");
}

const ALLOWED_PREFIXES = ["/v1/", "/health", "/v1"];

export function assertProxiedPath(path: string): string {
  if (!path.startsWith("/")) {
    throw new Error("path must be absolute");
  }
  if (path.includes("..") || path.includes("//") || path.includes("\\")) {
    throw new Error("path refused");
  }
  if (path.startsWith("/webhooks") || path.startsWith("/v1/public") || path.startsWith("/v1/admin")) {
    throw new Error("path is not exposed through the dashboard BFF");
  }
  if (!ALLOWED_PREFIXES.some((prefix) => path === prefix || path.startsWith(prefix))) {
    throw new Error("path is outside the MAS API");
  }
  return path;
}
