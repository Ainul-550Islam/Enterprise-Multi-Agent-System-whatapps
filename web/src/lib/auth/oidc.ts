/**
 * Authorization-code + PKCE against an operator-configured OIDC issuer.
 * Discovery is fetched server-side; tokens never reach the browser.
 */
import { createHash, randomBytes } from "node:crypto";

export interface OidcConfig {
  issuer: string;
  clientId: string;
  clientSecret: string;
  redirectUri: string;
}

export interface OidcDiscovery {
  authorization_endpoint: string;
  token_endpoint: string;
  userinfo_endpoint?: string;
}

export interface PkcePair {
  verifier: string;
  challenge: string;
}

export interface OidcStartState {
  state: string;
  nonce: string;
  verifier: string;
  redirect: string;
}

export class OidcError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OidcError";
  }
}

export function readOidcConfig(origin: string): OidcConfig {
  const issuer = process.env.OIDC_ISSUER?.trim();
  const clientId = process.env.OIDC_CLIENT_ID?.trim();
  const clientSecret = process.env.OIDC_CLIENT_SECRET?.trim();
  if (!issuer || !clientId || !clientSecret) {
    throw new OidcError("OIDC is not configured (OIDC_ISSUER, OIDC_CLIENT_ID, OIDC_CLIENT_SECRET)");
  }
  return {
    issuer: issuer.replace(/\/+$/, ""),
    clientId,
    clientSecret,
    redirectUri: `${origin.replace(/\/+$/, "")}/api/auth/callback`,
  };
}

export function oidcConfigured(): boolean {
  return Boolean(
    process.env.OIDC_ISSUER?.trim() &&
      process.env.OIDC_CLIENT_ID?.trim() &&
      process.env.OIDC_CLIENT_SECRET?.trim(),
  );
}

export function generatePkce(): PkcePair {
  const verifier = randomBytes(32).toString("base64url");
  const challenge = createHash("sha256").update(verifier).digest("base64url");
  return { verifier, challenge };
}

export async function discover(issuer: string, fetchImpl: typeof fetch = fetch): Promise<OidcDiscovery> {
  const url = `${issuer.replace(/\/+$/, "")}/.well-known/openid-configuration`;
  const response = await fetchImpl(url, { headers: { accept: "application/json" } });
  if (!response.ok) {
    throw new OidcError(`OIDC discovery failed: HTTP ${response.status}`);
  }
  const document = (await response.json()) as Partial<OidcDiscovery>;
  if (!document.authorization_endpoint || !document.token_endpoint) {
    throw new OidcError("OIDC discovery document is missing endpoints");
  }
  return document as OidcDiscovery;
}

export function authorizationUrl(
  discovery: OidcDiscovery,
  config: OidcConfig,
  start: Pick<OidcStartState, "state" | "nonce"> & { challenge: string },
): string {
  const url = new URL(discovery.authorization_endpoint);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", config.clientId);
  url.searchParams.set("redirect_uri", config.redirectUri);
  url.searchParams.set("scope", "openid profile email");
  url.searchParams.set("state", start.state);
  url.searchParams.set("nonce", start.nonce);
  url.searchParams.set("code_challenge", start.challenge);
  url.searchParams.set("code_challenge_method", "S256");
  return url.toString();
}

export async function exchangeCode(options: {
  discovery: OidcDiscovery;
  config: OidcConfig;
  code: string;
  verifier: string;
  fetchImpl?: typeof fetch;
}): Promise<{ accessToken: string; idToken?: string; subject: string }> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const body = new URLSearchParams({
    grant_type: "authorization_code",
    code: options.code,
    redirect_uri: options.config.redirectUri,
    client_id: options.config.clientId,
    client_secret: options.config.clientSecret,
    code_verifier: options.verifier,
  });
  const response = await fetchImpl(options.discovery.token_endpoint, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded", accept: "application/json" },
    body,
  });
  if (!response.ok) {
    throw new OidcError(`OIDC token exchange failed: HTTP ${response.status}`);
  }
  const token = (await response.json()) as {
    access_token?: string;
    id_token?: string;
  };
  if (!token.access_token) {
    throw new OidcError("OIDC token response omitted access_token");
  }
  const subject = subjectFromIdToken(token.id_token) ?? "oidc";
  return { accessToken: token.access_token, idToken: token.id_token, subject };
}

function subjectFromIdToken(idToken: string | undefined): string | undefined {
  if (!idToken) {
    return undefined;
  }
  const parts = idToken.split(".");
  if (parts.length < 2) {
    return undefined;
  }
  try {
    const payload = JSON.parse(Buffer.from(parts[1] ?? "", "base64url").toString("utf8")) as {
      sub?: string;
    };
    return payload.sub;
  } catch {
    return undefined;
  }
}
