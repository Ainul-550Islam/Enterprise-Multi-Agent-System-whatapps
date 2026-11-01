# Key rotation runbook

## JWKS rotation

OIDC resource-server verification uses `crates/security/src/jwks.rs` with a
bounded JWKS cache (`jwks_cache_ttl_seconds`, default 300s). Rotate by
publishing the new key to the IdP JWKS endpoint; the cache TTL bounds staleness.
Production OIDC URLs must be HTTPS (`validate_production_oidc_url`).

## API key rotation

`POST /v1/api-keys/{id}/rotate` (handler `crates/api/src/handlers/api_keys.rs`
`rotate`) rotates with an optional `grace_seconds` (capped at 86400 by the
security service). Old key remains valid during grace; audit-logged.

## Internal-service secret rotation

Per `docs/internal-service-authentication-v2.md`: rotate the shared signing
secret (`MAS_AI_BRIDGE_INTERNAL_SIGNING_SECRET` / Python
`security.internal_service_secret_ref`) on both sides atomically; bridge calls
fail closed during the window.

## OAuth secret locator rotation

OAuth material is stored as secret references only (`crates/security/src/
secret_gate.rs`); rotate the referenced secret in the secret manager, not the
locator.

## Production guardrail

`enforce_production_guardrails` refuses non-empty `server.dev_tokens` in
production (`crates/api/src/main.rs`) — rotation must use real secret
references, never static dev tokens.
