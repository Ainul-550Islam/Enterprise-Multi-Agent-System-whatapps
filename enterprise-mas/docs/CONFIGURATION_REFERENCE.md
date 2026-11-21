# Configuration reference

Only names that exist in this tree. A variable that is not listed here is
not read.

Loading order for the API matches `mas-api`: environment variable, then
config key, then empty/default. Loading order for the orchestrator:
built-in defaults, `config/{MAS_ENV}.toml`, then `MAS_ORCH_*` overlays
(`MAS_ORCH_RUST_CORE__ENDPOINT` nests on `__`).

## `mas-api` environment

| Variable | Config key | Role |
|---|---|---|
| `MAS_DEV_TOKENS` | `server.dev_tokens` | Dev-only token table `token:subject:kind,...`. Empty in production (guardrail). |
| `MAS_DATABASE_URL` | `database.url` | Primary Postgres DSN. |
| `MAS_DATABASE_AUDIT_URL` | `database.audit_url` | Audit-store DSN. |
| `MAS_DATABASE_DIRECTORY_URL` | `database.directory_url` | Directory-store DSN. |
| `MAS_NATS_URL` | `broker.nats_url` | JetStream URL. |
| `MAS_AUDIT_CURSOR_KEY` | `audit.cursor_key` | HMAC key for audit cursors. |
| `MAS_AI_BRIDGE_ENDPOINT` | `ai_bridge.endpoint` | Orchestrator gRPC target. |
| `MAS_AI_BRIDGE_TOKEN` | `ai_bridge.token` | Bearer presented to the bridge. |
| `MAS_OIDC_ISSUER` | `oidc.issuer` | IdP issuer. |
| `MAS_OIDC_JWKS_URL` | `oidc.jwks_url` | JWKS URL. |
| `MAS_OIDC_AUDIENCE` | `oidc.audience` | Expected audience. |
| `MAS_STRIPE_SECRET_KEY` | `billing.stripe_secret_key` | Stripe secret key. Empty → billing routes 503. |
| `MAS_STRIPE_WEBHOOK_SECRET` | `billing.stripe_webhook_secret` | Stripe webhook signing secret. |
| `MAS_WHATSAPP_ACCESS_TOKEN` | `whatsapp.access_token` | Graph API token. |
| `MAS_WHATSAPP_APP_SECRET` | `whatsapp.app_secret` | Inbound `X-Hub-Signature-256`. |
| `MAS_WHATSAPP_VERIFY_TOKEN` | `whatsapp.verify_token` | Webhook GET handshake. |

Client / demo scripts (not `mas-api`):

| Variable | Used by |
|---|---|
| `MAS_API_BASE_URL` / `MAS_API_URL` | e2e scripts and examples (default `http://localhost:8080`) |
| `MAS_API_TOKEN` | examples (Bearer) |
| `MAS_TENANT_ID` | `x-tenant-id` |
| `MAS_ORGANIZATION_ID` | `x-organization-id` |
| `MAS_SANDBOX_HMAC_KEY` | `sandbox-runner` HMAC for `POST /execute` |

## Orchestrator (`MAS_ORCH_` prefix)

Prefix `MAS_ORCH_`. Nested fields use `__`. Credentials are **secret
references**, never inline keys. A raw key in configuration is rejected.

Notable fields (see `Settings` in `config/settings.py`):

| Overlay | Default | Notes |
|---|---|---|
| `MAS_ORCH_ENVIRONMENT` | development | Production refuses insecure bridge/plaintext. |
| `rust_core.endpoint` | `127.0.0.1:50051` | |
| `rust_core.transport` | `tls` | `insecure` refused in production. |
| `rust_core.request_timeout_seconds` | 10 | Cap 300. |
| `service.bind_host` | `127.0.0.1` | Loopback by default. |
| `service.bind_port` | 9100 | |
| `service.allow_remote_bridge_clients` | false | |
| `providers.mode` | `disabled` | Empty `enabled_providers` means no provider may be used. |
| `tools.execution_mode` | `disabled` | |
| `tools.allow_http_tools` | false | Empty host allowlist still refuses every host. |
| `memory.backend` | `disabled` | |
| `security.credential_backend` | `env` | Resolves refs from `MAS_ORCH_SECRET_*`. |
| `security.trust_caller_tenant_header` | false | Must stay false. |
| `security.service_token_ref` | none | e.g. `env:MAS_ORCH_BRIDGE_TOKEN`. |
| `security.internal_service_secret_ref` | none | e.g. `env:MAS_ORCH_INTERNAL_SECRET`. |

Provider API keys are resolved from secret references
(`MAS_ORCH_SECRET_…`). There is no `GEMINI_API_KEY` / `OPENAI_API_KEY`
read as a raw process env by Settings.

## Sandbox runner

| Variable / flag | Default | Role |
|---|---|---|
| `MAS_SANDBOX_HMAC_KEY` | unset | Required for `--serve`. Missing → HTTP 503. |
| `--bind` | `127.0.0.1` | Container image binds `0.0.0.0`. |
| `--port` | 8090 | |
| `--memory-bytes` | 67108864 | RLIMIT_AS |
| `--cpu-seconds` | 2 | RLIMIT_CPU |
| `--output-bytes` | 65536 | RLIMIT_FSIZE |
| `--nproc` | 32 | RLIMIT_NPROC |
