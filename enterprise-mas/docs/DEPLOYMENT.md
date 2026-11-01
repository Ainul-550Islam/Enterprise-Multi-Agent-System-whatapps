# Deployment

Two supported lanes: Compose (local/dev) and Helm (Kubernetes production).

## Compose

`deploy/compose/docker-compose.yml` brings up postgres(pgvector) + nats +
migrate (one-shot) + api + worker + scheduler + orchestrator. Real healthchecks,
`depends_on: condition: service_healthy`, internal network, only api port
published, named volume `pgdata`, secrets via `.env.example` placepoints.

Required env vars (grep-verified against config loaders) are listed in
`deploy/compose/.env.example`. Production values come from secret references,
never committed.

Fail-closed settings from `config/production.toml` (production checklist):
* `database.url` must be set (in-memory fallback forbidden)
* `server.dev_tokens` must be empty (static tokens refused)
* `scheduler.api_bearer` must not be set in files (env only)
* metrics listener not world-open
* OIDC issuer/JWKS must be HTTPS with host, no credentials/fragment
  (`validate_production_oidc_url`)

## Helm

`deploy/helm/enterprise-mas/`: Chart + values + templates.

Hardening (every workload): `runAsNonRoot`, `readOnlyRootFilesystem`, drop ALL
caps, `seccomp RuntimeDefault`, probes, resources. Default-deny NetworkPolicy;
allow api→orchestrator gRPC, worker→orchestrator gRPC, services→postgres/nats,
ingress→api; orchestrator egress limited. HPA for api/worker disabled by
default (`hpa.enabled`). Ingress off by default, optional TLS.

Migrate Job runs as pre-install/pre-upgrade hook; exits non-zero on failure;
idempotent (`mas_migrate up` + `provision-plane-roles.sh`).

Secret references only: `existingSecret` names for postgres/nats credentials;
provider mode default `disabled`.

## DB role provisioning

`scripts/provision-plane-roles.sh` creates the least-privilege roles
(`mas`, `mas_app`, `mas_oauth`, `mas_oauth_refresh`, `mas_rag`, …). Each
migration is applied once under an advisory lock; the runner refuses drift
(checksum gap, unknown version) rather than self-healing.

## Scaling notes

Grounded in config keys only:
* Worker concurrency: `worker.max_in_flight` (default 16 dev / 64 prod)
* Scheduler leasing: `scheduler.tick_interval_ms`, `recover_on_start`,
  `error_backoff_cap_ms` — one active scheduler (replicas respect lease
  semantics in `crates/scheduling`)
* NATS consumer scaling: pull consumers with `AckWait` = `ack_wait_ms`
* Postgres pool: `database.max_connections` (10 dev / 32 prod)
