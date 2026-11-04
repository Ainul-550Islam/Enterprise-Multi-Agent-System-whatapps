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

Python's guarded HTTP transport blocks non-global, reserved, site-local and
known IPv4-in-IPv6 translation/tunnel ranges before dialing. The Rust guarded
egress path also blocks known translation/tunnel prefixes, including for
operator-declared internal endpoints. These checks cannot detect an
operator-defined NAT64 prefix or other network-specific routing that maps a
public-looking destination to a private service. Egress infrastructure must
independently deny routes to private networks and metadata services, including
through configured translators, tunnels and proxies; verify those controls in
the deployment environment. A hostname allowlist is not a substitute.

## Billing receipt invariant migration

Migration 0055 preserves historical unverified billing receipts but prevents new
unverified rows and blocks changes to receipt identity and finalized verdicts.
Stop older writers before applying it: versions that record unverified event IDs
will start failing. The `NOT VALID` constraint intentionally leaves existing
unverified evidence in place; investigate those event IDs before accepting a
matching provider event, and do not delete evidence merely to validate it.
This migration does not implement a Stripe webhook verifier or worker.

The migration runner has no down command. The included `down.sql` restores the
previous, weaker trigger and removes the new insert constraint; use it only
under an operator-managed rollback procedure with writers stopped and ledger
reconciliation, and recognize that it reopens the old integrity gaps.

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

### Browser cross-origin access

The API defaults to no cross-origin browser access. To enable it, set
`MAS_CORS_ALLOWED_ORIGINS` to a comma-separated list of exact, canonical HTTPS
origins, for example `https://console.example.com,https://admin.example.com`.
Alternatively configure `server.cors_allowed_origins` as a nonempty TOML string
array. Omit the key to disable CORS. At most 32 entries are accepted. Wildcards,
`null`, credentials, paths, query strings, fragments, and noncanonical spellings
(such as an explicit default port) fail startup. A trailing slash is permitted
and removed. Plain HTTP loopback origins are accepted only in the explicit
`--dev-inmemory` lane. Trust every origin on this list: scripts running there
can read API responses when supplied with a valid bearer token.

CORS runs outside authentication to allow unauthenticated preflight; actual API
requests still pass through the normal authentication and scope checks. It
allows the documented scope, correlation and idempotency headers and exposes
`x-correlation-id` and `x-idempotent-replay`. Cookie credentials and private
network access are not enabled. CORS is a browser response-sharing policy,
not authorization or CSRF protection: requests without preflight can still
reach handlers, and non-browser clients can forge Origin. Deployments using
ambient credentials must implement their own CSRF defenses. Reverse proxies
must preserve `Origin` and `Vary` and must not inject permissive CORS headers.

## OIDC identity binding rollout

Durable API deployments require `MAS_OIDC_AUDIENCE` (or `oidc.audience`) in
addition to the pinned HTTPS issuer and JWKS URL. Access tokens for other
audiences are rejected. External OIDC scopes cannot grant `platform:admin`,
`platform:*`, or `*`; platform administration requires a separately trusted
local mechanism.

Migration 0056 makes `(users.identity_provider, users.external_subject)` unique
for linked identities and grants the `mas_directory` role SELECT on only the
user ID, status and those two identity columns. It does not grant email/profile
access or writes. The migration fails if existing links duplicate an
issuer/subject pair; resolve duplicates before upgrading. Provision links using
a trusted, audited operator process: `identity_provider` must equal the exact
configured OIDC issuer string and `external_subject` must equal the verified
IdP subject. Activate the local user and create an appropriate active local
membership. There is no self-service linking, auto-provisioning, or email-based
matching; disabled, locked, pending, unlinked, ambiguous or wrongly issued
identities are rejected. Authenticated principals use the local user ID, not
an IdP subject that happens to look like a UUID.

Deploy the migration before the new API, and ensure the configured directory
login actually has BYPASSRLS and the migration's column grants. The identity
lookup is performed on every OIDC authentication, but disabling a user or
revoking a membership cannot cancel a request already authorized. Before
rolling back 0056, stop all new API processes; revoking its grant causes them
to reject OIDC logins. Rolling back restores the old unsafe UUID-subject
behavior if an older API is deployed, so only do so with OIDC ingress disabled
and an explicit security plan. Database links and existing membership rows are
not deleted by the down migration.
