# MAS Python AI Orchestrator

The Python service owns AI execution only: agent runtime, prompt assembly, model-provider adapters, tools, memory, guardrails, and the RAG plane. Rust remains authoritative for user and service identity, tenant scope, authorization, policy, quota, execution/task state, audit, and platform-owned persistence. Python must not read or write Rust-owned tables.

## Runtime boundary

The inbound service is the versioned `mas.api.v1.AgentBridgeApi` gRPC contract, generated from `rust-core/crates/api/proto/mas/api/v1/mas.proto` with `scripts/generate_proto.sh`. Rust calls `RunAgent`; the orchestrator authenticates the bearer service token, verifies the body-bound internal signature when configured, reconciles body identity with metadata, enforces deadlines/cancellation, and returns typed success/failure plus usage. Data Hub operations use the same authenticated bridge boundary and a separate RAG-plane database role; source/document metadata remains Rust-owned.

The separate internal HTTP API is for health and local service diagnostics. It does not replace Rust authorization and is not a public product API.

## Configuration and secrets

Settings are immutable, typed, and loaded in this order: built-in defaults, `config/default.toml`, `config/{MAS_ENV}.toml`, `MAS_ORCH_*` environment variables, then explicit overrides. Run `mas-orchestrator --help` for process options. Cross-field startup checks fail closed for unsafe production combinations.

Credential settings contain secret-reference names only. Configure the resolver using the deployment's mounted secret source; do not put provider keys, gRPC credentials, or database URLs into TOML, command-line arguments, container image layers, or logs. The RAG database URL must authenticate directly as a dedicated non-superuser, non-`BYPASSRLS` login that can `SET ROLE mas_rag` (for example, a login granted membership in the `mas_rag` NOLOGIN role), with no reachable superuser or `BYPASSRLS` role. Startup rejects pre-selected roles and verifies that server-side identity; every RAG transaction uses `SET LOCAL ROLE mas_rag`. It is not the Rust runtime URL.

Data Hub defaults to the upload source only. `object_storage` and `web` sources require explicit DNS-host allow-lists. Requests still pass through the SSRF guard, which rejects private, loopback, link-local, and metadata addresses, revalidates redirects, and enforces response-size and timeout limits. Tenant scope is accepted only from signed Rust bridge identity, never from an upload field or caller-supplied HTTP header.

## Provider modes

`providers.mode` is one of `live`, `record`, `replay`, or `disabled`. Production rejects record/replay and test-only providers. A live deployment must configure a non-empty model allow-list, enabled provider(s), secret references, and real provider credentials. Disabled mode fails model calls with a typed error; it never fabricates a response.

## Optional dependency groups

- `pip install -e '.[pgvector]'` installs Psycopg 3 with its pool and pgvector adaptation for the separate RAG plane.
- `pip install -e '.[ingest]'` installs the PDF, DOCX, and HTML parsing extras.
- `pip install -e '.[otel]'` and `pip install -e '.[metrics]'` install optional exporters.
- `pip install -e '.[dev]'` is not a supported setuptools group; install development tools from the project `dev` dependency group with the repository's pinned toolchain/bootstrap helper.

Keep production dependencies minimal: do not install optional RAG/parser extras in deployments where those features are disabled.

## Development and verification

From `python-orchestrator/`:

```bash
python -m pip install -e '.[pgvector,ingest]'
python -m compileall -q src
ruff check src tests
ruff format --check src tests
mypy src/mas_orchestrator
pytest -q
```

The repository also provides `scripts/bootstrap.sh`, `scripts/check.sh`, `scripts/generate_proto.sh`, and `scripts/verify_env.sh`. `scripts/check.sh` fails on any lint, typing, test, compile, or deliverable-completeness error. Tests that need a real PostgreSQL/pgvector server are marked as integration tests and must be run in the live integration lane; a unit pass does not prove database role grants or SQL extension availability.

## Production run

The Python process exposes only the private gRPC listener and its configured health surface. Run it as a non-root service, restrict network ingress to Rust API/worker callers, limit provider egress, mount secret references read-only, and shut it down through the registered lifecycle coordinator so active bridge calls can drain. Production composition refuses an empty provider/model registry or a missing required backend instead of serving a degraded health response.

## Deployment

The orchestrator ships as `deploy/docker/orchestrator.Dockerfile` (non-root, gRPC
private port only, no provider keys baked in) and is wired into
`deploy/compose/docker-compose.yml` (internal network; only the Rust API port
is published) and `deploy/helm/enterprise-mas/templates/orchestrator-deployment.yaml`
(ClusterIP gRPC service, default-deny NetworkPolicy). Provider mode defaults to
`disabled`; enable explicitly in production.
