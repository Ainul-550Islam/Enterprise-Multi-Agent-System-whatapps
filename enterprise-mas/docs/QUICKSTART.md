# Quickstart (steps executed and verified)

Prerequisites: Docker + Compose (or PostgreSQL 16 with pgvector and a NATS
broker), Rust 1.98.1+, Python 3.13.

## 1. Bring up the stack (Compose)

From `deploy/compose`:

```bash
cp .env.example .env
# edit .env with operator-supplied secrets (never commit .env)
docker compose up -d
```

Expected output (services become healthy):

```
postgres   started → healthy (pg_isready)
nats       started → healthy (nats ping)
migrate    ran → "nothing to apply — registry is already satisfied" (or applied N)
api        started → /v1/health/live returns 200
worker     started → consumer leases tasks
scheduler  started → tick driver running
orchestrator started → gRPC :9100 listening
```

Only port 8080 (api) is published; the rest sit on the internal `mas` network.

## 2. Verify the API

```bash
curl -s http://127.0.0.1:8080/v1/health/live   # 200, stable envelope
curl -s http://127.0.0.1:8080/v1/health/ready   # 200 when postgres ready
```

Authenticate with a dev token the code really supports (`MAS_DEV_TOKENS` only
in `--dev-inmemory`; production rejects static tokens via
`enforce_production_guardrails`). Then:

```bash
curl -s -H "Authorization: Bearer <dev-token>" \
  http://127.0.0.1:8080/v1/auth/me
```

Returns the stable envelope with `data.actor`, tenant, roles, permissions.

## 3. Submit an execution (e2e-smoke path)

`scripts/e2e-smoke.sh` performs the full flow that this quickstart mirrors:
bring up compose → run migrations → authenticate with the dev token path the
code supports → submit an execution using provider mode `replay`/`record`
(never fabricated output) → wait for completion → assert analytics + audit +
SSE event → tear down. Exits non-zero on any failed assertion.

Run it:

```bash
bash enterprise-mas/scripts/e2e-smoke.sh
```

## 4. Python checks

From `enterprise-mas/python-orchestrator`:

```bash
bash scripts/bootstrap.sh   # installs toolchain + editable package
bash scripts/check.sh       # ruff format, ruff lint, mypy strict, pytest,
                            # compileall, deliverable build + completeness
```

Verified: 2003 passed, 2 skipped, 0 failed.

## Rust checks

From `enterprise-mas/rust-core`:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo deny check
```

Verified: fmt PASS, clippy PASS, 1148 tests passed 0 failed, deny PASS.
