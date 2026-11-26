# Enterprise MAS — Deliverable Index

This directory contains the Rust control plane, Python AI orchestrator, and product documentation. The dashboard, SDKs, deployment assets, examples, sandbox runner, and canonical scripts live at the repository root.

## Components

- `rust-core/` — Rust workspace, PostgreSQL migrations, API, worker, and scheduler. The migration runner has 64 registered versions (`0001`–`0064`).
- `python-orchestrator/` — Python agent runtime and gRPC bridge.
- `docs/` — architecture, configuration, deployment, upgrade, security, Stripe, WhatsApp, buyer handover, feature matrix, limitations, and verification record.

## Repository-root components

- `../web/` — Next.js operations dashboard.
- `../sdk/` — Python and TypeScript SDKs.
- `../deploy/` — Docker, Compose, Helm, and observability assets.
- `../sandbox-runner/` — isolated code-execution service.
- `../scripts/` — verification, E2E, mock-provider, SBOM, and maintenance scripts.
- `../examples/` — runnable examples.

## Verification

Run commands from the repository root unless stated otherwise:

```bash
python scripts/dev/verify_manifest.py --tier full

(cd enterprise-mas/rust-core && cargo fmt --all -- --check)
(cd enterprise-mas/rust-core && cargo test --workspace --locked)
(cd enterprise-mas/python-orchestrator && bash scripts/check.sh)
(cd web && npm ci && npm run lint && npm run typecheck && npm run test:coverage && npm run build)
```

Live PostgreSQL, NATS, Docker, and bridge end-to-end checks require their services and test credentials. See `docs/KNOWN_LIMITATIONS.md` and the generated `docs/VERIFICATION.md` for the latest observed results; unrun gates must not be treated as passed.
