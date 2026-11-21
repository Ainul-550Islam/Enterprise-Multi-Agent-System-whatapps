# Enterprise MAS — Deliverable Index

Index of what lives under `enterprise-mas/`. SDK, dashboard, sandbox-runner,
examples and compose live at the **repository root**, not here.

```
enterprise-mas/
├── README.md                  ← this file
├── rust-core/                 ← the Rust workspace (source, migrations, scripts, docs)
├── python-orchestrator/       ← the Python AI orchestrator
├── ci-evidence/               ← raw gate logs (fmt, clippy, test, deny, python)
├── docs/                      ← architecture, quickstart, deployment, security model, Stripe, WhatsApp, API_REFERENCE.html, postman
├── reports/                   ← programme item reports
├── scripts/                   ← e2e-smoke, postman generator
└── specs/                     ← programme specs
```

There is no `enterprise-mas/sdk`, `enterprise-mas/web`, `enterprise-mas/deploy`,
or `enterprise-mas/docs/internal`. Those paths are:

- `sdk/python`, `sdk/typescript` (repo root)
- `web/` (repo root)
- `deploy/` (repo root)
- `docs/REVIEW_RESOLUTION.md` (repo root)

---

## The six items

| # | Item | Report | Status |
|---|---|---|---|
| 1 | Full codebase review | `reports/CODE_REVIEW.md` | Complete |
| 2 | Python AI Orchestrator (from `first.md`) | `reports/PYTHON_ORCHESTRATOR_STATUS.md` | Complete — `scripts/check.sh` this tree: 2220 passed, 6 skipped, 0 failed |
| 3 | Rust ↔ Python Production AgentBridge | `reports/BRIDGE_INTEGRATION_STATUS.md` | Complete |
| 4 | Canonical Durable Execution Pipeline | `reports/CANONICAL_DURABLE_PIPELINE_REPORT.md` | Complete |
| 5 | Enterprise Security / Authorization / Tenancy / RLS Hardening | `reports/SECURITY_HARDENING_REPORT.md`, `reports/SECURITY_NARRATIVE_BN.md` | Complete |
| 6 | Enterprise Policy + Distributed Quota + Usage + Entitlement-Billing Control Plane | `reports/CONTROL_PLANE_REPORT.md` | Complete |

Item 6 is the most recent. Its report is the one to read first if you are picking the
work up: §4 covers what was built against the spec, §5 lists the 19 real defects found
by running the code, §6 is the completion gate, and §7 states plainly what is known and
not done (nothing).

---

## How to verify

Rust (from `enterprise-mas/rust-core`):

```bash
# Postgres must be running first.
bash pg_up.sh

export MAS_ENV=test CARGO_INCREMENTAL=0
export MAS_TEST_DATABASE_URL="postgres://mas:mas@127.0.0.1:55432/mas_live"
export MAS_TEST_DATABASE_APP_URL="postgres://mas_app:mas_app_pw@127.0.0.1:55432/mas_live"

cargo test --workspace --locked
cargo test --workspace --locked --features nats
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo doc --no-deps --workspace
```

The live-PostgreSQL suites skip loudly without those two URLs rather than passing
vacuously.

Python (from `enterprise-mas/python-orchestrator`):

```bash
bash scripts/bootstrap.sh
bash scripts/check.sh
```

`pytest-asyncio` ≥ 0.24 is required. Without it collection aborts with
`Unknown config option: asyncio_default_fixture_loop_scope` and reports "20 errors" —
that is a missing dependency, not a failing test.

The Rust toolchain is pinned by `rust-toolchain.toml` (1.98.1).
`[workspace.package] version` is `1.1.0`.

---

## What is deliberately not in this archive

- `target/` — build output. Excluded; `cargo test` regenerates it.
- `pg16/`, `pgdata/` — the PostgreSQL build and its data directory. Rebuild with
  `pg_up.sh`'s bootstrap, or point `MAS_TEST_DATABASE_URL` at your own instance.
- `.git/` — no history was authored by this work.
- `node_modules/`, `.venv/`, caches — none belong in the deliverable zip.

---

## Orientation for a reader

- **Start with** `reports/CONTROL_PLANE_REPORT.md` §3 (architecture) — the canonical gate is
  one: `ControlPlane` in `mas-application`, and everything else calls through it.
- **The chain** is `Plan → Subscription → Entitlement → Feature gate → Policy → Quota →
  Execution → Usage metering → Usage ledger → Billing output`. Limits are enforced
  **before** consumption and consumption is recorded **after**.
- **Nothing is synthetic.** There are no fabricated quota or usage counters; the
  worker does no quota arithmetic; the provider is never the source of truth for the
  platform ledger.
- **Every new and modified file** carries a `PURPOSE:` header stating responsibility,
  owning layer, production role, **data ownership**, and what it must **not** do.
