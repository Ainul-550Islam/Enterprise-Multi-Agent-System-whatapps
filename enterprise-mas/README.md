# Enterprise MAS — Deliverable Index

Everything produced across the six programme items, in one archive.

```
enterprise-mas/
├── README.md                  ← this file
├── reports/                   ← one report per item, plus the review and the patch
├── specs/                     ← the specifications the work was built against
├── rust-core/                 ← the Rust workspace (source, migrations, scripts, docs)
└── python-orchestrator/       ← the Python AI orchestrator (item 2)
```

---

## The six items

| # | Item | Report | Status |
|---|---|---|---|
| 1 | Full codebase review | `CODE_REVIEW.md` | Complete |
| 2 | Python AI Orchestrator (from `first.md`) | `PYTHON_ORCHESTRATOR_STATUS.md` | Complete — 1,936 tests, 0 failed |
| 3 | Rust ↔ Python Production AgentBridge | `BRIDGE_INTEGRATION_STATUS.md`, `RUST_PYTHON_BRIDGE_PHASE16_FILES.md` | Complete — 36/36, E2E 12 |
| 4 | Canonical Durable Execution Pipeline | `CANONICAL_DURABLE_PIPELINE_REPORT.md` | Complete |
| 5 | Enterprise Security / Authorization / Tenancy / RLS Hardening | `SECURITY_HARDENING_REPORT.md` (1.2 MB), `SECURITY_NARRATIVE_BN.md` (Bangla) | Complete — 66 files, 1,200,577 bytes |
| 6 | Enterprise Policy + Distributed Quota + Usage + Entitlement-Billing Control Plane | `CONTROL_PLANE_REPORT.md` | Complete — 956 tests, 0 failed |

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

cargo test --workspace --locked                  # 956 passed, 0 failed
cargo test --workspace --locked --features nats  # 956 passed, 0 failed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo doc --no-deps --workspace
```

The live-PostgreSQL suites skip loudly without those two URLs rather than passing
vacuously. Under them the totals are as above; without them the total is lower.

Python (from `enterprise-mas/python-orchestrator`):

```bash
pip install pytest-asyncio grpcio grpcio-tools
PYTHONPATH="$(pwd)/src:$(pwd)" python3 -m pytest tests -q   # 1,936 passed
```

`pytest-asyncio` ≥ 0.24 is required. Without it collection aborts with
`Unknown config option: asyncio_default_fixture_loop_scope` and reports "20 errors" —
that is a missing dependency, not a failing test.

The Rust toolchain is pinned by `rust-toolchain.toml` (1.98.1).

---

## What is deliberately not in this archive

- `target/` — build output (19 GB). Excluded; `cargo test` regenerates it.
- `pg16/`, `pgdata/` — the PostgreSQL build and its data directory. Rebuild with
  `pg_up.sh`'s bootstrap, or point `MAS_TEST_DATABASE_URL` at your own instance.
- `.git/` — no history was authored by this work.
- `node_modules/`, `.venv/`, caches — none exist.

---

## Orientation for a reader

- **Start with** `CONTROL_PLANE_REPORT.md` §3 (architecture) — the canonical gate is
  one: `ControlPlane` in `mas-application`, and everything else calls through it.
- **The chain** is `Plan → Subscription → Entitlement → Feature gate → Policy → Quota →
  Execution → Usage metering → Usage ledger → Billing output`. Limits are enforced
  **before** consumption and consumption is recorded **after**.
- **Nothing is synthetic.** There are no fabricated quota or usage counters; the
  worker does no quota arithmetic; the provider is never the source of truth for the
  platform ledger.
- **Every new and modified file** carries a `PURPOSE:` header stating responsibility,
  owning layer, production role, **data ownership**, and what it must **not** do.
