# Review worktrack — live repair log

Authority order: user prompt > review documents > repo conventions. Every entry
records what was done, where, and how it was verified. Nothing is marked done
without a gate result.

Status legend: `done` (fixed + tested + gated) / `partial` (fixed in part, the
rest named explicitly) / `open` / `NOT_RUN` (cannot run here; reason stated).

---

## P0 — restore / E2E / DB / handoff / tests (done, earlier turns)

- HEAD `3da5869` build breakage (R1–R5), Python↔Rust bridge auth (v1/v2), replay
  protection, flaky tests, `webhook.py:188`, provision-plane roles + migration
  0054 + `postgres_store.py`, hygiene/CI — all restored and green.
- Deep review triaged into `docs/REVIEW_RESOLUTION.md` (C-01…C-12, W-01…W-08).
- Deliverable: `scope-P0.zip` (sha256 `82b3de6278187b987708c13a63d43910001e4b6e049763c5f849d107a4211621`).
- Four files removed from the tree, preserved under `/home/user/_removed_files/`.

## P1-rust R-1…R-8 (done, earlier turns)

- Migrations 0055–0061 (whatsapp_channels/messages, billing_customers,
  plan_prices, billing_checkout_sessions, billing_event_applications,
  cross_tenant_guard), domain `billing.rs`/`whatsapp.rs`, persistence stores
  (incl. billing repositories — closes review M-5), application services,
  integrations provider ports + Stripe/WhatsApp providers, api handlers +
  `wire_billing_and_whatsapp()`, openapi 105 paths + parity test.
- Gates at the time: Rust 1195 passed / 0 failed / 13 ignored;
  `cargo fmt` 0, `cargo clippy -D warnings` 0.

## P1-python Y-1…Y-3 (done, earlier turns)

- `tools/backends/` — 7 adapters (Smtp/SendGrid email, Brave/Tavily/Searxng
  search, PostgresReadOnly, RemoteSandbox) with the strict error taxonomy and
  no hidden retries; `tests/unit/test_backends.py` 39 tests.
- Hollow-test sweep: 18 assert-less tests replaced with observable assertions
  across 12 files (nothing deleted or weakened; AST rescan 0 hollow).
- Y-2/Y-3(a) WhatsApp Python files (`whatsapp_media.py`,
  `whatsapp_support_agent.py`) are absent from the repo, the baseline and
  `Module1_Full_Source.zip` — **NOT_RUN (infrastructure absent)**.

## P2 — open review items + M-1…M-6 (this turn, 2026-11-15)

- **C-04 done** — worker: per-attempt stop signal; lease loss and durable
  cancellation now stop the run (`consumer.rs`); handler spawn has a
  panic-supervision boundary. Tests: `a_lost_lease_stops_the_run_…`,
  `a_durable_cancellation_stops_the_run_…`.
- **C-08 done** — `services/execution_state.py` (`RustExecutionStateReader`,
  `RustStateTransitionReporter`, `ExecutionInProgressError`, `StateReportError`),
  tenant-scoped `state_of` and durable terminal write-back, WAIT → typed error,
  SKIP → authoritative termination, composition wiring. Tests:
  `TestAuthoritativeState` (8), `TestDurableStateReporter` (8). Live-DB
  behaviour NOT_RUN (no live infra).
- **C-10 done** — `runtime_context` fallible (typed validation error, terminal
  classification) + panic containment at the handler boundary. Test:
  `build_dispatch_refuses_a_corrupt_scope_instead_of_panicking`.
- **C-11 done** — admission-before-download/retention verified in the adapters
  (remaining-budget fetch; S3 declared-size admission pre-GET); service
  post-collection sum kept as third-party backstop. Tests:
  `test_object_sync_resumes_after_the_checkpointed_prefix`,
  `test_object_sync_admits_the_aggregate_budget_before_downloading`.
- **W-04 done** — signed per-delivery id (`x-mas-delivery-id`) inside the HMAC
  material + atomic bounded `DeliveryDedup` receiver guard. Tests:
  `TestWebhookReplay` (8).
- **M-1 done** — tracked 21 MB `rustup` binary + `.cargo/env` removed
  (preserved in `/home/user/_removed_files/dot-cargo/`), git-untracked,
  `.gitignore` documents them.
- **M-2 done (verified, no change needed)** — every README §6/§13 path exists.
- **M-3 done** — README test/migration-count claims refreshed to verified
  numbers (see below).
- **M-4 done** — `rust-core/Cargo.toml` stale `[todo]` header replaced.
- **M-5 done (P1-rust)** — billing repositories implemented and green.
- **M-6 done** — `dev_verifier()` no longer registers a built-in `"dev-token"`;
  tokens come from `MAS_DEV_TOKENS` / `server.dev_tokens`
  (`scripts/dev-up.sh` declares one locally; QUICKSTART updated).

### Gates (verified this turn)

| Gate | Result |
|---|---|
| Python `bash scripts/check.sh` (ruff format+lint, mypy strict, pytest, compileall) | **2127 passed, 6 skipped** (before 2109) — ALL PASS |
| Rust `cargo test` mas-worker + mas-worker-service | 73+5+15+25 **all passed** (3 new tests) |
| Rust workspace test + clippy `-D warnings` + fmt | see the P2 final tally below |
| `cargo fmt --all -- --check` | PASS |

NOT_RUN (honest): live-database and live-infrastructure behaviour (no live
infra in this environment); E2E/manual flows are the user's.

## Open / awaiting

- **P2-web W-1…W-4** — the item texts are not recoverable from the workspace
  (`CODE_REVIEW_026-11-15.md` and every other source checked; grep/find clean).
  **Asked the user** for the four item descriptions; awaiting their paste
  before touching web-scope work (no guessing).
- Baseline zip + manifest: the user will supply them (waiting).

## P2 final tally (fill at gate completion)

- Rust workspace `cargo test --workspace --locked`: **1198 passed, 0 failed, 13 ignored** (before 1195; +3 new regression tests)
- clippy `--workspace --all-targets -D warnings`: **clean** (Finished with no diagnostics)
- Python: **2127 passed, 6 skipped** (before 2109)


---

## FINAL পর্যায় (২০২৬-১১-১৫)

- `scope-P1-rust.zip` তৈরি: ৮০টি ফাইল (P0-স্ন্যাপশটের সাথে ট্রি diff করে নির্ধারিত, P2-এর ১৯টি বাদ) — sha256 `7407c45a…a642e`।
- R-2 route smoke যাচাই: `application/src/stores.rs:336` `InMemoryServices` = "single-struct in-memory implementation of every port"; `api/src/inmemory_directory.rs`-এ টেন্যান্সি-সার্ভিসের সাথে শেয়ারড; api crate টেস্ট (৩৮) সবুজ।
- P2-web W-1…W-4: `specs/` সহ সর্বশেষ word-boundary grep (`\bW-[1-4]\b`) ফাঁকা → NOT_RUN (কারণসহ ডকড; ইউজারকে জিজ্ঞাসা করা হয়েছে)।
- Notices: `bash scripts/gen-third-party-notices.sh` চেষ্টা চিহ্নিত হলো — স্যান্ডবক্স টুলচেইন (cargo/venv) বাষ্পীভূত হওয়ায় আউটপুট অসম্পূর্ণ (৪৩৯→৩৮ লাইন); `git checkout` দিয়ে পূর্ণ-বাস্তব-metadata সংস্করণ পুনরুদ্ধার (সব গেট যে ফাইলের উপর চলেছিল তার সমতুল্য)।
- `CHANGELOG.md`-তে `[1.1.0] — 2026-11-15` release entry (Fixed/Changed/Verified/Known gaps)।
- রিপোর্ট ডক: `REPO_STATE_2026-11-15.md` (audit-ভূমিকা) + `CHANGE_SUMMARY_2026-11-15.md` (PR-বর্ণনা-ভূমিকা) — নির্দেশ অনুযায়ী ফাইলনেমে `REPO_AUDIT`/`PR_DESCRIPTION` নেই।
- `/home/user/final.py` চালানো হয়েছে: চূড়ান্ত স্ন্যাপশট (৯৪৩ ফাইল) + `_zips.sha256` + `_RESULTs.json` + সিক্রেট স্ক্যান।
- **সিক্রেট স্ক্যান: CLEAN** — ৯০৬ ফাইল; ৭৯টি পরিচিত dev/test উল্লেখ অনুমোদিত; ২টি হাতে-যাচাই টেস্ট-ফিক্সচার (guardrail/transport); ০ সন্দেহজনক।

### চূড়ান্ত checksums (`/home/user/_zips.sha256`)
| জিপ | sha256 | এন্ট্রি |
|---|---|---|
| scope-P0.zip | `82b3de6278187b98…07a4211621` | 932 |
| scope-P1-rust.zip | `7407c45a089ffd10…58b3a642e` | 80 |
| scope-P2.zip | `a5693fc0e9c758a0…64b209f5aa` | 19 |
| Enterprise-Multi-Agent-System-whatapps-FINAL.zip | `b56c12ff1fb0be2e…848d05c0a3490dc5` | 943 |


---

## ফুল টেস্ট + ফুল কোড রান (২০২৬-১১-১৫, ইউজার নির্দেশে)

**টেস্ট:** Python `bash scripts/check.sh` = সব স্টেজ সবুজ; Rust `cargo test --workspace --locked` = **1198/0/13**, clippy `-D warnings` clean, fmt clean (target/ থেকে পূর্ণ রিকম্পাইল সহ)।

**ফুল রান (dev-up.sh রেসিপি):**
- `mas-api --dev-inmemory` — HTTP :8080 + gRPC :50051 চালু; `/v1/health/ready` → ready:true; টোকেন ছাড়া → UNAUTHENTICATED envelope; `Bearer dev-token` (M-6 টেবিল) → 200; `mas status` CLI → live: ok।
- `mas-scheduler --dev-inmemory` — 1000ms tick + completion listener :8090 চালু।
- `mas-worker --dev-inmemory` — **ইচ্ছাকৃতভাবে বন্ধ**: "durable pipeline has no in-memory mode" (claim-এর জন্য সত্যি DB বাধ্যতামূলক) → লাইভ DB ছাড়া NOT_RUN।
- `python -m mas_orchestrator.main` — config/development.toml + env-ref সিক্রেট দিয়ে **পূর্ণ বুট**: provider কনস্ট্রাক্ট, prompt registration, runtime composed, **bridge listening :9100**, SIGTERM-এ clean drain (pending: 0)।

**রান-এ ধরা ২টি বাগ (ফিক্স + রিগ্রেশন টেস্ট সহ, ২১২৯/৬ সবুজ):**
1. `composition._allowlist_for` — `key in registry` (object membership) স্ট্রিং কী কখনোই পায় না → non-empty allowlist দিয়ে কোনো deploy boot করতো না (live mode আসলে allowlist বাধ্যতামূলক!) → `frozenset(registry.models())` দিয়ে কী-তুলনা; টেস্ট: `TestAllowlistMatchesRegistryKeys`।
2. `prompts/registry.py` — `extra={"name": ...}` = LogRecord-রিজার্ভড কী → INFO লেভেলে `makeRecord` KeyError (টেস্টে লেভেল WARNING থাকায় আগে কখনো ধরা পড়েনি) → `prompt_name`; টেস্ট: `test_registration_log_survives_info_level`।

**সহায়ক পরিবর্তন:** `python-orchestrator/config/development.toml` (নতুন dev overlay — tuple কনফিগ ছাড়া TOML-তে সম্ভব নয়); `test_main_entrypoint.py`-র একটি টেস্ট এখন অবৈধ কম্বিনেশন স্পষ্ট করে (ambient overlay-এর উপর নির্ভরশীল ছিল)।

**পুনঃজিপ:** scope-P2.zip রি-ওপেন (নতুন/পরিবর্তিত ৭ ফাইল যোগ) — নতুন checksum `_zips.sha256`-এ।
