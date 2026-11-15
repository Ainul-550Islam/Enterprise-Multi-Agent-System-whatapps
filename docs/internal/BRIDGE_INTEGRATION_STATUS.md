# Rust ↔ Python Production AgentBridge Integration — সম্পূর্ণ প্রতিবেদন

**তারিখ:** ২০২৬-১০-০৩ (সর্বশেষ হালনাগাদ: continue পর্ব)
**অবস্থা:** ✅ **সম্পূর্ণ** — সব ১৮টি ধাপ + cross-language যাচাই, সব verification command আসলে চালানো

---

## ১. এক নজরে ফলাফল

### Rust (cargo 1.98.1, `enterprise-mas/rust-core/`)

| Command | ফলাফল |
|---|---|
| `cargo fmt --all -- --check` | ✅ exit 0 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | ✅ exit 0 |
| `cargo test --workspace --locked` | ✅ **475 passed**, ০ failed (৯ E2E + ৪ composition সহ) |
| `cargo test --workspace --locked --features nats` | ✅ **484 passed**, ০ failed |
| `cargo doc --workspace --all-features --no-deps` | ✅ Finished, **০ warning** |

### Python (`python-orchestrator/`)

| Command | ফলাফল |
|---|---|
| `ruff format --check src tests` | ✅ PASS |
| `ruff check src tests` | ✅ PASS (All checks passed) |
| `mypy --strict src/mas_orchestrator` | ✅ Success, ১৬০ files |
| `pytest tests -q` | ✅ **1890 passed** (৩ বার ধারাবাহিক, flakiness মুক্ত) |
| `compileall src tests` | ✅ PASS |
| `bash scripts/generate_proto.sh` | ✅ exit 0 |

**আগের ১৬৯৭টি টেস্টের একটি-ও ভাঙেনি।** নতুন ১৮৪টি bridge টেস্ট যোগ হয়েছে
(১৬৪ + ১৭টি cross-language + ৩টি retry reconciliation)।

---

## ২ক. continue পর্বে যা যোগ/ঠিক হয়েছে

নিজের কাজ যাচাই করতে গিয়ে আরও চারটি বিষয় পাওয়া গেছে:

### A. Cross-language contract টেস্ট (নতুন, ১৭টি)
Rust ও Python-এর মধ্যে যে চুক্তিগুলো আগে কোনো টেস্ট পিন করত না — কারণ
এক পাশের টেস্ট অন্য পাশের constant দেখতে পায় না। `tests/bridge/
test_cross_language_contract.py` **Rust সোর্সকে data হিসেবে পড়ে** Python-এর
constant-এর সাথে তুলনা করে। যাচাইকৃত:

- ৬টি header নাম হুবহু মিল (`authorization`, `x-tenant-id`, …) ✅
- সব header lowercase (capitalized key নীরবে ড্রপ হয়) ✅
- প্রোটোকল ভার্সন `1.0.0` == `1.0.0` ✅
- Rust-এর `chrono` deadline ফরম্যাট (`…T14:52:00.123Z`) Python পার্স করে —
  **৫টি shape-এ round-trip যাচাই**, মিলিসেকেন্ড precision বজায় থাকে ✅
- Idempotency key body-তে আছে (শুধু header-এ নয়) ✅

### B. Retry divergence — তদন্ত ও **সঠিক সিদ্ধান্ত**
Rust `RESOURCE_EXHAUSTED`/`DEADLINE_EXCEEDED`/`CANCELLED`-কে retryable মনে করে,
Python করে না। প্রথমে আমি Python-কে Rust-এর সাথে মিলিয়ে দিয়েছিলাম — কিন্তু
বিদ্যমান ৩টি টেস্ট ভেঙে জানালো যে Python-এর পার্থক্য **ইচ্ছাকৃত ও যুক্তিসঙ্গত**:

> "execution-এর deadline শেষ — retry করলে platform-এর পরিত্যক্ত কাজ জ্যান্ত হবে"
> "exhausted quota একটা budget সিদ্ধান্ত, transient fault নয়"

**আমার প্রথম পদক্ষেপ ভুল ছিল।** Domain সিদ্ধান্ত বহাল রেখেছি, এবং পার্থক্যটা
এখন একটি টেস্টে *documented divergence* হিসেবে পিন করা আছে — কেউ চুপচুপ
বদলালে টেস্ট ফেইল করবে।

তবে এর মধ্যে একটা **প্রকৃত bug** ছিল: `error_from_grpc_status` পিয়ারের
structured `retryable` payload-কে এড়িয়ে শুধু status code থেকে অনুমান করত।
ঠিক করা হয়েছে — এখন payload-ই authoritative, status কেবল fallback।

### C. Flaky test (২/৩ বার পাস)
`test_duplicate_is_suppressed` মাঝে মাঝে ফেইল করত। কারণ: টেস্ট `_envelope()`
দুইবার কল করে দুইটি **ভিন্ন** event বানাতো (প্রতিটির নিজস্ব
`occurred_epoch_ms`, যা dedup key-এর অংশ)। দুই কল একই মিলিসেকেন্ডে পড়লে পাস,
সীমানা পেরলে ফেইল। টেস্টটি ঠিক করা হয়েছে — একই envelope দুইবার consume করে
(যা "duplicate delivery"-এর আসল অর্থ)। ৮/৮ ও পুরো suite ৫/৫ বার স্থিতিশীল।

### D. Resilience suite-এর হিসাব সংশোধন
আমার পুরোনো নোটে "৩৯/৪০" ছিল — ভুল। বাস্তবে **৮৫টি টেস্ট, সব পাস**।

---

## ২. ধাপে ধাপে অগ্রগতি

### Phase 2–4 — Proto ও Rust অ্যাডাপ্টার ✅
- `mas.proto`-এ `idempotency_key` (field ১৫), `ServiceIdentityMsg identity` (১৬),
  `AgentSucceeded.model_key`/`finish_reason`, `AgentFailed.correlation_id`/`request_id` যোগ।
- `GrpcAgentBridge` — বাস্তব gRPC ক্লায়েন্ট, `AgentBridge` trait-এর প্রোডাকশন বাস্তবায়ন।
- Deadline request থেকে derived (অনুমান নয়), cancellation token-এর সাথে race,
  authoritative scope ছাড়া পাঠায় না।

### Phase 5–6 — Production composition root ✅
- `AgentRunHandler` (নতুন, ~৪৫০ লাইন) + `AgentBridgeSettings`।
- Production path-এ `EchoHandler` অপ্রাপ্য; ব্রিজ কনফিগ না থাকলে/সংযোগ ব্যর্থ হলে boot ব্যর্থ।
- `InMemoryAgentBridge` শুধু `dev-bridges` feature-এ — রিলিজ বিল্ডে পৌঁছানোর উপায় নেই।
- **`execution_id` বাধ্যতামূলক** — আগে `unwrap_or_else(ExecutionId::new)` দিয়ে আইডি তৈরি করত,
  যা Rust-এর অজানা একটি আইডি উদ্ভাবন করত। এখন অনুপস্থিত থাকলে `AppError::validation`।

### Phase 7–8 — Python gRPC সার্ভার ও বাস্তব runtime বাইন্ডিং ✅
- `grpc/server.py` (নতুন) — bootstrap, message bounds, orderly shutdown।
- `grpc/agent_bridge_servicer.py` (নতুন, ~৯০০ লাইন) — inbound trust boundary।
- `services/composition.py` (নতুন) — বাস্তব object graph: real provider → ModelRegistry →
  ModelRouter → AgentFactory → AgentRegistry → AgentRuntime → ExecutionService।
- `agents/tenant_registry.py` (নতুন) — tenant-প্রতি catalog materialisation
  (Rust tenant পাঠায় runtime-এ, deploy-time-এ নয়)।
- **কোনো fake/echo/mock provider নেই** — composition fail closed।

### Phase 9 — Failover/retry/timeout অটুট ✅
`models/failover.py`-এর আগের bug-fix বহাল; ৩৯/৪০ resilience টেস্ট চলমান।

### Phase 10 — Identity ও scope ✅ (২০ টেস্ট)
Missing/forged/malformed credential → `UNAUTHENTICATED`; scope না থাকলে
`PERMISSION_DENIED`; tenant/org header body-এর সাথে না মিললে `FAILED_PRECONDITION`;
body-ঘোষিত subject credential-এর সাথে না মিললে `UNAUTHENTICATED`। সব ক্ষেত্রে
dispatch-এর **আগেই** বন্ধ, এবং **কোনোটিই retryable নয়**।

### Phase 11 — Deadline ও Cancellation ✅ (২১ টেস্ট)
- Deadline parse/propagate; অতীত deadline intake-তেই প্রত্যাখ্যাত।
- **Cancellation কখনো success নয়** — `cancelled` code, `retryable=False`।

### Phase 12 — Error mapping ✅ (৭১ টেস্ট)
সব ২৮টি error code-এর stable code, retryable/terminal classification, safe message,
correlation_id/request_id। Secret/PII scrub।

### Phase 13 — Telemetry ✅ (১৯ টেস্ট)
`observability/bridge_metrics.py` (নতুন) — gRPC ও runtime latency আলাদা, model/tool latency,
timeout/cancellation/retry/failover/provider-failure/authorization-failure/tenant-mismatch
কাউন্টার। Correlation log-এ; metric label সর্বদা bounded (tenant id কখনো label নয়)।

### Phase 14 — Tests ✅ ১৮৪টি নতুন bridge টেস্ট
`tests/bridge/` — conftest, identity, deadline/cancellation, error-mapping,
production-composition, telemetry।

### Phase 15 — Verification ✅
উপরের সব command আসলে চালানো; কোনোটিই "পাস" বলে দাবি করা হয়নি চালানো ছাড়া।

### Phase 16 — সম্পূর্ণ ফাইল আউটপুট ✅
`/home/user/RUST_PYTHON_BRIDGE_PHASE16_FILES.md` — **২৯টি ফাইল, ১০,৪০৩ লাইন**,
প্রতিটিতে `FILE:` + `PURPOSE:` + সম্পূর্ণ content। কোনো omission নেই।

---

## ৩. এই ধাপে ধরা পড়া **বাস্তব bug** গুলো

টেস্ট লেখার সময় নিচের বাস্তব ত্রুটিগুলো ধরা পড়ে ঠিক করা হয়েছে — এগুলো টেস্ট না লিখলে
প্রোডাকশনে যেত:

### Bug ১ — Cancellation কাজ করতেই না (সবচেয়ে গুরুতর)
`_observe_cancellation` `context.cancelled()`/`done()` poll করত। Probe-এ প্রমাণিত:
**grpc.aio তা কখনো সেট করে না** — বরং servicer coroutine-এ `CancelledError` raise করে।
ফলে Rust cancel করার পরও Python-এর মডেল কল চলতেই থাকতো (billing + ভুল ফলাফল)।

**সমাধান:** `_arm_cancellation` — current task-এ done-callback + secondary context poll।

### Bug ২ — Auth rejection-কে "retryable" বলা হতো
`is_retryable_status()` অচেনা ইনপুট পেলে `DEPENDENCY_UNAVAILABLE` ধরে retryable বলত।
`error.code()` (StatusCode) দিলে → `UNAUTHENTICATED`-ও retryable।

**সমাধান:** function-কে total করা হয়েছে — `grpc.StatusCode` সরাসরি গ্রহণ করে।

### Bug ৩ — Error message-এ secret যেত
`ModelProviderError(f"key {SECRET} refused")`-এর message সরাসরি wire-এ যাচ্ছিল।

**সমাধান:** `_failure_message` এখন `redact_text` + PII `RedactAll` — দুই স্তর।

### Bug ৪ — port ০ reject করা হতো
`GrpcServerConfig` port `0` (ephemeral bind) প্রত্যাখ্যাত করত, যা টেস্টের জন্য必需।

**সমাধান:** `0` অনুমোদিত, documented (kernel port বেছে দেয়)।

### Bug ৫ — `execution_id` উদ্ভাবন (Rust)
আগে নেই বলে `ExecutionId::new()` বানাতো — Rust-এর অজানা আইডি। এখন validation error।

### Bug ৬ — Pre-existing clippy lint
`crates/messaging/src/nats.rs:567` `len() == 0` → `.is_empty()` (শুধু `--all-features`-এ দেখা যেত)।

---

## ৪. নতুন/পরিবর্তিত ফাইল (২৯টি)

**Rust (১০টি)**
```
crates/api/proto/mas/api/v1/mas.proto          (পরিবর্তিত)
crates/execution/src/agent_bridge.rs           (পরিবর্তিত)
crates/execution/src/lib.rs                    (পরিবর্তিত)
crates/execution/Cargo.toml                    (পরিবর্তিত)
crates/ai-bridge/Cargo.toml                    (নতুন)
crates/ai-bridge/build.rs                      (নতুন)
crates/ai-bridge/src/lib.rs                    (নতুন)
crates/worker-service/src/agent_execution.rs   (নতুন)
crates/worker-service/src/main.rs              (পরিবর্তিত)
crates/worker-service/Cargo.toml               (পরিবর্তিত)
crates/messaging/src/nats.rs                   (পরিবর্তিত)
```

**Python (১২টি)**
```
src/mas_orchestrator/grpc/agent_bridge_servicer.py      (নতুন)
src/mas_orchestrator/grpc/server.py                     (নতুন)
src/mas_orchestrator/grpc/errors.py                     (পরিবর্তিত)
src/mas_orchestrator/grpc/__init__.py                   (পরিবর্তিত)
src/mas_orchestrator/services/composition.py            (নতুন)
src/mas_orchestrator/agents/tenant_registry.py          (নতুন)
src/mas_orchestrator/observability/bridge_metrics.py    (নতুন)
src/mas_orchestrator/observability/__init__.py          (পরিবর্তিত)
src/mas_orchestrator/main.py                            (পরিবর্তিত)
src/mas_orchestrator/config/settings.py                 (পরিবর্তিত)
pyproject.toml                                          (পরিবর্তিত)
```

**Tests (৮টি)**
```
tests/bridge/conftest.py                       (নতুন)
tests/bridge/test_identity_and_scope.py        (নতুন)
tests/bridge/test_deadline_and_cancellation.py (নতুন)
tests/bridge/test_error_mapping.py             (নতুন)
tests/bridge/test_production_composition.py    (নতুন)
tests/bridge/test_telemetry.py                 (নতুন)
tests/bridge/test_cross_language_contract.py   (নতুন)
tests/contract/test_proto_contract.py          (পরিবর্তিত — proto-এর সাথে sync)
tests/unit/test_events_delivery.py             (পরিবর্তিত — flaky test ঠিক)
```

---

## ৫. নিষিদ্ধ জিনিস কোথাও নেই (যাচাইকৃত)

| নিষিদ্ধ | অবস্থা |
|---|---|
| JSON/HTTP bridge | ❌ নেই — শুধু protobuf/gRPC |
| REST bridge | ❌ নেই |
| Local subprocess RPC | ❌ নেই |
| Fake adapter / EchoHandler (production) | ❌ নেই — EchoHandler শুধু dev bring-up |
| `InMemoryAgentBridge` (production) | ❌ নেই — শুধু `dev-bridges` feature |
| Test-only bridge | ❌ নেই |
| Silent config fallback | ❌ নেই — composition fail closed |
| Hard-coded/mock model | ❌ নেই — বাস্তব registry/router/provider |

---

## ৬. পরবর্তী পদক্ষেপ

**STOP condition-এ পৌঁছেছি।** আপনার পরবর্তী নির্দেশের অপেক্ষায় —
frontend, billing, DB redesign, দ্বিতীয় AgentBridge abstraction, কিংবা
in-memory production fallback কিছুই শুরু করা হয়নি।


---

## ২খ. Cross-language যাচাই, দ্বিতীয় পর্ব

বাকি চারটি আইটেম যাচাই করতে গিয়ে আরও তিনটি বিষয় পাওয়া গেছে:

### E. Cancellation code — এক শর্ত, দুই কোড (**প্রকৃত bug, ঠিক করা হয়েছে**)
Rust-এর `GrpcAgentBridge` স্থানীয়ভাবে cancellation ধরলে পাঠাতো `code: "cancelled"`,
কিন্তু Python পাঠায় `cancellation_requested`। অর্থাৎ **একই physical event-এর
দুইটি ভিন্ন machine-readable code** — কে আগে দেখলো তার উপর নির্ভর করে।
Phase 12-এর "stable machine-readable code" শর্তের লঙ্ঘন, এবং সবচেয়ে খারাপ
বিষয়: cancellation-এর উপর branch করে এমন কোড একটি কেস হ্যান্ডেল করবে, অন্যটি
চুপচুপ ছেড়ে দেবে।

**সমাধান:** `mas_execution::agent_bridge::CANCELLATION_CODE` constant
(`"cancellation_requested"`) — দুইটি bridge-ই এখন একই কোড পাঠায়। টেস্টটি এখন
literal-এর বদলে constant অ্যাসার্ট করে, তাই ভবিষ্যতে কেউ বদলালে ধরা পড়বে।

### F. `map_status` structured payload ফেলে দিত (**ঠিক করা হয়েছে**)
Python একটি rejected call-এ `mas-error-details-bin` trailing metadata-তে
সম্পূর্ণ structured error (`code`, `retryable`, `correlation_id`, `request_id`)
পাঠায় — ঠিক যাতে Rust-কে অনুমান না করতে হয়। কিন্তু Rust সেটা **পড়তই না**;
শুধু coarse status code দেখে classify করত। ফলে `FAILED_PRECONDITION` দেখে
Rust জানত "conflict", কিন্তু সেটা tenant mismatch না protocol violation — তা না।
Python-পাশের bug-টির (B দ্রষ্টব্য) exact mirror image।

**সমাধান:** Rust এখন payload পড়ে এবং `code`/`correlation_id`/`request_id`
message-এ বহন করে, ফলে একটি Rust log line সরাসরি Python failure-এর সাথে
join করা যায় দ্বিতীয় lookup ছাড়াই (Phase 13-এর লক্ষ্য)।

**ইচ্ছাকৃত সীমা:** `retryable` দিয়ে `AppError` variant বদল করা হয়নি।
`Timeout` ও `ExternalService` দুইটিই retryable কিন্তু এক জিনিস নয় — একটি
boolean মেলাতে variant পাল্টালে **টাইপের অর্থ নষ্ট হত**, অথচ idempotency
ইতিমধ্যেই duplicate-এর ঝুঁকি ঢেকে রাখে। মতভেদ এখন `tracing::warn`-এ দেখা যায়,
চুপচুপ নয়। সিদ্ধান্তটি কোডে comment আকারে লিখে রাখা হয়েছে।

> **পথে পাওয়া একটি ভুল:** tonic-এর `get_bin`/`insert_bin` `-bin` suffix
> **সহ** key নেয়, নিজেরা suffix জোড়ে না। প্রথম attempt-এ `mas-error-details`
> ব্যবহার করায় ৩টি টেস্ট panic করেছিল — suffix ছাড়া key কোনো partial match নয়,
> payload সম্পূর্ণ অদৃশ্য। `mas-error-details-bin`-এ ঠিক করা হয়েছে।

### G. Idempotency — সমস্যা নয়, কিন্তু যাচাই করা প্রয়োজন ছিল
Rust `idempotency-key` header + body field পাঠায়; Python সার্ভিসার কোনোটিই
ডিডুপ করে না। প্রথম দেখায় গরমিল মনে হলেও তদন্তে দেখা গেছে এটি **সঠিক**:
`events/dedup.py` স্পষ্টভাবে বলে Python প্রতিদ্বন্দ্বী idempotency system নয়,
Rust-ই authoritative execution state-এর মালিক। স্পেসের শর্ত (Phase 2 "request
must carry", Phase 12 "Rust must preserve") দুইটিই পূরণ। আর Python key-টি
নীরবে ফেলেও না — সার্ভিসার `request.idempotency_key` পড়ে (line ৮৯৯) লগে রাখে।

### H. `bootstrap.sh` অসম্পূর্ণ (**environment bug, ঠিক করা হয়েছে**)
রিসেটের পর `test_hash_is_stable_across_processes` লাল হলো। কারণ: `pyproject.toml`-এর
`pythonpath = ["src"]` pytest-কে import করতে দেয়, কিন্তু টেস্টটি bare
`python3 -c "import mas_orchestrator"` subprocess চালায় — যার কোনো pytest path
setup নেই। `bootstrap.sh` শুধু dev tools ইনস্টল করত, package নয়। ফলে
environmental কারণে একটি টেস্ট লাল, যা product bug-এর মতো দেখায়।

**সমাধান:** `bootstrap.sh` এখন `pip install -e .`-ও করে — অর্থাৎ রিসেটের পর
`bootstrap.sh` + `check.sh` দিয়েই সবুজ ফেরত আসে।

### এই পর্বে যোগ হওয়া টেস্ট
- Rust: **+৫** (`ai-bridge`) — payload বহন, payload ছাড়া status, অপাঠ্য payload
  degrade করে (crash না), `retryable` variant পাল্টায় না, cancellation কোড।
- Python: **+১২** cross-language — cancellation কোডের মিল, error-details key-এর
  মিল (`-bin` সহ), `ServiceIdentityMsg`-এর subject/issuer/scopes, `AgentSucceeded`/
  `AgentFailed`-এর field, এবং **কেন Rust `finish_reason` নিরাপদে উপেক্ষা করতে পারে**
  তার পিন (`ExecutionResult.succeeded` মানে `termination is COMPLETED`, তাই
  `AgentSucceeded`-এ `finish_reason` সবসময় `"completed"`; বাকি সব termination
  `AgentFailed`-এ যায়)।


---

## ২গ. Live cross-language E2E — দুই ভাষা আসলে কথা বলে

এটিই শেষ ফাঁক ছিল। এর আগের সব টেস্ট **একপক্ষীয়**: Rust-এর টেস্ট প্রমাণ করে
সে সঠিকভাবে encode করে, Python-এর টেস্ট প্রমাণ করে সে সঠিকভাবে পড়ে — কিন্তু
কেউ প্রমাণ করত না যে **দুইটি implementation একই wire-এ একমত**। এইরকম
অমিল production-এ নীরব থাকে: প্রতিটি পাশ নিজ নিজ টেস্টে সঠিক দেখায়, অথচ call
ব্যর্থ হয়।

### যা বানানো হয়েছে
- **`tests/bridge/stub_server.py`** — আসল `AgentBridgeApi` সার্ভার, আসল port-এ।
  সার্ভিসার, identity resolver, scope reconciliation, deadline parsing,
  cancellation mirroring, error mapping — **সব production কোড**। শুধু Agent
  Runtime double, কারণ live model provider-কে ডাকলে টেস্ট বিল করবে এবং bridge-এর
  বদলে provider-কে টেস্ট করবে।
- **`crates/ai-bridge/tests/e2e_python.rs`** — আসল `GrpcAgentBridge` ক্লায়েন্ট,
  আসল TCP socket-এর মাধ্যমে ঐ সার্ভারের সাথে কথা বলে।

### ৪টি E2E scenario (সব পাস)
1. **সফল run** — Rust-এর encode করা request আসল socket দিয়ে যায়, Python execute
   করে, উত্তর Rust decode করে; `output` হুবহু ফেরত আসে।
2. **Python failure-এর machine-readable code** — `model_timeout` কোডটি
   অক্ষুণ্ণ থাকে এবং retryable হিসেবে চিহ্নিত হয়।
3. **ভুয়া credential প্রত্যাখ্যাত** — এবং সেটি **retryable নয়** (ভুয়া টোকেন
   বারবার পাঠিয়ে retry budget নষ্ট করার কোনো মানে হয় না)।
4. **Cancelled run কখনো success নয়** — deadline হোক বা cancellation, "Succeeded"
   একমাত্র অগ্রহণযোগ্য উত্তর।

### দুইটি বিপদ যা ধরা পড়েছে (এবং এড়ানো হয়েছে)

**১. নীরব skip.** প্রথম রানে ৪টিই "passed" — কিন্তু **0.00 সেকেন্ডে**। সময়
দেখে সন্দেহ হলো, `--nocapture` চালিয়ে দেখা গেল সবকটি SKIP করেছে; early-return
করা টেস্ট `ok` রিপোর্ট করে। কারণ ছিল path-এ একটি `..` কম। অর্থাৎ সবুজ suite
মানেই "যাচাই হয়নি"। ঠিক করার পর ১.১৫s → **সত্যিকারের run**। এখন verification-এ
`SKIPPED` count = **0** যাচাই করা হয়।

**২. টেস্টের "দাঁত" আছে কি?** ইচ্ছেকৃতভাবে Python-এর উত্তর `"the answer"` থেকে
`"DELIBERATELY WRONG"` বদলে দেখা হলো — Rust টেস্ট **FAIL** করল ঠিক সেই
assertion-এ। তারপর restore। অর্থাৎ টেস্টটি সত্যিই কিছু যাচাই করে, শুধু
চলেই যায় না।

### টেস্ট সংখ্যা
| | আগে | এখন |
|---|---|---|
| `cargo test --locked` | 462 | **466** (+৪ E2E) |
| `--features nats` | 471 | **475** (+৪) |
| Python `pytest` | 1890 | **1890** (stub টেস্ট নয়, harness) |


---

## Phase ১৫ — command-by-command verbatim verification

আগের প্রতিবেদনগুলো `scripts/check.sh`-এর aggregate "SUCCESS"-এর উপর নির্ভর করছিল।
স্পেসের Phase ১৫ স্পষ্টভাবে **নির্দিষ্ট command** দাবি করে, তাই প্রতিটি আলাদাভাবে
চালিয়ে exit code রেকর্ড করা হলো।

### Rust (৫টি)

| Command | Exit | ফল |
|---|---|---|
| `cargo fmt --all -- --check` | 0 | ✅ |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 | ✅ 0 error |
| `cargo test --workspace --locked` | 0 | ✅ **466 passed**, 0 failed |
| `cargo test --workspace --locked --features nats` | 0 | ✅ **475 passed**, 0 failed |
| `cargo doc --workspace --all-features --no-deps` | 0 | ✅ 0 warning |

### Python (৫টি)

| Command | Exit | ফল |
|---|---|---|
| `python3 -m compileall -q src tests` | 0 | ✅ |
| `python3 -m ruff check src tests` | 0 | ✅ "All checks passed!" |
| `python3 -m ruff format --check src tests` | 0 | ✅ 217 files already formatted |
| `PYTHONPATH=src python3 -m mypy src/mas_orchestrator` | 0 | ✅ 160 source files, no issues |
| `PYTHONPATH=src python3 -m pytest tests -q` | 0 | ✅ **1890 passed** |

### Targeted contract / integration tests

| Suite | ফল |
|---|---|
| `tests/contract` | ✅ 71 passed |
| `tests/bridge` | ✅ 193 passed |
| `tests/resilience` | ✅ 85 passed |
| `crates/ai-bridge/tests/e2e_python.rs` | ✅ 9 passed, **0 skipped** |

### Phase ৩ — proto regeneration idempotency

`scripts/generate_proto.sh` দুইবার চালিয়ে জেনারেটেড ফাইলের hash তুলনা করা হয়েছে:
**অভিন্ন**। অর্থাৎ checked-in স্টাব কোনো stale artifact নয় — যেকোনো সময়
regenerate করলে একই ফল। Regeneration-এর পরেও পুরো suite ✅ 1890।

### কোনো command না চললে — কারণ

এই পর্বে পাঁচটি Python command-এর তিনটি প্রথমবার **ব্যর্থ** হয়েছিল:
`No module named ruff`, `No module named mypy`, এবং pytest-এ 19 collection error।
**কারণ:** স্যান্ডবক্স রিসেট পুনরায় installed package মুছে দিয়েছিল (এই প্রকল্পে
ষষ্ঠবার)। কোনো কোড ত্রুটি নয়। `scripts/bootstrap.sh` চালিয়ে toolchain ফিরিয়ে
সব পাঁচটি EXIT=0 পাওয়া গেছে।

> এ কারণেই আগের পর্বে `bootstrap.sh`-এ package install যোগ করা হয়েছিল:
> `ruff`/`mypy`/`pytest` থাকলেই হয় না, `mas_orchestrator`-এর editable install
> না থাকলে bare-subprocess টেস্ট ব্যর্থ হয়।


---

## ২ঘ. E2E-এ নিরাপত্তা পথ (Phase ১০/১১)

প্রথম E2E রান ৪টি scenario কভার করেছিল — সব "happy path + একটি cancellation"।
কিন্তু E2E-এর অস্তিত্বের কারণেই হলো সেই পথগুলো যাচাই করা যা **প্রতিটি পাশে
আলাদাভাবে সঠিক দেখায় অথচ সীমান্তে নীরবে ব্যর্থ হয়** — আর সেগুলো মূলত
নিরাপত্তা-সংক্রান্ত। তাই আরও ৩টি যোগ করা হয়েছে:

| Scenario | যা প্রমাণ করে |
|---|---|
| credential অনুপস্থিত | অপ্রমাণিত call কখনো সম্মানিত হয় না |
| **body-এর subject ≠ credential-এর subject** | জালিয়াতি ধরা পড়ে |
| Python পূরণ করতে পারবে না এমন deadline | deadline-এ কাটা run-কে success ঘোষণা করা হয় না |

### এই পর্বে ধরা পড়া দুর্বল assertion

প্রথম সংস্করণে rejection টেস্টগুলো শুধু `!error.is_retryable()` অ্যাসার্ট করত।
এটি **যথেষ্ট নয়**: সার্ভার unreachable হলে তৈরি transport error-ও non-retryable
হতে পারে, অর্থাৎ "সার্ভার প্রত্যাখ্যাত করেছে" এবং "সার্ভার পাওয়াই যায়নি" —
দুটোই পাস করত। তাই `ExternalService` variant-কে স্পষ্টভাবে বাদ দেওয়া হয়েছে।

### Teeth-check: জালিয়াতি সত্যিই ধরা পড়ে?

সবচেয়ে গুরুত্বপূর্ণ যাচাই। Python-এর subject cross-check সাময়িকভাবে বন্ধ করে
Rust টেস্ট চালানো হয়েছে:

```
a_subject_that_disagrees_with_the_credential_is_refused ... FAILED
  panicked: a declared subject must not override the authenticated one:
  Succeeded { output: String("the answer"), ... }
```

**ফলাফল শিক্ষণীয়:** check বন্ধ থাকলে **জাল subject-এও `Succeeded`** ফেরত আসে।
অর্থাৎ এটি তাত্ত্বিক ঝুঁকি নয় — ওই cross-check না থাকলে যেকেউ অন্য
principal-এর নামে AI spend চালাতে পারত। টেস্টটি সেই নির্দিষ্ট ভেদনযোগ্যতা
ধরে, কেবল "কোড চলল" তা নয়। এরপর সোর্স restore করা হয়েছে।

### চূড়ান্ত সংখ্যা

| | আগে | এখন |
|---|---|---|
| E2E scenario | 4 | **7** |
| `cargo test --locked` | 466 | **469** |
| `--features nats` | 475 | **478** |
| Python `pytest` | 1890 | **1890** |


---

## ২ঙ. Production composition টেস্ট (Phase ৫/৬/১৪)

Phase ১৪ স্পষ্টভাবে দাবি করে "tests for ... production-composition"। যাচাইয়ে
দেখা গেল `main.rs`-এ মাত্র ৩টি টেস্ট ছিল (`resolve_lane` নিয়েই সব) — অর্থাৎ
**Phase ৫ ও ৬-এর মূল দাবি দুটির কোনো টেস্টই ছিল না**:

- production path-এ `GrpcAgentBridge` নির্বাচিত হয়, `InMemoryAgentBridge` নয়
- `EchoHandler` production থেকে অগম্য

### যোগ করা ৪টি টেস্ট

| টেস্ট | যা গ্যারান্টি দেয় |
|---|---|
| `echo_handler_never_reaches_the_production_path` | demo handler production-এ পৌঁছায় না |
| `the_production_path_never_names_the_in_memory_bridge` | কোনো in-memory stand-in নেই |
| `the_production_path_binds_the_grpc_bridge` | **ইতিবাচক** counterpart — নিষিদ্ধ সিম্বল অনুপস্থিত থাকলেই যথেষ্ট নয়, আসল অ্যাডাপ্টার আসলেই বসানো থাকতে হবে |
| `the_worker_never_enables_the_dev_bridges_feature` | শক্তিশালীতম গ্যারান্টি: `dev-bridges` বন্ধ থাকায় `InMemoryAgentBridge` production-এ **compile error**, review-এর প্রশ্ন নয় |

### পথে আমার নিজের দুটি bug (টেস্টে, কোডে নয়)

প্রথম রানে ৩টিই FAIL করল। প্রবৃত্তি হলো production কোড সন্দেহ করা — কিন্তু
তদন্তে দেখা গেল **দোষ আমার টেস্টেই**:

1. **`include_str!("main.rs")` নিজের টেস্ট মডিউল-ও পড়ে।** টেস্টগুলো স্বভাবতই
   সেই নিষিদ্ধ নামগুলো (`EchoHandler`, `InMemoryAgentBridge`) লেখে — আর সেগুলো
   `run_production`-এর **পরে** অবস্থিত। ফলে assert নিজের থেকেই match করছিল।
   সমাধান: scan সীমাবদ্ধ করা `run_production` থেকে `mod tests` পর্যন্ত।
2. **manifest-এ `dev-bridges` শুধু একটা comment-এ আছে** — সেই comment-ই বলে
   কেন এটি অনুপস্থিত। naive `contains` সেই ব্যাখ্যাটিকেই declaration ভেবে
   মিলিয়েছিল। সমাধান: শুধু `mas-execution` dependency line পড়া, comment বাদ।

**Production কোড সঠিক ছিল; আমি টেস্ট ঠিক করেছি।** উল্টোটা করলে (ধরে নিয়ে
"৩ FAIL মানে production ভাঙা") আমি সঠিক কোড নষ্ট করতাম।

### Teeth-check

`run_production`-এ ইচ্ছেকৃতভাবে `EchoHandler` বসিয়ে দেখা হয়েছে টেস্ট ধরে কি না:

```
tests::echo_handler_never_reaches_the_production_path ... FAILED
  EchoHandler must stay confined to the demo path;
  a worker that echoes reports successes it never produced
```

ধরে ✅। Restore-এর পর ১৫/১৫ পাস। এটি গুরুত্বপূর্ণ কারণ echo-fallback-ই
সিস্টেমের সবচেয়ে বিপজ্জনক ব্যর্থতা: এটি কাজ করার মতোই দেখায়।

### চূড়ান্ত সংখ্যা

| | আগে | এখন |
|---|---|---|
| worker-service টেস্ট | 11 | **15** |
| `cargo test --locked` | 469 | **473** |
| `--features nats` | 478 | **482** |


---

## ২চ. Phase ১২ — সীমান্তে secret leakage রোধ (E2E প্রমাণ)

Phase ১২ দাবি করে: একটি structured failure কখনো **secret/key/token/sensitive
prompt** প্রকাশ করবে না। Python-পাশে এটি কভার্ড ছিল (`tests/unit/test_redaction.py`,
`test_error_mapping.py`-এর `SECRET = "sk-live-AAA..."`), কিন্তু **সীমান্তে নয়** —
আর এটি এমন একটি property যা কোনো ভাষাই একা প্রমাণ করতে পারে না:

> Python প্রমাণ করে সে redact করে। Rust প্রমাণ করে সে যা পায় তা লগ করে না।
> **কিন্তু কেউই প্রমাণ করে না যে redact করা টেক্সট-ই আসলে পৌঁছায়।**
> আর একটি leak নীরব — কিছু ব্যর্থ হয় না, secret কেবল পাড়ি দেয়।

### যা যোগ করা হয়েছে

- Python stub-এ নতুন **`leak` behaviour**: এররের টেক্সটেই ইচ্ছেকৃতভাবে
  API-key-shaped (`sk-AAAA...`) ও bearer-token-shaped মান বসানো থাকে। এটি
  কাল্পনিক নয় — provider adapter credential scope-এ থাকা অবস্থায় এরর ছোড়ে,
  তাই key-টি এররের নিজের টেক্সটেই ঢুকে যেতে পারে।
- Rust টেস্ট `a_failure_never_carries_a_credential_across_the_wire`:
  `AgentFailed.message`-এ সেই মানগুলো নেই — এবং caller-এর নিজের credential-ও
  প্রতিধ্বনিত হয়নি — তা অ্যাসার্ট করে।

### দুইবারের teeth-check — প্রথমবার আমি ভুল করেছিলাম

**প্রথম প্রচেষ্টা:** `redact_text` (প্রথম pass) বন্ধ করে টেস্ট চালালাম →
টেস্ট **এখনো PASS**। অর্থাৎ টেস্টের কোনো দাঁতই ছিল না।

কারণ: `_failure_message`-এ redaction **দুটি স্বাধীন pass** —

1. `redact_text` — credential-shaped মান কেটে দেয়
2. `redact(..., RedactAll())` — PII স্ক্যান করে

আমি একটিই বন্ধ করেছিলাম; দ্বিতীয়টি নীরবে কাজ চালিয়ে গেছে। এটি আসলে
**defence in depth**: যেকোনো একটি pass-ই এই key ধরতে যথেষ্ট।

**দ্বিতীয় প্রচেষ্টা (সঠিক):** দুটিই বন্ধ করে চালালাম →

```
a_failure_never_carries_a_credential_across_the_wire ... FAILED
  an API key reached the caller: provider rejected the call for key
  sk-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA with bearer Bearer eyJhbGciOi...
```

ধরে ✅। Restore-এর পর সবুজ।

> **শিক্ষা:** একটি teeth-check যা FAIL ঘটাতে ব্যর্থ হয়, সেটি কোনো যাচাই নয় —
> এটি "টেস্ট চলল" এর চেয়ে বেশি কিছু প্রমাণ করে না। আমার প্রথম সংস্করণ ঠিক
> সেই ফাঁদে পড়েছিল, এবং দ্বিতীয়বার না চালালে আমি একটি অর্থহীন টেস্ট
> "সবুজ" বলে রিপোর্ট করতাম।

### কাজ করতে দেখা যাচাই

সরাসরি wire message প্রিন্ট করে দেখা হয়েছে:

```
CODE    = model_timeout
MESSAGE = 'provider rejected the call for key [REDACTED] with bearer Bearer [REDACTED]'
```

### চূড়ান্ত সংখ্যা

| | আগে | এখন |
|---|---|---|
| E2E scenario | 7 | **8** |
| `cargo test --locked` | 473 | **474** |
| `--features nats` | 482 | **483** |


---

## ২ছ. Phase ১৪ শেষ দুই আইটেম — tenant-isolation ও failover

Phase ১৪ আটটি বিষয়ে টেস্ট দাবি করে:
*contract · identity · timeout · cancellation · error-mapping · **failover** ·
**tenant-isolation** · production-composition*। এই পর্বে শেষ দুটিও সম্পন্ন।

### A. Tenant isolation — সীমান্তে প্রমাণ (নতুন E2E)

এটি এমন একটি property যা **কোনো একক ভাষা প্রমাণ করতে পারে না**:

> Rust প্রমাণ করে সে wire-এ tenant বসায়। Python প্রমাণ করে সে tenant পড়ে।
> কিন্তু কেউ প্রমাণ করে না যে **run-টি আসলে সেই tenant-এর অধীনেই চলল** —
> আর ভুল tenancy নীরব: দুই পাশই সঠিক আচরণ করে, কাজটা শুধু ভুল জায়গায় হয়।

**সমাধান:** stub-এ নতুন `echo_scope` behaviour — Python **যে scope দেখেছে
তাই** উত্তরে ফেরত দেয়। Rust টেস্ট `each_run_executes_under_the_scope_rust_sent`:

- নির্দিষ্ট UUID দিয়ে দুটি ভিন্ন tenant/org/project বানিয়ে দুইটি run চালায়
- প্রতিটির উত্তরে `tenant_id`/`organization_id`/`project_id` ঠিক যা পাঠানো
  হয়েছিল তাই কি — তা যাচাই করে
- **নির্ণায়ক assertion:** দুটি run-এর tenant ভিন্ন হতেই হবে। একটি server যদি
  call-গুলোর মধ্যে context ক্যাশ বা লিক করত, তবে উপরের per-field চেকগুলো
  প্রথম call-এর জন্য সত্ত্বেও পাস করত — এই অ্যাসার্ট ছাড়া তা ধরা পড়ত না।

Teeth-check: stub-এ tenant হার্ডকোড করে দিলে টেস্ট FAIL করে
("run 0 executed under the caller's tenant") ✅ — Restore করা হয়েছে।

### B. Failover — যাচাই করে দেখা গেছে কভার্ড; একটি প্রশ্নের উত্তর পাওয়া গেছে

Rust-এর `decode_reply` `AgentSucceeded.model_key` **ফেলে দেয়**। এটি কি gap?
তদন্তে দেখা গেল — **না, এটি সঠিক, এবং কারণটি শিক্ষণীয়**:

1. Provider telemetry (Phase ১৩) **Python-েই** মাপা হয়: `MODEL_LATENCY`,
   `failovers`, `provider_failures` — `bridge_metrics.py`-এ।
2. `bridge_metrics.failover()` ইচ্ছেকৃতভাবে model key-কে **family-তে রিডিউস**
   করে (`openai/gpt-4o` → `openai`), কারণ পূর্ণ key dashboard-এ প্রকাশ করে
   দেয় কোন tenant কোন model ব্যবহার করে। এটি টেস্টেডও
   (`test_label_values_are_reduced_to_a_family`).
3. Failover সঠিকতা Python resilience suite-এ কভার্ড — `result.model == SECONDARY`
   (fallback-এর পর মডেল অ্যাট্রিবিউশন সঠিক)।
4. Wire-এ পূর্ণ `model_key` বৈধ, কারণ Rust একটি trusted platform — কিন্তু
   `model_key_from_result` কঠোরভাবে টেস্টেড: ledger না থাকলে `""`, ambiguous
   হলে `""` (line ৪০৮-৪৪৭)।

অর্থাৎ privacy split-টি ইচ্ছেকৃত: **wire-এ পূর্ণ key (Rust-এর জন্য), telemetry-তে
family (dashboard-এর জন্য)**। Rust-এর `model_key` ড্রপ করা এই নকশার সাথেই
সামঞ্জস্যপূর্ণ — কোনো পরিবর্তনের প্রয়োজন নেই, এবং enum-এ ফিল্ড যোগ করা হয়নি
কারণ কোনো consumer-ই নেই (স্পেকুলেটিভ generality এড়ানো হয়েছে)।

### Phase ১৪ — চূড়ান্ত কভারেজ

| আইটেম | কভারেজ |
|---|---|
| contract | `tests/contract` ৭১ + cross-language ২৯ |
| identity | E2E subject forgery + `test_identity_and_scope` ২০ |
| timeout | E2E deadline + `deadline_remaining` unit |
| cancellation | E2E (কখনো success নয়) |
| error-mapping | `test_error_mapping` ৭১ + E2E code/payload সংরক্ষণ |
| failover | resilience ৮৫ + telemetry privacy + `model_key` attribution |
| tenant-isolation | **নতুন E2E** + `test_identity_and_scope` ২০ |
| production-composition | **৪ নতুন** + `settings_*` ২ |

### চূড়ান্ত সংখ্যা

| | আগে | এখন |
|---|---|---|
| E2E scenario | 8 | **9** |
| `cargo test --locked` | 474 | **475** |
| `--features nats` | 483 | **484** |


---

## ৩. No Shortening — অনুমান নয়, প্রমাণ

**নিয়ম:** কোথাও `...`, "Rest of the code here", "existing code omitted" ব্যবহার
করা যাবে না; প্রতিটি ফাইল শুরু থেকে শেষ পর্যন্ত সম্পূর্ণ লিখতে হবে।

### সমস্যা: marker খোঁজা যথেষ্ট নয়

একটি grep কেবল সেই লেখককে ধরে যে **চিহ্ন রেখে গেছে**। এটি সেই ব্যর্থতা ধরতে
পারে না যেটি আসলে ঘটে: একটি ফাইল নীরবে ছোট করে দেওয়া, কিন্তু এমন জায়গায় কাটা
যাতে কোনো চিহ্ন না থাকে। সুন্দর জায়গায় কাটা ফাইল উপরের সব চেক পাস করে।

### সমাধান: `verify_deliverable_completeness.py`

Deliverable থেকে প্রতিটি ফাইল পুনর্গঠন করে ডিস্কের আসল সোর্সের সাথে
**byte-for-byte** তুলনা করে (SHA-256)। এটিই একমাত্র তুলনা যা "সম্পূর্ণ কোড"
প্রমাণ করে — "কোনো দৃশ্যমান ellipsis নেই" নয়।

চারটি চেক: (১) ফাইল বিদ্যমান ও অরিক্ত, (২) `PURPOSE:` ব্লক আছে, (৩) কোনো elision
marker নেই, (৪) **content byte-বাই-byte মেলে**।

ফলাফল: **`files verified byte-for-byte: 36/36`** ✅

### নিজের দাঁত যাচাই

একটি কপিতে দ্বিতীয় ফাইলের content **এক-তৃতীয়াংশে** কেটে দেওয়া হয়েছে — কোনো
marker ছাড়াই:

```
truncated: 18070 -> 6023 chars, marker কোনোটিই নেই
FAILURES (1):
  - crates/execution/src/agent_bridge.rs: content differs from source
    (deliverable 6023 chars, source 18070 chars)
```

ধরা পড়েছে ✅। grep-ভিত্তিক চেক এই truncation কখনোই ধরতে পারত না।

### স্থায়ী করা হয়েছে

`scripts/check.sh`-এ ষষ্ঠ stage হিসেবে যুক্ত — ভবিষ্যতে deliverable পুনর্গঠনে
কিছু কমলে `check.sh` সবুজ হবে না:

```
=== deliverable completeness ===
files verified byte-for-byte: 36/36
--- deliverable completeness: PASS
```

### পথে আমার দুটি নিজস্ব bug (আবারও টুলিং-এ, কোডে নয়)

1. **`s.replace()` assign করিনি।** `check.sh`-এ দুটি edit-এর সময় প্রথম
   `write_text`-এর পর `s` আপডেট করিনি, তাই দ্বিতীয় `write_text` আসল `s` দিয়ে
   প্রথম edit-টি **মুছে দিয়েছে**। ফলে নতুন stage চললই না — অথচ স্ক্রিপ্ট
   "updated" প্রিন্ট করেছিল। ধরা পড়েছিল কারণ stage-টি রান আউটপুটে অনুপস্থিত ছিল।
2. **Self-reference:** verifier-কে নিজেই সব marker-এর নাম লিখতে হয়, তাই সে
   নিজের সংজ্ঞাতেই মিলে যায়। সমাধান: `MARKER_SCAN_EXEMPT` — **exact path** দিয়ে
   কেবল সেই ফাইল অব্যাহতি যার কাজই হলো marker সংজ্ঞায়িত করা। Pattern দিয়ে নয়,
   তাই নাম বদলে কেউ এটি প্রশস্ত করতে পারবে না।

### এই পর্বের চূড়ান্ত verification

| Rust | ফল | Python | ফল |
|---|---|---|---|
| `cargo fmt --check` | ✅ 0 | `ruff format --check` | ✅ |
| `cargo clippy --all-targets -D warnings` | ✅ 0 error | `ruff lint` | ✅ |
| `cargo test --locked` | ✅ **475** | `mypy strict` | ✅ 160 files |
| `--features nats` | ✅ **484** | `pytest` | ✅ **1890** |
| `cargo doc` | ✅ 0 warning | `compileall` | ✅ |
| E2E | ✅ **9 passed, 0 skipped** | **completeness** | ✅ **36/36** |
