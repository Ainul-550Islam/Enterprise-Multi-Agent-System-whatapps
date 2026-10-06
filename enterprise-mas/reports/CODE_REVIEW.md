# Enterprise Multi-Agent System (rust-core) — কোড রিভিউ

**রিপোজিটরি:** `Ainul-550Islam/Enterprise-Multi-Agent-System-whatapps`
**রিভিউয়ের তারিখ:** ২ অক্টোবর ২০২৬
**পদ্ধতি:** শুধু পড়েই সীমাবদ্ধ থাকিনি — Rust 1.98.1 টুলচেইন ইনস্টল করে `cargo check` / `clippy` / `fmt` / `test` চালিয়েছি, এবং **PostgreSQL 17.11 বাস্তবে চালিয়ে ১২টি মাইগ্রেশন + live টেস্ট সুইট রান করেছি**।

---

## ১. সারাংশ (ভার্ডিক্ট)

| দিক | অবস্থা |
|---|---|
| স্থাপত্য (architecture) | ⭐⭐⭐⭐⭐ খুবই ভালো — পরিষ্কার layered boundary, discipline দেখা যায় |
| কোড স্টাইল / লিন্ট | ⭐⭐⭐⭐⭐ `clippy -D warnings` + `rustfmt` ১০০% ক্লিন |
| কম্পাইল | ✅ `cargo check --workspace --all-targets` — ০ error, ০ warning |
| টেস্ট (শুকনো) | ✅ ৪২৯ পাস / ০ ফেল |
| **টেস্ট (বাস্তবে)** | 🔴 **live_pg সুইট আসল PostgreSQL-এ ৬/১০ → ফিক্সের পর ৯/১০** |
| **ডাটাবেস লেয়ার** | 🔴 **একটিও মাইগ্রেশন কখনো রান করা হয়নি** — প্রথমবার রান করাতেই সিনট্যাক্স এরর |
| **Production readiness** | 🔴 **ডিফল্ট কনফিগারেশনে worker একটি টাস্কও চালাতে পারবে না** |

> **মূল কথা:** কোডটা দেখতে অসাধারণ পরিপক্ব — কিন্তু সে কখনো **রিয়েল ডাটাবেসের সাথে চালানোই হয়নি**। `cargo test` সবুজ দেখাচ্ছে কারণ live টেস্টগুলো env ভ্যারিয়েবল না থাকলে **চুপচাপ স্কিপ** হয়ে "পাস" হিসেবে গণ্য হয়। CI কখনোই আসল PostgreSQL স্পর্শ করেনি।

---

## ২. যা যা ভেরিফাই করেছি

```
✅ rustup 1.98.1 (রেপোতে পিন করা চ্যানেল) ইনস্টল
✅ cargo check --workspace --all-targets      → Finished, 0 warning
✅ cargo clippy --workspace --all-targets -D warnings → 0 warning
✅ cargo fmt --all -- --check                 → 0 diff
✅ cargo test --workspace --locked            → 429 passed, 0 failed
🐘 PostgreSQL 17.11 চালু করে:
   ❌ psql -f migrations/0008_billing/up.sql   → syntax error at or near "window"
   ✅ বাকি ১১টি মাইগ্রেশন → ৩৩টি টেবিল, সবগুলোতে RLS
   ❌ cargo test -p mas-persistence --test live_pg (parallel) → ৩/১০
   ❌ cargo test ... --test-threads=1                        → ৬/১০
   ✅ আমার ফিক্সগুলোর পর                                     → ৯/১০
```

---

## ৩. ভালো দিক (এগুলো আসলেই প্রশংসনীয়)

রিপোর্টে শুধু সমস্যা থাকলে অন্যায় হবে — নিচের জিনিসগুলো সত্যিই ভালো করা হয়েছে:

1. **স্থাপত্য শৃঙ্খলা** — `domain` ক্রেটটি সত্যিই infrastructure-মুক্ত; `common → domain/contracts → infrastructure → application → api` লেয়ারিং কার্যকরভাবে মানা হয়েছে।
2. **ক্রমবর্ধমান ID** — সব জায়গায় UUIDv7; DB index locality-র কথা মাথায় রাখা হয়েছে।
3. **RLS সর্বত্র** — ৩৩টি টেবিলের **সবগুলোতে** Row Level Security + `FORCE`। মাল্টি-টেনেন্সি সিরিয়াসলি নেওয়া হয়েছে।
4. **নিরাপত্তা-সচেতন কোড** — `SecretString` (zeroize), constant-time digest compare, HMAC-SHA256, `alg: none` রিজেকশন, SSRF guard, redaction — এগুলো টুকিটাকি করে লেখা হয়নি, ভেবেচিন্তে লেখা।
5. **API key ডিজাইন** — শুধু hash সংরক্ষণ, prefix lookup, revoked key সাথে সাথে কার্যকর।
6. **`unwrap` শৃঙ্খলা** — `unsafe_code = "forbid"`, লাইব্রেরি কোডে `unwrap` নেই (যেগুলো আছে সব `#[cfg(test)]`-এর ভেতরে)।
7. **টেস্টের নামকরণ** — টেস্টগুলোর নামই ডকুমেন্টেশন (`suspended_or_cancelled_subscriptions_deny_everything`)।
8. **কনফিগ লেয়ারিং** — `default → {env} → env var → CLI flag`, আর production guardrail যা dev token থাকলে বুটই拒绝 করে।
9. **টাইপ-নিরাপত্তা** — ২৭টি typed ID, typed value object (`Slug`, `Email`, `SafeUrl`, `SemanticVersion`)।

---

## ৪. 🔴 CRITICAL বাগ (বাস্তবে রান করলে সিস্টেম চলবেই না)

### C-1. `0008_billing` মাইগ্রেশন কখনোই রান হয়নি — `window` একটি reserved keyword

**ফাইল:** `migrations/0008_billing/up.sql:51,55`

```sql
window      text NOT NULL CHECK (window IN ('second','minute',...)),
UNIQUE (tenant_id, scope, resource, window)
```

```
psql:0008_billing/up.sql:56: ERROR: syntax error at or near "window"
```

`WINDOW` PostgreSQL-এ reserved (window function-এর জন্য)। মাইগ্রেশন রানার কঠোর ক্রমে চলে, তাই **এই এররের কারণে 0007-এর পরেই সব আটকে যায়** — billing, schedules, integrations, audit, outbox কোনোটিই তৈরি হয় না।

**ফিক্স:** `"window"` কোট করুন, অথবা কলামের নাম বদলে `window_size` / `period` রাখুন। (আমি স্যান্ডবক্সে কোটিং করে ঠিক করেছি — ১২টিই মাইগ্রেশন তখন চলেছে।)

---

### C-2. `PgTaskBroker`-এর event lane সবসময় **unavailable** — ডিফল্ট production lane

**ফাইল:** `crates/persistence/src/repositories/broker.rs:115`

`InMemoryBroker::new()` **ইচ্ছাকৃতভাবে unavailable অবস্থায় শুরু হয়** (`crates/messaging/src/broker.rs:837`-এর টেস্টে লেখাই আছে: `// starts unavailable`)। কিন্তু —

```rust
pub fn new(pool: PgPool) -> Self {
    Self { pool, events: InMemoryBroker::new(), ... }   // ← set_available(true) কোথাও নেই
}
```

আর `PgTaskBroker` নিচের সবকিছু `self.events`-এ delegate করে:

| মেথড | ফলাফল |
|---|---|
| `ping()` | 🔴 `SELECT 1` সফল → তারপর events lane ফেল → **readiness probe সবসময় DOWN** |
| `fetch()` | 🔴 durable lane-এ `max`-এর কম মেসেজ থাকলেই (অর্থাৎ প্রায় সবসময়) → ফেল |
| `dead_letters()` | 🔴 ফেল |
| `consumer_stats()` | 🔴 ফেল |
| `ack()` (non-durable) | 🔴 ফেল |

`worker-service/src/main.rs:321`-এ `PgTaskBroker` **ডিফল্ট লেন** (`broker.kind = "pg"`)। অর্থাৎ **worker বুট হলেই readiness ফেল করবে — Kubernetes কখনোই ready মার্ক করবে না।**

**ফিক্স:**
```rust
let events = InMemoryBroker::new();
events.set_available(true);
```

---

### C-3. `fetch()` undecodable frame ফেরত দেয় → **প্রতিটি টাস্ক dead-letter**

**ফাইল:** `crates/persistence/src/repositories/broker.rs` (`publish` বনাম `fetch_durable`)

`publish()` একটি `TaskQueueMessage`-কে ভেঙে আলাদা কলামে রাখে (`id`, `tenant_id`, `kind`, `priority`, …) আর `payload` jsonb-তে রাখে শুধু:

```json
{ "input": {...}, "_mas_spec": { "attempt_count":0, "max_attempts":8, "operation":"execution.run", "queued_at":"..." } }
```

কিন্তু `fetch_durable()` সেই `payload`-টাই **যেমন আছে তেমন** ব্রোকার মেসেজ হিসেবে ফেরত দেয়। consumer আবার পুরো `TaskQueueMessage` আশা করে (`task_id`, `tenant_id`, `priority`, `idempotency_key`, `correlation_id`, `enqueued_at`…)।

```
decode: Malformed { reason: "json decode failed at line 1 column 144: data" }
```

আর `crates/worker/src/consumer.rs:271-285`-এ ডিকোড ফেল করলে —

```rust
AckInstruction::Term   // undecodable → সরাসরি dead-letter, আর কখনো retry নয়
```

**অর্থাৎ: ডিফল্ট production lane-এ পাবলিশ করা প্রতিটি টাস্ক সাথে সাথে dead-letter হয়ে যাবে। কোনো কাজই হবে না।**

**ফিক্স:** `CLAIM_SQL`-এ প্রয়োজনীয় কলামগুলো `RETURNING` করে `fetch_durable()`-এ frame টি আবার `TaskQueueMessage`-এ assemble করতে হবে। (আমি স্যান্ডবক্সে ঠিক করেছি — decode টেস্ট তখন পাস করেছে।)

---

### C-4. broker-এর লেখা row-কে `TaskStore` hydrate-ই করতে পারে না (`created_by`)

```
lifecycle load: Database("row hydration failed for table tasks at line 0 column 0")
```

`domain::Task.created_by` একটি **required** ফিল্ড (`#[serde(default)` নেই), কিন্তু —
- `tasks` টেবিলে `created_by` কলাম **নেই**, এবং
- `PgTaskBroker::publish()` সেটা `_mas_spec`-এও লেখে না।

ফলে broker-এর তৈরি row `TaskStore::load()`-এ সবসময় ব্যর্থ হয়। `worker/src/consumer.rs:310-324`-এ lifecycle load ফেল করলে → `Settlement::Nak` → retry শেষ → dead-letter।

> ⚠️ এটির ফিক্সের জন্য একটি **ডিজাইন সিদ্ধান্ত** দরকার (নিচে §৭ দেখুন), তাই আমি এটা প্যাচ করিনি।

---

### C-5. `schedules`: `MATERIALIZED` তালিকা থেকে `tenant_id` বাদ → **প্রতিটি schedule save ব্যর্থ**

**ফাইল:** `crates/persistence/src/rows.rs` (`ScheduleRow`)

```rust
const MATERIALIZED: &[&str] = &["id", "organization_id", "name", "status", ...];  // "tenant_id" নেই!
...
tenant_id: take_uuid(&map, "schedules", "tenant_id")?,   // ← map-এ নেই → সবসময় error
```

```
schedule save: Database("missing/invalid column 'tenant_id' for table schedules")
```

**ফিক্স:** `MATERIALIZED`-এ `"tenant_id"` যোগ করুন।

---

### C-6. `schedules`: `task_template` কখনো সংরক্ষিত হয়ই না → hydrate ব্যর্থ

**একই ফাইল, `from_domain()`:**

```rust
let (map, spec) = split_spec(...)?;
let mut target = map.get("task_template")   // ← task_template materialized নয়, তাই spec-এ আছে, map-এ নয় → None
```

ফলে `task_template` — **না** `target` jsonb-তে যায়, **না** `_mas_spec`-এ (সেখান থেকে `remove` করা হয়)। সম্পূর্ণরূপে হারিয়ে যায়। আর `into_domain()`-এ `task_template` required হওয়ায় hydration ব্যর্থ:

```
list_due after: Database("row hydration failed for table schedules at line 0 column 0")
```

**ফিক্স:** `task_template` ও `kind` পড়তে হবে **সম্পূর্ণ dehydrated JSON** থেকে, `map` (materialized projection) থেকে নয়। (স্যান্ডবক্সে ঠিক করেছি — schedules টেস্ট তখন পাস।)

---

## ৫. 🟠 HIGH বাগ

### H-1. `jsonb_set`-এ path-এর টাইপ ভুল → `idempotency_put` সবসময় ব্যর্থ

**ফাইল:** `crates/persistence/src/repositories/executions.rs`

```sql
jsonb_set(state, '{' || $2 || '}', ..., true)   -- $2 টাইপ: text;  দরকার: text[]
```

```
ERROR: function jsonb_set(jsonb, text, jsonb, boolean) does not exist   (sqlstate 42883)
```

**ফিক্স:** `ARRAY[$2]::text[]` — psql-এ সরাসরি যাচাই করেছি:
```
❌ jsonb_set('{"a":1}', '{' || '_mas_spec' || '}', '{}', true)  → function does not exist
✅ jsonb_set('{"a":1}', ARRAY['_mas_spec']::text[], '{}', true)  → {"a": 1, "_mas_spec": {}}
```

---

### H-2. `MigrationRunner::migrate()` concurrency-safe নয় (কোনো advisory lock নেই)

১০টি টেস্ট একসাথে চালালেই:
```
migrations applied: Conflict("a record already exists (constraint pg_type_typname_nsp_index)")
```

**প্রোডাকশনে:** `MAS_DATABASE_MIGRATE_ON_BOOT=true` দিয়ে ২টি API রেপ্লিকা একসাথে বুট করলে একটি ক্র্যাশ করবে।

**ফিক্স:** মাইগ্রেশনের শুরুতে `SELECT pg_advisory_lock(<fixed key>)` নিন।

---

### H-3. `PgTaskBroker::ensure_consumer` durable consumer-কে event lane-এ রেজিস্টার করে না

```rust
if !durable { self.events.ensure_consumer(config).await?; }   // durable হলে রেজিস্টারই হয় না
```
কিন্তু `fetch()` সবসময় event lane-এও খোঁজে → `NotFound { resource: "broker consumer" }`।
**ফিক্স:** শর্ত সরিয়ে সবসময় event lane-এ রেজিস্টার করুন। (স্যান্ডবক্সে করেছি।)

---

### H-4. 🔴 **live টেস্টগুলো চুপচাপ "পাস" হয় — CI-এর সবুজ বাতি মিথ্যা**

`crates/persistence/tests/live_pg.rs`-এর ১০টি টেস্ট, env ভ্যারিয়েবল না থাকলে:

```rust
let Some(pool) = support::live_pool("agents-round-trip").await else { return; };  // শুধু return
```

→ `ok` হিসেবে গণ্য। `cargo test --workspace` রিপোর্ট করে **"429 passed"**, কিন্তু বাস্তবে PostgreSQL স্পর্শই করা হয়নি। NATS সুইটেও একই (`live_nats`)।

**এটাই মূল কারণ যে উপরের সব বাগ এতদিন লুকিয়ে ছিল।**

**ফিক্স:** CI-এ PostgreSQL/NATS সার্ভিস কন্টেইনার চালু করুন + `MAS_TEST_DATABASE_URL` সেট করুন, এবং স্কিপ হওয়াকে **visible** করুন (যেমন `#[ignore]` + আলাদা job, অথবা skip হলে স্পষ্ট WARNING)।

---

### H-5. JWT ভ্যালিডেটর কখনো **wire** করা হয়নি

`mas_security::JwtValidator` (HS256, alg pinning, exp/iss/aud চেক — কোডটা চমৎকার) লেখা হয়েছে, টেস্টও আছে… কিন্তু **কোনো request path-এ ব্যবহার হয়নি**:

```
grep JwtValidator → শুধু security/src/tokens.rs + তার নিজের টেস্ট
```

API বাস্তবে ব্যবহার করে শুধু `StaticTokenVerifier` (dev) ও `PostgresApiKeyVerifier` (prod)। README §7-এ JWT-এর কথা লেখা — **dead code**।

---

### H-6. 📱 রিপোর নাম "what**apps**" — কিন্তু WhatsApp কোড **একটিও নেই**

```
grep -ril "whatsapp|waba|twilio|360dialog|meta_cloud" .  →  zero match
```

কোনো WhatsApp/Cloud API connector, webhook handler, message template — কিছুই নেই। `crates/integrations`-এ আছে generic HTTP/OAuth/webhook। যদি WhatsApp-ই মূল লক্ষ্য হয়, তবে **সেটা এখনো শুরুই হয়নি**।

---

## ৬. 🟡 MEDIUM / LOW

| # | বিষয় | বিস্তারিত |
|---|---|---|
| M-1 | **২১ MB `rustup` বাইনারি git-এ কমিট** | `.cargo/bin/rustup` — ২১,১১৩,২৩২ বাইট, tracked। রেপোর আকার অযথা বাড়াচ্ছে। `.gitignore`-এ ডকুমেন্ট করুন বা বাদ দিন। |
| M-2 | **README-এর রেফারেন্সগুলো মিথ্যা** | `scripts/test.sh`, `scripts/generate-proto.sh`, `contracts/proto`, `scripts/dev.sh` — **কোনোটিই আসলে নেই** (README §6, §13 এ উল্লেখ)। বাস্তবে আছে `check.sh`, `dev-up.sh`, `crates/api/proto`। |
| M-3 | **README-এর টেস্ট সংখ্যা ভুল** | এক জায়গায় "372 tests", অন্য জায়গায় "378"। বাস্তবে ৪২৯। |
| M-4 | **Workspace `Cargo.toml`-এর হেডার পুরনো** | `[todo] crates/messaging, crates/persistence, ...` — কিন্তু এরাই মেম্বার ও ইমপ্লিমেন্টেড। |
| M-5 | **Billing স্কিমা সম্পূর্ণ এতিম** | `quotas`, `usage_records`, `entitlements`, `subscriptions` টেবিল আছে, RLS আছে — কিন্তু **কোনো Rust রিপোজিটরি নেই**। কেউ পড়ে/লিখে না। |
| M-6 | **Dev token হার্ডকোডেড** | `api/src/main.rs::dev_verifier()` সবসময় `"dev-token"` রেজিস্টার করে (`MAS_DEV_TOKENS` খালি থাকলেও)। `--dev-inmemory` দিয়ে কেউ staging-এ চালালেই ঢুকে পড়া যাবে। |
| M-7 | **টেস্ট ফিক্সচারে UUIDv7-prefix ব্যবহার** | `&Uuid::now_v7().to_string()[..8]` — UUIDv7 **সময়-ভিত্তিক**, তাই প্রথম ৮ হেক্স = মিলিসেকেন্ড টাইমস্ট্যাম্প → একই মিলিসেকেন্ডে চালালে slug কলিশন (`organizations_slug_key`)। সম্পূর্ণ UUID ব্যবহার করুন। |
| M-8 | **Dead code / redundancy** | `api/src/main.rs`: `Ok(0) => 0, Ok(_) => 0`; addr দুইবার parse; `let _ = (&http_addr, &grpc_addr);` |
| M-9 | **কোনো CI নেই** | `.github/` / `.gitlab-ci.yml` / `Dockerfile` / `docker-compose.yml` — কিছুই নেই। "CI runs …" বলে README-এ বারবার লেখা হলেও CI-এর কোনো অস্তিত্ব নেই। |
| M-10 | **কোনো LICENSE / .gitignore নেই** | `license = "Proprietary"` — ঠিক আছে, কিন্তু LICENSE ফাইল নেই; `.gitignore` না থাকায় `target/` কমিট হওয়ার ঝুঁকি। |
| M-11 | **একটিই large initial commit** | ৩টি কমিট (২টি "Remove unnecessary files") — সবকিছু একসাথে। রিভিউ/বিসেক্ট করা অসম্ভব। |

---

## ৭. সাজানো পরামর্শ (priority অনুযায়ী)

### ধাপ ১ — প্রথমেই CI-কে সৎ করুন (সবচেয়ে জরুরি)
```yaml
# .github/workflows/ci.yml
services:
  postgres:
    image: postgres:17
    env: { POSTGRES_PASSWORD: postgres }
    options: >-
      --health-cmd pg_isready --health-interval 10s --health-timeout 5s --health-retries 5
    ports: ['5432:5432']
env:
  MAS_TEST_DATABASE_URL: postgres://postgres:postgres@localhost:5432/mas_test
steps:
  - cargo fmt --all -- --check
  - cargo clippy --workspace --all-targets -- -D warnings
  - cargo test --workspace --locked
```
**এতটুকু করলেই C-1 … C-6 সব নিজে নিজেই ধরা পড়বে।** স্কিপ-কে দৃশ্যমান করুন (skip হলে job হলুদ/লাল)।

### ধাপ ২ — CRITICAL গুলো ঠিক করুন
1. `0008_billing`: `"window"` কোট করুন
2. `PgTaskBroker::new`: `events.set_available(true)`
3. `PgTaskBroker::fetch_durable`: frame পুনরায় assemble করুন
4. `ScheduleRow::MATERIALIZED` + `"tenant_id"`
5. `ScheduleRow::from_domain`: `task_template`/`kind` পূর্ণ JSON থেকে নিন
6. `executions.rs`: `jsonb_set(..., ARRAY[$2]::text[], ...)`
7. `ensure_consumer`: শর্ত সরান
8. `MigrationRunner`: advisory lock যোগ করুন

### ধাপ ৩ — C-4-এর ডিজাইন সিদ্ধান্ত নিন (`created_by`)
তিনটির যেকোনো একটি বেছে নিন:
- **(ক)** `tasks` টেবিলে `created_by text NOT NULL DEFAULT 'system'` কলাম যোগ + `TaskRow` সেটি materialized করে (সবচেয়ে পরিষ্কার)
- **(খ)** `domain::Task.created_by`-কে `#[serde(default)]` করুন
- **(গ)** broker-কে `TaskStore::save()`-এর মাধ্যমেই row লিখতে বাধ্য করুন (hand-rolled INSERT বন্ধ) — **আমার মতে এটিই সবচেয়ে শক্তিশালী**, কারণ এতে row-লেখার path একটিই থাকে এবং এই ধরনের drift আর হবে না।

### ধাপ ৪ — বাকিগুলো
- JWT-কে API-তে wire করুন, নয়তো README §7 সংশোধন করুন
- ২১ MB rustup বাইনারি বাদ দিন, `.gitignore` + LICENSE যোগ করুন
- README-এর ভুল রেফারেন্স/সংখ্যা ঠিক করুন
- WhatsApp connector-এর পরিকল্পনা স্পষ্ট করুন (নাকি নাম বদল করুন)

---

## ৮. সংযুক্তি

**`mas-review-fixes.patch`** — আমি স্যান্ডবক্সে যেই ফিক্সগুলো লিখে **রিয়েল PostgreSQL-এ যাচাই** করেছি, সেগুলোর diff (৫টি ফাইল, +১৫৬/−৩৪):

```
crates/persistence/src/repositories/broker.rs     | 106 +++++++++++--
crates/persistence/src/repositories/executions.rs |   2 +-
crates/persistence/src/rows.rs                    |  16 +-
crates/persistence/tests/live_pg.rs               |  62 ++++++--
migrations/0008_billing/up.sql                    |   4 +-
```

প্রয়োগ করার আগে মনে রাখবেন: patch-টি `git`-এ কমিট করা হয়নি, এটি আমার স্যান্ডবক্স ক্লোন থেকে তৈরি — `git apply mas-review-fixes.patch` দিয়ে পরীক্ষা করুন।

**ফলাফল:** patch প্রয়োগের পর live_pg সুইট **৬/১০ → ৯/১০** (বাকি ১টি = C-4, যার জন্য ডিজাইন সিদ্ধান্ত প্রয়োজন), এবং `cargo test --workspace` **৪২৯/৪২৯** — কোনো রিগ্রেশন নেই।

---

*রিভিউতে ব্যবহৃত পরিবেশ: Rust 1.98.1 · PostgreSQL 17.11 · Debian 13 · `clippy -D warnings` + `rustfmt` + `cargo test --workspace --locked`*
