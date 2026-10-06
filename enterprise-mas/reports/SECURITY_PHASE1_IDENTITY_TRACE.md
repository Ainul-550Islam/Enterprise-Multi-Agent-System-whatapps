# Phase 1 — Full Identity Trace (Security / Authorization / Tenancy / RLS)

**তারিখ:** 2026-10-05
**স্পেসিফিকেশন:** `NEXT PROMPT — Enterprise Security, Authorization, Tenancy & RLS Hardening.md`
**পদ্ধতি:** প্রতিটি production entrypoint-এর credential → principal → tenant → org → permission → DB scope চেইন লাইন বাই লাইন ট্রেস করা হয়েছে।

---

## ০. এক লাইনে সারাংশ

Authentication আছে, কিন্তু **authorization নেই**। Tenant identity হেডার থেকে আসে, credential থেকে নয় — অর্থাৎ যেকোনো বৈধ API key দিয়ে যেকোনো tenant-এর নাম ঘোষণা করা যায়। RLS-এর SQL পলিসি সঠিক, কিন্তু প্রোডাকশন কোড কখনো সেই GUC সেট করে না (`ScopedStore`-এর কোনো production caller নেই)।

---

## ১. Entrypoint ইনভেন্টরি

| # | Entrypoint | Binary | Credential source | Authentication | Tenant source |
|---|---|---|---|---|---|
| 1 | HTTP API | `crates/api/src/main.rs` | `Authorization: Bearer` | `PostgresApiKeyVerifier` (`prod_auth.rs`) | **`x-tenant-id` header** ❌ |
| 2 | gRPC API | `crates/api/src/grpc.rs` | `authorization` metadata | একই verifier | **`x-tenant-id` metadata** ❌ |
| 3 | Scheduler (production) | `crates/scheduler-service/src/main.rs` | process identity | none (in-process) | durable row (authoritative) ✅ |
| 4 | Worker | `crates/worker-service/src/main.rs` | process identity | none (in-process) | durable task row ✅ |
| 5 | Scheduler → API (dev lane) | `crates/scheduler-service/src/dispatch.rs` | static bearer (`Option`) | **absent when `None`** ❌ | payload |
| 6 | CLI | `crates/cli/src/client.rs` | `MAS_API_TOKEN` | server-side | `--tenant` flag → header (server must authorize; currently doesn't) |
| 7 | Webhook/integration | `crates/integrations/src/webhooks.rs` | — | — | **production-এ wired নয়** (`mas-integrations` কারো dependency নয়) ❌ |
| 8 | Rust → Python | `crates/ai-bridge` | service bearer | `ServiceIdentityResolver` (Python) | request body (Rust authoritative) ✅ |

---

## ২. Findings

### F1 — CRITICAL — Caller-trusted tenant identity (Phase 3 violation)

**স্থান:** `crates/api/src/context.rs:16-33`, `crates/api/src/middleware.rs:26-46`, `crates/api/src/grpc.rs:66-86`

```rust
// context.rs
pub const TENANT_HEADER: &str = "x-tenant-id";
pub const ORGANIZATION_HEADER: &str = "x-organization-id";

pub struct RequestContext {
    pub principal: Principal,          // verified
    pub tenant_id: Option<TenantId>,   // ← header থেকে, caller-supplied
    pub organization_id: Option<OrganizationId>, // ← header থেকে
    ...
}
```

`RequestContext::require_scope()` (`context.rs:65`) যা যাচাই করে তা কেবল **উপস্থিতি**:

```rust
pub fn require_scope(&self) -> Result<&Self> {
    if self.tenant_id.is_none() || self.organization_id.is_none() {
        return Err(AppError::forbidden("x-tenant-id and x-organization-id headers are required"));
    }
    Ok(self)
}
```

কোনো membership যাচাই নেই। ফলে: **যেকোনো authenticated caller যেকোনো tenant id দাবি করতে পারে।**

**প্রভাব:** Spec Phase 3 — "Tenant identity MUST NOT be accepted as authoritative merely because it appears in `x-tenant-id`" — সরাসরি লঙ্ঘন। Phase 29 prohibition list-এর প্রথম দুইটি item-এরও লঙ্ঘন।

---

### F2 — CRITICAL — API key-এর authoritative tenant binding ফেলে দেওয়া হয় (Phase 8)

**স্থান:** `crates/api/src/prod_auth.rs:96-120`

```rust
Ok(Principal {
    subject: row.id.to_string(),   // ← key id
    kind: PrincipalKind::ApiKey,
})
```

`ApiKeyRow.tenant_id` (`crates/persistence/src/repositories/api_keys.rs:25`) ডেটাবেজে আছে — অর্থাৎ key-টি একটি tenant-এ বাঁধা। কিন্তু verifier সেই tenant রিটার্ন করে না; `Principal`-এ tenant-এর কোনো ক্ষেত্রই নেই। ফলে authoritative tenant (credential binding) বাতিল হয়ে header-এর tenant জিতে যায়।

---

### F3 — HIGH — দুইটি incompatible Principal মডেল (Phase 2 violation)

1. `mas_security::SecurityPrincipal` (`crates/security/src/principal.rs`) — সমৃদ্ধ: `kind`, `tenant_id`, `organization_id`, `roles`, `scopes`, `expires_at`; `checked(tenant, scope)` gate সহ।
2. `mas_api::auth::Principal` (`crates/api/src/auth.rs`) — ক্ষীণ: `subject: String`, `kind: PrincipalKind`। কোনো tenant নেই, কোনো role নেই, কোনো scope নেই।

আরো আছে `mas_domain::AuditActor` (audit-এর জন্য) এবং `TransitionActor` (domain transition-এর জন্য) — সব মিলিয়ে চারটি actor representation।

Spec: "Do NOT create separate incompatible principal models."

**বিশেষভাবে সমস্যাজনক:** `SecurityPrincipal` ইতিমধ্যে সঠিক tenant invariant বহন করে —

```rust
pub fn tenant_matches(&self, tenant_id: TenantId) -> bool {
    self.tenant_id == Some(tenant_id)
}
pub fn checked(&self, tenant_id: TenantId, scope: &str) -> bool {
    self.is_usable() && self.tenant_matches(tenant_id) && self.permits(scope)
}
```

— কিন্তু এটি **কোথাও ব্যবহার হয় না** (`grep` এ API/w path-এ কোনো call site নেই)। সঠিক মডেল আছে, তারের দেওয়া হয়নি।

---

### F4 — CRITICAL — RLS প্রোডাকশন ট্রানজাকশন পাথে কখনো applied হয় না (Phase 11 + 15 violation)

**স্থান:** `crates/persistence/src/repositories/scoped.rs:102-104`

```rust
pub async fn scoped_tx<'c>(&self, ctx: &RlsContext) -> Result<UnitOfWork<'c>> {
    ...
    ctx.apply(uow.executor()).await?;   // ← RLS GUC সেট হওয়ার একমাত্র production জায়গা
}
```

`ScopedStore` / `scoped_tx`-এর **কোনো production caller নেই**:

```
$ grep -rn "ScopedStore\|scoped_tx" crates/ --include=*.rs | grep -v "repositories/scoped.rs\|repositories/tenancy.rs"
crates/persistence/src/repositories/mod.rs:42:pub use scoped::{DocumentTable, ScopedStore};
```

মাত্র একটি re-export। প্রোডাকশন composition (`crates/api/src/main.rs:268-293`) `PgServices` ব্যবহার করে — unscoped repositories, কোনো `RlsContext` নয়।

**অর্থ:** ডেটাবেজ ডিজাইন সঠিক (`mas_app` NOLOGIN, কোনো BYPASSRLS নেই, সব ৩৩টি টেবিলে FORCE RLS), কিন্তু অ্যাপ্লিকেশন GUC সেট না করায় —
* হয় প্রোডাকশন রানটাইম রোলে BYPASSRLS আছে (Phase 14 violation),
* নয় প্রতিটি tenant-owned কোয়েরি শূন্য রিটার্ন করে।

দুটিই অগ্রহণযোগ্য; `with_authoritative_scope` abstraction-টি production path-এ বসাতে হবে।

---

### F5 — HIGH — AuthorizationService-এর অস্তিত্ব নেই (Phase 5 + 6 + 7 violation)

কোথাও কোনো canonical authorization service নেই। `crates/policy/` আছে কিন্তু তা ABAC policy engine (approval/rate-limit/expression), resource-level `can_read`/`can_cancel`/... সিদ্ধান্ত নয়। হ্যান্ডলারগুলো সরাসরি `ctx.require_scope()` ডাকে (উপস্থিতি যাচাই) এবং এরপর সার্ভিসে চলে যায় — কোথাও permission যাচাই হয় না।

Spec চায় ১২টি সিদ্ধান্ত: read / create / update / delete / execute / cancel / publish / administer / manage credentials / manage policies / view audit / manage billing।

---

### F6 — MEDIUM — Scheduler → API internal call-এ auth ঐচ্ছিক (Phase 9)

**স্থান:** `crates/scheduler-service/src/dispatch.rs:206-235`

```rust
pub struct HttpDispatcher {
    client: reqwest::Client,          // ← raw client, কোনো SSRF guard নেই
    bearer: Option<String>,           // ← None হলে কোনো auth-ই পাঠানো হয় না
}
```

Production lane এই dispatcher ব্যবহার করে না (`main.rs:238` `DurableDispatcher`), কিন্তু dev lane-এ (`main.rs:117` `LoopbackDispatcher`) পথটি জীবিত। অধিকন্তু `/v1/internal/schedule-runs` রুটটি API router-এ **নেই** — অর্থাৎ dispatcher-টি একটি অস্তিত্বহীন এন্ডপয়েন্টে পোস্ট করে।

---

### F7 — MEDIUM — SSRF guard ব্যবহারকারী webhook path production-এ wired নয় (Phase 23)

`crates/integrations/src/http_guard.rs` একটি যথাযথ guard (`EgressPolicy::check_destination`, `pin_resolution`, `is_publicly_routable`, `GuardedHttpClient`)। `webhooks.rs:31` এটি ব্যবহার করে। কিন্তু `mas-integrations` **কোনো crate-এর dependency নয়**:

```
$ grep -rn "mas-integrations" crates/*/Cargo.toml
crates/integrations/Cargo.toml:2:name = "mas-integrations"
```

Spec: "Do not mark SSRF protection complete merely because a helper exists."

---

### F8 — INFO / OK — যেগুলো সঠিক আছে

* **RLS SQL পলিসি:** ৩৩/৩৩ টেবিলে `ENABLE` + `FORCE ROW LEVEL SECURITY`; `mas_app` NOLOGIN, BYPASSRLS নেই; `0001_extensions/up.sql:34-46`। ✅
* **`RlsContext` বাস্তবায়ন:** `set_config(..., true)` transaction-local, মান bound parameter, unscoped হলে fail-closed। ✅ (কিন্তু ব্যবহার হয় না — F4)
* **Python bridge trust boundary:** `ServiceIdentityResolver` + `reconcile_tenant_scope` (`grpc/metadata.py:202`) — body-কে authoritative ধরে header mismatch-এ `TenantContextError`। ✅
* **API key hashing:** SHA-256 + constant-time compare + display-once + revocation + `status`/`expires_at` যাচাই। ✅ (শুধু tenant binding enforce হয় না — F2)
* **SecretString:** Debug redacted, zeroize on drop, `expose()` verb। ✅
* **Worker / scheduler identity:** durable row থেকে tenant আসে, caller-supplied নয়। ✅
* **Durable pipeline tenant scoping:** `tenant_sql_contract.rs` (১১ টেস্ট) + `tenant_isolation.rs` (১২ টেস্ট)। ✅

---

## ৩. সংশোধন পরিকল্পনা (Phase 2 → 32)

| Phase | করণীয় |
|---|---|
| 2 | `mas_security::SecurityPrincipal`-কে একমাত্র canonical principal বানানো; API-কে এটি তৈরি করতে হবে |
| 3 | Tenant resolution: credential binding / membership → authoritative; header কেবল hint; conflict ⇒ deterministic failure |
| 4 | Org→Tenant→Project সম্পর্ক যাচাই (composite) |
| 5 | `AuthorizationService` — ১২টি সিদ্ধান্ত, deny-by-default |
| 6 | RBAC: viewer/operator/developer/administrator/service/system |
| 7 | ABAC: principal+tenant+org+project+resource+operation |
| 8 | API key tenant binding enforce |
| 9 | Internal service identity (scheduler/worker/CLI) |
| 10 | Python bridge — ইতিমধ্যে সঠিক, রিগ্রেশন নেই তা টেস্ট |
| 11/15 | `with_authoritative_scope` → transaction → RLS SET LOCAL → repositories; production path-এ wire |
| 12 | Pooled connection reuse + rollback টেস্ট |
| 13 | RLS policy audit (৩৩/৩৩ OK) — টেস্টে ধরা |
| 14 | DB role audit → ডকুমেন্ট |
| 16/17 | Cross-tenant matrix + ID confusion টেস্ট |
| 18 | Privileged operations তালিকা + gate |
| 19 | Secret scanning + secret manager abstraction |
| 20 | Encryption/integrity posture — ডকুমেন্ট |
| 21 | Security audit logging (১০টি ঘটনা) |
| 22 | ৮টি stable auth failure classification |
| 23 | SSRF guard-কে production egress path-এ |
| 24 | AI security boundary টেস্ট |
| 25 | Worker/scheduler identity protection |
| 26 | CLI privileged operation গেটিং |
| 27 | ২০টি security টেস্ট |
| 28 | Static security scan |
| 29 | Production prohibition list যাচাই |
| 30 | Build/test verification |
| 31 | No regression |
| 32 | Full file output |
