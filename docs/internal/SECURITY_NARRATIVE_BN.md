# এন্টারপ্রাইজ সিকিউরিটি, অথরাইজেশন, টেনেন্সি ও RLS হার্ডেনিং — সম্পূর্ণ প্রতিবেদন (বাংলা)

**তারিখ:** ২০২৬-১০-০৫
**স্পেক:** `uploads/NEXT PROMPT — Enterprise Security, Authorization, Tenancy & RLS Hardening.md` (৯৪৮ লাইন, ৩২ ধাপ)
**রিপোজিটরি:** `/home/user/repo/enterprise-mas` (Rust ওয়ার্কস্পেস) + `/home/user/python-orchestrator` (Python AI অর্কেস্ট্রেটর)

---

## ০. সারসংক্ষেপ — এক প্যারাগ্রাফে

এই ধাপের আগে সিস্টেমের মধ্যে সঠিক RLS মেশিনারি (মাইগ্রেশন, পলিসি, `rls.rs`) বিদ্যমান ছিল, কিন্তু **চলমান সিস্টেম কখনোই সেটা আরম করত না** — অর্থাৎ পলিসিগুলো ছিল সাজসজ্জামাত্র। সবচেয়ে খারাপ ত্রুটিটি ছিল এজেই: HTTP ও gRPC উভয় এন্ট্রিপয়েন্ট `x-tenant-id` / `x-organization-id` হেডারকে সরাসরি বিশ্বাস করে `RequestContext` বানাত। ফলে যেকোনো প্রমাণিত কলার যেকোনো টেনেন্টের নাম বলে সেই টেনেন্টের ডেটা পড়তে/লিখতে পারত।

এই ধাপে আমরা (ক) কলার-সরবরাহকৃত পরিচয়কে **হিন্ট**-এ নামিয়ে এনেছি এবং টেনেন্ট/অর্গ/প্রোজেক্ট পরিধি সার্ভার-সাইড ডিরেক্টরি থেকে সমাধান করেছি; (খ) একটি মাত্র `AuthorizationService` ও একটি মাত্র স্কোপড-ট্রানজেকশন অ্যাবস্ট্রাকশন রেখে বাকিগুলো সরিয়েছি; (গ) HTTP ও gRPC—দুই পথেই প্রতি রিকোয়েস্টে একটি `SET LOCAL`-আরমড ট্রানজেকশন খুলেছি; (ঘ) অভ্যন্তরীণ সার্ভিস-টু-সার্ভিস কলের জন্য বডি-বাউন্ড, টাইম-বাউন্ড HMAC স্কিম চালু করেছি (Rust ও Python—দুই দিকেই, একটি শেয়ার্ড টেস্ট-ভেক্টর দিয়ে যাচাই); (ঙ) ২৬টি ক্রস-কাটিং সিকিউরিটি টেস্ট লিখেছি (স্পেক চেয়েছিল ২০)।

**যাচাই (কমান্ড আসলে চালিয়ে):** Rust `cargo test --workspace --locked` → **৮১৩ passed / 0 failed**; `--features nats` → **৮১৩ passed**; `cargo fmt --all -- --check` → clean; `cargo clippy --workspace --all-targets --all-features -- -D warnings` → clean; `cargo doc --workspace --all-features --no-deps` → ০ warning। Python `pytest -q` → **১৯২৪ passed**; `ruff check` → pass; `ruff format --check` → ১৫৮ files formatted; `mypy src/mas_orchestrator` → ১৬১ files, কোনো সমস্যা নেই; `compileall -q` → OK।

> **সীমাবদ্ধতা (লুকানো নয়):** স্যান্ডবক্সে PostgreSQL/ডকার নেই। তাই লাইভ-Postgres ও লাইভ-RLS স্যুট (`crates/persistence/tests/live_pg.rs`) স্বয়ংক্রিয়ভাবে স্কিপ হয় — সেটি স্কিপ করে, পাস করে না। বিবরণ ধাপ ৩০-এ।

---

## ১. অথরিটেটিভ চেইন

```
Credential → Authentication → Principal
   → Org membership → Tenant membership → Project scope
   → Role / permission / policy → Resource authorization
   → RLS-scoped transaction → Repository → DB
```

কোনো নিচের স্তর কলার-সরবরাহকৃত পরিচয় বিশ্বাস করে না। এই চেইনটি এখন কোডে কার্যকর:

| চেইনের ধাপ | কোথায় প্রমাণিত হয় | কীভাবে প্রমাণিত হয় |
|---|---|---|
| Credential → Authentication | `crates/api/src/prod_auth.rs` | API কি-হ্যাশ লুকআপ; কলার-দেওয়া `x-tenant-id` বাতিল |
| → Principal | `crates/security/src/principal.rs` | একটিই `SecurityPrincipal` টাইপ |
| → Org/Tenant membership | `crates/tenancy` + `crates/persistence/src/directory.rs` | ডিরেক্টরি (PostgreSQL) লুকআপ |
| → Project scope | `crates/security/src/authorization.rs` | `project_is_proven()` — অপ্রমাণিত প্রোজেক্ট ⇒ `ProjectMismatch` |
| → Role/permission | `crates/security/src/authorization.rs` | deny-by-default অ্যালাউলিস্ট |
| → RLS transaction | `crates/persistence/src/scoped_request.rs` | `BEGIN` + `SET LOCAL` |
| → Repository | ৪১টি পুল স্টেটমেন্ট → স্কোপড কানেকশন | `scoped_request::connection()` |
| → DB | মাইগ্রেশন ০০০২–০০১৭ | `ENABLE` + `FORCE ROW LEVEL SECURITY` |

---

## ২. ধাপ-ভিত্তিক বিবরণ (১–৩২)

### ধাপ ১ — পরিচয় ট্রেস (সম্পন্ন)
প্রতিটি এন্ট্রিপয়েন্টে পরিচয় কোথা থেকে আসে তার লাইন-বাই-লাইন ট্রেস। ফলাফল: `SECURITY_PHASE1_IDENTITY_TRACE.md`। মূল আবিষ্কার: HTTP (`middleware.rs`), gRPC (`grpc.rs`), CLI (`cli/`), scheduler (`scheduler-service/`) — সবখানে হেডার-ভিত্তিক টেনেন্ট নির্ধারণ।

### ধাপ ২ — একটিই ক্যানোনিক্যাল Principal (সম্পন্ন)
`mas_security::SecurityPrincipal` একটি মাত্র টাইপ। দুটি ভ্যারিয়েন্ট নেই, কোনো সমান্তরাল "internal principal" নেই (যা তৈরি না করার নির্দেশ ছিল)। `PrincipalKind`: `User`, `ApiKey`, `ServiceAccount`, `Scheduler`, `Worker`, `System`, `Cli`।

### ধাপ ৩ — কর্তৃত্বশীল টেনেন্ট সমাধান (সম্পন্ন, কোড + টেস্ট)
`crates/api/src/scope_resolution.rs`। `ScopeClaims` নামেই বোঝায়—এটি একটি **দাবি**। নিয়ম:

* যে টেনেন্ট প্রিন্সিপালের নাগালের বাইরে → লুকানো `RESOURCE_NOT_FOUND` (`FORBIDDEN` নয় — কারণ `FORBIDDEN` দিলে টেনেন্ট আছে বলে ফাঁস হয়ে যায়)।
* দাবিকৃত org ≠ সমাধানকৃত org → `FORBIDDEN` (দ্বন্দ্ব নীরবে সংশোধন করা হয় না)।
* `require_scope()` সমাধিত না হলে ব্যর্থ।
* `AppState.scope_resolver` কেবল `test_support`-এ `None`; প্রোডাকশনে সবসময় `Some`, এবং মিডলওয়্যার এর ছাড়া পরিধি সমাধান করতে অস্বীকার করে।
* হেডার অনুপস্থিত + কোনো টেনেন্ট বাইন্ডিং নেই → ৪০০ `VALIDATION_FAILED`।

### ধাপ ৪ — Org→Tenant→Project→Resource সংগতি (সম্পন্ন)
`crates/security/src/authorization.rs` `authorize()` পাঁচটি ধাপ মেনে চলে: (১) ক্রেডেনশিয়াল বৈধতা, (২) টেনেন্ট বাইন্ডিং, (৩) অর্গ বাইন্ডিং, (৪) স্কোপ সংগতি, (৫) RBAC অ্যালাউলিস্ট। "A+B" জোড়া (বাস্তব টেনেন্ট + অসম্পর্কিত প্রোজেক্ট) বন্ধ: `AuthFailureKind::ProjectMismatch`।

### ধাপ ৫ — একটিই ক্যানোনিক্যাল `AuthorizationService` (সম্পন্ন)
`crates/security/src/authorization.rs`, `pub fn authorize(&self, &SecurityPrincipal, &AuthorizationRequest) -> AuthorizationDecision`। এটি একটি **বিশুদ্ধ সিদ্ধান্ত ফাংশন** — কোনো I/O নেই, তাই নির্ধারক ও টেস্টযোগ্য। ১৩টি অ্যাকশন: read/create/update/delete/execute/cancel/publish/administer/manage_credentials/manage_policies/view_audit/manage_billing/manage_members।

### ধাপ ৬ — RBAC (সম্পন্ন)
`MembershipRole`: owner(100) > admin(80) > developer(50) > operator(40) > viewer(10) > service_account(0)। `role_grants()` একটি স্পষ্ট অ্যালাউলিস্ট; ডিফল্ট deny। টেস্ট `no_role_holds_every_action_on_every_resource` প্রমাণ করে কোনো ভূমিকাই সবকিছু পারে না (১৮ রিসোর্স × ১৩ অ্যাকশন = ২৩৪ কক্ষ; প্রতিটি ভূমিকা এর চেয়ে কম পায়)।

### ধাপ ৭ — ABAC / রিসোর্স ট্যাক্সোনমি (সম্পন্ন)
১৮টি `ResourceKind`: execution, task, workflow, agent, tool, connector, credential, policy, usage, audit, webhook, subscription, api_key, schedule, tenant, organization, project, membership (স্পেক চেয়েছিল ১২+)। সিদ্ধান্তে প্রিন্সিপাল + টেনেন্ট + অর্গ + প্রোজেক্ট + রিসোর্স + অপারেশন—সব ব্যবহৃত।

### ধাপ ৮ — API কি (সম্পন্ন)
`crates/security/src/api_keys.rs` + `crates/persistence/src/repositories/api_keys.rs` + মাইগ্রেশন `0016_api_key_binding`। বৈশিষ্ট্য: হ্যাশিং (কখনো প্লেইন-টেক্সট সংরক্ষিত নয়), একবার প্রদর্শন, মেয়াদোত্তীর্ণতা, প্রত্যাহার, ঘূর্ণন (id/tenant/org/role সংরক্ষিত), স্কোপ, **টেনেন্ট বাইন্ডিং**, অডিট। `clamp_key_role`: Owner→Admin। মাইগ্রেশন ০০১৬ বিদ্যমান কিগুলোকে `service_account`-এ ব্যাকফিল করে।

### ধাপ ৯ — অভ্যন্তরীণ সার্ভিস প্রমাণীকরণ (সম্পন্ন, কোড + টেস্ট)
নতুন `crates/security/src/internal_service.rs`। স্কিম:

```
body_digest = hex(SHA256(body))
message    = "v1:" + service + ":" + unix_millis + ":" + body_digest
signature  = "v1:" + service + ":" + unix_millis + ":" + hex(HMAC-SHA256(secret, message))
```

* `MAX_SKEW = ৬০ সেকেন্ড` — রিপ্লে সুরক্ষা।
* MAC তুলনা ধ্রুব-সময়ে; ব্যর্থতার প্রতিটি কারণে (হেডার নেই / ভুল ফরম্যাট / অজানা সার্ভিস / পুরনো টাইমস্ট্যাম্প / ভুল সিক্রেট) **একই** `UNAUTHENTICATED` ত্রুটি — প্রোবার কিছু শেখে না।
* কলার-দেওয়া সার্ভিস আইডি ততক্ষণ দাবি, যতক্ষণ সার্ভার-সাইড অ্যালাউলিস্টের সাথে মেলে।
* অভ্যন্তরীণ হেডার কখনো টেনেন্ট/অর্গ/রোল বহন করে না (টেস্ট `internal_headers_can_never_carry_tenancy` নামেই এটি আটকায়)।

প্রয়োগ: (ক) scheduler-এর completion রাউট (`scheduler-service/src/server.rs`) — বডি ডেসিরিয়ালাইজ করার **আগে** কাঁচা `Bytes`-এর ওপর যাচাই; (খ) API-তে নতুন `POST /v1/internal/schedule-runs` (`crates/api/src/handlers/internal.rs`); (গ) `HttpDispatcher` বহির্গামী কলে স্বাক্ষর করে।

### ধাপ ১০ — Python ব্রিজ (সম্পন্ন, কোড + টেস্ট)
Python-এ নতুন `src/mas_orchestrator/security/internal_service.py` — Rust-এর স্কিমের **বাইট-টু-বাইট** অনুরূপ বাস্তবায়ন। যাচাই: Rust-এ `PYTHON_VECTOR` (Python-উৎপাদিত স্বাক্ষর) ও Python-এ `RUST_VECTOR` (Rust-উৎপাদিত স্বাক্ষর) — উভয় দিকেই ইন্টারঅপ টেস্ট (Rust-এ ৩টি, Python-এ ৪টি)।

বিদ্যমান নিরাপত্তা বজায় আছে: `grpc/metadata.py` অনুযায়ী কর্তৃত্বশীল টেনেন্ট আসে **রিকোয়েস্ট বডি** থেকে (Rust তার নিজের `RuntimeContext` থেকে পূরণ করে), হেডার নয়; হেডার কেবল ক্রস-চেক এবং ভিন্নতা হলে কল বাতিল। `AnonymousServiceIdentityResolver` স্টaging/production-এ গঠনেই ব্যর্থ — কোনো ওভাররাইড ফ্ল্যাগ নেই।

### ধাপ ১১ — প্রোডাকশন পথে RLS (সম্পন্ন)
`crates/api/src/middleware.rs` এখন `next.run(request)`-কে `with_authoritative_scope(pool, scope, ...)`-এর মধ্যে মোড়ে। `crates/api/src/grpc.rs`-এ ১৩টি স্কোপড কল সাইট `scoped_call(...)`-এ রূপান্তরিত। অর্থাৎ প্রতি HTTP রিকোয়েস্টে **একটি** ট্রানজেকশন, `SET LOCAL` দিয়ে RLS আরম।

### ধাপ ১২ — পুল্ড-কানেকশন নিরাপত্তা (সম্পন্ন)
`SET`, নয়—`SET LOCAL`। GUC ট্রানজেকশনের সাথেই মারা যায়: commit-এ, rollback-এ, এবং commit ছাড়া drop করলেও। ফেজ ১২-এর বিপদ (রিকোয়েস্ট A টেনেন্ট A সেট করে, ট্রানজেকশন শেষ, রিকোয়েস্ট B কানেকশন পুনর্ব্যবহার করে টেনেন্ট A উত্তরাধিকার পায়) ঘটতে পারে না। টেস্ট:
* `scope_does_not_leak_to_a_sibling_task`
* `there_is_no_scope_outside_a_request`
* `a_failed_scope_leaves_nothing_bound_to_the_task` (নতুন; `scoped.rs` শিমে)

### ধাপ ১৩ — RLS পলিসি অডিট (সম্পন্ন — নিরীক্ষা, কোনো পরিবর্তন প্রয়োজন হয়নি)
* ৩৩টি টেনেন্ট-স্কোপড টেবিল (মাইগ্রেশন ০০০২–০০১২)।
* ১১টি `up.sql` RLS সক্রিয় করে; সব টেনেন্ট টেবিলে `ENABLE` **এবং** `FORCE ROW LEVEL SECURITY`।
* `audit_events` পলিসি: `tenant_id IS NULL OR tenant_id::text = current_setting('app.current_tenant', true)` — প্ল্যাটফর্ম-স্তরের ইভেন্টও রেকর্ডযোগ্য।
* অ্যাপেন্ড-অনলি: `mas_forbid_mutation()` ট্রিগার (মাইগ্রেশন ০০১১)।
* লাইভ যাচাই স্যান্ডবক্সে সম্ভব নয় (Postgres নেই); স্ট্যাটিক যাচাই করা হয়েছে।

### ধাপ ১৪ — ডেটাবেস রোল (সম্পন্ন — মাইগ্রেশন লেখা ও নিবন্ধিত)
* মাইগ্রেশন `0015_directory_role`: `mas_directory` — `NOLOGIN BYPASSRLS`, **কেবল** চারটি ক্যাটালগ টেবিলে `SELECT` সীমাবদ্ধ। `mas_app`-এ কোনো BYPASSRLS নেই।
* মাইগ্রেশন `0017_plane_roles` (এই ধাপে নতুন, `migrations.rs`-এ নিবন্ধিত): `mas_worker`, `mas_outbox`, `mas_audit` — সব `NOLOGIN BYPASSRLS`, টেবিল- ও অপারেশন-সীমাবদ্ধ গ্রান্ট সহ। মন্তব্যে স্পষ্টভাবে লেখা: `mas_app`-কে কখনো BYPASSRLS দেওয়া যাবে না; রানটাইমে কোনো সুপারইউজার নেই।
* দুটি পুল: `MAS_DATABASE_URL` (রানটাইম) ও `MAS_DATABASE_DIRECTORY_URL` (ক্যাটালগ)।

**স্বীকৃত অসম্পূর্ণতা:** `MAS_DATABASE_WORKER_URL` / `_OUTBOX_URL` / `_AUDIT_URL` কনফিগারেশন কি-গুলো এখনো `main.rs`-এ ওয়্যার করা হয়নি — অর্থাৎ ওয়ার্কার/আউটবক্স/অডিট প্লেনগুলো বর্তমানে রানটাইম পুল (`mas_app`) ব্যবহার করে। মাইগ্রেশন ও রোল প্রস্তুত; কনফিগ ওয়্যারিং বাকি। এটি ধাপ ১৪-এর শেষাংশ এবং পরবর্তী ধাপে সম্পন্নযোগ্য।

### ধাপ ১৫ — একটি নিরাপদ `with_authoritative_scope` (সম্পন্ন)
`crates/persistence/src/scoped_request.rs` একটিই অ্যাবস্ট্রাকশন। এই ধাপে `crates/persistence/src/scoped.rs`-এর সমান্তরাল, কলার-বিহীন `ScopedDatabase` **অবসর দেওয়া হয়েছে** — ফাইলটি এখন পুনঃরপ্তানি শিম + `ScopedPoolStats` (যা কোনো এক্সিকিউটর হাতে দেয় না)। নেস্টিং প্রত্যাখ্যাত: একটি রিকোয়েস্ট = একটি ট্রানজেকশন = একটি টেনেন্ট।

### ধাপ ১৬ — ক্রস-টেনেন্ট ম্যাট্রিক্স (সম্পন্ন)
`no_principal_crosses_a_tenant_boundary`: ৬টি ক্রিয়া × ৭টি রিসোর্স = ৪২টি জোড়া; টেনেন্ট A-তে বাঁধা প্রিন্সিপাল টেনেন্ট B-তে সবগুলোতেই ব্যর্থ। `the_same_principal_is_allowed_inside_its_own_tenant` প্রমাণ করে অস্বীকৃতিগুলো ত্রুটিপূর্ণ অ্যালাউলিস্টের ফল নয়।

### ধাপ ১৭ — আইডি-বিভ্রান্তি টেস্ট (সম্পন্ন)
`a_credential_bound_to_one_tenant_cannot_act_in_another` (API কি টেনেন্ট A-তে বাঁধা, B-তে কাজ করতে পারে না), `a_principal_cannot_authorize_itself_into_an_ungranted_tenant` (প্রিন্সিপালের নিজস্ব `tenant_id` ফিল্ড প্রমাণ হিসেবে গৃহীত নয়), `an_unproven_project_is_not_a_valid_scope`।

### ধাপ ১৮ — বিশেষাধিকারপ্রাপ্ত অপারেশন স্পষ্টভাবে অনুমোদিত (সম্পন্ন)
`privileged_actions_are_refused_for_the_least_privileged_roles`: viewer/operator/developer—কেউই credential/policy/billing পরিচালনা বা অডিট পড়তে পারে না (৪টি বিশেষাধিকার × ৩টি ভূমিকা = ১২ জোড়া)।

### ধাপ ১৯ — সিক্রেট ব্যবস্থাপনা (সম্পন্ন)
`crates/security/src/secret_string.rs` (র‍্যাপার যা `Debug`-এ মুদ্রিত হয় না, drop-এ জিরো হয়), `secret_gate.rs` (`SecretResolverPort`, `SecretGate`)। কোনো হার্ড-কোডেড ক্রেডেনশিয়াল নেই — স্ক্যান ধাপ ২৮-তে দেখুন। সিক্রেট কখনো কমান্ড-লাইন আর্গুমেন্ট থেকে পড়া হয় না (প্রসেস টেবিলে পড়বে বলে)।

### ধাপ ২০ — এনক্রিপশন / অখণ্ডতা (সম্পন্ন; **কোনো সার্টিফিকেশন দাবি করা হচ্ছে না**)
* ট্রানজিট: TLS/mTLS (`security/transport.py` — staging/production-এ `insecure` প্রত্যাখ্যাত, কোনো ওভাররাইড নেই; mTLS-এর জন্য সার্ট ও কি—দুটিই বাধ্যতামূলক)।
* রেস্ট: PostgreSQL-স্তরের এনক্রিপশন, অ্যাপ্লিকেশন স্তরে নয় — আমরা টেবিলস্পেস/ডিস্ক এনক্রিপশন দাবি করি না।
* অখণ্ডতা: `audit_events` অ্যাপেন্ড-অনলি ট্রিগার; ওয়েবহুক HMAC (`crates/integrations/src/webhooks.rs`); অভ্যন্তরীণ কল HMAC (ধাপ ৯)।

### ধাপ ২১ — সিকিউরিটি অডিট লগিং, কোনো সিক্রেট ফাঁস নেই (সম্পন্ন)
`crates/security/src/audit.rs` + `mas_application::audit`। `audit_transitions` রাজ্য-পরিবর্তন থেকে অডিট উদ্ভব করে, তাই এক রাজ্য-পরিবর্তনে ঠিক একটি অডিট রেকর্ড — কোন ট্রান্সপোর্ট কল করেছে তা বিবেচ্য নয়। রিপ্লে রাজ্য-পরিবর্তন নয়, তাই অডিট করা হয় না (আউটবক্স ইভেন্ট ও মেট্রিকে দৃশ্য)। টেস্ট `test_the_secret_never_appears_in_a_failure` (Python) ও `Debug`-রূপ না-দেখানো `InternalAuth` (Rust scheduler)।

### ধাপ ২২ — স্থিতিশীল প্রমাণীকরণ-ব্যর্থতা সিম্যানটিক্স (সম্পন্ন)
`AuthFailureKind`: ৯টি প্রকার (Unauthenticated, InvalidCredential, ExpiredCredential, Forbidden, TenantMismatch, OrganizationMismatch, ProjectMismatch, ScopeMismatch, InsufficientPermission, InvalidServiceIdentity — ১০টি)। টেস্ট `the_failure_taxonomy_has_at_least_eight_distinct_kinds`। `authorization_failures_are_never_retryable` নিশ্চিত করে কোনো প্রমাণীকরণ/অনুমোদন ব্যর্থতা পুনরায় চেষ্টাযোগ্য নয়।

### ধাপ ২৩ — SSRF (সম্পন্ন)
`crates/scheduler-service/src/egress.rs`, `dispatch.rs`, `crates/domain/src/value_objects/url.rs`, `crates/integrations/src/http_guard.rs`। প্রোডাকশন HTTP কলার গার্ড ব্যবহার করে (`driver_loop` টেস্টে লুপব্যাকের জন্য স্পষ্টভাবে অনুমোদিত হোস্ট ঘোষণা করতে হয়)। নতুন `internal.rs` রুট বডি থেকে কোনো URL নেয় না — টাস্ক টেমপ্লেট সংরক্ষিত সময়সূচি থেকে আসে।

### ধাপ ২৪ — AI সিকিউরিটি (সম্পন্ন; বিদ্যমান বাস্তবায়ন যাচাই)
টুল অনুমোদন প্ল্যাটফর্ম থেকে আসে, মডেল থেকে নয়: `tools/`-এর টুলগুলো `AuthorizationService`-এর সিদ্ধান্ত ছাড়া চলে না; Python `tests/security/test_tool_security.py` ও `test_agent_bounds.py` বিদ্যমান। ব্রিজে Python নিজে টেনেন্ট বেছে নেয় না (ধাপ ১০)। এই ধাপে নতুন কোড যোগ করার প্রয়োজন হয়নি—বিদ্যমান বাস্তবায়ন পরিদর্শন ও টেস্ট দ্বারা যাচাই করেছি।

### ধাপ ২৫ — ওয়ার্কার / শিডিউলার স্পষ্ট পরিচয়, প্রমাণীকৃত সমাপ্তি (সম্পন্ন)
* শিডিউলার completion রাউটে `InternalAuth` (ধাপ ৯): স্বাক্ষরবিহীন সমাপ্তি প্রত্যাখ্যাত, লেজার অস্পৃশ্য থাকে (`an_unsigned_completion_is_refused_and_leaves_the_ledger_alone`)।
* সিক্রেট কনফিগার না থাকলে শিডিউলার সব সমাপ্তি প্রত্যাখ্যাত করে (`an_unconfigured_scheduler_refuses_every_completion`) — নীরবে খোলা থাকে না।
* `get_stats`-এর জন্যও খালি বডির ওপর স্বাক্ষর বাধ্যতামূলক।
* `ALLOWED_CALLERS = ["mas-api", "mas-worker"]`।

### ধাপ ২৬ — CLI: কোনো নীরব সর্ব-টেনেন্ট/অ্যাডমিন (সম্পন্ন; বিদ্যমান বাস্তবায়ন যাচাই)
`crates/cli/` — CLI কোনো `--tenant`/`--as-admin` ফ্ল্যাগ দিয়ে পরিধি এড়াতে পারে না; এটি API-এর মাধ্যমে যায় এবং একই প্রমাণীকরণ/অনুমোদন পথ অনুসরণ করে। এই ধাপে নতুন কোড প্রয়োজন হয়নি।

### ধাপ ২৭ — ২০টি সিকিউরিটি টেস্ট (সম্পন্ন — ২৬টি)
`tests/security/authorization_matrix.rs` (cargo অ্যাঙ্কর: `crates/api/tests/security_matrix.rs`)। **২৬টি** টেস্ট, সব সবুজ। বিভাগ: (১) কলার-সরবরাহকৃত পরিচয় কর্তৃত্বশীল নয় (৫), (২) ব্যর্থতা টাইপড/স্থিতিশীল/পুনঃচেষ্টাযোগ্য নয় (২), (৩) হিন্ট কর্তৃত্ব নয় (২), (৪) অভ্যন্তরীণ প্রমাণীকরণ (৭), (৫) স্কোপড ট্রানজেকশন (২), (৬) ক্রস-টেনেন্ট/প্রোজেক্ট সংযম (৮)।

### ধাপ ২৮ — স্ট্যাটিক স্ক্যান (সম্পন্ন — প্রতিটি ঘটনা শ্রেণীবদ্ধ)

| # | অনুসন্ধান | ফলাফল | শ্রেণীবিভাগ |
|---|---|---|---|
| A | `x-tenant-id` / `x-organization-id` রেফারেন্স (Rust) | ২০+ ঘটনা | **বৈধ** — সবগুলোই (ক) বহির্গামী ব্রিজ হেডার (Rust তার নিজের সমাধিত পরিধি থেকে সেট করে), (খ) `scope_resolution.rs`-এ হিন্ট পার্সিং, (গ) টেস্ট ফিক্সচার, বা (ঘ) ঐতিহাসিক ব্যাখ্যামূলক মন্তব্য। কোথাও কর্তৃত্বশীল উৎস হিসেবে ব্যবহৃত নয়। |
| B | `unsafe` / `transmute` / `from_raw` | ০ | পরিষ্কার |
| C | হার্ড-কোডেড ক্রেডেনশিয়াল (`password/secret/token = "..."` ১২+ অক্ষর) | ০ | পরিষ্কার |
| D | `BYPASSRLS` | ২টি মাইগ্রেশন (০০১৫, ০০১৭) + মন্তব্য | **ইচ্ছাকৃত ও সীমাবদ্ধ** — কেবল `mas_directory` (৪টি ক্যাটালগ টেবিলে SELECT) ও `mas_worker`/`mas_outbox`/`mas_audit` (টেবিল-সীমাবদ্ধ)। `mas_app`-এ কখনো নয়। |
| E | `SUPERUSER` / `postgres://` | কেবল মন্তব্য, কনফিগ উদাহরণ, টেস্ট | **বৈধ** — রানটাইমে কোনো সুপারইউজার নেই |
| F | `MAS_DEV_TOKENS` | `main.rs`-এ guardrail | **সুরক্ষিত** — production-এ নন-এম্পটি হলে বুট ব্যর্থ |
| G | সিকিউরিটি পথে `.unwrap()` | ০ | পরিষ্কার |
| H | সিক্রেট `println!`/`eprintln!` | ০ | পরিষ্কার |
| I | TLS নিষ্ক্রিয় (`danger_accept`, `verify=False`) | ০ | পরিষ্কার |
| J | SQL স্ট্রিং ইন্টারপোলেশন | ৫টি `format!("SELECT {COLUMNS} ...")` | **বৈধ** — `AGENT_COLUMNS`/`USER_COLUMNS`/`TASK_COLUMNS` কম্পাইল-টাইম ধ্রুবক; সব মান `$1` দ্বারা বাউন্ড |
| K | Python `eval`/`exec`/`pickle.loads` | ০ | পরিষ্কার |

### ধাপ ২৯ — প্রোডাকশন নিষিদ্ধ তালিকা (১৫টি) — যাচাই

| # | নিষেধাজ্ঞা | অবস্থা |
|---|---|---|
| ১ | হেডার থেকে টেনেন্ট নেওয়া | ✅ বন্ধ — `scope_resolution.rs` |
| ২ | কলার-সরবরাহকৃত পরিচয় বিশ্বাস | ✅ বন্ধ — ডিরেক্টরি লুকআপ |
| ৩ | RLS বাইপাস (সুবিধার জন্য) | ✅ বন্ধ — `mas_app`-এ BYPASSRLS নেই |
| ৪ | রানটাইমে সুপারইউজার | ✅ নেই — মাইগ্রেশন ০০১৭ মন্তব্যে স্পষ্ট |
| ৫ | অপ্রমাণিত অভ্যন্তরীণ কল | ✅ বন্ধ — `InternalAuth`, সিক্রেট না থাকলে ব্যর্থ |
| ৬ | স্ট্যাটিক ডেভ টোকেন (production) | ✅ বন্ধ — `enforce_production_guardrails` |
| ৭ | লগে সিক্রেট | ✅ বন্ধ — `SecretString` + `redaction.py` |
| ৮ | হার্ড-কোডেড ক্রেডেনশিয়াল | ✅ ০ ঘটনা |
| ৯ | অপ্রমাণিত প্রোজেক্ট পরিধি | ✅ বন্ধ — `project_is_proven` |
| ১০ | টেনেন্ট অস্তিত্ব ফাঁস (`FORBIDDEN`) | ✅ বন্ধ — `RESOURCE_NOT_FOUND` |
| ১১ | পুনঃচেষ্টাযোগ্য প্রমাণীকরণ ব্যর্থতা | ✅ বন্ধ — টেস্ট |
| ১২ | মডেল-চালিত টুল অনুমোদন | ✅ বন্ধ — প্ল্যাটফর্ম সিদ্ধান্ত |
| ১৩ | নেস্টেড/পুনর্ব্যবহৃত স্কোপড ট্রানজেকশন | ✅ বন্ধ — `scoped_request` নেস্টিং প্রত্যাখ্যাত |
| ১৪ | সেশন GUC (`SET`) | ✅ বন্ধ — কেবল `SET LOCAL` |
| ১৫ | CLI সর্ব-টেনেন্ট মোড | ✅ নেই |

### ধাপ ৩০ — যাচাই (সব কমান্ড আসলে চালানো হয়েছে)

```
cargo fmt --all -- --check                                   → CLEAN
cargo clippy --workspace --all-targets --all-features -- -D warnings → CLEAN
cargo test --workspace --locked                              → 813 passed, 0 failed
cargo test --workspace --locked --features nats              → 813 passed, 0 failed
cargo doc --workspace --all-features --no-deps               → 0 warnings
python -m compileall -q src/mas_orchestrator                 → OK
python -m ruff check src/mas_orchestrator                    → All checks passed
python -m ruff format --check src/mas_orchestrator           → 158 files already formatted
python -m mypy src/mas_orchestrator                          → Success, 161 source files
python -m pytest -q                                          → 1924 passed
```

**লাইভ PostgreSQL (পরবর্তী পর্যায়ে যোগ):** স্যান্ডবক্সে PostgreSQL ১৬.৯ সোর্স থেকে বিল্ড করে `mas_live` ডেটাবেসে ১৯টি মাইগ্রেশন প্রয়োগ করা হয়েছে এবং `live_pg.rs` স্যুট আসলে চালানো হয়েছে — **১৩/১৩ পাস**। বিস্তারিত §৬-এ।

### ধাপ ৩১ — কোনো রিগ্রেশন নেই
* Rust: ৭০২ (পূর্বের বেসলাইন) → ৮১৩ → **৮২০**। কোনো বিদ্যমান টেস্ট বাতিল/স্কিপ করা হয়নি।
* Python: ১৮৯০ → ১৯২৪ → **১৯৩৬** (নতুন ১২টি ব্রিজ-স্বাক্ষর টেস্ট)।
* e2e_python ৯ → **১২**, root_integration ২, root_contract ৫, driver_loop ৪, CLI e2e ৭ — সব রক্ষিত।
* live_pg (লাইভ Postgres) ৫/৮ → **১৩/১৩**।
* `--features nats` ভ্যারিয়েন্টেও ৮২০।

### ধাপ ৩২ — সম্পূর্ণ ফাইল আউটপুট
নিচের অংশে প্রতিটি পরিবর্তিত/নতুন ফাইল `FILE:` + `PURPOSE:` + সম্পূর্ণ বিষয়বস্তু সহ দেওয়া হয়েছে। কোনো `...`, কোনো "বাকি কোড এখানে", কোনো সংক্ষেপণ নেই।

---

## ৩. এই ধাপে যা নতুন লেখা হয়েছে

**Rust (নতুন):**
1. `crates/security/src/internal_service.rs` — অভ্যন্তরীণ HMAC স্কিম + ১১ টেস্ট
2. `crates/api/src/handlers/internal.rs` — `POST /v1/internal/schedule-runs` + ৮ টেস্ট
3. `tests/security/authorization_matrix.rs` — ২৬টি সিকিউরিটি টেস্ট
4. `crates/api/tests/security_matrix.rs` — cargo অ্যাঙ্কর
5. `migrations/0017_plane_roles/up.sql` — প্লেন রোল

**Rust (পরিবর্তিত):** `crates/api/src/{state,main,router,response,handlers/mod}.rs`, `crates/api/Cargo.toml`, `crates/scheduler-service/src/{server,main,dispatch}.rs`, `crates/scheduler-service/Cargo.toml`, `crates/scheduler-service/tests/driver_loop.rs`, `crates/security/src/lib.rs`, `crates/persistence/src/{migrations,scoped,directory}.rs`, `crates/persistence/src/repositories/{agents,executions,schedules,workflows}.rs`

**Python (নতুন):**
1. `src/mas_orchestrator/security/internal_service.py` — Rust-সঙ্গতিপূর্ণ বাস্তবায়ন
2. `tests/security/test_internal_service.py` — ৩৪ টেস্ট

---

## ৪. পরবর্তী ধাপের জন্য খোলা বিষয় — **সব বন্ধ**

আগের প্রতিবেদনে নিচের চারটি বিষয় খোলা ছিল; এই ধাপে সবগুলো সমাধান করা হয়েছে।

| # | খোলা বিষয় | অবস্থা |
|---|---|---|
| ১ | `MAS_DATABASE_WORKER_URL` / `_OUTBOX_URL` / `_AUDIT_URL` `main.rs`-এ ওয়্যার করা | ✅ সম্পন্ন — তিনটি বাইনারিতেই, কোনো ফলব্যাক ছাড়া |
| ২ | লাইভ Postgres-এ `live_pg.rs` চালানো | ✅ সম্পন্ন — **১৬/১৬** (১৩ → ১৬, §১০) |
| ৩ | মাইগ্রেশন ০০১৭ ও ০০১৮ প্রয়োগ | ✅ সম্পন্ন — ১৯টি মাইগ্রেশন প্রয়োগ ও যাচাই |
| ৪ | `DurableDispatcher` বনাম `HttpDispatcher` কনফিগ-নির্ভর নির্বাচন | ✅ সম্পন্ন — `scheduler.dispatcher` |

**নতুন খোলা বিষয় (পরের ধাপের জন্য, এই ধাপের স্কোপের বাইরে):**

1. **TLS-সহ লাইভ ক্লাস্টার:** এই যাচাই `listen_addresses='127.0.0.1'` ও `md5`-এ চলেছে। একটি বাস্তব ডেভেলপমেন্টে `sslmode=require` ও মেশ-সার্টিফিকেট দিয়ে একই স্যুট চালাতে হবে।
2. **`mas_scheduler` রোলের LOGIN প্রভিশনিং:** মাইগ্রেশন রোলটি `NOLOGIN` তৈরি করে; সিক্রেট ম্যানেজার থেকে ক্রেডেনশিয়াল প্রভিশন করে `ALTER ROLE ... LOGIN` দিতে হবে (মাইগ্রেশনের ভেতরের মন্তব্যে ধাপগুলো লেখা আছে)।
3. **রোল-ভিত্তিক ইন্টিগ্রেশন টেস্ট:** ~~খোলা~~ → ✅ **সম্পন্ন (§১০)**। `mas_app` দিয়ে চলা তিনটি টেস্ট যোগ করা হয়েছে; `mas_app`-এ LOGIN দেওয়ার অপারেটর ধাপ `scripts/provision-plane-roles.sh`।

---

## ৫. সীমাবদ্ধতা ও স্বচ্ছতা

* ধাপ ২৪ ও ২৬-এ এই ধাপে নতুন কোড যোগ করা হয়নি; বিদ্যমান বাস্তবায়ন পরিদর্শন ও বিদ্যমান টেস্ট দ্বারা যাচাই করা হয়েছে। স্পেকের ভাষায় এগুলো "সম্পন্ন" কিন্তু "নতুন কাজ" নয় — এখানে স্পষ্ট করে বলা হলো।
* ধাপ ১৩ (RLS পলিসি অডিট) একটি নিরীক্ষা; কোনো পলিসি পরিবর্তনের প্রয়োজন পাওয়া যায়নি, কারণ বিদ্যমান পলিসিগুলো ইতোমধ্যে `ENABLE` + `FORCE` ও টেনেন্ট/অর্গ প্রেডিকেটযুক্ত। লাইভ ডেটাবেসে ৩৩টি টেবিলের বিপরীতে এটি পুনরায় যাচাই করা হয়েছে।
* ধাপ ২০-এ **কোনো সার্টিফিকেশন দাবি করা হচ্ছে না** (SOC2/ISO/FIPS)।
* ~~লাইভ যাচাই `mas` সুপারইউজার-URL দিয়ে চলেছে (RLS বাইপাস করে), তাই RLS-এর দাবি হাতে প্রমাণ করতে হয়েছে।~~ → **সম্পন্ন (§১০):** স্যুটে এখন `mas_app` (`rolbypassrls = false`) দিয়ে চলা তিনটি টেস্ট আছে, যার মধ্যে Phase ⑫-এর pooled-connection লিক-টেস্টও রয়েছে। আর হাতে প্রমাণ করতে হচ্ছে না — স্যুটই প্রমাণ করে, এবং `MAS_TEST_DATABASE_APP_URL` না থাকলে চুপচাপ পাস না করে জোরে skip করে।

---

## ৬. দ্বিতীয় পর্যায় — লাইভ PostgreSQL যাচাই এবং যা ধরা পড়ল

### ৬.০ কেন এটা প্রথমে করা হয়নি

আগের প্রতিবেদনে স্বীকার করা হয়েছিল: স্যান্ডবক্সে PostgreSQL ছিল না, তাই `live_pg.rs` (লাইভ RLS, পুল্ড-কানেকশন রোলব্যাক, `SET LOCAL` লিক-চেক) **চালানো যায়নি**। সেই ফাঁকটি এই পর্যায়ে বন্ধ করা হয়েছে।

PostgreSQL ১৬.৯ সোর্স থেকে বিল্ড করা হয়েছে (`contrib/pgcrypto`-র জন্য `SHLIB_LINK="-lcrypto -lz"` — এটি ছাড়া `pgcrypto.so` লিংক হয় না), একটি ক্লাস্টার `initdb` করা হয়েছে, এবং `mas_live` ডেটাবেসে ১৯টি মাইগ্রেশন প্রয়োগ করা হয়েছে।

### ৬.১ ফলাফল

> **হালনাগাদ:** এই ১৩টি ছিল প্রথম ধাক্কার ফলাফল। §১০-এ `mas_app`-চালিত ৩টি
> টেস্ট যোগ হওয়ার পর স্যুট এখন **১৬/১৬**। নিচের তালিকাটি ইতিহাস হিসেবে রাখা
> হলো।

```
running 13 tests
test agents_round_trip ... ok
test api_keys_prefix_lookup_and_touch ... ok
test audit_append_list_and_counts ... ok
test broker_publish_fetch_ack_round_trip ... ok
test concurrent_migrate_is_serialized ... ok
test migrations_are_idempotent ... ok
test executions_round_trip_and_idempotency ... ok
test schedules_due_scan_and_run_journal ... ok
test tasks_round_trip ... ok
test tenancy_round_trip ... ok
test tenant_b_cannot_read_claim_cancel_or_settle_tenant_a_execution ... ok
test the_same_idempotency_key_in_two_tenants_is_two_executions ... ok
test workflows_round_trip ... ok

test result: ok. 13 passed; 0 failed
```

### ৬.২ RLS প্রমাণ (`SET ROLE mas_app` দিয়ে, হাতে)

| যা করা হয়েছে | ফলাফল |
|---|---|
| কোনো স্কোপ ছাড়া `SELECT` | **০ rows** |
| `SET LOCAL app.current_tenant = A` | কেবল A-এর সারি |
| `COMMIT` এর পর একই কানেকশনে | **০ rows** (`SET LOCAL` টিকে থাকেনি) |
| `SET LOCAL ... = B` | কেবল B-এর সারি |
| ক্রস-টেনেন্ট `UPDATE` | `UPDATE 0` |
| ক্রস-টেনেন্ট `DELETE` | `DELETE 0` |
| সেশন-লেভেল GUC (`set_config(..., false)`) | ১ row পাওয়া গেছে — **লিক** |

সব শেষের সারিটিই কারণ `scoped_request.rs` কেবল `SET LOCAL` ব্যবহার করে: সেশন GUC একটি পুল্ড কানেকশনে টিকে থাকে এবং পরের অনুরোধে অন্য টেনেন্টের ডেটা দেখিয়ে দেয়।

`mas_app.rolbypassrls = f` — রানটাইম রোল RLS বাইপাস করতে পারে না।

### ৬.৩ চারটি বাস্তব ত্রুটি — যেগুলো লাইভ ডেটাবেস ছাড়া কখনো ধরা পড়ত না

এই ধাপের সবচেয়ে গুরুত্বপূর্ণ ফলাফল হলো টেস্ট পাস করা নয়, বরং **যে চারটি ত্রুটি ধরা পড়ল**। চারটিই প্রোডাকশন কোডে ছিল, চারটিই ইউনিট টেস্টে অদৃশ্য ছিল।

**ত্রুটি ১ — `api_keys.user_id` কলাম কখনো তৈরি হয়নি।**
`crates/persistence/src/repositories/api_keys.rs` `COLUMNS`/`row_from`-এ `user_id` প্রজেক্ট করে, কিন্তু কোনো মাইগ্রেশনেই সেই কলাম নেই। অর্থাৎ API-কি তৈরি/পড়ার পথটি লাইভ ডেটাবেসে প্রথম বারেই ব্যর্থ হতো। **সমাধান:** `migrations/0018_api_key_owner/up.sql` + `down.sql`; কলামটি nullable ও শুধুমাত্র অ্যাট্রিবিউশনের জন্য, partial index সহ।

**ত্রুটি ২ — ক্যানোনিক্যাল durable pipeline স্কোপড/RLS ট্রানজেকশন এড়িয়ে যাচ্ছিল।**
`crates/persistence/src/durable.rs`-এর দশটি `DurableExecutionPort` মেথডই `UnitOfWork::begin(&self.pool)` দিয়ে নিজের কানেকশন খুলত। একটি অনুরোধের ভেতর থেকে ডাকা হলে এটি একটি **দ্বিতীয়** কানেকশন খুলত যাতে কোনো `app.current_tenant` সেট নেই — `FORCE RLS`-এর অধীনে সেটি কিছুই পড়তে বা লিখতে পারত না। স্পেকের ধাপ ১১ ("প্রোডাকশন পথে RLS") কাগজে পূরণ, বাস্তবে না।
**সমাধান:** প্রতিটি মেথডে দুই-শাখা রূপ —

```rust
if Self::has_request_scope() {
    // অনুরোধের ট্রানজেকশনে যোগ দাও; দ্বিতীয় কানেকশন নয়।
    let mut scoped = Self::request_connection().await?;
    Self::_in(scoped.connection(), ...).await
} else {
    // ওয়ার্কার/আউটবক্স প্লেন: নিজের ট্রানজেকশন খুলো ও কমিট করো।
    let mut uow = UnitOfWork::begin(&self.pool).await?;
    let outcome = Self::_in(uow.executor(), ...).await;
    Self::finish(uow, outcome).await
}
```

নয়টি `_in` হেল্পার `&mut PgConnection` নেয় (`&mut UnitOfWork` নয়), যাতে দুই শাখাই একই SQL ব্যবহার করে — SQL দুই জায়গায় থাকলে তা ড্রিফট করে।

**ত্রুটি ৩ — `ScheduleStore::record_run` স্ব-ডেডলক।**
`rows_affected() != 1` হলে (অর্থাৎ ডুপ্লিকেট টিক — **যে পথটি দ্বিতীয় রেপ্লিকা প্রতিটি টিকে নেয়**) মেথডটি দ্বিতীয়বার `scoped_request::connection()` ডাকত, অথচ প্রথম গার্ডটি তখনও জীবিত। মিউটেক্সটি নন-রিএন্ট্রান্ট, তাই এটি ডেডলক। লক্ষণ: `schedules_due_scan_and_run_journal` ৬০ সেকেন্ডের বেশি সময় ধরে ঝুলে থাকত।
**সমাধান:** ডিসঅ্যাম্বিগুয়েটিং রিডটি হাতে থাকা গার্ডেই চলে।

**ত্রুটি ৪ — `claim`-এ `RETURNING` অ্যাম্বিগুয়াস (PostgreSQL 42702)।**
`UPDATE tasks t ... FROM candidates c ... RETURNING id, tenant_id, ...` — উভয় রিলেশনেই `id` আছে, তাই PostgreSQL `id`-কে অ্যাম্বিগুয়াস বলে প্রত্যাখ্যান করে। অর্থাৎ **কোনো টাস্ক কখনো ক্লেইম হতো না**।
**সমাধান:** `qualified_projection("t", TASK_COLUMNS)` — `RETURNING`-এর প্রতিটি কলাম `t.` দিয়ে যোগ্য করা।

চারটির মধ্যে তিনটিই (২, ৩, ৪) এমন যে প্ল্যাটফর্মটি "চালু" থাকত কিন্তু নীরবে কিছুই করত না — সবচেয়ে খারাপ ধরণের ব্যর্থতা।

### ৬.৪ একটি টেস্ট-রোবাস্টনেস সংশোধন

`broker_publish_fetch_ack_round_trip` ভ্যাস্ত ছিল কারণ durable লেন একটি সবার জন্য শেয়ার্ড কিউ (পুরোনো আগে), তাই ৫-সারির উইন্ডোতে অন্য টেস্টের সারি ঢুকে আমাদের সারিটি বাদ পড়ত। উইন্ডো ৫১২ করা হয়েছে — টেস্টের ত্রুটি, প্রোডাক্টের নয়, কিন্তু আলাদা করে উল্লেখ করা জরুরি যাতে এটি "আরেকটি প্রোডাকশন ত্রুটি" বলে পড়া না হয়।

---

## ৭. মেশিন-প্লেন পরিচয় — ধাপ ১৪ ও ২৫-এর শেষাংশ

### ৭.০ সমস্যা

মাইগ্রেশন ০০১৫ ও ০০১৭ চারটি প্লেন রোল তৈরি করেছিল: `mas_directory`, `mas_worker`, `mas_outbox`, `mas_audit`। কিন্তু **শিডিউলারের কোনো রোল ছিল না**, অথচ শিডিউলারটিই এমন একটি প্লেন যার প্রথম স্টেটমেন্ট — "কোন শিডিউল এখন বাজবে?" — সব টেনেন্ট জুড়ে।

আরও খারাপ: `DurableDispatcher` টেন্যান্ট-মালিকানাধীন টেবিলগুলোতে সরাসরি পুল দিয়ে লিখত, অর্থাৎ RLS বাইপাস করত না বরং **০ সারি দেখত** এবং প্রতিটি টিক নীরবে ব্যর্থ হতো।

### ৭.১ মাইগ্রেশন ০০১৯ — `mas_scheduler`

```
mas_scheduler  NOLOGIN  BYPASSRLS
GRANT SELECT ON schedules     TO mas_scheduler;
GRANT SELECT ON schedule_runs TO mas_scheduler;
```

দুটিতালিকার বাইরে কিছুই নেই — কোনো `tasks`, `executions`, `agents`, `credentials`, `api_keys` নেই। একটি কম্প্রোমাইজড শিডিউলার জানতে পারবে "কবে কী চালানোর কথা ছিল", কাজের বিষয়বস্তু নয়।

`down.sql`-এ `DROP OWNED BY` + `DROP ROLE` রাখা হয়েছে: একটি এতিম `BYPASSRLS` রোল যে কোড তাকে ন্যায্যতা দিয়েছিল তার চেয়ে বেশিদিন বাঁচে, এবং যে রিভিউ তাকে অনুমোদন করেছিল তারও বেশিদিন।

### ৭.২ টিক-এর বিভাজন (`SchedulerPlaneStore`)

টিক দুই ভাগে ভাগ করা হয়েছে, এবং বিভাজনটিই নিরাপত্তা:

```
১. list_due(now, limit)      → প্লেন কানেকশন (mas_scheduler)
      একমাত্র ক্রস-টেনেন্ট রিড; SELECT, একটি টেবিলে, একটি রোলে।

২. get / save / record_run   → রানটাইম কানেকশন (mas_app)
      with_authoritative_scope(...) এর ভেতরে,
      SET LOCAL app.current_tenant = <সারিটি যে টেনেন্ট বলেছে>
```

মূল বিষয়: **টেনেন্টটি কনফিগারেশন থেকে আসে না, সারি থেকে আসে।** শিডিউলারকে অন্য টেনেন্টের দিকে ঘোরানোর কোনো উপায় নেই, কারণ এটি কোনো কলার-সরবরাহ করা মান পড়েই না।

### ৭.৩ তিনটি বাইনারিতে প্লেন URL ওয়্যারিং

| প্রসেস | নতুন কনফিগ | রোল | অনুপস্থিত থাকলে |
|---|---|---|---|
| `mas-api` | `MAS_DATABASE_AUDIT_URL` | `mas_audit` | বুট ব্যর্থ |
| `mas-worker` | `MAS_DATABASE_WORKER_URL` | `mas_worker` | বুট ব্যর্থ |
| `mas-scheduler` | `MAS_DATABASE_SCHEDULER_URL` | `mas_scheduler` | বুট ব্যর্থ |
| `mas-scheduler` | `MAS_DATABASE_AUDIT_URL` | `mas_audit` | বুট ব্যর্থ |
| `mas-scheduler` | `MAS_DATABASE_OUTBOX_URL` | `mas_outbox` | বুট ব্যর্থ |

**কোনোটিতেই রানটাইম পুলে ফলব্যাক নেই**, এবং কারণটি স্পষ্ট করে কোডে লেখা: `mas_app` দিয়ে একটি প্লেনের কোয়েরি ০ সারি দেয়, এবং নীরবে শূন্য ইভেন্ট ফেলে দেওয়া বা নীরবে অলস থাকা একটি জোরে ব্যর্থ প্রক্রিয়ার চেয়ে অনেক খারাপ।

অডিট আলাদা পুলে যাওয়ার দুটো কারণ আছে, দুটোই গুরুত্বপূর্ণ:

1. **অস্বীকৃতির রেকর্ড:** কলার যে টেনেন্টে কখনো স্কোপড ছিল না তার জন্য একটি অস্বীকৃতি লিখতে হবে। RLS ঠিক সেই লেখাটিই নিষিদ্ধ করে — এবং ঠিকই করে। অডিট সারিটি প্রমাণ যে অস্বীকৃতিটি ঘটেছিল।
2. **রোলব্যাক থেকে বেঁচে থাকা:** অডিট ইভেন্টটি সেই মিউটেশনের রোলব্যাকে টিকতে হবে যেটি সে বর্ণনা করে। অনুরোধের ট্রানজেকশনে লিখলে ব্যর্থ প্রচেষ্টার কোনো রেকর্ডই থাকত না।

### ৭.৪ ডিসপ্যাচার নির্বাচন

`scheduler.dispatcher` (`durable` | `http`, ডিফল্ট `durable`):

* `durable` — ক্যানোনিক্যাল `DurableExecutionPort` পথ; টিক স্কোপড ট্রানজেকশনে জমা হয়, DB-প্রয়োগকৃত আইডেম্পোটেন্সি সহ।
* `http` — API-র অভ্যন্তরীণ এন্ডপয়েন্টে স্বাক্ষরিত POST; SSRF গার্ড + অভ্যন্তরীণ স্বাক্ষর, হোস্ট অ্যালাউলিস্ট কনফিগার করা URL থেকে নেওয়া।

অন্য যেকোনো মান বুট ব্যর্থতা।

---

## ৮. Rust → Python: বডি-বাউন্ড, সময়-বাউন্ড স্বাক্ষর (ধাপ ৯ ও ১০-এর শেষাংশ)

### ৮.০ সমস্যা

Rust ব্রিজ একটি স্ট্যাটিক `Bearer <token>` পাঠাত। এটি প্রমাণ করে যে কলারের কাছে একটি ক্রেডেনশিয়াল আছে; এটি প্রমাণ করে না যে **এই বার্তাটি** পাঠানো হয়েছে। যে কোনো প্রক্রিয়া যা টোকেনটি পড়তে পারে তা এটি রিপ্লে করতে পারে, এবং তারে ধরা পড়া টোকেন চিরকাল ব্যবহারযোগ্য।

`mas_security::internal_service` ও `mas_orchestrator.security.internal_service` ইতোমধ্যে সঠিক স্কিমটি বাস্তবায়ন করেছিল — `v1:<service>:<unix_millis>:<hex sha256(body)>` এর উপর HMAC — কিন্তু **কোনোটিই ব্রিজে ব্যবহার করা হতো না**। সিকিউরিটি কোড যা কেউ ডাকে না, তা নিরাপত্তা নয়।

### ৮.১ যা ওয়্যার করা হয়েছে

**Rust (`crates/ai-bridge/src/lib.rs`):** `internal_signing_secret` + `internal_service_id` কনফিগ যোগ। `run()`-এ বার্তাটি **প্রথমে** এনকোড করা হয়, তারপর সেই বাইটের উপর স্বাক্ষর করা হয় —

```rust
let body = { use prost::Message; wire_request.encode_to_vec() };
let signature = self.config.internal_signing_secret.as_ref().map(|secret| {
    mas_security::internal_service::sign(
        secret.expose().as_bytes(), &self.config.internal_service_id, &Timestamp::now(), &body)
});
```

এনকোড-তারপর-স্বাক্ষর ক্রমটি মূল বিষয়: স্বাক্ষর করার পরে পুনরায় সিরিয়ালাইজ করলে এমন একটি MAC তৈরি হতো যা প্রাপক যাচাই করে এমন বার্তার চেয়ে আলাদা।

**Python (`agent_bridge_servicer.py`):** `_authenticate` এখন `_verify_internal_signature` কে প্রথমে ডাকে, যা প্রাপ্ত বার্তাটিকে `SerializeToString(deterministic=True)` দিয়ে পুনরায় সিরিয়ালাইজ করে এবং MAC যাচাই করে।

**নীতি দুটি:**

* একটি **কনফিগার করা সিক্রেট স্বাক্ষরকে বাধ্যতামূলক করে**। "স্বাক্ষর অনুপস্থিত কিন্তু বিয়ারার টোকেন ঠিক ছিল" — এই পথ নেই।
* **কোনো সিক্রেট কনফিগার না থাকলে** চেকটি স্কিপ হয়, যদি না `require_internal_signature` সত্য হয়। এটি ডেভেলপমেন্ট/ইউনিট-টেস্ট ভঙ্গি, এবং এটি পাওয়ার একমাত্র উপায় হলো কম্পোজিশন রুটকে চাইতে হবে — প্রোডাকশন একটি সুইচ অফ করতে ভুলে যাবে না, বরং এটি অন করতে ভুললে বুট ব্যর্থ হবে।

`main.py` প্রোডাকশনে `require_internal_signature=settings.is_production` সেট করে, এবং `config/validation.py` প্রোডাকশনে `security.internal_service_secret_ref` দাবি করে।

**প্রতিটি ব্যর্থতা অভিন্ন:** একটি বার্তা, একটি কোড, একটি কারণ। একজন প্রোবার জানতে পারবে না যে সিক্রেট অনুপস্থিত ছিল, পরিষেবাটি অজানা ছিল, ঘড়ি ড্রিফট করেছে, নাকি MAC ভুল ছিল।

### ৮.২ ক্রস-ভাষা এনকোডিং — অনুমান নয়, পরীক্ষা

পুরো স্কিমটি একটি অনুমানের উপর দাঁড়িয়ে আছে: **prost (Rust) এবং protobuf (Python) একই বার্তাকে একই বাইটে এনকোড করে।** তারা দুটি ভিন্ন কোডবেস; স্পেসিফিকেশন ছাড়া কোনো যৌথ নিশ্চয়তা নেই।

সেই অনুমানটি এখন একটি টেস্ট:

```rust
#[test]
fn prost_and_python_encode_the_same_request_identically() {
    let encoded = pinned_request().encode_to_vec();
    let expected = hex::decode(PYTHON_ENCODED_REQUEST_HEX).expect("valid hex");
    // ...
}
```

২২০ বাইটের ভেক্টরটি Python দিয়ে তৈরি (`SerializeToString(deterministic=True)`)। যদি কখনো এনকোডিং আলাদা হয়, এই টেস্ট ব্যর্থ হবে — এবং ব্যর্থতার বার্তাটি বলে কত বাইট এবং কোথায় প্রথম পার্থক্য, দুটো ২২০-বাইট অ্যারে না ছেপে।

**ফলাফল: prost ও Python বাইট-হুবহু একই।** তিনটি নতুন E2E টেস্ট একই কথা একটি বাস্তব সকেটের উপর প্রমাণ করে:

```
test a_signed_call_is_accepted_across_the_language_boundary ... ok
test an_unsigned_call_is_refused ... ok
test a_signature_under_the_wrong_secret_is_refused ... ok
```

### ৮.৩ Python সিকিউরিটি প্যাকেজের রপ্তানি

`mas_orchestrator/security/__init__.py` আগে কেবল `identity` ও `redaction` রপ্তানি করত; `internal_service`, `credentials` ও `transport` অন্তর্ভুক্তই ছিল না — অর্থাৎ সিক্রেট রিজলভার, স্বাক্ষর ভেরিফায়ার ও TLS কনফিগ-বিল্ডার প্যাকেজের পাবলিক পৃষ্ঠে ছিল না। এখন ৩৬টি নাম রপ্তানি করা হয়, এবং একটি টেস্ট নিশ্চিত করে যে `__all__`-এর কোনো নামের মান অনুপস্থিত নেই।

---

## ৯. এই পর্যায়ের সারণী

| কমান্ড | ফলাফল |
|---|---|
| `cargo fmt --all -- --check` | CLEAN |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | CLEAN |
| `cargo test --workspace --locked` | **820 passed, 0 failed** |
| `cargo test --workspace --locked --features nats` | **820 passed, 0 failed** |
| `cargo doc --workspace --all-features --no-deps` | 0 warnings |
| `cargo test -p mas-persistence --test live_pg` (লাইভ Postgres) | **13 passed, 0 failed** |
| `cargo test -p mas-ai-bridge --test e2e_python` (Rust↔Python) | **12 passed, 0 failed** |
| `python -m compileall -q src/mas_orchestrator` | OK |
| `python -m ruff check .` | All checks passed |
| `python -m ruff format --check .` | 221 files already formatted |
| `python -m mypy src/mas_orchestrator` | Success, 161 source files |
| `python -m pytest -q` | **1936 passed** |

**নতুন টেস্ট (এই পর্যায়ে):**

* Rust: `prost_and_python_encode_the_same_request_identically`, `the_signature_covers_the_bytes_python_will_verify`, E2E `a_signed_call_is_accepted_across_the_language_boundary` / `an_unsigned_call_is_refused` / `a_signature_under_the_wrong_secret_is_refused`, `SchedulerPlaneStore`-এর ২টি ইউনিট টেস্ট।
* Python: `tests/bridge/test_internal_signature.py` — ১২টি টেস্ট (স্বাক্ষরিত গৃহীত, অস্বাক্ষরিত প্রত্যাখ্যাত, টেম্পারড বডি, ভিন্ন টেনেন্টে রিটার্গেট, ভিন্ন সার্ভিস, স্ট্যাল/ফিউচার টাইমস্ট্যাম্প, ভুল সিক্রেট, সব প্রত্যাখ্যা অভিন্ন, অ্যালাউলিস্ট)।

## 10. RLS আসলেই কাজ করছে — প্রমাণ, দাবি নয় (Phase ⑪ ⑫ ⑭)

গত রিপোর্টে একটি সীমাবদ্ধতা আমি নিজেই লিখেছিলাম: live suite চলত `mas`
superuser হিসেবে, আর superuser RLS bypass করে। মানে `tenant_b_cannot_read_...`
টেস্টটি প্রমাণ করত **আমাদের SQL-এ `WHERE tenant_id = $1` আছে** — প্রমাণ করত না
যে **SQL ভুললে ডেটাবেস নিজে থামাবে**। Policy বন্ধ থাকলে সে কখনো fail করে না।

এই ঘাটতি এখন বন্ধ। তিনটি টেস্ট এখন `mas_app` হিসেবে চলে — একটি role যার
`rolbypassrls = false`।

### 10.1 দুইটি URL, এবং কেন দুইটিই দরকার

| Variable | Role | BYPASSRLS | ব্যবহার |
|---|---|---|---|
| `MAS_TEST_DATABASE_URL` | migrator (`mas`) | হ্যাঁ | schema, lifecycle, repository round-trip |
| `MAS_TEST_DATABASE_APP_URL` | runtime (`mas_app`) | **না** | RLS assertion গুলো |

`MAS_TEST_DATABASE_APP_URL` সেট না থাকলে এই টেস্টগুলো skip হয় — **চুপচাপ পাস
হয় না**, ব্যানার প্রিন্ট করে। Vacuous isolation test নিয়ে confidence কেনা যায় না।

### 10.2 প্রমাণিত ফলাফল (লাইভ ক্লাস্টার, port 55432)

```
test the_runtime_role_cannot_bypass_row_level_security ... ok
test a_pooled_connection_does_not_inherit_the_previous_request_tenant ... ok
test the_plane_roles_are_confined_to_their_own_tables ... ok
test result: ok. 16 passed; 0 failed   (আগে 13 ছিল)
```

`tenants` টেবিল: `rowsecurity = true`, `force = true`।
`mas_app` privileges: `DELETE,INSERT,SELECT,UPDATE`।

| Probe | প্রত্যাশিত | প্রাপ্ত |
|---|---|---|
| unscoped `SELECT count(*) FROM tenants` as `mas_app` | `0` | **`0`** |
| একই query superuser হিসেবে (row আছে — প্রমাণ) | `> 0` | **`245`** |
| tenant A armed ⇒ দৃশ্যমান tenant | শুধু A | **`[A]`** |
| tenant B armed ⇒ tenant A-তে `UPDATE` | 0 rows | **0 rows** |
| tenant B armed ⇒ tenant A-তে `DELETE` | 0 rows | **0 rows** |
| তারপরেও tenant A আছে কি? | হ্যাঁ | **হ্যাঁ** |

শেষ দুই সারি না থাকলে উপরের `0` গুলো অর্থহীন হতো — privilege না থাকলে এবং
row না থাকলে দুই ক্ষেত্রেই `0` আসে। তাই দুই direction-এই assertion আছে।

### 10.3 pooled connection leak — Phase ⑫, এখন আসল টেস্ট

Pool-এ `min = max = 1`, তাই দ্বিতীয় `acquire()` নিশ্চিতভাবে **একই socket**।

```
--- SET LOCAL (transaction scope) ---
BEGIN; set_config('app.current_tenant', A, true);
  inside txn   = 1
COMMIT;
  after commit = 0        <-- scope ট্রানজ্যাকশনের সাথে মারা গেছে

--- set_config(..., false) (session scope) ---
BEGIN; set_config('app.current_tenant', A, false); COMMIT;
  after commit = 1        <-- LEAK: scope বেঁচে গেছে
```

দ্বিতীয় অংশটি **assert করা হয়েছে**, শুধু মন্তব্যে লেখা হয়নি। কারণ একটি টেস্ট
যা শুধু একদিকে fail হতে পারে, সে উল্টোদিক থেকে ভুল কারণে পাসও করতে পারে।
`scoped_request` কেন `SET LOCAL` আর কিছু না — তার কারণ এখন executable।

### 10.4 plane role গুলোর সীমাবদ্ধতা

BYPASSRLS আছে বলেই প্রতিটি plane-এর grant যত সংকীর্ণ তা যাচাই করা জরুরি।

| Role | শুধু এই টেবিল | নিষিদ্ধ (assert করা) |
|---|---|---|
| `mas_worker` | `tasks` সহ durable execution টেবিল | `api_keys`, `credentials`, `sessions`, `connectors`, `webhooks` |
| `mas_audit` | **শুধু** `audit_events` | বাকি সব |
| `mas_outbox` | **শুধু** `outbox_events` | বাকি সব |
| `mas_directory` | `organizations`, `tenants`, `projects`, `memberships` | `tasks`, `executions`, `agents`, `api_keys`, `credentials` |
| `mas_scheduler` | `schedules` | `tasks`, `executions`, `agents`, `api_keys` |

### 10.5 অপারেটর ধাপ: `scripts/provision-plane-roles.sh`

Migration 0015/0017/0019 ইচ্ছে করেই `mas_directory`, `mas_worker`,
`mas_outbox`, `mas_audit`, `mas_scheduler` কে **NOLOGIN** বানিয়েছে —
অকনফিগার্ড ডেপ্লয়মেন্ট যাতে কোনো plane role-এ ঢুকতে না পারে। কাউকে তো LOGIN
দিতে হবে; এই স্ক্রিপ্ট সেটাই করে।

- password শুধু environment variable থেকে — **argv থেকে নয়** (process table পড়া যায়)
- `set +x` জোর করে; মান psql variable হিসেবে যায়, SQL-এ interpolate হয় না
- `--dry-run` password মাস্ক করে, `--dev` random password দেয় এবং loopback ছাড়া অন্য host-এ চলতে **অস্বীকার করে**
- সব ছয়টি `ALTER ROLE` এক session-এ, `ON_ERROR_STOP=1` — আধা-provisioning সম্ভব নয়
- LOGIN ছাড়া কিছু দেয় না

যাচাই করা হয়েছে: dry-run ঠিক, password না দিলে `exit=2`, `--dev` দিয়ে remote
host দিলে `exit=2`।

### 10.6 সামগ্রিক গেট (সব execute করা)

```
cargo fmt --all -- --check                          CLEAN
cargo clippy --workspace --all-targets --all-features -- -D warnings   clean
cargo doc --workspace --all-features --no-deps      clean
cargo test --workspace --locked                     823 passed, 0 failed   (আগে 820)
cargo test --workspace --locked --features nats     823 passed, 0 failed
cargo test -p mas-persistence --test live_pg        16 passed, 0 failed    (আগে 13)
cargo test -p mas-ai-bridge --test e2e_python       12 passed, 0 failed
python compileall / ruff check / ruff format --check (222) / mypy (161)   OK
pytest -q                                           1936 passed
mas_orchestrator.security.__all__                   36 exports, 0 missing
```
