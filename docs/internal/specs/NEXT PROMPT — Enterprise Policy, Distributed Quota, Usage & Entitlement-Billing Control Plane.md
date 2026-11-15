# NEXT PHASE — ENTERPRISE POLICY + DISTRIBUTED QUOTA + USAGE + ENTITLEMENT/BILLING

## ROLE

Act as a Principal Backend Engineer + Distributed Systems Engineer + SaaS Billing Architect + Security Engineer + PostgreSQL Engineer.

The following phases are already reported complete:

- Python AI Orchestrator
- Rust ↔ Python AgentBridge
- canonical durable execution pipeline
- durable task attempts
- worker ownership/fencing
- production outbox relay
- tenant isolation
- authentication/security hardening
- authoritative tenancy
- RLS production path
- pooled connection leak protection

Use the ACTUAL repository as source of truth.

Do not blindly trust status reports.

---

# PRIMARY OBJECTIVE

Complete the enterprise SaaS control-plane chain:

```text
Plan
  ↓
Subscription
  ↓
Entitlement
  ↓
Feature Gate
  ↓
Policy
  ↓
Quota
  ↓
Execution/Resource Enforcement
  ↓
Usage Metering
  ↓
Usage Ledger
  ↓
Billing/Metering Output
```

The system must enforce limits BEFORE resource consumption where appropriate, then record authoritative consumption AFTER actual work.

There must be no synthetic quota or usage counters.

---

# ABSOLUTE RULE — NO SHORTENING

For every modified/new file:

- full file
- first line to final line
- no `...`
- no omitted sections
- no placeholder
- no pseudo-code
- no "rest unchanged"
- no partial implementation

Preserve all unrelated working code.

---

# FILE COMMENT RULE

Every new file MUST contain a specific `PURPOSE:` comment.

The comment must explain:

- responsibility
- owning layer
- production role
- data ownership
- what it must NOT do

Do not use generic comments.

---

# PHASE 1 — EXISTING CONTROL-PLANE INVENTORY

Inspect actual implementation of:

- policy crate
- quota crate
- application services
- subscription models
- entitlement models
- usage repositories
- billing-related domain objects
- migrations
- API routes
- worker enforcement
- execution enforcement
- audit events

Find:

- all policy evaluators
- all quota counters
- all entitlement checks
- all usage writers
- all billing abstractions
- all in-memory implementations

Classify each implementation:

- CANONICAL
- PRODUCTION
- TEST-ONLY
- DEV-ONLY
- DUPLICATE
- DEAD
- STUB
- UNSAFE

---

# PHASE 2 — ONE CANONICAL POLICY ENGINE

There MUST be one authoritative policy decision path.

Required conceptual flow:

```text
Request
 ↓
Principal
 ↓
Tenant
 ↓
Resource
 ↓
Policy Context
 ↓
Policy Evaluator
 ↓
Allow / Deny
 ↓
Audit Decision
```

Do not create independent policy logic inside:

- API handlers
- worker
- scheduler
- Python agent
- tool executor

Those components may call the canonical policy service.

---

# PHASE 3 — POLICY PERSISTENCE

Policies must be durably stored.

Verify support for:

- tenant scope
- organization scope where applicable
- project scope
- policy version
- enabled/disabled
- rule definition
- priority/order
- created/updated timestamps
- actor identity

Policy updates must be auditable.

Avoid unsafe arbitrary dynamic code execution.

---

# PHASE 4 — POLICY EVALUATION

A policy decision must include:

- decision
- policy identity/version
- reason
- subject/resource scope
- correlation ID
- evaluation timestamp

Do NOT return only `true/false` when operational explainability is required.

Policy denial must map to a stable:

`policy_denied`

error semantics.

It must NOT become:

- provider failure
- generic internal error
- retryable dependency error

---

# PHASE 5 — POLICY CACHE

If policy caching exists or is needed:

- define TTL
- define invalidation
- prevent cross-tenant cache leakage
- include policy version
- avoid stale authorization decisions after privileged policy changes

A cache miss must never bypass policy.

A stale cache must not silently grant access when deny is authoritative.

---

# PHASE 6 — DISTRIBUTED QUOTA ENGINE

Replace all production in-memory quota counters.

The quota system must work across multiple replicas.

At minimum support:

- requests
- concurrent executions
- token/resource budgets
- tool calls where applicable
- tenant-level limits
- project-level limits where required
- plan-level limits

Do not use process-local counters as the source of truth.

---

# PHASE 7 — QUOTA ATOMICITY

Quota reservation/enforcement must be concurrency-safe.

For example:

```text
Replica A → reserve 70
Replica B → reserve 50
Limit = 100
```

must NOT permit 120 simply because each replica had its own local counter.

Use the database/distributed backend to make the reservation decision atomically.

The exact implementation must follow the existing architecture and dependencies.

---

# PHASE 8 — QUOTA RESERVATION / RELEASE

Where a resource is consumed asynchronously, implement:

```text
check
→ reserve
→ execute
→ settle actual usage
→ release unused reservation
```

Failure must release or reconcile reservations correctly.

Process crash must not permanently consume quota.

Lease/reservation expiry must be recoverable.

---

# PHASE 9 — QUOTA ERROR SEMANTICS

Use a dedicated:

`quota_exceeded`

classification.

It must be:

- deterministic
- non-retryable unless the system explicitly supports time-window retry
- auditable
- tenant-scoped

Do not classify quota denial as:

- model error
- network error
- generic 500
- transient broker failure

---

# PHASE 10 — USAGE LEDGER

Create one authoritative usage write path.

Usage must be attributable to:

- tenant
- organization where applicable
- project where applicable
- execution
- task
- attempt
- model/provider
- resource type
- quantity
- timestamp

Where applicable include:

- input tokens
- output tokens
- tool calls
- runtime duration
- provider/model
- estimated/actual cost

Do not fabricate usage.

---

# PHASE 11 — EXACT-ONCE METERING SEMANTICS

Duplicate broker delivery or duplicate worker execution must NOT double-meter the same logical work.

Use a durable uniqueness/idempotency dimension.

Test:

```text
same execution + same task + same usage event
→ one usage record
```

and:

```text
same idempotency request replay
→ no second billable usage
```

---

# PHASE 12 — FAILED/CANCELLED USAGE

Define clear accounting rules.

At minimum verify:

- successful execution records actual usage
- failed-before-provider-execution records zero applicable provider usage
- provider-failed execution records whatever usage was actually consumed, if available
- cancelled execution does not invent usage
- partial workflow usage is attributable to actual completed work

Never equate:

`execution failed = usage must be zero`

unless the underlying system can prove no resource was consumed.

---

# PHASE 13 — PLAN MODEL

Verify durable plan support.

A plan should define the applicable:

- features
- quota limits
- usage limits
- concurrency
- model/tool permissions
- retention where applicable

Do not hard-code plan names deep inside execution code.

---

# PHASE 14 — SUBSCRIPTION

Implement/complete subscription lifecycle:

- active
- trial where supported
- suspended
- cancelled
- expired

Verify:

- tenant ownership
- plan reference
- effective timestamps
- renewal period
- external billing reference where applicable
- audit history

State transitions must be deterministic.

---

# PHASE 15 — ENTITLEMENT

Implement canonical entitlement resolution:

```text
tenant
→ active subscription
→ plan
→ plan entitlements
→ overrides
→ effective feature set
```

Support explicit tenant-level overrides where the product model requires enterprise contracts.

Do not let callers submit arbitrary entitlement claims.

---

# PHASE 16 — FEATURE GATING

Every metered/privileged product feature must use the canonical entitlement service.

Examples:

- advanced model
- high token budget
- premium connector
- workflow execution
- scheduled execution
- RAG
- tool execution
- export
- audit retention
- high concurrency

Frontend checks may hide UI actions but MUST NOT be authoritative.

Backend enforcement is mandatory.

---

# PHASE 17 — BILLING DATA BOUNDARY

Separate:

### Authoritative platform usage

from:

### External billing provider state

The external billing provider must not become the source of truth for whether a historical execution actually happened.

Platform execution ledger remains authoritative.

Billing integration consumes controlled billing events/data.

---

# PHASE 18 — BILLING PROVIDER ABSTRACTION

If an external billing provider exists in the architecture:

Create one canonical abstraction for:

- customer mapping
- subscription creation/update
- cancellation
- invoice/reference handling
- webhook verification
- plan mapping

Do not hard-wire provider calls throughout application services.

Do not put provider secrets into domain models.

---

# PHASE 19 — BILLING WEBHOOK SECURITY

If billing webhooks are present:

- verify signature
- prevent replay
- preserve event ID
- make processing idempotent
- persist webhook receipt
- audit state changes
- reject invalid signatures

Never trust:

- customer ID alone
- tenant ID in unsigned JSON
- plan ID from an unauthenticated callback

---

# PHASE 20 — PLAN OVERRIDES

Enterprise customers may require negotiated limits.

Implement a canonical override layer.

Override must identify:

- tenant
- feature/quota
- value
- reason
- actor
- effective period
- audit event

Do not modify plan definitions for one tenant.

---

# PHASE 21 — POLICY + QUOTA ORDERING

Define the canonical order.

Recommended conceptual path:

```text
Authenticate
 ↓
Authorize
 ↓
Entitlement
 ↓
Policy
 ↓
Quota reservation
 ↓
Execute
 ↓
Settle usage
 ↓
Audit/event
```

Do not consume expensive resources before a required denial can be determined.

Do not reserve quota after the resource has already been consumed.

---

# PHASE 22 — EXECUTION INTEGRATION

Integrate control-plane enforcement into the existing canonical execution pipeline.

Do NOT create a second execution service.

Before creating/dispatching executable work, evaluate:

- entitlement
- policy
- quota

The existing durable execution pipeline remains canonical.

---

# PHASE 23 — WORKER INTEGRATION

Worker must enforce resource limits according to the canonical runtime contract.

Worker must NOT invent a second quota calculation.

Worker may:

- consume reservation
- report actual usage
- settle reservation

But policy/quota semantics belong to the canonical control-plane services.

---

# PHASE 24 — PYTHON INTEGRATION

Python must NOT directly write Rust-owned billing/platform tables unless the architecture explicitly declares such ownership.

Python should return:

- model usage
- tool usage
- resource usage
- cost metadata where available

Rust remains authoritative for platform accounting.

---

# PHASE 25 — USAGE EVENTING

Usage events must use the existing outbox/event infrastructure.

No direct unreliable side-channel.

Required conceptual path:

```text
Execution result
 ↓
durable usage record
 ↓
outbox
 ↓
billing/analytics consumer
```

Do not emit an event that claims usage before the durable usage record exists.

---

# PHASE 26 — AUDIT

Audit:

- policy decision
- quota denial
- quota override
- entitlement change
- subscription change
- plan override
- usage adjustment
- billing webhook
- billing-state transition

Do not log payment secrets or provider credentials.

---

# PHASE 27 — ADMIN SECURITY

Protect:

- plan administration
- quota overrides
- entitlement overrides
- billing settings
- usage adjustments
- subscription state changes

These operations require explicit privileged authorization.

Every change must be auditable.

---

# PHASE 28 — DATA INTEGRITY

Inspect migrations for:

- unique constraints
- foreign keys
- tenant scope
- effective-date overlap
- duplicate subscription prevention
- duplicate entitlement prevention
- duplicate usage prevention
- quota reservation consistency

Database constraints should enforce invariants that can be enforced relationally.

---

# PHASE 29 — MULTI-TENANT BILLING ISOLATION

Test:

Tenant A MUST NOT:

- see Tenant B subscription
- modify Tenant B quota
- read Tenant B usage
- access Tenant B billing data
- apply Tenant B entitlement
- create a billing event for Tenant B

Test API + application + DB/RLS paths.

---

# PHASE 30 — CONCURRENCY / RACE TESTS

Test at minimum:

1. two replicas reserving final quota simultaneously
2. duplicate subscription update
3. duplicate entitlement update
4. duplicate usage event
5. duplicate billing webhook
6. concurrent quota override
7. concurrent usage settlement
8. process crash during reservation
9. process crash during usage settlement
10. replay after successful settlement

No race may create negative quota or duplicate billing.

---

# PHASE 31 — FAILURE / RECOVERY

Test:

- database failure
- broker failure
- billing provider timeout
- billing webhook duplicate
- billing webhook out-of-order delivery
- provider usage unavailable
- execution retry
- worker restart
- quota reservation expiry
- policy cache invalidation failure

No failure should silently grant entitlement or quota.

---

# PHASE 32 — API SURFACE

Complete backend APIs for:

- plans
- subscriptions
- entitlements
- quotas
- usage
- policy administration
- billing status
- usage history

Every route must have:

- authentication
- authorization
- tenant resolution
- validation
- pagination where required
- deterministic errors
- audit where applicable

---

# PHASE 33 — TESTING

Add tests for:

- policy allow
- policy deny
- policy versioning
- policy persistence
- quota allow
- quota deny
- distributed reservation
- quota release
- crash recovery
- exact-once usage
- subscription lifecycle
- entitlement resolution
- enterprise override
- billing webhook signature/idempotency
- cross-tenant billing isolation
- audit correctness

Use real PostgreSQL for database invariants.

Use real broker infrastructure where event semantics matter.

---

# PHASE 34 — STATIC COMPLETENESS AUDIT

Search for:

- InMemoryQuota
- in-memory quota
- synthetic usage
- fake billing
- mock billing
- AllowAllPolicy
- NoopPolicy
- hard-coded plan
- hard-coded quota
- hard-coded entitlement
- direct usage counter
- direct billing mutation
- TODO
- FIXME
- placeholder
- stub
- mock
- fake
- noop
- test-only

Classify every occurrence.

Do not automatically treat test fixtures as production defects.

---

# PHASE 35 — BUILD / TEST VERIFICATION

Run:

```bash id="5l8hpd"
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --locked
cargo test --workspace --locked --features nats
cargo doc --workspace --all-features --no-deps
```

Python:

```bash id="rx7m3p"
python3 -m compileall -q src tests
python3 -m ruff check src tests
python3 -m ruff format --check src tests
PYTHONPATH=src python3 -m mypy src/mas_orchestrator
PYTHONPATH=src python3 -m pytest -q
```

Run live PostgreSQL and relevant E2E suites.

If a command cannot execute:

STATE EXACTLY WHY.

Never claim green without actual execution.

---

# PHASE 36 — NO REGRESSION

Do not break:

- canonical execution lifecycle
- AgentBridge
- RLS
- tenant isolation
- worker fencing
- outbox relay
- retry classification
- provider failover
- existing Python tests
- existing Rust tests

Do not modify the execution ownership model.

---

# PHASE 37 — FULL FILE OUTPUT

For EVERY changed file:

```text
FILE: exact/path
PURPOSE: exact responsibility
```

Then the COMPLETE file from beginning to end.

No shortening.

No snippets.

No omitted sections.

No placeholders.

---

# COMPLETION GATE

This phase is complete ONLY when:

- one canonical policy service exists
- policy is durably persisted
- policy decisions are auditable
- production quota is distributed
- no in-memory quota is authoritative
- quota reservation is concurrency-safe
- quota release/recovery works
- `quota_exceeded` is deterministic
- one canonical usage ledger exists
- duplicate work does not double-meter
- failed/cancelled usage semantics are explicit
- plans are persisted
- subscriptions are persisted
- entitlement resolution is canonical
- enterprise overrides are supported safely
- feature gates are backend-enforced
- billing integration has a clear ownership boundary
- billing webhook security is enforced where applicable
- billing events are idempotent
- usage flows through durable eventing
- policy/quota/entitlement changes are auditable
- cross-tenant billing isolation passes
- distributed concurrency tests pass
- all existing execution/security/bridge tests remain green

---

# FINAL STOP CONDITION

STOP after completing:

**Policy + Distributed Quota + Usage + Entitlement/Billing Control Plane**

Do NOT start:

- React frontend
- dashboard UI
- marketing pages
- cosmetic refactoring
- unrelated integrations

Do NOT create:

- second policy engine
- second quota engine
- second usage ledger
- second subscription authority
- direct Python writes to Rust-owned platform state
- in-memory production quota
- synthetic usage
- unauthenticated billing webhook processing

Do NOT shorten files.

Do NOT provide partial files.

Wait for explicit approval before the next phase.