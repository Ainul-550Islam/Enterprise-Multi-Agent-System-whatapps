# NEXT PHASE — ENTERPRISE INTEGRATIONS + CONNECTORS + WEBHOOKS + NOTIFICATIONS + SSRF

## ROLE

Act as a Principal Backend Engineer + Enterprise Integration Architect + Application Security Engineer + Distributed Systems Engineer.

The current platform has already completed and revalidated:

- Rust core
- Python AI Orchestrator
- Rust ↔ Python AgentBridge
- canonical durable execution pipeline
- authentication / authorization / tenancy / RLS hardening
- worker fencing / durable attempts
- outbox relay
- usage / policy / control-plane foundations
- pgvector RAG database-principal security boundary
- live PostgreSQL validation
- Rust/Python integration testing

Use the ACTUAL repository as the source of truth.

Do NOT trust status reports without inspecting the source.

---

# ABSOLUTE RULE — NO SHORTENING

For every new or modified file:

- write the FULL file
- first line through final line
- no `...`
- no omitted sections
- no placeholder
- no pseudo-code
- no "rest unchanged"
- no "existing code"
- no partial files

Preserve all unrelated existing functionality.

---

# FILE COMMENT RULE

Every new file MUST contain a specific:

`PURPOSE:`

comment.

Explain:

- file responsibility
- owning layer
- production role
- security boundary
- data ownership
- what the file must NOT do

---

# PRIMARY OBJECTIVE

Complete the enterprise integration plane:

```text id="q8qvgy"
Connector
  ↓
Credential Reference
  ↓
Authorization / Policy
  ↓
Secure Transport
  ↓
External Provider
  ↓
Result
  ↓
Durable State
  ↓
Webhook / Event
  ↓
Notification
  ↓
Audit / Usage
```

The integration plane must work across multiple replicas and must not rely on process-local authoritative state.

---

# PHASE 1 — FULL INTEGRATION INVENTORY

Inspect actual implementation of:

- `crates/integrations`
- connector domain models
- credential models
- webhook models
- webhook delivery models
- notification models
- HTTP client utilities
- SSRF guard
- OAuth abstractions
- application services
- repositories
- migrations
- API routes
- worker/tool runtime
- outbox/event infrastructure

Find all:

- connector implementations
- credential abstractions
- webhook handlers
- notification dispatchers
- retry logic
- callback routes
- HTTP clients
- external provider clients
- mock/fake adapters

Classify:

- CANONICAL
- PRODUCTION
- TEST-ONLY
- DEV-ONLY
- DUPLICATE
- DEAD
- STUB
- UNSAFE

---

# PHASE 2 — CANONICAL CONNECTOR MODEL

There MUST be one canonical connector abstraction.

A connector should identify:

- connector type
- tenant
- organization/project scope where applicable
- configuration metadata
- credential reference
- enabled/disabled state
- capabilities
- lifecycle timestamps
- version where required

Do not store provider secrets directly inside general connector configuration.

---

# PHASE 3 — CONNECTOR OWNERSHIP

Define clearly:

Rust owns:

- connector metadata
- tenant scope
- authorization
- policy
- credential references
- lifecycle state
- audit
- durable execution state

External provider owns:

- provider-side resources
- provider-side identifiers
- provider-specific state

Python may execute AI/runtime operations but must not silently become the owner of Rust platform connector state.

---

# PHASE 4 — CREDENTIAL REFERENCES

Verify credential lifecycle:

- create
- bind
- rotate
- revoke
- expire
- delete
- audit

The platform must use:

```text id="g3qtbc"
connector
 → secret_reference
 → secret_manager
 → short-lived/use-scoped credential
```

Do NOT:

- store raw credentials in connector rows
- serialize credentials into outbox events
- send credentials through normal audit logs
- expose credentials to frontend
- expose credentials in error responses

---

# PHASE 5 — SECRET MANAGER

Complete a production secret-manager abstraction where missing.

Support:

- read
- write where required
- rotate
- delete
- version
- reference metadata

The application must receive secrets only when the operation actually needs them.

Do not cache secrets indefinitely in application memory.

Do not place provider credentials into domain events.

---

# PHASE 6 — SSRF PRODUCTION ENFORCEMENT

The existing SSRF guard MUST be traced to every actual outbound HTTP caller.

Do not mark SSRF complete because the helper exists.

All connector/tool HTTP requests must enforce:

- scheme validation
- hostname validation
- allowed ports
- DNS resolution
- private IPv4 blocking
- private IPv6 blocking
- loopback blocking
- link-local blocking
- cloud metadata endpoint blocking
- redirect target validation
- DNS rebinding protections where applicable

The security check must occur immediately before actual network access.

---

# PHASE 7 — SSRF REDIRECT TESTING

Test:

```text id="aj2f2a"
Public URL
 ↓
301 redirect
 ↓
Private IP
```

The request MUST be blocked.

Also test:

- localhost
- 127.0.0.1
- ::1
- link-local
- RFC1918
- metadata endpoints
- IPv4-mapped IPv6
- unusual DNS resolution
- restricted ports

Do not rely only on hostname string matching.

---

# PHASE 8 — CONNECTOR AUTHORIZATION

Before connector execution:

```text id="4whq0p"
Principal
 ↓
Tenant
 ↓
Project/resource
 ↓
Connector
 ↓
Permission
 ↓
Policy
 ↓
Execution
```

The AI model must not be able to select an arbitrary connector.

Tool/agent output must never override connector scope.

---

# PHASE 9 — CONNECTOR REGISTRY

Implement/complete one canonical connector registry.

The registry must declare:

- connector type
- supported operations
- credential requirements
- permission requirements
- timeout
- retry classification
- network policy
- webhook capabilities

Do not scatter connector capability checks across random handlers.

---

# PHASE 10 — REAL PROVIDER ADAPTERS

For providers already represented in the repository:

Implement actual adapters rather than interface-only classes.

Each adapter must include:

- timeout
- retry classification
- provider error mapping
- rate-limit handling
- safe logging
- response validation
- pagination where relevant
- idempotency where supported

Do not create fake success responses.

---

# PHASE 11 — OAUTH

Where OAuth connectors are supported:

Implement:

- authorization initiation
- callback
- state parameter
- CSRF protection
- authorization-code exchange
- token storage through secret manager
- refresh
- revocation where provider supports it
- tenant binding
- redirect URI validation

Never accept arbitrary callback redirects.

Never bind an OAuth token to a tenant based only on unsigned client state.

---

# PHASE 12 — WEBHOOK RECEIVER

Implement/complete canonical inbound webhook handling.

Required path:

```text id="p6g7kq"
HTTP webhook
 ↓
provider verification
 ↓
event ID
 ↓
signature validation
 ↓
replay protection
 ↓
tenant/connector resolution
 ↓
durable webhook receipt
 ↓
outbox/event processing
```

Never perform large synchronous business processing directly inside the HTTP webhook handler.

---

# PHASE 13 — WEBHOOK SIGNATURE VERIFICATION

Support provider-specific signature mechanisms where applicable.

At minimum support the architecture needed for:

- HMAC signatures
- timestamp tolerance
- replay prevention
- constant-time comparison
- signed payload verification

Never trust:

- tenant_id from unsigned JSON
- connector_id from unsigned JSON
- customer identity alone

---

# PHASE 14 — WEBHOOK IDEMPOTENCY

Persist provider event identity.

Duplicate delivery:

```text id="jz5x3b"
same provider event ID
→ one logical processing
```

must not:

- create duplicate records
- trigger duplicate execution
- double-meter usage
- send duplicate notifications

Use database-enforced uniqueness wherever relationally possible.

---

# PHASE 15 — OUT-OF-ORDER WEBHOOKS

Test webhook ordering.

Example:

```text id="xe62r7"
event 3 arrives
event 2 arrives
event 1 arrives
```

The system must define deterministic behavior.

Where ordering matters:

- preserve provider sequence/version
- reject stale transitions
- or reconcile from provider state

Never blindly apply stale webhook state.

---

# PHASE 16 — WEBHOOK DELIVERY OUTBOUND

For platform-generated outbound webhooks:

```text id="p41kd7"
Domain change
 ↓
transaction
 ↓
outbox
 ↓
webhook delivery record
 ↓
worker/relay
 ↓
HTTP delivery
 ↓
ack / retry / dead-letter
```

Delivery state must be durable.

Never maintain authoritative delivery count only in memory.

---

# PHASE 17 — OUTBOUND WEBHOOK SECURITY

Outbound webhook requests must support:

- signed payload
- timestamp
- event ID
- replay protection
- secret rotation
- timeout
- retry
- exponential backoff
- dead-letter

Never expose internal secrets.

Do not allow arbitrary user-controlled signing secrets to leak into logs.

---

# PHASE 18 — WEBHOOK TARGET SECURITY

If users can configure arbitrary webhook URLs, apply outbound network policy.

Prevent:

- private IP targets
- localhost
- metadata endpoints
- disallowed ports

Do not disable SSRF protection simply because the URL is user-configured.

---

# PHASE 19 — WEBHOOK DELIVERY RETRIES

Classify:

### Retryable

- connection reset
- timeout
- 429
- transient 5xx

### Terminal

- malformed target
- invalid TLS configuration
- permanently rejected endpoint where provider semantics establish terminality

Honor:

- Retry-After
- maximum attempts
- exponential backoff
- jitter

Do not retry forever.

---

# PHASE 20 — WEBHOOK DEAD LETTER

Exhausted delivery MUST become durable dead-letter state.

Store:

- delivery ID
- event ID
- endpoint ID
- attempt count
- last error
- first failure
- last failure
- next retry
- dead-letter timestamp

Operators must be able to inspect the state.

---

# PHASE 21 — NOTIFICATION MODEL

Complete notification lifecycle:

- created
- pending
- delivered
- failed
- read where applicable
- archived where applicable

Support tenant-scoped notification ownership.

Do not generate notifications from process-local queues only.

---

# PHASE 22 — NOTIFICATION DELIVERY

Notification delivery must be durable.

Support the channels actually represented by the project, such as:

- webhook
- email
- in-app
- event stream

Do not implement channels that the architecture does not require just for appearance.

---

# PHASE 23 — EMAIL SECURITY

Where email is supported:

- provider abstraction
- secret manager credentials
- recipient validation
- template validation
- rate limits
- retry classification
- bounce/failure recording

Do not place provider API keys inside email jobs.

---

# PHASE 24 — NOTIFICATION DEDUPLICATION

A single logical event must not produce uncontrolled duplicate notifications.

Use deterministic notification identity/idempotency.

Test:

```text id="x8nl1j"
event replay
→ no duplicate logical notification
```

---

# PHASE 25 — INTEGRATION STATE MACHINE

Connector and webhook delivery transitions must be explicit.

Examples:

Connector:

```text id="v5ksw4"
pending
→ active
→ disabled
→ revoked
→ deleted
```

Delivery:

```text id="0vl46c"
pending
→ delivered
→ retrying
→ failed
→ dead_lettered
```

Reject invalid transitions.

---

# PHASE 26 — API SURFACE

Complete APIs for actual supported capabilities:

### Connectors

- create
- list
- get
- update
- enable/disable
- rotate credential
- delete

### Webhooks

- create
- list
- get
- update
- disable
- rotate secret
- delete

### Notifications

- list
- get
- mark read
- archive/delete where supported

Every endpoint must include:

- authentication
- authorization
- tenant resolution
- validation
- pagination
- deterministic errors
- audit where applicable

---

# PHASE 27 — FRONTEND CONTRACT PREPARATION

Even if the frontend is not yet implemented, backend contracts must be stable for future UI.

Define canonical DTOs for:

- connector
- connector capability
- webhook endpoint
- webhook delivery
- notification

Do not create frontend-only representations that diverge from backend semantics.

---

# PHASE 28 — AUDIT

Audit all:

- connector creation
- connector deletion
- credential rotation
- OAuth authorization
- webhook creation
- webhook secret rotation
- webhook failure
- dead-letter
- notification delivery
- privileged integration configuration

Do not log credentials or signed secrets.

---

# PHASE 29 — USAGE

Where integrations consume billable resources:

record usage through the existing authoritative usage pipeline.

Never create a second usage ledger.

External integration activity must remain attributable to:

- tenant
- project where applicable
- execution/task
- connector
- operation

---

# PHASE 30 — MULTI-TENANT TESTING

Test:

Tenant A cannot:

- list Tenant B connectors
- invoke Tenant B connector
- read Tenant B webhook endpoint
- read Tenant B delivery records
- mutate Tenant B integration
- read Tenant B notifications
- access Tenant B credential metadata

Use API + application + PostgreSQL/RLS tests.

---

# PHASE 31 — FAILURE / RECOVERY

Test:

1. connector timeout
2. provider 429
3. provider 5xx
4. invalid credentials
5. revoked credentials
6. SSRF target
7. webhook duplicate
8. webhook signature failure
9. webhook replay
10. outbound webhook timeout
11. outbound webhook retry
12. dead-letter
13. worker crash during delivery
14. process restart
15. credential rotation during active operation
16. notification duplicate
17. broker failure
18. database failure

No failure may silently become success.

---

# PHASE 32 — DISTRIBUTED OPERATION

Assume multiple worker/relay replicas.

Verify:

- durable delivery ownership
- no duplicate state transition
- no stale delivery settlement
- no process-local authoritative count
- durable retries
- lease/fencing where required
- safe restart
- safe failover

---

# PHASE 33 — OBSERVABILITY

Add metrics for:

- connector invocation latency
- connector failures
- provider 429
- provider 5xx
- credential failures
- SSRF denials
- webhook received
- webhook signature failures
- webhook duplicate events
- webhook delivery latency
- webhook delivery retries
- webhook dead-letter count
- notification failures
- integration timeout

Use low-cardinality labels.

Never put:

- URL
- credential
- token
- arbitrary customer payload

into metric labels.

---

# PHASE 34 — SECURITY STATIC AUDIT

Search for:

- unrestricted reqwest
- raw URLs
- http://
- webhook URL
- callback URL
- OAuth state
- secret
- credential
- bearer
- token
- HMAC
- signature
- redirect
- localhost
- 127.0.0.1
- metadata
- private IP
- in-memory delivery
- synthetic count
- fake connector
- mock provider
- noop integration

Classify every occurrence.

---

# PHASE 35 — TESTING

Add:

- connector authorization tests
- credential lifecycle tests
- SSRF tests
- redirect SSRF tests
- webhook signature tests
- webhook replay tests
- webhook idempotency tests
- webhook ordering tests
- outbound delivery tests
- dead-letter tests
- notification deduplication tests
- restart/recovery tests
- cross-tenant isolation tests
- live PostgreSQL tests
- broker-backed delivery tests

---

# PHASE 36 — BUILD / TEST VERIFICATION

Run the project's actual supported gates.

Rust:

```bash id="e8blxy"
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets
cargo test --workspace --all-targets --features nats
cargo doc --workspace --all-features --no-deps
```

Python:

```bash id="9fxv6t"
python3 -m compileall -q src tests
python3 -m ruff check .
python3 -m ruff format --check .
python3 -m mypy src/mas_orchestrator
python3 -m pytest -q
```

Run:

- live PostgreSQL integration
- live broker tests
- relevant ignored/opt-in E2E tests

If a command cannot run:

STATE EXACTLY WHY.

Never claim success without executing it.

---

# PHASE 37 — NO REGRESSION

Preserve:

- canonical execution
- AgentBridge
- RLS
- tenancy
- authorization
- pgvector security boundary
- provider adapters
- retry/failover
- durable outbox
- worker fencing
- usage ledger
- audit

---

# PHASE 38 — FULL FILE OUTPUT

For every changed file:

```text
FILE: exact/path
PURPOSE: exact responsibility
```

Then the COMPLETE FILE.

No shortening.

No snippets.

No omissions.

---

# COMPLETION GATE

This phase is complete ONLY when:

- canonical connector abstraction exists
- connector metadata is tenant-scoped
- credential references are separate from connector metadata
- production secret-manager path is used
- every outbound HTTP caller enforces SSRF protection
- redirects are revalidated
- connector authorization is backend-enforced
- real provider adapters are used
- OAuth is secure where applicable
- inbound webhooks are signature-verified
- webhook replay is prevented
- webhook events are idempotent
- webhook ordering behavior is defined
- outbound webhook delivery is durable
- delivery retries are bounded
- dead-letter is durable
- notifications are durable
- notification duplication is prevented
- connector/webhook/notification state transitions are explicit
- APIs are authenticated and authorized
- cross-tenant integration isolation passes
- failure/recovery tests pass
- distributed delivery is safe
- observability is present
- existing Rust/Python/security/RAG tests remain green

---

# FINAL STOP CONDITION

STOP after completing:

**Enterprise Integrations + Connectors + Webhooks + Notifications + SSRF Production Wiring**

Do NOT start:

- React frontend
- dashboard UI
- marketing pages
- cosmetic redesign
- unrelated AI features

Do NOT create:

- second connector registry
- second webhook delivery engine
- second notification queue
- second secret authority
- unrestricted HTTP client
- in-memory production delivery state
- synthetic delivery counters

Do NOT shorten files.

Do NOT provide partial files.

Wait for explicit approval before the next phase.