# NEXT PHASE — ENTERPRISE SECURITY + AUTHORIZATION + TENANCY + RLS HARDENING

## ROLE

Act as a Principal Security Engineer + Enterprise Architect + Rust Application Security Engineer + PostgreSQL/RLS Specialist + Identity Engineer.

The canonical durable execution pipeline is now reported complete.

Do NOT assume that report means the entire enterprise security model is complete.

This phase is specifically for:

- authentication
- authorization
- principal identity
- organization identity
- tenant identity
- project/workspace scope
- RBAC
- ABAC where applicable
- service identity
- API keys
- internal service authentication
- RLS
- scoped database access
- privileged operations
- secret handling
- security boundaries
- auditability

---

# ABSOLUTE RULE — NO SHORTENING

For EVERY changed/new file:

- output full file
- first line through final line
- no `...`
- no omitted content
- no placeholder
- no pseudo-code
- no "rest unchanged"
- no "existing code"
- no shortened implementation

Preserve unrelated existing functionality.

---

# FILE COMMENT RULE

Every new file MUST contain a specific:

`PURPOSE:`

comment.

The comment must explain:

- file responsibility
- owning layer
- production role
- security boundary
- what the file must NOT do

Do not use generic boilerplate.

---

# PRIMARY SECURITY GOAL

The authoritative security chain must be:

```text
Credential
   ↓
Authentication
   ↓
Principal
   ↓
Organization membership
   ↓
Tenant membership
   ↓
Project/workspace scope
   ↓
Role / permission / policy
   ↓
Resource authorization
   ↓
RLS-scoped transaction
   ↓
Repository
   ↓
Database
```

No lower layer may trust identity information supplied by an untrusted caller.

---

# PHASE 1 — FULL IDENTITY TRACE

Trace every production entrypoint:

- HTTP API
- gRPC API
- scheduler completion
- worker
- internal service-to-service calls
- CLI privileged operations
- webhook/integration callbacks
- future Python bridge boundary

For each path identify:

- credential source
- authentication function
- principal creation
- tenant resolution
- organization resolution
- permission resolution
- database scope
- audit identity

Find every place where:

- tenant comes from headers
- organization comes from headers
- role comes from request body
- scope comes from request metadata
- user identity is reconstructed downstream
- caller can override authoritative context

---

# PHASE 2 — PRINCIPAL MODEL

Create or complete one canonical principal representation.

It must represent the authenticated actor.

Depending on existing architecture, distinguish:

- human user
- API key principal
- service account
- internal service
- scheduler identity
- worker identity

Do NOT create separate incompatible principal models.

Principal information must include whatever the platform actually needs for:

- authorization
- audit
- tenant resolution
- service-to-service trust

---

# PHASE 3 — TENANT RESOLUTION

Tenant identity MUST NOT be accepted as authoritative merely because it appears in:

- `x-tenant-id`
- `x-organization-id`
- arbitrary metadata
- request JSON
- query parameter

Headers may be treated only as:

- routing hints
- compatibility metadata
- diagnostics

when appropriate.

The authoritative tenant must come from authenticated membership or an explicitly trusted internal identity chain.

When a supplied scope conflicts with authoritative scope:

return deterministic authorization failure.

Never silently switch tenant.

---

# PHASE 4 — ORGANIZATION / TENANT / PROJECT RELATIONSHIP

Verify the complete hierarchy:

```text
Organization
  ↓
Tenant
  ↓
Project / Workspace
  ↓
Resource
```

A caller must not combine unrelated IDs from separate scopes.

Explicitly prevent:

```text
Tenant A + Organization B
Tenant A + Project B
Organization A + Project B
```

when those relationships are invalid.

Prefer database-enforced composite relationships where the current schema supports them.

---

# PHASE 5 — AUTHORIZATION SERVICE

Implement or complete one canonical:

`AuthorizationService`

It must provide resource-level decisions such as:

- can read
- can create
- can update
- can delete
- can execute
- can cancel
- can publish
- can administer
- can manage credentials
- can manage policies
- can view audit data
- can manage billing

Do NOT scatter ad-hoc role checks throughout handlers.

The handler may call the authorization service, but the service owns authorization semantics.

---

# PHASE 6 — RBAC

Audit existing role definitions.

Verify:

- role identity is authoritative
- permissions are stable
- privileged roles are explicit
- deny-by-default applies where appropriate
- role changes are auditable

Test:

- viewer
- operator
- developer/editor
- administrator
- service account
- internal service

Do not grant broad admin capability merely because a request is authenticated.

---

# PHASE 7 — ABAC / RESOURCE AUTHORIZATION

Where resource-specific access is required, verify:

```text
principal
+
tenant
+
organization
+
project
+
resource
+
operation
```

before mutation or sensitive read.

At minimum secure:

- execution
- task
- workflow
- agent
- tool
- connector
- credential metadata
- policy
- usage
- audit
- webhook
- subscription/entitlement

Do not rely on frontend permission checks.

Backend authorization is authoritative.

---

# PHASE 8 — API KEY SECURITY

Audit API key lifecycle:

- create
- display-once secret
- hashing
- lookup
- expiration
- revocation
- rotation
- scope
- tenant binding
- audit

Never store raw API keys.

Never return raw secrets after creation.

Never allow an API key to select an unrelated tenant.

---

# PHASE 9 — INTERNAL SERVICE AUTHENTICATION

Secure:

- Rust → Python
- worker → internal services
- scheduler → internal services
- CLI → privileged endpoints

An internal request must have authenticated service identity.

Do NOT equate:

```text
network access = trusted
```

Use explicit credentials/service identity according to the existing architecture.

---

# PHASE 10 — PYTHON BRIDGE SECURITY

Preserve the current AgentBridge contract.

Rust remains authoritative for:

- tenant
- organization
- project
- execution
- task

Python must validate the incoming internal service identity and scope.

Python must not accept arbitrary tenant switching.

Do not let an AI model determine its own:

- tenant
- organization
- permissions
- credential scope

---

# PHASE 11 — RLS PRODUCTION PATH

The database guarantees must be real.

Verify every production query path:

```text
authenticated request
→ authoritative tenant context
→ transaction starts
→ RLS context applied
→ repository queries
→ commit/rollback
```

The RLS context MUST be transaction-local.

Do not set tenant state globally on a pooled connection.

Do not rely on application filtering alone.

---

# PHASE 12 — POOLED CONNECTION SAFETY

Test connection pool reuse.

Scenario:

1. Request A sets Tenant A context
2. transaction completes
3. connection returns to pool
4. Request B uses same connection
5. Request B must see only Tenant B

Ensure no leaked transaction-local state.

Explicitly test rollback and connection reuse.

---

# PHASE 13 — RLS POLICY AUDIT

Inspect every RLS-enabled table.

Verify:

- SELECT policy
- INSERT policy
- UPDATE policy
- DELETE policy
- tenant predicate
- organization predicate where required
- FORCE RLS
- privileged-role behavior

Detect any table that is:

- tenant-owned but not RLS-protected
- reachable through an unscoped repository
- mutable without scope predicate
- indirectly exposed through joins

---

# PHASE 14 — SUPERUSER / BYPASSRLS

Audit DB roles.

Identify:

- application role
- migration role
- admin role
- test role

Production application role must not unintentionally bypass RLS.

Do not use a superuser as normal application runtime identity.

Document the legitimate privileged migration path separately.

---

# PHASE 15 — SCOPED DATABASE API

Create one safe production abstraction for scoped DB operations.

Conceptually:

```text
with_authoritative_scope(...)
    ↓
transaction
    ↓
RLS SET LOCAL
    ↓
repositories
```

Repositories that require tenant data MUST be impossible or difficult to accidentally call without scope.

Do not make every caller manually remember security context.

---

# PHASE 16 — CROSS-TENANT TEST MATRIX

Add real tests for:

### Read

Tenant A cannot read Tenant B.

### Write

Tenant A cannot update Tenant B.

### Delete

Tenant A cannot delete Tenant B.

### Execute

Tenant A cannot execute Tenant B's agent/workflow/tool.

### Cancel

Tenant A cannot cancel Tenant B's execution.

### Usage

Tenant A cannot read Tenant B usage.

### Audit

Tenant A cannot read Tenant B audit entries.

### Webhook

Tenant A cannot access Tenant B webhook configuration/deliveries.

### Credentials

Tenant A cannot access Tenant B credential metadata.

---

# PHASE 17 — ID CONFUSION TESTING

Explicitly test UUID substitution.

Example:

Valid:

```text
tenant=A
project=A1
execution=A1E1
```

Attack:

```text
tenant=A
project=B1
execution=B1E1
```

Every mismatched combination must fail safely.

Do not rely solely on frontend validation.

---

# PHASE 18 — PRIVILEGED OPERATIONS

Identify all operations that require elevated privilege:

- tenant deletion
- organization administration
- API key management
- credentials
- policy changes
- quota overrides
- subscription changes
- billing changes
- audit access
- user/membership administration
- environment configuration

Every privileged operation must have explicit authorization.

---

# PHASE 19 — SECRET MANAGEMENT

Audit all secret paths:

- environment variables
- database
- configuration
- logs
- protobuf
- broker messages
- Python runtime
- Rust runtime
- tests
- CI
- container/image configuration

Secrets must never be:

- committed
- logged
- serialized into normal events
- included in error messages
- returned by normal API responses

Implement/complete a production secret-manager abstraction.

Do NOT hard-code vendor credentials.

---

# PHASE 20 — ENCRYPTION / INTEGRITY

Audit where the system stores sensitive values.

Determine:

- encryption at rest support
- TLS requirements
- key management
- key versioning
- rotation strategy
- integrity protection

Do not claim compliance certifications.

Only implement/support the technical controls.

---

# PHASE 21 — SECURITY AUDIT LOGGING

Every security-sensitive event must be auditable where applicable:

- login/authentication failure
- API key creation/revocation
- permission denial
- tenant mismatch
- credential access
- policy change
- privilege change
- execution authorization
- cancellation
- admin operation

Audit events must not leak secrets.

---

# PHASE 22 — AUTH FAILURE SEMANTICS

Use stable classifications.

At minimum:

- unauthenticated
- invalid credential
- expired credential
- forbidden
- tenant mismatch
- organization mismatch
- insufficient permission
- invalid service identity

Do NOT convert all security failures into generic internal errors.

Do NOT retry authentication/authorization failures.

---

# PHASE 23 — SSRF SECURITY

Preserve the existing SSRF guard.

Now verify the REAL production caller.

The production HTTP integration path must actually invoke the guard.

Validate:

- scheme
- hostname
- DNS resolution
- private IP
- loopback
- link-local
- metadata services
- redirect targets
- IPv4/IPv6
- ports
- DNS rebinding concerns

Do not mark SSRF protection complete merely because a helper exists.

---

# PHASE 24 — AI SECURITY

Verify that the Python agent/tool layer cannot escape its assigned scope.

Prevent:

- cross-tenant retrieval
- unauthorized tool calls
- credential exfiltration
- prompt-induced privilege escalation
- unsafe tool output execution
- model-controlled permission escalation

Tool execution must receive authorization context from the platform, not from model output.

---

# PHASE 25 — WORKER / SCHEDULER SECURITY

Worker and scheduler identities must be explicit.

Workers must not be able to arbitrarily:

- impersonate another tenant
- claim another tenant's task
- alter owner identity
- bypass authorization

Scheduler completion must authenticate its source.

Do not trust completion payload identity without validating service identity.

---

# PHASE 26 — CLI SECURITY

Audit CLI commands for privileged operations.

The CLI must not silently operate as:

- arbitrary tenant
- arbitrary organization
- arbitrary admin

without authenticated and authorized context.

Migration operations may legitimately require elevated DB credentials, but runtime API credentials must remain separate.

---

# PHASE 27 — SECURITY TESTS

Add tests for:

1. forged tenant header
2. forged organization header
3. forged role
4. forged service scope
5. cross-tenant read
6. cross-tenant write
7. cross-project access
8. revoked API key
9. expired API key
10. invalid API key
11. unauthorized service identity
12. scheduler completion without auth
13. RLS connection reuse
14. RLS transaction rollback
15. privileged operation denial
16. credential metadata isolation
17. SSRF private address
18. SSRF redirect to private address
19. AI tool permission escalation
20. stale security context after transaction

---

# PHASE 28 — STATIC SECURITY SCAN

Search source for:

- tenant headers
- organization headers
- bypassrls
- superuser
- secret
- credential
- bearer
- authorization
- unwrap
- expect
- unsafe
- debug logging
- println
- hard-coded tokens
- test credentials
- default passwords

Classify every occurrence.

Do not mark legitimate test-only references as production defects.

---

# PHASE 29 — PRODUCTION PROHIBITION LIST

The following MUST NOT occur in production:

- caller-trusted tenant identity
- caller-trusted organization identity
- caller-trusted role
- caller-trusted permission
- caller-trusted service scope
- unauthenticated scheduler completion
- RLS bypass through normal runtime role
- pooled connection tenant leakage
- raw API keys in DB
- raw provider credentials in logs
- secrets in broker payloads
- model-controlled authorization
- tool-controlled tenant identity
- unrestricted outbound HTTP
- privilege escalation through internal endpoints

---

# PHASE 30 — BUILD / TEST VERIFICATION

Run:

```bash id="v9o8hy"
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --locked
cargo test --workspace --locked --features nats
cargo doc --workspace --all-features --no-deps
```

Python:

```bash id="b8ka39"
python3 -m compileall -q src tests
python3 -m ruff check src tests
python3 -m ruff format --check src tests
PYTHONPATH=src python3 -m mypy src/mas_orchestrator
PYTHONPATH=src python3 -m pytest -q
```

Run all relevant security/RLS/live-PostgreSQL/integration tests.

If a command cannot run, state the exact reason.

Never claim success without executing it.

---

# PHASE 31 — NO REGRESSION

Preserve:

- canonical execution lifecycle
- durable attempts
- fencing
- outbox
- worker capacity
- Rust→Python AgentBridge
- Python identity reconciliation
- provider failover
- tenant-safe tools
- usage
- audit
- cancellation

Security hardening must not break execution functionality.

---

# PHASE 32 — FULL FILE OUTPUT

For every changed file:

```text
FILE: exact/path
PURPOSE: exact responsibility
```

Then the FULL file.

No snippets.

No omissions.

No shortening.

No replacement pseudo-code.

---

# COMPLETION GATE

This phase is complete ONLY when:

- one canonical Principal model exists
- tenant comes from authoritative identity
- organization comes from authoritative identity
- project/resource scope is validated
- AuthorizationService is canonical
- RBAC is enforced server-side
- privileged operations are protected
- API keys are securely managed
- service-to-service authentication is enforced
- Rust→Python identity is authenticated
- Python cannot select its own tenant
- RLS is applied in the real production transaction path
- pooled connection leakage is prevented
- production runtime cannot bypass RLS unintentionally
- cross-tenant reads/writes are blocked
- cross-project access is blocked
- scheduler completion is authenticated
- secrets are protected
- SSRF guard is used by production caller
- AI tools cannot escalate permissions
- worker/scheduler identities are protected
- security audit events are generated
- security test matrix passes
- existing execution/bridge tests remain green

---

# FINAL STOP CONDITION

STOP after Enterprise Security + Authorization + Tenancy + RLS hardening.

Do NOT start:

- frontend
- billing UI
- cosmetic refactoring
- unrelated integrations
- RAG enhancements
- marketing/demo work

Do NOT create a second identity system.

Do NOT create a second authorization system.

Do NOT bypass RLS for convenience.

Do NOT trust user-provided tenant/org headers.

Do NOT shorten files.

Do NOT provide partial files.

Wait for explicit approval before the next phase.