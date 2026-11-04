# Security Model (English whitepaper)

Verified against `reports/SECURITY_NARRATIVE_BN.md` (Bangla) and the CODE.
Anything the code does not enforce is dropped here; nothing is implied.

## Enforced properties (each with implementing file)

1. **Bearer-token verification boundary** — `crates/security/src/tokens.rs`,
   `crates/security/src/jwks.rs`. Unauthenticated protected calls return 401
   with zero token material in errors (`response.rs` `stable_error`). The
   durable OIDC lane requires an audience and maps the exact verified issuer
   and subject to an explicitly linked active local user via the directory
   pool. External platform-administration scopes and wildcards are rejected;
   local membership authorization is resolved independently on each request.
   Identity linking and provisioning remain trusted operator responsibilities.
   Unknown signing-key IDs and failed JWKS refreshes are throttled per verifier
   instance for five seconds; known unexpired cached keys remain usable. A new
   signing key may be rejected during this window, including after a transient
   fetch failure or forged unknown-key request. Deployments should publish new
   keys before using them, retain old keys during rotation, rate-limit inbound
   authentication requests, and account for independent caches per instance.
2. **Internal service HMAC / body-bound signatures** — Rust
   `crates/worker-service/src/agent_execution.rs` (`AgentBridgeSettings`,
   `require_internal_signing_secret`); Python
   `src/mas_orchestrator/main.py` (`build_bridge_server`,
   `require_internal_signature`). Production refuses unsigned calls.
3. **Tenant-scoped reads (foreign tenant → 404, never 403)** —
   `crates/application/src/execution_service.rs`, handler layer.
4. **Row-level security (RLS) armed per request** —
   `crates/persistence/src/transaction.rs`, `crates/persistence/src/rls.rs`.
5. **Secret references, never inline secrets** —
   `crates/security/src/secret_string.rs`, `crates/security/src/secret_gate.rs`;
   Python `config/secrets.py`.
6. **Audit sink and read APIs** — `crates/application/src/audit.rs`,
   `crates/application/src/audit_query_service.rs`. Some mutation paths record
   audit events and propagate sink errors; audit coverage and atomicity with
   every mutation are not established. The API's separately pooled audit writer
   is not in the request transaction, and error responses can still commit the
   request transaction.
7. **SSRF guard on outbound egress** — `crates/integrations` (guarded HTTP
   client, `EgressPolicy`: scheme/port/hostname allowlists, DNS pinning,
   redirect re-validation, response caps).
8. **Production fail-closed guardrails** —
   `crates/api/src/main.rs` (`enforce_production_guardrails`, production OIDC
   HTTPS validation); Python `main.py` production composition.

## Not enforced (dropped from BN narrative where code doesn't implement)

* SOC2 / ISO certification — not implemented; docs say "not implemented".
* Frontend / dashboard UI — not in code.
* WhatsApp channel — not in code.
* Stripe checkout and a production Stripe webhook handler — not in code. A
  Stripe raw-body signature verifier and verified-receipt adapter exist, but
  are not wired to an ingress route. The generic billing receipt store relies
  on its caller to verify signatures; neither store path provides a processing
  lease, recovery worker, or atomic coupling of a receipt to billing effects. Old unverified receipts may already
  occupy event IDs and require operator investigation rather than automatic
  replacement. Migration 0055 blocks new unverified receipt rows and makes
  receipt identity and terminal verdicts immutable at the database boundary;
  it preserves pre-existing unverified rows. Neither a database boolean nor
  these guards prove that a provider signature was actually verified.

## Supported versions

0.1.0 (current development line). Earlier releases unsupported.

## Reporting

See `SECURITY.md` at the repo root.


### Membership invitation delegation

HTTP and gRPC membership creation use the same application-layer delegation
check. A tenant manager must hold a directory-resolved role with
`manage_members`, and may invite only a role strictly below their own. Thus an
admin cannot invite another admin or an owner; even an owner cannot invite a
peer owner through this path. Initial owner provisioning requires a trusted
system bootstrap. A service actor without a resolved role is not a system
bootstrap. The target tenant must belong to the resolved organization.

This restricts creation of invited membership rows; it is **not** a completed
invitation acceptance or delivery flow. Existing invitations created before
this check require separate review, and revocation between resolution and the
write is not atomically fenced. As with other application writes, audit and
mutation are not yet committed atomically.

### Python execution memory boundary

The Python in-process memory backend is for development/testing, not durable
production storage. Task recall and deletion are execution-scoped; episodic
recall and deletion are agent-scoped; durable and semantic records currently
use tenant-wide visibility. Sensitive writes require an explicitly configured
redaction hook, whose effectiveness remains a trusted deployment boundary.

Execution cleanup uses an atomic backend operation that deletes the entire
working set and fences later task-memory creation. The in-process fence is
bounded and disappears on process restart; reaching its capacity fails cleanup
rather than claiming success. It is not a distributed lifecycle authority.
Custom memory stores must implement atomic create, scoped deletion, and
execution cleanup/fencing; the manager fails closed when those operations are
missing. Rust-owned production memory and its lifecycle still require their own
independent enforcement.

### Orchestrator outbound HTTP state and redirects

The shared guarded HTTP client does not accept or persist server-set cookies;
otherwise a later request sharing the client could replay another tenant's
session. Explicit caller-supplied `Cookie` headers remain the caller's
responsibility. Redirects are limited to the original origin even when both
hosts are allowlisted, because custom authentication headers cannot be
reliably recognized by a finite denylist. Configure canonical provider and
crawl URLs rather than relying on cross-origin redirects. The serialized JSON
request body is checked against the transport's byte ceiling before network
I/O; serialization itself still allocates memory, so upstream request-size
limits remain necessary. DNS pinning and network-layer egress controls remain
required.

The `security.credentials.Secret` value wrapper is deliberately unhashable:
it compares underlying values independently of their references, and hashing
by reference violated Python's equality/hash contract. Callers that need a
mapping key should use an authorized non-secret identifier, not the secret
object. This does not change the separate `config.secrets.Secret` type.

### Data Hub object-storage credential bindings

Source configuration is tenant-controlled metadata, not authority to read an
orchestrator environment variable or mounted file. S3 sync therefore defaults
to denying all secret resolution unless an operator-owned
`data_hub.object_storage_bindings` entry exactly matches the tenant,
organization, project, source, normalized HTTPS endpoint, bucket, prefix,
region, and both secret references. The check occurs before resolving either
reference or making an outbound request. Endpoint hosts must independently be
on `allowed_object_hosts`. A signed bridge request does not grant authority to
choose arbitrary secret references or redirect credentials to another host.

Provision the source first, then place a binding in an operator-controlled
TOML overlay and restart the orchestrator. For example (replace every value):

```toml
[[data_hub.object_storage_bindings]]
tenant_id = "00000000-0000-4000-8000-000000000012"
organization_id = "00000000-0000-4000-8000-000000000013"
project_id = "00000000-0000-4000-8000-000000000014"
source_id = "00000000-0000-4000-8000-000000000011"
endpoint_url = "https://storage.example.test"
bucket = "test-bucket"
prefix = "docs/"
region = "us-east-1"
access_key_secret_ref = "env:TEST_ACCESS_KEY"
secret_key_secret_ref = "env:TEST_SECRET_KEY"
```

For container deployments the same list can be provided as a JSON array in
the operator-controlled `MAS_ORCH_DATA_HUB__OBJECT_STORAGE_BINDINGS`
environment variable; it is bounded to 1 MiB and invalid JSON fails startup.

The binding configuration itself must not be tenant-writable; references
must resolve to least-privilege credentials restricted by the provider to the
approved bucket and prefix. Source edits, reference rotation by name, endpoint
changes, and scope changes require operator review and matching configuration.
Rotating the value behind an unchanged approved reference remains supported.
Deploy this fail-closed change only after provisioning approved bindings for
existing sources. Exact binding is not a substitute for IAM isolation, egress
controls, or a tenant-aware secret manager.

Environment-backed secret values are not configuration settings. The settings
loader excludes variables in the reserved `MAS_ORCH_SECRET_` namespace and
variables explicitly named by `env:` references in operator configuration,
including superseded layers and object-binding JSON. This prevents both
startup failures and validation errors that echo those values. Unknown
non-secret settings still fail validation. Secret references must not name a
root or nested `MAS_ORCH_` settings variable; that collision fails startup.
Keep other unreferenced secret variables out of the `MAS_ORCH_` settings
namespace, and protect process environments and configuration from disclosure.

### Structured logs and exception handling

With production redaction enabled, the orchestrator's configured logging
handler recursively redacts structured fields (including sensitive field names)
and drops exception and stack text rather than printing arbitrary validation
inputs, provider payloads, or custom exception strings. It retains an explicit
redaction marker; applications should log safe error classifications and
correlation IDs for diagnosis. Redaction failure replaces the record instead
of emitting the original. Free-form pattern scrubbing also handles quoted and
short credential assignments, but pattern matching cannot identify every
secret: do not log request bodies, arbitrary exception text, URLs containing
credentials, or secret values. Third-party handlers outside the configured
handler are not covered by this filter. Disabling logging redaction in
non-production removes these protections.

### Data Hub source URL metadata

Source registration and synchronization reject configured web start URLs and
object-storage endpoint URLs containing userinfo, query strings, fragments,
controls, non-HTTPS schemes, or nonstandard ports. Data Hub source DTOs mask
URL and secret-reference values, including values in legacy rows. This is a
compatibility change: existing sources with query-bearing root URLs must be
reconfigured before sync; API clients must not expect full URLs in source
metadata. URL paths, arbitrary config values, document titles and upstream
crawl links can still contain sensitive text; do not put credentials there,
and review legacy data. The catalogue metadata read authorization is distinct
from RAG document-content ACL enforcement and is not an ACL-protected search
surface.

Citation source URIs are untrusted metadata. Data Hub strips queries and
fragments from both absolute URLs and relative paths, and rejects malformed,
credential-bearing, protocol-relative and backslash-containing references.
Paths may still contain sensitive text, and earlier indexed records are not
retroactively rewritten; purge or reindex legacy content if needed. This is
not a substitute for escaping and validating URLs in every rendering client.
The web crawler ignores invalid, off-origin or disallowed links discovered in
untrusted pages, but still rejects invalid configured roots. The HTTP transport
continues enforcing destination and redirect policy for every fetched URL.

JSON document parsing limits flattened output while accumulating it, before a
long parent key can be multiplied across many leaves, and rejects nesting past
100 traversal levels. This may reject deeply nested documents or documents
whose unnormalized flattened text exceeds the limit even if later whitespace
normalization would have shortened it. Input decoding still builds a JSON
object before flattening; use the configured isolated parser workers and
container memory/CPU limits rather than relying on the output limit alone.
