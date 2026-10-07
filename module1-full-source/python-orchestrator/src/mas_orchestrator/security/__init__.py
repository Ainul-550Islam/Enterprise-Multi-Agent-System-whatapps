# PURPOSE:
# The public surface of Python's local security controls: redaction, secret
# retrieval, caller identity resolution, internal-signature verification, and
# transport configuration.
#
# Owning layer: `mas_orchestrator.security` (the Python process edge).
#
# Production responsibility: guarantee that Python never leaks a secret, never
# accepts an unauthenticated caller in production, and never lets a header
# establish tenancy — and expose exactly one import path for all of it, so a
# caller cannot accidentally reach past the intended surface.
#
# Security boundary: two independent proofs guard the bridge, and both are
# reachable from here. The bearer credential proves possession; the HMAC
# proves that the holder sent *this* message, recently, unaltered. Either
# alone is insufficient.
#
# It must NOT:
#   • replace platform security — authorization, tenancy, policy and audit
#     remain owned by Rust;
#   • re-export a symbol that is not part of the intended public surface.

"""Python-local security controls.

These are *local* controls: redaction before anything is logged, retrieval of
credentials from a secret manager rather than from source or committed config,
validation of the Rust caller's service identity, verification of the
body-bound signature on every inbound internal call, and TLS/mTLS transport
configuration.

They do not replace platform security. Authorization, tenancy, policy and
audit remain owned by Rust (spec §5); this package only ensures Python never
leaks a secret, never accepts an unauthenticated caller in production, and
never lets a header establish tenancy.

Two independent proofs guard the bridge:

* **Proof of possession** — the bearer credential, resolved by
  :class:`ServiceIdentityResolver`. It says which subject the caller claims.
* **Proof of this message** — the HMAC in
  :func:`~mas_orchestrator.security.internal_service.verify_internal_signature`,
  computed over the exact request bytes and bound to a timestamp. It says the
  holder of the shared secret sent *this* call, recently, unaltered.

Either alone is insufficient: a static token is replayable by anything that
can read it, and a signature identifies a service rather than a subject.
"""

from __future__ import annotations

from mas_orchestrator.security.credentials import (
    DEFAULT_RESOLVER_CHAIN,
    ENVIRONMENT_SCHEME,
    FILE_SCHEME,
    EnvironmentSecretResolver,
    FileSecretResolver,
    Secret,
    SecretResolver,
    SecretResolverChain,
    resolve_secret,
)
from mas_orchestrator.security.identity import (
    ANONYMOUS_SUBJECT,
    RUST_CORE_SUBJECT,
    AnonymousServiceIdentityResolver,
    ServiceIdentity,
    ServiceIdentityResolver,
    ServiceScope,
    StaticTokenServiceIdentityResolver,
    require_scope,
)
from mas_orchestrator.security.internal_service import (
    DEFAULT_ALLOWED_CALLERS,
    MAX_SKEW_SECONDS,
    SERVICE_HEADER,
    SIGNATURE_HEADER,
    InternalServiceIdentity,
    now_millis,
    signed_internal_signature,
    verify_internal_signature,
)
from mas_orchestrator.security.redaction import (
    REDACTED,
    SENSITIVE_KEY_SUBSTRINGS,
    is_sensitive_key,
    redact_key,
    redact_mapping,
    redact_text,
    redact_value,
)
from mas_orchestrator.security.transport import (
    INSECURE_ALLOWED_ENVIRONMENTS,
    ClientTransportConfig,
    ServerTransportConfig,
    validate_transport_security,
)

__all__ = [
    "ANONYMOUS_SUBJECT",
    "DEFAULT_ALLOWED_CALLERS",
    "DEFAULT_RESOLVER_CHAIN",
    "ENVIRONMENT_SCHEME",
    "FILE_SCHEME",
    "INSECURE_ALLOWED_ENVIRONMENTS",
    "MAX_SKEW_SECONDS",
    "REDACTED",
    "RUST_CORE_SUBJECT",
    "SENSITIVE_KEY_SUBSTRINGS",
    "SERVICE_HEADER",
    "SIGNATURE_HEADER",
    "AnonymousServiceIdentityResolver",
    "ClientTransportConfig",
    "EnvironmentSecretResolver",
    "FileSecretResolver",
    "InternalServiceIdentity",
    "Secret",
    "SecretResolver",
    "SecretResolverChain",
    "ServerTransportConfig",
    "ServiceIdentity",
    "ServiceIdentityResolver",
    "ServiceScope",
    "StaticTokenServiceIdentityResolver",
    "is_sensitive_key",
    "now_millis",
    "redact_key",
    "redact_mapping",
    "redact_text",
    "redact_value",
    "require_scope",
    "resolve_secret",
    "signed_internal_signature",
    "validate_transport_security",
    "verify_internal_signature",
]
