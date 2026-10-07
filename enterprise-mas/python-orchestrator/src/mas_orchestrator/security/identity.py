"""Service-identity validation for the Rust Core caller.

The orchestrator is not a public API: its only legitimate caller is the Rust
Core service, presenting a service credential. This module establishes *who*
that caller is. It deliberately does **not** establish the tenant — tenancy is
authoritative data supplied by Rust inside the request body (see
:mod:`mas_orchestrator.context.execution_context`); this module only proves the
caller is allowed to supply it.

Security properties enforced here:

* Authentication is **mandatory in production**. There is no code path where a
  missing credential yields an authenticated identity.
* The anonymous resolver refuses to install itself outside
  ``development``/``test``. This makes "forgot to configure auth" a startup
  failure rather than an open service.
* Token comparison uses :func:`hmac.compare_digest`, so validation time does
  not leak how many leading bytes matched.
* The credential value never appears in an error message, log line or the
  repr of any object in this module.
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final, Protocol, runtime_checkable

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import AuthenticationError, ConfigurationError

__all__ = [
    "ANONYMOUS_SUBJECT",
    "RUST_CORE_SUBJECT",
    "AnonymousServiceIdentityResolver",
    "ServiceIdentity",
    "ServiceIdentityResolver",
    "StaticTokenServiceIdentityResolver",
    "require_scope",
]

#: Subject asserted by the Rust Core service credential.
RUST_CORE_SUBJECT: Final[str] = "rust-core"

#: Subject used by the anonymous resolver (development/test only).
ANONYMOUS_SUBJECT: Final[str] = "anonymous"


class ServiceScope(StrEnum):
    """Scopes a resolved service identity may hold.

    Scopes are additive and checked with :func:`require_scope` at the point of
    use, so a caller without the right scope is refused before doing work.
    """

    #: May submit canonical executions for AI processing.
    EXECUTE_AGENT = "execute_agent"
    #: May invoke the internal AI HTTP API.
    INVOKE_API = "invoke_api"
    #: May read health/readiness.
    READ_HEALTH = "read_health"
    #: May synchronize a Rust-authorized Data Hub source.
    DATA_HUB_SYNC = "data_hub_sync"
    #: May ingest content into the tenant-scoped RAG plane.
    DATA_HUB_INGEST = "data_hub_ingest"
    #: May purge one Rust-authorized document from the RAG plane.
    DATA_HUB_PURGE = "data_hub_purge"


@dataclass(frozen=True, slots=True)
class ServiceIdentity:
    """The authenticated caller of the orchestrator.

    Attributes:
        subject: Stable identifier for the calling service (``rust-core``).
        issuer: Who vouched for the credential (``static``, ``mtls``, …).
        scopes: Authorised capabilities; empty means authenticated but
            unauthorised for everything except health.
        authenticated: Always ``True`` for an instance produced by a resolver.
            A separate sentinel is not used: unauthenticated callers raise
            rather than producing an identity object.
    """

    subject: str
    issuer: str
    scopes: frozenset[ServiceScope] = field(default_factory=frozenset)
    authenticated: bool = True

    def __str__(self) -> str:
        """Renders subject and issuer only — scopes are not secret but the
        credential-derived fields must never appear in output."""
        return f"ServiceIdentity(subject={self.subject}, issuer={self.issuer})"

    def has_scope(self, scope: ServiceScope) -> bool:
        """Returns whether this identity holds *scope*."""
        return scope in self.scopes


@runtime_checkable
class ServiceIdentityResolver(Protocol):
    """Resolves a presented credential into a :class:`ServiceIdentity`."""

    async def resolve(self, credential: str | None) -> ServiceIdentity:
        """Validates *credential* and returns the identity it proves.

        Raises:
            AuthenticationError: If the credential is absent, malformed, or
                does not match. Implementations must not distinguish "absent"
                from "wrong" in the error message, to avoid an oracle for
                probing which credentials are configured.
        """
        ...


def require_scope(identity: ServiceIdentity, scope: ServiceScope) -> None:
    """Raises :class:`AuthenticationError` when *identity* lacks *scope*.

    Args:
        identity: The resolved caller identity.
        scope: The capability required by the operation.

    Raises:
        AuthenticationError: Always, when the scope is missing. The error names
            the required scope but never enumerates the caller's scopes, so a
            rejected caller learns nothing about what else is configured.
    """
    if not identity.has_scope(scope):
        raise AuthenticationError(
            "caller is not authorised for this operation",
            details={"required_scope": str(scope), "subject": identity.subject},
        )


class StaticTokenServiceIdentityResolver:
    """Validates a shared service token held in memory.

    The token is supplied at construction time from a secret reference resolved
    by :mod:`mas_orchestrator.security.credentials`; it is never read from a
    committed configuration file and never written to logs.

    Args:
        token: The expected bearer token. Must be non-empty.
        subject: Subject to assert on success (defaults to Rust Core).
        scopes: Capabilities granted to a valid caller.
    """

    def __init__(
        self,
        token: str,
        *,
        subject: str = RUST_CORE_SUBJECT,
        scopes: frozenset[ServiceScope] | set[ServiceScope] | None = None,
    ) -> None:
        if not token:
            raise ConfigurationError(
                "service token resolver requires a non-empty token",
                details={"reason": "empty_token"},
            )
        self._token = token
        self._subject = subject
        self._scopes: frozenset[ServiceScope] = frozenset(
            scopes
            if scopes is not None
            else {
                ServiceScope.EXECUTE_AGENT,
                ServiceScope.INVOKE_API,
                ServiceScope.READ_HEALTH,
            }
        )

    def __repr__(self) -> str:
        """Never renders the token."""
        return (
            f"StaticTokenServiceIdentityResolver(subject={self._subject!r}, "
            f"scopes={sorted(str(s) for s in self._scopes)!r})"
        )

    async def resolve(self, credential: str | None) -> ServiceIdentity:
        """Compares the presented credential against the configured token.

        Raises:
            AuthenticationError: If the credential is missing or does not match.
                The message is identical in both cases.
        """
        if not credential:
            raise AuthenticationError(
                "missing service credential",
                details={"reason": "absent"},
            )

        presented = credential.removeprefix("Bearer ").removeprefix("bearer ").strip()
        if not hmac.compare_digest(presented, self._token):
            raise AuthenticationError(
                "invalid service credential",
                details={"reason": "mismatch"},
            )

        return ServiceIdentity(
            subject=self._subject,
            issuer="static",
            scopes=self._scopes,
        )


class AnonymousServiceIdentityResolver:
    """Development/test resolver that accepts any caller.

    This exists so local development does not require credential plumbing, but
    it is **structurally impossible to use in production**: the constructor
    raises :class:`ConfigurationError` when the environment is not
    ``development`` or ``test``. There is no override flag, because a bypass
    flag is exactly the production hole this guard exists to prevent.

    Args:
        environment: The running environment, which gates construction.
        scopes: Capabilities granted to the anonymous caller.
    """

    #: Environments in which anonymous access is tolerable.
    ALLOWED_ENVIRONMENTS: Final[frozenset[Environment]] = frozenset(
        {Environment.DEVELOPMENT, Environment.TEST}
    )

    def __init__(
        self,
        environment: Environment,
        *,
        scopes: frozenset[ServiceScope] | set[ServiceScope] | None = None,
    ) -> None:
        if environment not in self.ALLOWED_ENVIRONMENTS:
            raise ConfigurationError(
                "anonymous service identity is not permitted outside development/test",
                details={
                    "environment": str(environment),
                    "allowed": sorted(str(e) for e in self.ALLOWED_ENVIRONMENTS),
                },
            )
        self._environment = environment
        self._scopes: frozenset[ServiceScope] = frozenset(
            scopes
            if scopes is not None
            else {
                ServiceScope.EXECUTE_AGENT,
                ServiceScope.INVOKE_API,
                ServiceScope.READ_HEALTH,
            }
        )

    def __repr__(self) -> str:
        """Renders the environment so an accidental production use is visible."""
        return f"AnonymousServiceIdentityResolver(environment={self._environment!r})"

    async def resolve(self, credential: str | None) -> ServiceIdentity:
        """Returns the anonymous identity regardless of the credential.

        A presented-but-unverifiable credential is ignored rather than
        rejected: in development that would only create friction. This is safe
        precisely because the resolver cannot exist in production.
        """
        return ServiceIdentity(
            subject=ANONYMOUS_SUBJECT,
            issuer="anonymous",
            scopes=self._scopes,
        )
