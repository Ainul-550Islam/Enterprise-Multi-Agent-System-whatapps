"""Secret and credential retrieval.

There is exactly one rule this module exists to enforce: **no plaintext secret
ever appears in source, in a committed config file, or in a settings object**.
Configuration carries a *reference* (an environment-variable name, or a
provider-specific URI); the value is fetched here, at the point of use.

Design rules:

* A resolver returns the secret or raises. It never returns ``None``, an empty
  string, or a placeholder: a missing secret is a configuration error and must
  fail loudly at startup rather than degrade at runtime.
* The resolved value is wrapped in a :class:`Secret` whose ``repr``/``str`` are
  redacted, so an accidental log of a settings object cannot leak it.
* Resolvers are read-only and side-effect free; caching, if any, is explicit.

What this module deliberately does **not** do:

* It does not read ``.env`` files. Secret material belongs in the platform's
  secret manager; a file on disk is a fallback that becomes permanent.
* It does not accept a literal secret as a configuration value, because a
  literal in config is one commit away from being public.
"""

from __future__ import annotations

import os
from typing import Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import ConfigurationError

__all__ = [
    "ENVIRONMENT_SCHEME",
    "FILE_SCHEME",
    "EnvironmentSecretResolver",
    "Secret",
    "SecretResolver",
    "SecretResolverChain",
    "resolve_secret",
]


#: Reference scheme for environment-variable-backed secrets: ``env:NAME``.
ENVIRONMENT_SCHEME: Final[str] = "env"

#: Reference scheme for file-backed secrets: ``file:/path/to/secret``.
FILE_SCHEME: Final[str] = "file"


class Secret:
    """An in-memory secret value whose rendering is always redacted.

    The value is exposed only through :meth:`reveal`, which makes every read
    site explicit and greppable. This is the same discipline as Rust's
    ``SecretString::expose()``.

    Attributes:
        reference: The reference the value was resolved from (not a secret).
    """

    __slots__ = ("_value", "reference")

    def __init__(self, value: str, *, reference: str) -> None:
        if not value:
            raise ConfigurationError(
                "secret resolved to an empty value",
                details={"reference": reference},
            )
        self._value = value
        self.reference = reference

    def __repr__(self) -> str:
        """Redacted; safe to include in logs and settings dumps."""
        return f"Secret(reference={self.reference!r}, value=<redacted>)"

    __str__ = __repr__

    def reveal(self) -> str:
        """Returns the raw secret. Call sites must be deliberate."""
        return self._value

    def __len__(self) -> int:
        """Length only; never the content."""
        return len(self._value)

    def __eq__(self, other: object) -> bool:
        """Equality compares values without exposing them in a repr."""
        if not isinstance(other, Secret):
            return NotImplemented
        return self._value == other._value

    def __hash__(self) -> int:
        """Hashable so secrets can live in sets/dicts without being printed."""
        return hash(("Secret", self.reference))


@runtime_checkable
class SecretResolver(Protocol):
    """Resolves a secret *reference* into a :class:`Secret`."""

    def supports(self, reference: str) -> bool:
        """Returns whether this resolver handles *reference*."""
        ...

    def resolve(self, reference: str) -> Secret:
        """Returns the secret for *reference*.

        Raises:
            ConfigurationError: If the reference is malformed, the secret is
                absent, or the value is empty. Never returns a default.
        """
        ...


class EnvironmentSecretResolver:
    """Resolves ``env:NAME`` references from process environment variables.

    This is the default resolver and is always available. It is appropriate
    where the platform injects secrets as environment variables (Kubernetes
    secrets, systemd credentials, CI); it is not a licence to commit a ``.env``.
    """

    def supports(self, reference: str) -> bool:
        """Returns ``True`` for ``env:`` references."""
        return reference.startswith(f"{ENVIRONMENT_SCHEME}:")

    def resolve(self, reference: str) -> Secret:
        """Reads the named environment variable.

        Raises:
            ConfigurationError: If the reference is not an ``env:`` reference,
                the variable is unset, or the value is empty/whitespace.
        """
        if not self.supports(reference):
            raise ConfigurationError(
                "environment resolver cannot handle this reference",
                details={"reference": reference, "scheme": ENVIRONMENT_SCHEME},
            )

        name = reference.split(":", 1)[1].strip()
        if not name:
            raise ConfigurationError(
                "environment secret reference is missing the variable name",
                details={"reference": reference},
            )

        # The error names the variable but never its value.
        raw = os.environ.get(name)
        if raw is None:
            raise ConfigurationError(
                "required secret environment variable is not set",
                details={"variable": name, "reference": reference},
            )
        if not raw.strip():
            raise ConfigurationError(
                "required secret environment variable is empty",
                details={"variable": name, "reference": reference},
            )

        return Secret(raw, reference=reference)


class FileSecretResolver:
    """Resolves ``file:/path`` references by reading a file.

    Intended for mounted secret volumes (Kubernetes projected volumes, Docker
    secrets). The file is read on every resolution so a rotated secret is
    picked up without a restart.

    Directory traversal is not special-cased: the path comes from trusted
    configuration, and the process should not be able to read anything the
    operator did not configure.
    """

    def supports(self, reference: str) -> bool:
        """Returns ``True`` for ``file:`` references."""
        return reference.startswith(f"{FILE_SCHEME}:")

    def resolve(self, reference: str) -> Secret:
        """Reads and trims the file contents.

        Raises:
            ConfigurationError: If the reference is not a ``file:`` reference,
                the path is missing, unreadable, or empty.
        """
        if not self.supports(reference):
            raise ConfigurationError(
                "file resolver cannot handle this reference",
                details={"reference": reference, "scheme": FILE_SCHEME},
            )

        path = reference.split(":", 1)[1].strip()
        if not path:
            raise ConfigurationError(
                "file secret reference is missing the path",
                details={"reference": reference},
            )

        try:
            with open(path, encoding="utf-8") as handle:
                raw = handle.read().strip()
        except OSError as error:
            # Report the errno, never the file contents.
            raise ConfigurationError(
                "secret file could not be read",
                details={"reference": reference, "errno": error.errno},
            ) from error

        if not raw:
            raise ConfigurationError(
                "secret file is empty",
                details={"reference": reference},
            )

        return Secret(raw, reference=reference)


class SecretResolverChain:
    """Tries resolvers in order and returns the first that supports the reference.

    Args:
        resolvers: Ordered resolvers. Evaluation stops at the first one whose
            ``supports`` returns ``True``.
    """

    def __init__(self, *resolvers: SecretResolver) -> None:
        self._resolvers: tuple[SecretResolver, ...] = resolvers

    def resolve(self, reference: str) -> Secret:
        """Resolves *reference* using the first supporting resolver.

        Raises:
            ConfigurationError: If no resolver supports the reference. The
                empty-chain case is reported distinctly from the
                no-matching-resolver case, because one is a wiring bug and the
                other is a typo in configuration.
        """
        if not self._resolvers:
            raise ConfigurationError(
                "no secret resolvers are configured",
                details={"reference": reference},
            )

        for resolver in self._resolvers:
            if resolver.supports(reference):
                return resolver.resolve(reference)

        raise ConfigurationError(
            "no resolver supports this secret reference",
            details={
                "reference": reference,
                "schemes": [getattr(r, "supports", None) is not None for r in self._resolvers],
            },
        )


#: The default chain: environment first, then file mounts.
DEFAULT_RESOLVER_CHAIN: Final[SecretResolverChain] = SecretResolverChain(
    EnvironmentSecretResolver(), FileSecretResolver()
)


def resolve_secret(reference: str | None) -> Secret:
    """Resolves *reference* using the default resolver chain.

    Args:
        reference: A secret reference such as ``env:MAS_OPENAI_KEY``, or
            ``None`` when no secret is configured.

    Returns:
        The resolved :class:`Secret`.

    Raises:
        ConfigurationError: If the reference is ``None`` or empty — a caller
            that needs a secret must say which one, and there is no safe
            default value to substitute.
    """
    if not reference or not reference.strip():
        raise ConfigurationError(
            "a secret reference is required but none was configured",
            details={"reason": "missing_reference"},
        )
    return DEFAULT_RESOLVER_CHAIN.resolve(reference.strip())
