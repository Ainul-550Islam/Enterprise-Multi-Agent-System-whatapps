"""Secret resolution.

Spec §18 sets four rules that this module exists to make structural rather
than advisory:

* secrets come from the environment or an approved secret-manager abstraction
* credentials are never committed
* secrets are never echoed in health endpoints or diagnostics
* production fails closed when a mandatory provider has no credentials

The central mechanism is :class:`Secret`, a wrapper whose ``__repr__``,
``__str__`` and error paths all refuse to reveal the value. A secret that can
be printed by an f-string is a secret that will eventually be printed, so the
refusal is built into the type rather than left to the caller.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Final, Protocol, runtime_checkable

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: How much of a secret may appear in a diagnostic. Enough to tell two keys
#: apart, not enough to reconstruct one.
VISIBLE_SUFFIX: Final[int] = 4

#: Environment variable names that must never be echoed, even partially.
SENSITIVE_ENV_MARKERS: Final[frozenset[str]] = frozenset(
    {
        "KEY",
        "SECRET",
        "TOKEN",
        "PASSWORD",
        "CREDENTIAL",
        "PASSPHRASE",
        "PRIVATE",
    }
)


class Secret:
    """A credential that will not print itself.

    The value is reachable only through :meth:`reveal`, which makes every use
    a visible, greppable decision.
    """

    __slots__ = ("_name", "_value")

    def __init__(self, value: str, *, name: str = "secret") -> None:
        if not isinstance(value, str):
            raise ConfigurationError("a secret must be a string", details={"field": name})
        if not value.strip():
            raise ConfigurationError("a secret must not be empty", details={"field": name})
        self._value = value
        self._name = name

    def reveal(self) -> str:
        """Returns the value.

        Call this only at the point of use, and do not store the result in
        anything that renders it.
        """
        return self._value

    @property
    def name(self) -> str:
        """The name this secret was resolved under, never the value."""
        return self._name

    def fingerprint(self) -> str:
        """A diagnostic-safe label.

        Shows the name and the last few characters, which is enough to confirm
        *which* credential is configured without disclosing it.
        """
        tail = self._value[-VISIBLE_SUFFIX:] if len(self._value) > VISIBLE_SUFFIX else ""
        return f"{self._name}(...{tail})" if tail else f"{self._name}(...)"

    def __repr__(self) -> str:
        """Never the value."""
        return f"Secret({self.fingerprint()})"

    __str__ = __repr__

    def __bool__(self) -> bool:
        """Whether a non-empty value is held."""
        return bool(self._value)

    def __len__(self) -> int:
        """Length of the value, which is safe to expose."""
        return len(self._value)

    def __eq__(self, other: object) -> bool:
        """Constant-time comparison.

        Comparing credentials with ``==`` leaks length and prefix through
        timing. Nothing here is security-critical enough to warrant a full
        HMAC, but a plain short-circuit compare is the wrong habit to model.
        """
        if not isinstance(other, Secret):
            return NotImplemented
        return _constant_time_eq(self._value, other._value)

    def __hash__(self) -> int:
        return hash(self.fingerprint())


def _constant_time_eq(left: str, right: str) -> bool:
    """Compares two strings without short-circuiting on the first difference."""
    if len(left) != len(right):
        return False
    difference = 0
    for a, b in zip(left, right, strict=False):
        difference |= ord(a) ^ ord(b)
    return difference == 0


@runtime_checkable
class SecretResolver(Protocol):
    """Where secrets come from."""

    def resolve(self, name: str) -> Secret | None:
        """Returns the named secret, or ``None`` if it is not configured."""
        ...


class EnvironmentResolver:
    """Reads secrets from process environment variables.

    The default resolver, and the only one that needs no dependencies. It is
    deliberately read-on-demand rather than cached at import, so a value can be
    rotated without a restart of the resolver.
    """

    def __init__(self, *, prefix: str = "") -> None:
        self._prefix = prefix

    def resolve(self, name: str) -> Secret | None:
        """Reads ``<prefix><NAME>`` from the environment."""
        key = f"{self._prefix}{name}"
        raw = os.environ.get(key)
        if raw is None or not raw.strip():
            return None
        return Secret(raw, name=name)


class ChainedResolver:
    """Tries resolvers in order, first hit wins.

    The usual arrangement is an environment resolver for local development in
    front of a secret manager for deployment.
    """

    def __init__(self, *resolvers: SecretResolver) -> None:
        if not resolvers:
            raise ConfigurationError(
                "a chained resolver needs at least one source",
                details={"field": "resolvers"},
            )
        self._resolvers = resolvers

    def resolve(self, name: str) -> Secret | None:
        """Returns the first non-empty result."""
        for resolver in self._resolvers:
            found = resolver.resolve(name)
            if found is not None:
                return found
        return None


class MappingResolver:
    """Reads secrets from an in-memory mapping.

    For tests. Refuses to be constructed in production, so a literal credential
    cannot survive into a deployed configuration by way of a test helper.
    """

    def __init__(self, values: dict[str, str], *, environment: str) -> None:
        if environment == Environment.PRODUCTION.value:
            raise ConfigurationError(
                "an in-memory secret source is not permitted in production: "
                "secrets must come from the environment or an approved manager",
                details={"environment": environment},
            )
        self._values = values

    def resolve(self, name: str) -> Secret | None:
        """Looks the name up in the mapping."""
        raw = self._values.get(name)
        if raw is None or not raw.strip():
            return None
        return Secret(raw, name=name)


# --- Provider credentials ---------------------------------------------------


@runtime_checkable
class CredentialRequirement(Protocol):
    """What a named provider needs in order to be usable."""

    @property
    def provider(self) -> str: ...

    @property
    def required_names(self) -> tuple[str, ...]: ...


@runtime_checkable
class ProviderCredentialSpec(Protocol):
    """A provider's credential contract."""

    name: str
    enabled: bool
    secret_names: tuple[str, ...]
    mandatory: bool


def resolve_provider_credentials(
    spec: ProviderCredentialSpec,
    resolver: SecretResolver,
    *,
    environment: str,
) -> dict[str, Secret]:
    """Resolves a provider's credentials, failing closed when mandatory.

    Args:
        spec: The provider's credential contract.
        resolver: Where to read from.
        environment: Deployment environment, which decides whether a missing
            mandatory credential is fatal.

    Returns:
        The resolved secrets, keyed by secret name. A disabled provider
        returns an empty mapping without consulting the resolver, so a
        disabled-but-unconfigured provider never blocks startup.

    Raises:
        ConfigurationError: If the provider is enabled and mandatory and any
            required credential is missing, in production.
    """
    if not spec.enabled:
        return {}

    resolved: dict[str, Secret] = {}
    missing: list[str] = []

    for name in spec.secret_names:
        secret = resolver.resolve(name)
        if secret is None:
            missing.append(name)
        else:
            resolved[name] = secret

    if missing and spec.mandatory and environment == Environment.PRODUCTION.value:
        raise ConfigurationError(
            "provider is enabled and mandatory but has no credentials",
            details={
                "provider": spec.name,
                "missing": sorted(missing),
                "environment": environment,
            },
        )

    if missing:
        # Outside production a missing credential is a warning: blocking a
        # developer on a key they cannot use yet would be obstructive without
        # being safe. The provider simply will not work.
        _LOGGER.warning(
            "provider is missing credentials",
            extra={
                "event": "config.provider_missing_credentials",
                "provider": spec.name,
                "missing": sorted(missing),
                "environment": environment,
            },
        )
    return resolved


def assert_no_committed_credentials(text: str, *, where: str = "source") -> None:
    """Refuses text that looks like it contains a live credential.

    Intended for configuration files and diagnostics. It is a backstop, not a
    guarantee: a well-formed key the scanner does not recognise will pass, and
    it will flag a test fixture that merely looks like a key.

    Raises:
        ConfigurationError: If a credential shape is found.
    """
    from mas_orchestrator.guardrails.pii import RedactSecretsOnly, scan

    report = scan(text)
    kinds = {finding.kind for finding in report.findings}
    if kinds & RedactSecretsOnly.SECRET_KINDS:
        raise ConfigurationError(
            f"{where} contains what looks like a committed credential",
            details={"kinds": sorted(kind.value for kind in kinds)},
        )


def safe_environment_summary(
    *,
    include: tuple[str, ...] = (),
) -> dict[str, str]:
    """Summarises environment configuration without echoing secrets.

    Spec §18 forbids printing complete environment variables in diagnostics.
    This emits names and lengths only, and marks anything whose name looks
    sensitive as ``[redacted]`` rather than reporting its length — even a
    length is a small leak when the set of possible values is known.

    Args:
        include: If given, only these variable names are summarised.

    Returns:
        A mapping of variable name to a safe description.
    """
    names = include or tuple(sorted(os.environ))
    summary: dict[str, str] = {}
    for name in names:
        if _looks_sensitive(name):
            summary[name] = "[redacted]"
            continue
        value = os.environ.get(name)
        if value is None:
            summary[name] = "[unset]"
        else:
            summary[name] = f"[set, {len(value)} chars]"
    return summary


def _looks_sensitive(name: str) -> bool:
    """Whether a variable name is likely to hold a credential."""
    upper = name.upper()
    return any(marker in upper for marker in SENSITIVE_ENV_MARKERS)


#: Provider credential names, by provider. The values are environment variable
#: *names*, not credentials.
PROVIDER_SECRET_NAMES: Final[dict[str, tuple[str, ...]]] = {
    "openai": ("OPENAI_API_KEY",),
    "anthropic": ("ANTHROPIC_API_KEY",),
    "compatible": ("COMPATIBLE_API_KEY",),
}


def secret_names_for(provider: str) -> tuple[str, ...]:
    """The environment variable names a provider reads.

    Raises:
        ConfigurationError: If the provider is unknown. An unknown provider
            cannot be credentialed, and silently returning an empty tuple
            would let it start up and fail on first call instead.
    """
    names = PROVIDER_SECRET_NAMES.get(provider)
    if names is None:
        raise ConfigurationError("unknown provider", details={"provider": provider})
    return names


def redact_in_text(text: str) -> str:
    """Replaces any credential-looking substring with a placeholder.

    Use this before writing a diagnostic that may contain configuration.
    """
    from mas_orchestrator.guardrails.pii import redact

    result, _ = redact(text)
    return result


_SECRET_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Z][A-Z0-9_]*$")


def is_valid_secret_name(name: str) -> bool:
    """Whether a name is a well-formed environment variable name."""
    return bool(_SECRET_NAME_RE.match(name))
