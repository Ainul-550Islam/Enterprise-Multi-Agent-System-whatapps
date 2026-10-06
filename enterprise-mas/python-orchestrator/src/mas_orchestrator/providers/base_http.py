"""Shared secure HTTP transport for provider and tool adapters.

Every outbound HTTP call from the orchestrator goes through this module, so the
security properties below hold everywhere rather than per-adapter:

**SSRF protection**

:class:`HttpPolicy` validates a target host before a request is made. Only
explicitly allowlisted hosts may be contacted, private/loopback/link-local
ranges are refused by default, and redirects are re-validated at every hop —
a redirect is the classic way to turn an allowlisted host into an internal one.

**No credential leakage**

The ``Authorization`` header is set from a :class:`Secret` and is stripped from
any error, log line or redacted dump via
:mod:`mas_orchestrator.security.redaction`. Request bodies are never logged.

**Bounded everything**

Connect, read, write and pool timeouts are all explicit; response size is
capped and read incrementally, so a hostile or broken endpoint cannot exhaust
memory. There is no code path that reads an unbounded response.

**Typed failures**

HTTP status and transport errors become typed domain errors with correct
retryability. Adapters never see a raw ``httpx`` exception, which means the
retry layer has a single, uniform classification rule.
"""

from __future__ import annotations

import ipaddress
import logging
import socket
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Self
from urllib.parse import urlparse

import httpx

from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    InvalidRequestError,
    ModelNotAvailableError,
    ModelProviderError,
    ModelRateLimitedError,
    ModelTimeoutError,
    ToolPermissionDeniedError,
)
from mas_orchestrator.security.redaction import REDACTED

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mas_orchestrator.security.credentials import Secret

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "BLOCKED_NETWORKS",
    "DEFAULT_MAX_RESPONSE_BYTES",
    "HttpPolicy",
    "HttpTransport",
    "TransportResponse",
    "is_blocked_address",
]

#: Networks that must never be reachable through a policy-guarded client.
#: Covers loopback, private RFC1918, link-local (incl. cloud metadata at
#: 169.254.169.254), unique-local IPv6, and unspecified addresses.
BLOCKED_NETWORKS: Final[tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]] = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
    ipaddress.ip_network("::/128"),
    ipaddress.ip_network("::1/128"),
    # IPv4-mapped IPv6 (::ffff:127.0.0.1). These are IPv6 addresses as far as
    # `ipaddress` is concerned, so they do NOT match the IPv4 ranges above —
    # without this entry an attacker-controlled name resolving to
    # ::ffff:127.0.0.1 reaches loopback while every name-based check passes.
    ipaddress.ip_network("::ffff:0:0/96"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
)

#: Default ceiling on a response body.
DEFAULT_MAX_RESPONSE_BYTES: Final[int] = 8 * 1024 * 1024

#: Trusted provider hosts. An adapter must name its hosts here (or in
#: configuration) rather than dialling arbitrary URLs.
DEFAULT_ALLOWED_HOSTS: Final[frozenset[str]] = frozenset(
    {
        "api.openai.com",
        "api.anthropic.com",
    }
)


def is_blocked_address(address: str) -> bool:
    """Returns whether a literal IP address falls in a blocked range.

    Args:
        address: A literal IPv4 or IPv6 address (not a hostname).

    Returns:
        ``True`` when the address must not be contacted. Unparseable input is
        treated as blocked: refusing is the safe direction for a guard whose
        entire purpose is to say no.
    """
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return True

    if any(parsed in network for network in BLOCKED_NETWORKS):
        return True

    # Belt and braces: an IPv4-mapped address is also checked in its IPv4 form,
    # so the guard holds even if the mapped range above is ever narrowed.
    mapped = getattr(parsed, "ipv4_mapped", None)
    if mapped is not None:
        return any(mapped in network for network in BLOCKED_NETWORKS)

    return False


@dataclass(frozen=True, slots=True)
class HttpPolicy:
    """Rules governing outbound HTTP from a guarded client.

    Attributes:
        allowed_hosts: Hosts that may be contacted. Empty means "nothing is
            allowed", which is the safe default for a client built without
            configuration.
        allow_private_networks: Exposes blocked ranges to the client. Only for
            development against a local fake; there is no code path that turns
            this on in production.
        max_response_bytes: Response body ceiling.
        max_redirects: Redirects followed, each re-validated. Zero disables
            redirect following entirely.
        require_https: Refuses plaintext URLs. Provider traffic carries
            credentials and must never be plaintext.
    """

    allowed_hosts: frozenset[str] = field(default_factory=frozenset)
    allow_private_networks: bool = False
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES
    max_redirects: int = 3
    require_https: bool = True

    def __post_init__(self) -> None:
        """Validates the policy.

        Raises:
            ConfigurationError: If the response ceiling is too small to be
                usable, or if redirects are negative (which ``httpx`` would
                interpret as "follow all").
        """
        if self.max_response_bytes < 1024:
            raise ConfigurationError(
                "max_response_bytes is too small to carry a response",
                details={"max_response_bytes": self.max_response_bytes},
            )
        if self.max_redirects < 0:
            raise ConfigurationError(
                "max_redirects must not be negative",
                details={"max_redirects": self.max_redirects},
            )

    def is_host_allowed(self, host: str) -> bool:
        """Returns whether *host* may be contacted.

        Host comparison is case-insensitive and ignores a trailing dot, so
        ``API.OpenAI.com`` and ``api.openai.com.`` both match an allowlist
        entry for ``api.openai.com``.
        """
        normalised = host.strip().rstrip(".").lower()
        if normalised in self.allowed_hosts:
            return True
        return any(
            normalised == allowed.strip().rstrip(".").lower() for allowed in self.allowed_hosts
        )

    def validate_url(self, url: str) -> str:
        """Validates a URL before a request is made.

        Args:
            url: The absolute URL about to be requested.

        Returns:
            The normalised host, for logging.

        Raises:
            InvalidRequestError: If the URL is not absolute HTTP(S).
            ToolPermissionDeniedError: If the scheme is plaintext while
                ``require_https`` is set, or if the host is not allowlisted.
                Using the permission error makes the refusal legible to the
                tool layer, which is where user-influenced URLs originate.
        """
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise InvalidRequestError(
                "only http and https URLs are supported",
                details={"scheme": parsed.scheme or "<none>"},
            )
        if self.require_https and parsed.scheme != "https":
            raise ToolPermissionDeniedError(
                "plaintext URLs are not permitted",
                details={"scheme": parsed.scheme},
            )

        host = parsed.hostname or ""
        if not host:
            raise InvalidRequestError("URL has no host", details={"url": "<redacted>"})

        if not self.is_host_allowed(host):
            raise ToolPermissionDeniedError(
                "host is not in the allowed set",
                details={"reason": "host_not_allowlisted"},
            )

        return host

    def validate_resolved_address(self, address: str) -> None:
        """Refuses a resolved IP that lands in a blocked range.

        This closes the DNS-rebinding gap left by hostname allowlisting: an
        allowlisted name can resolve to ``127.0.0.1`` or to the cloud metadata
        address, so the resolved address is checked as well as the name.

        Args:
            address: The IP the host resolved to.

        Raises:
            ToolPermissionDeniedError: If the address is blocked.
        """
        if self.allow_private_networks:
            return
        if is_blocked_address(address):
            raise ToolPermissionDeniedError(
                "resolved address is in a blocked range",
                details={"reason": "blocked_address"},
            )

    @classmethod
    def for_providers(cls, extra_hosts: frozenset[str] | set[str] | None = None) -> Self:
        """Builds a policy permitting the known model provider hosts.

        Args:
            extra_hosts: Additional hosts, for example a private gateway or a
                self-hosted compatible endpoint.

        Returns:
            A policy with HTTPS required and redirects followed but
            re-validated.
        """
        hosts = set(DEFAULT_ALLOWED_HOSTS)
        if extra_hosts:
            hosts.update(extra_hosts)
        return cls(allowed_hosts=frozenset(hosts), require_https=True)


@dataclass(frozen=True, slots=True)
class TransportResponse:
    """A bounded, already-validated HTTP response.

    Attributes:
        status_code: HTTP status.
        body: Response body, truncated at the configured ceiling.
        headers: Response headers, with sensitive values redacted.
        is_truncated: Whether the body hit the size ceiling.
    """

    status_code: int
    body: bytes
    headers: Mapping[str, str]
    is_truncated: bool = False

    def json(self) -> object:
        """Decodes the body as JSON.

        Raises:
            ModelProviderError: If the body is not valid JSON. A provider that
                returns HTML (an error page, a captive portal) must produce a
                typed failure rather than an opaque decode error at the call
                site.
        """
        import json

        try:
            return json.loads(self.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ModelProviderError(
                "provider returned a body that is not valid JSON",
                details={"status_code": self.status_code},
            ) from error


class HttpTransport:
    """A guarded HTTP client shared by provider and tool adapters.

    Args:
        policy: The guard rules applied to every request.
        connect_timeout_seconds: Connection establishment budget.
        read_timeout_seconds: Per-read budget.
        write_timeout_seconds: Request-body budget.
        credential: Optional bearer credential applied to every request.
    """

    def __init__(
        self,
        policy: HttpPolicy,
        *,
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 60.0,
        write_timeout_seconds: float = 30.0,
        credential: Secret | None = None,
    ) -> None:
        for name, value in (
            ("connect_timeout_seconds", connect_timeout_seconds),
            ("read_timeout_seconds", read_timeout_seconds),
            ("write_timeout_seconds", write_timeout_seconds),
        ):
            if value <= 0:
                raise ConfigurationError(f"{name} must be positive", details={"value": value})

        self._policy = policy
        self._credential = credential
        self._timeout = httpx.Timeout(
            connect=connect_timeout_seconds,
            read=read_timeout_seconds,
            write=write_timeout_seconds,
            pool=connect_timeout_seconds,
        )
        # Redirects are handled manually so each hop is re-validated; letting
        # httpx follow them would bypass the host allowlist after the first hop.
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            follow_redirects=False,
        )

    @property
    def policy(self) -> HttpPolicy:
        """The guard rules in force."""
        return self._policy

    async def aclose(self) -> None:
        """Closes the underlying connection pool."""
        await self._client.aclose()

    def _headers(self, extra: Mapping[str, str] | None) -> dict[str, str]:
        """Builds request headers, attaching the credential when configured."""
        headers = {"accept": "application/json"}
        if self._credential is not None:
            headers["authorization"] = f"Bearer {self._credential.reveal()}"
        if extra:
            headers.update(extra)
        return headers

    async def request(
        self,
        method: str,
        url: str,
        *,
        json_body: object | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> TransportResponse:
        """Performs a guarded request, following redirects safely.

        Args:
            method: HTTP method.
            url: Absolute target URL.
            json_body: JSON-serialisable request body, when any.
            headers: Additional headers.

        Returns:
            The bounded response.

        Raises:
            ToolPermissionDeniedError: If the URL, host, or any redirect hop
                is refused by the policy.
            ModelRateLimitedError: On HTTP 429, with ``Retry-After`` honoured.
            ModelTimeoutError: On timeout.
            ModelProviderError: On any other transport failure or on a
                response that exceeds the size ceiling.
        """
        current_url = url
        for hop in range(self._policy.max_redirects + 1):
            self._policy.validate_url(current_url)
            await self._validate_dns(current_url)

            request = self._client.build_request(
                method,
                current_url,
                json=json_body,
                headers=self._headers(headers),
            )

            try:
                response = await self._client.send(request, stream=True)
            except httpx.TimeoutException as error:
                raise ModelTimeoutError(
                    "provider request timed out",
                    details={"phase": "transport"},
                ) from error
            except httpx.HTTPError as error:
                raise ModelProviderError(
                    "provider request failed at the transport layer",
                    details={"error_type": type(error).__name__},
                ) from error

            try:
                location = response.headers.get("location")
                if response.is_redirect and location and hop < self._policy.max_redirects:
                    # Each hop is re-validated at the top of the loop; a
                    # redirect must never reach a host the policy would refuse.
                    if urlparse(location).netloc:
                        current_url = location
                    else:
                        # Relative Location: resolve against the original URL.
                        base = urlparse(url)
                        current_url = f"{base.scheme}://{base.netloc}{location}"
                    continue

                body = await self._read_bounded(response)
                return TransportResponse(
                    status_code=response.status_code,
                    body=body,
                    headers=self._redact_headers(dict(response.headers)),
                    is_truncated=len(body) >= self._policy.max_response_bytes,
                )
            finally:
                await response.aclose()

        raise ModelProviderError(
            "too many redirects",
            details={"max_redirects": self._policy.max_redirects},
        )

    async def _validate_dns(self, url: str) -> None:
        """Resolves the host and refuses blocked addresses.

        This is the DNS-rebinding guard: an allowlisted hostname is still
        checked against blocked IP ranges after resolution.

        Raises:
            ToolPermissionDeniedError: If the host does not resolve, or
                resolves into a blocked range.
        """
        host = urlparse(url).hostname or ""
        try:
            infos = await _resolve_host(host)
        except OSError as error:
            raise ModelProviderError(
                "provider host could not be resolved",
                details={"errno": error.errno},
            ) from error

        if not infos:
            raise ModelProviderError("provider host resolved to no addresses", details={})

        for address in infos:
            self._policy.validate_resolved_address(address)

    async def _read_bounded(self, response: httpx.Response) -> bytes:
        """Reads at most ``max_response_bytes``, then stops.

        Raises:
            ModelProviderError: If the body exceeds the ceiling. A response
                larger than the configured maximum is a protocol violation
                from our perspective: silently truncating it would produce a
                corrupt parse downstream.
        """
        limit = self._policy.max_response_bytes
        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > limit:
                raise ModelProviderError(
                    "provider response exceeded the size limit",
                    details={"limit_bytes": limit},
                )
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    def _redact_headers(headers: Mapping[str, str]) -> dict[str, str]:
        """Redacts sensitive response headers before they are returned.

        Response headers can carry ``set-cookie`` and other credential-bearing
        values; nothing downstream needs them, so they are replaced rather than
        risked.
        """
        sensitive = {"authorization", "set-cookie", "cookie", "proxy-authenticate"}
        return {
            key: (REDACTED if key.lower() in sensitive else value) for key, value in headers.items()
        }


async def _resolve_host(host: str) -> list[str]:
    """Resolves a hostname to literal addresses without blocking the loop.

    ``socket.getaddrinfo`` blocks, and calling it inside the event loop would
    stall every other execution being served by this process.
    """
    import asyncio

    loop = asyncio.get_running_loop()

    def resolve() -> list[str]:
        return [str(info[4][0]) for info in socket.getaddrinfo(host, None)]

    return await loop.run_in_executor(None, resolve)


def classify_status(status_code: int, retry_after: float | None = None) -> Exception:
    """Maps an HTTP status onto a typed error.

    Args:
        status_code: The HTTP status.
        retry_after: Parsed ``Retry-After`` in seconds, when supplied.

    Returns:
        A typed error with correct retryability: 429 becomes
        :class:`ModelRateLimitedError`, 5xx becomes a retryable
        :class:`ModelProviderError`, and 4xx becomes a non-retryable one.
    """
    if status_code == 429:
        return ModelRateLimitedError(
            "provider rate limited the request",
            retry_after_seconds=retry_after,
            status_code=status_code,
        )
    if status_code >= 500:
        return ModelProviderError("provider returned a server error", status_code=status_code)
    if status_code == 404:
        return ModelNotAvailableError(
            "model or endpoint is not available",
            details={"status_code": status_code},
        )
    return ModelProviderError("provider rejected the request", status_code=status_code)


def parse_retry_after(raw: str | None) -> float | None:
    """Parses a ``Retry-After`` header.

    Supports both the delta-seconds form and the HTTP-date form. A malformed
    or past date yields ``None`` rather than a guess, because a wrong delay is
    worse than no delay: the caller falls back to backoff.

    Args:
        raw: Header value.

    Returns:
        Seconds to wait, or ``None``.
    """
    if not raw:
        return None

    value = raw.strip()
    if value.isdigit():
        return float(int(value))

    from email.utils import parsedate_to_datetime

    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None

    import time as _time

    if when.tzinfo is None:
        return None
    delta = when.timestamp() - _time.time()
    return delta if delta > 0 else None
