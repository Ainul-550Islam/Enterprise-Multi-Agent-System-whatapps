"""Shared secure HTTP transport for provider and tool adapters.

Every outbound HTTP call from the orchestrator goes through this module.  The
transport does more than validate a URL before handing it to an HTTP client:
it pins the TCP connection to the public address it validated.  That distinction
matters for DNS-rebinding resistance: resolving a hostname once for a policy
check and allowing a second resolver invocation while connecting leaves a race
in which the second lookup can point at a private address.

The transport also has explicit response, redirect, timeout, and credential
boundaries.  Request bodies and secret values are never logged.
"""

from __future__ import annotations

import ipaddress
import logging
import socket
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Self, cast
from urllib.parse import urljoin, urlparse

import httpcore
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
    from collections.abc import AsyncIterable, AsyncIterator, Iterable, Mapping

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
#: 169.254.169.254), unique-local IPv6, multicast and unspecified addresses.
BLOCKED_NETWORKS: Final[tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]] = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.0.2.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("198.51.100.0/24"),
    ipaddress.ip_network("203.0.113.0/24"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
    # IPv4-compatible and IPv4-mapped IPv6 spellings are unsupported. They
    # are rejected rather than being routed through a subtly different IPv4
    # policy path.
    ipaddress.ip_network("::/96"),
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

#: Request headers that must never be forwarded to a different origin.  A
#: redirect can be controlled by an upstream service; stripping rather than
#: forwarding is not sufficient for a configured bearer credential because the
#: redirect itself is unexpected and is useful evidence of misconfiguration.
_SENSITIVE_REQUEST_HEADERS: Final[frozenset[str]] = frozenset(
    {
        "authorization",
        "cookie",
        "proxy-authorization",
        "x-api-key",
        "x-amz-security-token",
    }
)

#: Framing, routing, and proxy headers owned by the guarded transport. Letting
#: a provider or tool override them would undermine URL/SNI pinning or create
#: request-smuggling ambiguity between the client and an upstream proxy.
_TRANSPORT_OWNED_HEADERS: Final[frozenset[str]] = frozenset(
    {
        "host",
        "content-length",
        "transfer-encoding",
        "connection",
        "keep-alive",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "upgrade",
    }
)


def is_blocked_address(address: str) -> bool:
    """Returns whether a literal IP address falls in a blocked range.

    Unparseable input is blocked.  Refusing is the safe direction for an egress
    guard whose purpose is to prevent an unintended dial.
    """
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return True

    if any(parsed in network for network in BLOCKED_NETWORKS):
        return True

    mapped = getattr(parsed, "ipv4_mapped", None)
    if mapped is not None:
        return any(mapped in network for network in BLOCKED_NETWORKS)
    return False


@dataclass(frozen=True, slots=True)
class HttpPolicy:
    """Rules governing outbound HTTP from a guarded client."""

    allowed_hosts: frozenset[str] = field(default_factory=frozenset)
    # Compatibility field retained so an unsafe configuration fails loudly
    # rather than becoming an ignored bypass. Tenant-facing tools, connectors,
    # and provider adapters never get a private-network exception.
    allow_private_networks: bool = False
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES
    max_redirects: int = 3
    require_https: bool = True

    def __post_init__(self) -> None:
        """Rejects unsafe or unusable policy values."""
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
        if self.allow_private_networks:
            raise ConfigurationError(
                "private-network egress is not available to guarded HTTP clients",
                details={"reason": "private_network_bypass_forbidden"},
            )
        if any(not host.strip() or not host.isascii() for host in self.allowed_hosts):
            raise ConfigurationError(
                "allowed hosts must be non-empty ASCII hostnames",
                details={"field": "allowed_hosts"},
            )

    def is_host_allowed(self, host: str) -> bool:
        """Returns whether *host* matches the explicitly configured allowlist."""
        normalised = host.strip().rstrip(".").lower()
        return any(
            normalised == allowed.strip().rstrip(".").lower() for allowed in self.allowed_hosts
        )

    def validate_url(self, url: str) -> str:
        """Validates an absolute URL before it is constructed as a request."""
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        if scheme not in ("http", "https"):
            raise InvalidRequestError(
                "only http and https URLs are supported",
                details={"scheme": parsed.scheme or "<none>"},
            )
        if self.require_https and scheme != "https":
            raise ToolPermissionDeniedError(
                "plaintext URLs are not permitted",
                details={"scheme": scheme},
            )
        if parsed.username is not None or parsed.password is not None:
            raise ToolPermissionDeniedError("URLs with user credentials are not permitted")

        host = parsed.hostname or ""
        if not host:
            raise InvalidRequestError("URL has no host", details={"url": "<redacted>"})
        if not host.isascii():
            raise ToolPermissionDeniedError(
                "non-ASCII URL hosts are not permitted",
                details={"reason": "non_ascii_host"},
            )
        normalised_host = host.rstrip(".").lower()
        if (
            normalised_host == "localhost"
            or normalised_host.endswith(".localhost")
            or normalised_host.endswith(".internal")
            or normalised_host
            in {"metadata", "metadata.google.internal", "metadata.azure.internal", "instance-data"}
        ):
            raise ToolPermissionDeniedError(
                "local and metadata URL hosts are not permitted",
                details={"reason": "forbidden_host"},
            )
        try:
            port = parsed.port
        except ValueError as error:
            raise InvalidRequestError("URL has an invalid port") from error
        default_port = 443 if scheme == "https" else 80
        if port not in (None, default_port):
            raise ToolPermissionDeniedError("URLs using non-standard ports are not permitted")
        if not self.is_host_allowed(host):
            raise ToolPermissionDeniedError(
                "host is not in the allowed set",
                details={"reason": "host_not_allowlisted"},
            )
        return host

    def validate_resolved_address(self, address: str) -> None:
        """Refuses a resolved address that belongs to a blocked range."""
        if is_blocked_address(address):
            raise ToolPermissionDeniedError(
                "resolved address is in a blocked range",
                details={"reason": "blocked_address"},
            )

    @classmethod
    def for_providers(cls, extra_hosts: frozenset[str] | set[str] | None = None) -> Self:
        """Builds the restrictive policy used by first-party providers."""
        hosts = set(DEFAULT_ALLOWED_HOSTS)
        if extra_hosts:
            hosts.update(extra_hosts)
        return cls(allowed_hosts=frozenset(hosts), require_https=True)


@dataclass(frozen=True, slots=True)
class TransportResponse:
    """A bounded, already-validated HTTP response."""

    status_code: int
    body: bytes
    headers: Mapping[str, str]
    is_truncated: bool = False

    def json(self) -> object:
        """Decodes the body as JSON with a typed provider failure."""
        import json

        try:
            return json.loads(self.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ModelProviderError(
                "provider returned a body that is not valid JSON",
                details={"status_code": self.status_code},
            ) from error


class _ValidatedNetworkBackend(httpcore.AsyncNetworkBackend):
    """Pins each TCP dial to addresses checked by :class:`HttpPolicy`.

    HTTP Core keeps the requested hostname as the TLS SNI and certificate name,
    while this backend changes only the TCP destination to a validated literal
    address.  Consequently a second DNS lookup cannot redirect a connection to
    an internal service after the policy approved the hostname.
    """

    def __init__(self, policy: HttpPolicy) -> None:
        self._policy = policy
        self._delegate = httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        try:
            addresses = await _resolve_host(host)
        except OSError as error:
            # The public transport maps HTTP Core connection failures onto a
            # typed provider failure. Letting a raw `gaierror` escape here
            # would bypass that boundary and turn a routine DNS outage into an
            # unclassified RPC failure.
            raise httpcore.ConnectError("host could not be resolved") from error
        if not addresses:
            raise httpcore.ConnectError("host resolved to no addresses")

        # Reject mixed public/private answers rather than hoping address order
        # remains stable.  DNS answers are attacker-controlled at this boundary.
        for address in addresses:
            try:
                self._policy.validate_resolved_address(address)
            except ToolPermissionDeniedError as error:
                raise httpcore.ConnectError("resolved address is blocked") from error

        # Stable de-duplication avoids retries oscillating due to resolver order
        # while preserving the resolver's preferred address family/order.
        address = next(iter(dict.fromkeys(addresses)))
        return await self._delegate.connect_tcp(
            address,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        # Guarded egress never uses UDS.  Keeping this hard failure avoids a
        # future caller accidentally bypassing hostname policy through a custom
        # transport extension.
        del path, timeout, socket_options
        raise httpcore.ConnectError("unix sockets are not permitted for guarded HTTP")

    async def sleep(self, seconds: float) -> None:
        await self._delegate.sleep(seconds)


class _CoreResponseStream(httpx.AsyncByteStream):
    """Maps HTTP Core streaming errors onto public HTTPX exception classes."""

    def __init__(self, stream: AsyncIterable[bytes]) -> None:
        self._stream = stream

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self._stream:
                yield chunk
        except (httpcore.ConnectTimeout, httpcore.ReadTimeout, httpcore.WriteTimeout) as error:
            raise httpx.TimeoutException(str(error)) from error
        except httpcore.NetworkError as error:
            raise httpx.NetworkError(str(error)) from error
        except httpcore.ProtocolError as error:
            raise httpx.ProtocolError(str(error)) from error

    async def aclose(self) -> None:
        close = getattr(self._stream, "aclose", None)
        if close is not None:
            await close()


class _PinnedAsyncTransport(httpx.AsyncBaseTransport):
    """Small HTTPX transport exposing HTTP Core's validated network backend."""

    def __init__(self, policy: HttpPolicy) -> None:
        self._pool = httpcore.AsyncConnectionPool(
            network_backend=_ValidatedNetworkBackend(policy),
            retries=0,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        core_request = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        try:
            core_response = await self._pool.handle_async_request(core_request)
        except (httpcore.ConnectTimeout, httpcore.ReadTimeout, httpcore.WriteTimeout) as error:
            raise httpx.TimeoutException(str(error), request=request) from error
        except httpcore.NetworkError as error:
            raise httpx.NetworkError(str(error), request=request) from error
        except httpcore.ProtocolError as error:
            raise httpx.ProtocolError(str(error), request=request) from error

        return httpx.Response(
            status_code=core_response.status,
            headers=core_response.headers,
            stream=_CoreResponseStream(cast("AsyncIterable[bytes]", core_response.stream)),
            extensions=core_response.extensions,
            request=request,
        )

    async def aclose(self) -> None:
        await self._pool.aclose()


class HttpTransport:
    """A guarded HTTP client shared by provider and tool adapters."""

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
        # `trust_env=False` refuses proxy variables such as HTTPS_PROXY.  A
        # proxy is an egress path outside our DNS pinning and must be configured
        # explicitly in a future, separately policy-checked transport.
        self._client = httpx.AsyncClient(
            transport=_PinnedAsyncTransport(policy),
            timeout=self._timeout,
            follow_redirects=False,
            trust_env=False,
        )

    @property
    def policy(self) -> HttpPolicy:
        """The guard rules in force."""
        return self._policy

    async def aclose(self) -> None:
        """Closes the underlying connection pool."""
        await self._client.aclose()

    def _headers(self, extra: Mapping[str, str] | None) -> dict[str, str]:
        """Builds request headers, attaching the configured bearer credential.

        Callers can add provider-specific headers but cannot override transport
        framing/routing. A configured bearer credential also cannot be replaced
        by a caller-supplied ``Authorization`` value.
        """
        headers = {"accept": "application/json"}
        if self._credential is not None:
            headers["authorization"] = f"Bearer {self._credential.reveal()}"
        if extra:
            for raw_name, raw_value in extra.items():
                name = str(raw_name)
                value = str(raw_value)
                normalised = name.lower()
                if (
                    not name
                    or not name.isascii()
                    or not all(character.isalnum() or character == "-" for character in name)
                ):
                    raise InvalidRequestError(
                        "outbound request contains an invalid header name",
                        details={"reason": "invalid_header_name"},
                    )
                if normalised in _TRANSPORT_OWNED_HEADERS:
                    raise ToolPermissionDeniedError(
                        "outbound request attempted to set a transport-owned header",
                        details={"reason": "transport_owned_header"},
                    )
                if "\r" in value or "\n" in value or len(value) > 16 * 1024:
                    raise InvalidRequestError(
                        "outbound request contains an invalid header value",
                        details={"reason": "invalid_header_value"},
                    )
                if self._credential is not None and normalised == "authorization":
                    raise ToolPermissionDeniedError(
                        "caller cannot override the configured authorization credential",
                        details={"reason": "credential_override"},
                    )
                headers[name] = value
        return headers

    @staticmethod
    def _origin(url: str) -> tuple[str, str, int]:
        """Returns a normalised URL origin after URL validation has succeeded."""
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        host = (parsed.hostname or "").rstrip(".").lower()
        return scheme, host, parsed.port or (443 if scheme == "https" else 80)

    def _has_sensitive_headers(self, headers: Mapping[str, str] | None) -> bool:
        if self._credential is not None:
            return True
        return bool(headers and any(key.lower() in _SENSITIVE_REQUEST_HEADERS for key in headers))

    async def request(
        self,
        method: str,
        url: str,
        *,
        json_body: object | None = None,
        content: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        max_response_bytes: int | None = None,
    ) -> TransportResponse:
        """Performs a guarded request, following only safe validated redirects.

        ``max_response_bytes`` can lower (never raise) the policy ceiling for a
        single request.  Source synchronizers use it to enforce an aggregate
        byte budget before retaining another document in memory.
        """
        if json_body is not None and content is not None:
            raise ConfigurationError(
                "guarded HTTP request cannot use json_body and raw content together",
                details={"reason": "ambiguous_request_body"},
            )
        if content is not None and len(content) > self._policy.max_response_bytes:
            raise InvalidRequestError(
                "outbound request body exceeds the guarded transport limit",
                details={"limit_bytes": self._policy.max_response_bytes},
            )
        if (
            max_response_bytes is not None
            and not 1 <= max_response_bytes <= self._policy.max_response_bytes
        ):
            raise ConfigurationError(
                "per-request response limit must be within the transport policy limit",
                details={"limit": max_response_bytes},
            )
        body_limit = max_response_bytes or self._policy.max_response_bytes
        current_url = url
        request_method = method.upper()
        request_body = json_body
        initial_origin: tuple[str, str, int] | None = None

        for hop in range(self._policy.max_redirects + 1):
            self._policy.validate_url(current_url)
            origin = self._origin(current_url)
            if initial_origin is None:
                initial_origin = origin
            elif origin != initial_origin and self._has_sensitive_headers(headers):
                raise ToolPermissionDeniedError(
                    "cross-origin redirects are not permitted for credentialed requests",
                    details={"reason": "credential_redirect"},
                )

            request = self._client.build_request(
                request_method,
                current_url,
                json=request_body,
                content=content if request_body is None else None,
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
                if response.is_redirect and location:
                    if hop >= self._policy.max_redirects:
                        raise ModelProviderError(
                            "too many redirects",
                            details={"max_redirects": self._policy.max_redirects},
                        )
                    if request_method not in {"GET", "HEAD"}:
                        raise ToolPermissionDeniedError(
                            "redirects for non-idempotent requests are not permitted",
                            details={"reason": "unsafe_redirect_method"},
                        )
                    # urljoin preserves relative-path semantics (unlike manual
                    # scheme/netloc concatenation) and the next loop validates
                    # the complete resolved target again.
                    current_url = urljoin(current_url, location)
                    request_body = None
                    content = None
                    continue

                body = await self._read_bounded(response, body_limit)
                return TransportResponse(
                    status_code=response.status_code,
                    body=body,
                    headers=self._redact_headers(dict(response.headers)),
                    is_truncated=False,
                )
            finally:
                await response.aclose()

        raise ModelProviderError(
            "too many redirects",
            details={"max_redirects": self._policy.max_redirects},
        )

    async def _validate_dns(self, url: str) -> None:
        """Validates all current DNS answers for diagnostics and direct callers.

        Actual egress safety is enforced by :class:`_ValidatedNetworkBackend`,
        which resolves immediately before and uses the TCP connection.  This
        method remains useful to source validators and tests but must not be
        mistaken for the connection-pinning boundary.
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

    async def _read_bounded(self, response: httpx.Response, limit: int) -> bytes:
        """Reads at most ``limit`` bytes and fails closed on overflow."""
        declared_length = response.headers.get("content-length")
        if declared_length is not None:
            try:
                if int(declared_length) > limit:
                    raise ModelProviderError(
                        "provider response exceeded the size limit",
                        details={"limit_bytes": limit},
                    )
            except ValueError:
                # A malformed Content-Length is handled by HTTPX's framing;
                # preserve streaming enforcement below as defence in depth.
                pass

        chunks: list[bytes] = []
        total = 0
        try:
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > limit:
                    raise ModelProviderError(
                        "provider response exceeded the size limit",
                        details={"limit_bytes": limit},
                    )
                chunks.append(chunk)
        except httpx.TimeoutException as error:
            raise ModelTimeoutError(
                "provider response timed out",
                details={"phase": "read"},
            ) from error
        except httpx.HTTPError as error:
            raise ModelProviderError(
                "provider response failed while reading",
                details={"error_type": type(error).__name__},
            ) from error
        return b"".join(chunks)

    @staticmethod
    def _redact_headers(headers: Mapping[str, str]) -> dict[str, str]:
        """Redacts credential-bearing response headers."""
        sensitive = {"authorization", "set-cookie", "cookie", "proxy-authenticate"}
        return {
            key: (REDACTED if key.lower() in sensitive else value) for key, value in headers.items()
        }


async def _resolve_host(host: str) -> list[str]:
    """Resolves a hostname without blocking the event loop."""
    import asyncio

    loop = asyncio.get_running_loop()

    def resolve() -> list[str]:
        return [str(info[4][0]) for info in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)]

    return await loop.run_in_executor(None, resolve)


def classify_status(status_code: int, retry_after: float | None = None) -> Exception:
    """Maps an HTTP status onto a typed error."""
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
    """Parses a ``Retry-After`` header in delta-seconds or HTTP-date form."""
    if not raw:
        return None
    value = raw.strip()
    if value.isdigit():
        return float(int(value))

    import time as _time
    from email.utils import parsedate_to_datetime

    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        return None
    delta = when.timestamp() - _time.time()
    return delta if delta > 0 else None
