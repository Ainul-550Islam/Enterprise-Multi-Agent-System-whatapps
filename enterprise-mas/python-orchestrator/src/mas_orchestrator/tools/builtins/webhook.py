"""Webhook tool.

A webhook is an outbound call to a URL the platform chose, carrying a payload a
model composed. The danger is not the payload, it is the destination: a model
that can name the URL can exfiltrate to anywhere.

So the tool never accepts a URL from the model. Destinations are registered up
front by id, and the invocation names one of them. There is no code path that
takes a caller-supplied URL, which is why the input schema has no ``url`` field.

The call itself goes through the platform's SSRF-guarded transport, and the
payload is signed with an injected secret so the receiver can tell a real
platform call from a forgery.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import InvalidRequestError, ToolFailedError
from mas_orchestrator.providers.base_http import HttpPolicy, HttpTransport
from mas_orchestrator.tools.types import (
    ToolDefinition,
    ToolInvocationContext,
    ToolKind,
    ToolSafety,
    ToolVersion,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_PAYLOAD_CHARS: Final[int] = 64 * 1024
DEFAULT_TIMEOUT_SECONDS: Final[float] = 15.0

#: Signature header name.
SIGNATURE_HEADER: Final[str] = "x-mas-signature"
TIMESTAMP_HEADER: Final[str] = "x-mas-timestamp"

#: How far a signature timestamp may drift. Bounds replay without requiring
#: the receiver to keep state.
MAX_CLOCK_SKEW_SECONDS: Final[float] = 300.0


@dataclass(frozen=True, slots=True)
class WebhookTarget:
    """A registered destination.

    Attributes:
        target_id: How the model refers to it.
        url: The fixed destination URL.
        events: Event names this target accepts, or empty for all.
        max_payload_chars: Per-target payload ceiling.
    """

    target_id: str
    url: str
    events: frozenset[str] = frozenset()
    max_payload_chars: int = MAX_PAYLOAD_CHARS

    def __post_init__(self) -> None:
        if not self.target_id.strip():
            raise InvalidRequestError(
                "webhook target requires an id", details={"field": "target_id"}
            )
        if not self.url.startswith("https://"):
            raise InvalidRequestError("webhook targets must use https", details={"field": "url"})
        if self.max_payload_chars < 1:
            raise InvalidRequestError(
                "max_payload_chars must be at least 1",
                details={"field": "max_payload_chars"},
            )

    def accepts(self, event: str) -> bool:
        """Whether the target accepts this event name."""
        return not self.events or event in self.events


@runtime_checkable
class SigningSecret(Protocol):
    """Supplies the signing key without exposing it."""

    @property
    def key_material(self) -> str: ...


class StaticSecret:
    """A fixed signing key.

    For tests and single-tenant deployments. Refuses to construct in
    production, where a key belongs in a secret manager.
    """

    def __init__(self, key: str, *, environment: str) -> None:
        from mas_orchestrator.config.enums import Environment

        if environment == Environment.PRODUCTION.value:
            raise InvalidRequestError(
                "a static signing secret is not permitted in production",
                details={"environment": environment},
            )
        if not key:
            raise InvalidRequestError("signing key must not be empty", details={"field": "key"})
        self._key = key

    @property
    def key_material(self) -> str:
        """The key."""
        return self._key


@runtime_checkable
class WebhookTransport(Protocol):
    """Sends a signed request."""

    async def post(
        self,
        url: str,
        *,
        json_body: dict[str, object],
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> int:
        """Posts and returns the HTTP status code."""
        ...


@dataclass(frozen=True, slots=True)
class WebhookToolConfig:
    """What the tool permits.

    Attributes:
        targets: Registered destinations. The model may name one of these and
            nothing else.
        timeout_seconds: Delivery budget.
        sign_payloads: Whether to sign. Leave enabled: an unsigned webhook
            cannot be distinguished from a forgery by the receiver.
        read_status_codes: Status codes treated as delivery failure rather than
            success. Defaults to the 5xx range.
    """

    targets: frozenset[WebhookTarget]
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    sign_payloads: bool = True
    failure_status_threshold: int = 500

    def __post_init__(self) -> None:
        if not self.targets:
            raise InvalidRequestError(
                "webhook tool requires at least one registered target",
                details={"field": "targets"},
            )
        if self.timeout_seconds <= 0:
            raise InvalidRequestError(
                "timeout_seconds must be positive",
                details={"timeout": self.timeout_seconds},
            )


class WebhookTool:
    """Posts a payload to a registered target."""

    def __init__(
        self,
        *,
        config: WebhookToolConfig,
        secret: SigningSecret | None = None,
        transport: WebhookTransport | None = None,
    ) -> None:
        if config.sign_payloads and secret is None:
            raise InvalidRequestError(
                "signing is enabled but no signing secret was supplied",
                details={"field": "secret"},
            )
        self._config = config
        self._secret = secret
        self._transport = transport or _GuardedTransport(config)
        self._targets = {target.target_id: target for target in config.targets}

    @property
    def config(self) -> WebhookToolConfig:
        """The configuration in force."""
        return self._config

    def target_ids(self) -> tuple[str, ...]:
        """The target ids the model may name."""
        return tuple(sorted(self._targets))

    async def invoke(
        self,
        arguments: dict[str, Any],
        context: ToolInvocationContext,
    ) -> dict[str, Any]:
        """Posts to the named target.

        Returns:
            A mapping with ``target_id``, ``status_code`` and ``delivered``.

        Raises:
            InvalidRequestError: If the target is unknown or the payload is
                rejected.
            ToolFailedError: If delivery fails.
        """
        target_id = arguments.get("target_id")
        if not isinstance(target_id, str) or not target_id.strip():
            raise InvalidRequestError(
                "target_id must be a non-empty string", details={"field": "target_id"}
            )
        target = self._targets.get(target_id)
        if target is None:
            raise InvalidRequestError(
                "unknown webhook target: destinations must be registered in "
                "advance, because a model-supplied URL is an exfiltration path",
                details={"field": "target_id", "known": sorted(self._targets)},
            )

        event = arguments.get("event", "")
        if not isinstance(event, str):
            raise InvalidRequestError("event must be a string", details={"field": "event"})
        if event and not target.accepts(event):
            raise InvalidRequestError(
                "target does not accept this event",
                details={"target_id": target_id, "event": event},
            )

        payload = arguments.get("payload", {})
        envelope: dict[str, object] = {
            "event": event,
            "tenant_id": context.tenant_id,
            "execution_id": context.execution_id,
            "payload": payload,
        }
        body = canonical_bytes(envelope)

        if len(body) > target.max_payload_chars:
            raise InvalidRequestError(
                "payload exceeds the target's maximum size",
                details={
                    "target_id": target_id,
                    "limit": target.max_payload_chars,
                    "length": len(body),
                },
            )

        headers: dict[str, str] = {
            "content-type": "application/json",
            "user-agent": "mas-orchestrator/webhook",
        }
        if self._config.sign_payloads and self._secret is not None:
            timestamp = str(int(time.time()))
            headers[TIMESTAMP_HEADER] = timestamp
            headers[SIGNATURE_HEADER] = self._sign(body, timestamp)

        try:
            status = await self._transport.post(
                target.url,
                json_body=envelope,
                headers=headers,
                timeout_seconds=self._config.timeout_seconds,
            )
        except Exception as error:
            raise ToolFailedError(
                "webhook delivery failed",
                details={
                    "target_id": target_id,
                    "error": type(error).__name__,
                },
            ) from error

        delivered = status < self._config.failure_status_threshold
        _LOGGER.info(
            "webhook delivered",
            extra={
                "event": "tool.webhook.post",
                "execution_id": context.execution_id,
                "target_id": target_id,
                "status_code": status,
            },
        )
        return {
            "target_id": target_id,
            "status_code": status,
            "delivered": delivered,
        }

    def _sign(self, body: bytes, timestamp: str) -> str:
        """Signs the timestamped canonical body.

        The timestamp is inside the signed material, so a captured request
        cannot be replayed with a fresh timestamp.

        The signature covers the **canonical** form produced by
        :func:`canonical_bytes`, not the exact bytes on the wire: the transport
        serialises the envelope itself. A receiver must therefore parse the
        body and re-canonicalise it before verifying, which is the same
        arrangement commercial webhook senders use.
        """
        assert self._secret is not None
        message = f"{timestamp}.".encode() + body
        digest = hmac.new(self._secret.key_material.encode(), message, hashlib.sha256).hexdigest()
        return f"sha256={digest}"

    @staticmethod
    def verify(body: bytes, timestamp: str, signature: str, *, key: str) -> bool:
        """Verifies a signature, for use by a receiver.

        Uses a constant-time comparison, so a rejected signature does not leak
        how close it was.
        """
        if not timestamp.isdigit():
            return False
        if abs(time.time() - int(timestamp)) > MAX_CLOCK_SKEW_SECONDS:
            return False
        expected = hmac.new(
            key.encode(), f"{timestamp}.".encode() + body, hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(f"sha256={expected}", signature)


class _GuardedTransport:
    """Posts through the platform's SSRF-guarded HTTP transport."""

    def __init__(self, config: WebhookToolConfig) -> None:
        hosts = frozenset(_host_of(target.url) for target in config.targets)
        self._transport = HttpTransport(
            policy=HttpPolicy(
                allowed_hosts=hosts,
                max_response_bytes=8 * 1024,
                max_redirects=0,
                require_https=True,
            )
        )

    async def post(
        self,
        url: str,
        *,
        json_body: dict[str, object],
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> int:
        """Posts and returns the status code."""
        del timeout_seconds  # bounded by the transport's own policy
        response = await self._transport.request("POST", url, json_body=json_body, headers=headers)
        return int(response.status_code)


def _host_of(url: str) -> str:
    """Extracts the hostname from a URL."""
    from urllib.parse import urlsplit

    return urlsplit(url).hostname or ""


def build_webhook_tool(
    *,
    tool_id: str,
    name: str,
    tenant_id: str,
    targets: frozenset[WebhookTarget] | set[WebhookTarget],
    secret: SigningSecret | None = None,
    description: str | None = None,
    version: int = 1,
    config: WebhookToolConfig | None = None,
) -> tuple[ToolDefinition, WebhookTool]:
    """Builds a definition/handler pair ready for registration.

    Declared non-idempotent: two identical posts are two deliveries, and a
    receiver that is not deduplicating will process both.
    """
    settings = config or WebhookToolConfig(targets=frozenset(targets))
    definition = ToolDefinition(
        tool_id=tool_id,
        name=name,
        description=description or "Post a payload to a registered webhook target.",
        version=ToolVersion(version),
        kind=ToolKind.CONNECTOR,
        safety=ToolSafety.STANDARD,
        tenant_id=tenant_id,
        idempotent=False,
    )
    return definition, WebhookTool(config=settings, secret=secret)


def canonical_bytes(envelope: dict[str, object]) -> bytes:
    """Renders an envelope in the canonical signed form.

    Sorted keys and compact separators, so two representations of the same
    envelope produce the same bytes. Both sides must use this function for a
    signature to verify.
    """
    return json.dumps(envelope, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode(
        "utf-8"
    )
