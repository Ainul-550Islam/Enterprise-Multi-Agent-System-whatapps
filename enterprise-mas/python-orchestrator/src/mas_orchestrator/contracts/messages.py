"""Typed internal representations of Rust/Python messages.

Two things travel between Rust and Python: commands (do this work) and events
(this happened). Both are modelled here as frozen dataclasses with explicit
protocol versioning, because an untyped envelope is how a wire format acquires
fields nobody can safely remove.

The tenancy rule from spec §6 is enforced by construction:
:meth:`Envelope.from_headers` takes the whole metadata mapping and reads the
tenant from it, but the *only* header it will believe is the one Rust sets.
There is no path by which a caller-supplied value becomes the tenant.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from mas_orchestrator.contracts.errors import ProtocolError
from mas_orchestrator.contracts.protocol_version import (
    CURRENT_PROTOCOL_VERSION,
    ProtocolVersion,
    negotiate_protocol_version,
)

#: gRPC metadata key carrying the protocol version.
HEADER_PROTOCOL_VERSION: str = "x-ai-protocol-version"
#: gRPC metadata keys carrying correlation context set by Rust.
HEADER_REQUEST_ID: str = "x-request-id"
HEADER_CORRELATION_ID: str = "x-correlation-id"
HEADER_EXECUTION_ID: str = "x-execution-id"
HEADER_TASK_ID: str = "x-task-id"
HEADER_TASK_ATTEMPT_ID: str = "x-task-attempt-id"
HEADER_ORGANIZATION_ID: str = "x-organization-id"

#: Keys that identify the work, and that Python may read from metadata.
CORRELATION_KEYS: tuple[str, ...] = (
    HEADER_REQUEST_ID,
    HEADER_CORRELATION_ID,
    HEADER_EXECUTION_ID,
    HEADER_TASK_ID,
    HEADER_TASK_ATTEMPT_ID,
    HEADER_ORGANIZATION_ID,
)


class MessageKind(StrEnum):
    """What a message asks for or reports."""

    EXECUTION_REQUEST = "execution_request"
    EXECUTION_RESULT = "execution_result"
    TASK_REQUEST = "task_request"
    TASK_RESULT = "task_result"
    CANCEL_REQUEST = "cancel_request"
    HEALTH_CHECK = "health_check"


@dataclass(frozen=True, slots=True)
class Envelope:
    """The parts every message carries.

    Attributes:
        kind: What the message is.
        protocol_version: Negotiated wire version.
        request_id: Request identifier.
        correlation_id: Correlation identifier.
        execution_id: Execution identifier.
        task_id: Task identifier, when the work is task-scoped.
        task_attempt_id: Attempt identifier, when known.
        organization_id: Organization identifier.
        occurred_epoch_ms: When the message was created.
    """

    kind: MessageKind
    protocol_version: ProtocolVersion = CURRENT_PROTOCOL_VERSION
    request_id: str = ""
    correlation_id: str = ""
    execution_id: str = ""
    task_id: str | None = None
    task_attempt_id: str | None = None
    organization_id: str = ""
    occurred_epoch_ms: int = 0

    def __post_init__(self) -> None:
        if not self.occurred_epoch_ms:
            object.__setattr__(self, "occurred_epoch_ms", int(time.time() * 1000))

    @classmethod
    def from_headers(
        cls,
        kind: MessageKind,
        headers: dict[str, str],
    ) -> Envelope:
        """Builds an envelope from gRPC metadata set by Rust.

        The protocol version is negotiated, not read: an unsupported or absent
        version raises. Correlation fields are optional at the envelope level
        because a health check carries none of them; the execution path
        validates them separately.

        Args:
            kind: What the message is.
            headers: Lowercased gRPC metadata.

        Raises:
            ProtocolError: If the protocol version is missing or unsupported.
        """
        raw_version = headers.get(HEADER_PROTOCOL_VERSION)
        version = negotiate_protocol_version(raw_version)

        def _get(key: str) -> str | None:
            value = headers.get(key)
            return value if value else None

        return cls(
            kind=kind,
            protocol_version=version,
            request_id=_get(HEADER_REQUEST_ID) or "",
            correlation_id=_get(HEADER_CORRELATION_ID) or "",
            execution_id=_get(HEADER_EXECUTION_ID) or "",
            task_id=_get(HEADER_TASK_ID),
            task_attempt_id=_get(HEADER_TASK_ATTEMPT_ID),
            organization_id=_get(HEADER_ORGANIZATION_ID) or "",
        )

    def to_headers(self) -> dict[str, str]:
        """Renders the envelope as gRPC metadata for an outbound call."""
        headers: dict[str, str] = {
            HEADER_PROTOCOL_VERSION: str(self.protocol_version),
            HEADER_REQUEST_ID: self.request_id,
            HEADER_CORRELATION_ID: self.correlation_id,
            HEADER_EXECUTION_ID: self.execution_id,
            HEADER_ORGANIZATION_ID: self.organization_id,
        }
        if self.task_id:
            headers[HEADER_TASK_ID] = self.task_id
        if self.task_attempt_id:
            headers[HEADER_TASK_ATTEMPT_ID] = self.task_attempt_id
        return headers


@dataclass(frozen=True, slots=True)
class AiMessage:
    """A typed Rust/Python message.

    Attributes:
        envelope: Routing and correlation.
        payload: The message body. Opaque here; each handler validates the
            shape it expects rather than trusting it.
    """

    envelope: Envelope
    payload: dict[str, Any] = field(default_factory=dict)

    def require_execution_id(self) -> str:
        """Returns the execution id, refusing an empty one.

        Raises:
            ProtocolError: If no execution id was supplied.
        """
        if not self.envelope.execution_id.strip():
            raise ProtocolError(
                "message carries no execution id",
                details={"kind": self.envelope.kind.value},
            )
        return self.envelope.execution_id
