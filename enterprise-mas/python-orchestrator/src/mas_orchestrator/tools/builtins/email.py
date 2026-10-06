"""Email tool.

Email is the highest-consequence tool in the set: it is irreversible, it leaves
the organisation, and a wrong recipient is a disclosure. Three constraints
follow from that, and all three are enforced here rather than left to the
caller:

* **Recipient allowlist.** A tool with no allowlist cannot be constructed. There
  is no "send to anyone" mode, because that is the mode that leaks.
* **Approval required.** The definition is declared
  :attr:`~mas_orchestrator.tools.types.ToolSafety.SENSITIVE`, which routes it
  through the approval gate. A model may compose mail; it may not send it
  unattended.
* **Never idempotent.** Two identical sends are two emails. The tool declares
  itself non-idempotent so a retry cannot duplicate a message — the executor
  suppresses retries for anything with this safety class.

The handler sends through an injected :class:`EmailBackend`. There is no
default, so a misconfigured deployment fails at construction.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import InvalidRequestError, ToolFailedError
from mas_orchestrator.tools.types import (
    ToolDefinition,
    ToolInvocationContext,
    ToolKind,
    ToolSafety,
    ToolVersion,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_SUBJECT_CHARS: Final[int] = 200
MAX_BODY_CHARS: Final[int] = 32 * 1024
MAX_RECIPIENTS: Final[int] = 20
DEFAULT_TIMEOUT_SECONDS: Final[float] = 20.0

#: Deliberately stricter than RFC 5322. Rejecting a valid-but-exotic address is
#: far cheaper than accepting one that smuggles a header through a newline.
_ADDRESS_RE: Final[re.Pattern[str]] = re.compile(
    r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
)


def is_valid_address(value: str) -> bool:
    """Whether a string is a plausible email address.

    The pattern forbids whitespace, quotes, angle brackets and newlines, which
    is what header injection needs.
    """
    return bool(value) and len(value) <= 254 and bool(_ADDRESS_RE.match(value))


def normalise_domain(value: str) -> str:
    """Lowercases a domain for comparison."""
    return value.strip().lower().lstrip("@").rstrip(".")


@runtime_checkable
class EmailBackend(Protocol):
    """Delivers a message."""

    async def send(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        reply_to: str | None,
        timeout_seconds: float,
    ) -> str:
        """Sends one message and returns a provider message id.

        Implementations must not retry internally. The executor cannot
        deduplicate a send it does not know about, so a hidden retry is how two
        copies of one email get delivered.
        """
        ...


@dataclass(frozen=True, slots=True)
class EmailToolConfig:
    """What the tool permits.

    Attributes:
        allowed_domains: Recipient domains allowed. Empty blocks everything,
            which is the safe default.
        allowed_recipients: Individual addresses allowed in addition to the
            domains. Useful for a partner contact outside the org.
        max_recipients: Recipient ceiling per message.
        max_body_chars: Body ceiling.
        timeout_seconds: Delivery budget.
        require_approval: Whether sends need human approval. Refusing to
            disable this is enforced by :func:`build_email_tool`.
    """

    allowed_domains: frozenset[str] = frozenset()
    allowed_recipients: frozenset[str] = frozenset()
    max_recipients: int = MAX_RECIPIENTS
    max_body_chars: int = MAX_BODY_CHARS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    require_approval: bool = True

    def __post_init__(self) -> None:
        if not self.allowed_domains and not self.allowed_recipients:
            raise InvalidRequestError(
                "email tool requires a recipient allowlist: a tool that mails "
                "anyone is a disclosure waiting to happen",
                details={"field": "allowed_domains"},
            )
        if self.max_recipients < 1:
            raise InvalidRequestError(
                "max_recipients must be at least 1",
                details={"max_recipients": self.max_recipients},
            )
        if self.max_body_chars < 1:
            raise InvalidRequestError(
                "max_body_chars must be at least 1",
                details={"max_body_chars": self.max_body_chars},
            )
        if self.timeout_seconds <= 0:
            raise InvalidRequestError(
                "timeout_seconds must be positive",
                details={"timeout": self.timeout_seconds},
            )

    def permits(self, address: str) -> bool:
        """Whether an address is on the allowlist."""
        if not is_valid_address(address):
            return False
        lowered = address.lower()
        if lowered in {a.lower() for a in self.allowed_recipients}:
            return True
        domain = lowered.rsplit("@", 1)[-1]
        allowed = {normalise_domain(d) for d in self.allowed_domains}
        return domain in allowed


class EmailTool:
    """Composes and sends mail to allowlisted recipients only."""

    def __init__(
        self,
        backend: EmailBackend,
        *,
        config: EmailToolConfig,
    ) -> None:
        self._backend = backend
        self._config = config

    @property
    def config(self) -> EmailToolConfig:
        """The limits in force."""
        return self._config

    async def invoke(
        self,
        arguments: dict[str, object],
        context: ToolInvocationContext,
    ) -> dict[str, object]:
        """Sends one message.

        Returns:
            A mapping with ``message_id``, ``recipient_count`` and ``sent``.

        Raises:
            InvalidRequestError: If a recipient, the subject or the body is
                rejected.
            ToolFailedError: If delivery fails.
        """
        recipients = self._validate_recipients(arguments.get("to"))
        subject = self._validate_text(arguments.get("subject"), "subject", MAX_SUBJECT_CHARS)
        body = self._validate_text(arguments.get("body"), "body", self._config.max_body_chars)

        reply_to = arguments.get("reply_to")
        if reply_to is not None and (
            not isinstance(reply_to, str) or not is_valid_address(reply_to)
        ):
            raise InvalidRequestError(
                "reply_to is not a valid address", details={"field": "reply_to"}
            )

        try:
            message_id = await self._backend.send(
                to=recipients,
                subject=subject,
                body=body,
                reply_to=reply_to,
                timeout_seconds=self._config.timeout_seconds,
            )
        except Exception as error:
            raise ToolFailedError(
                "email delivery failed",
                details={"tool_id": "email", "error": type(error).__name__},
            ) from error

        _LOGGER.info(
            "email sent",
            extra={
                "event": "tool.email.sent",
                "execution_id": context.execution_id,
                "recipient_count": len(recipients),
            },
        )
        return {
            "sent": True,
            "message_id": message_id,
            "recipient_count": len(recipients),
        }

    def _validate_recipients(self, raw: object) -> list[str]:
        """Checks and deduplicates the recipient list."""
        if isinstance(raw, str):
            candidates = [raw]
        elif isinstance(raw, list):
            candidates = [str(item) for item in raw]
        else:
            raise InvalidRequestError(
                "to must be a string or a list of strings", details={"field": "to"}
            )

        if not candidates:
            raise InvalidRequestError("at least one recipient is required", details={"field": "to"})
        if len(candidates) > self._config.max_recipients:
            raise InvalidRequestError(
                "too many recipients",
                details={"limit": self._config.max_recipients, "count": len(candidates)},
            )

        recipients: list[str] = []
        for candidate in candidates:
            if not self._config.permits(candidate):
                raise InvalidRequestError(
                    "recipient is not on the allowlist",
                    details={"field": "to"},
                )
            lowered = candidate.lower()
            if lowered not in {r.lower() for r in recipients}:
                recipients.append(candidate)
        return recipients

    def _validate_text(self, raw: object, field: str, limit: int) -> str:
        """Validates a text field."""
        if not isinstance(raw, str) or not raw.strip():
            raise InvalidRequestError(
                f"{field} must be a non-empty string", details={"field": field}
            )
        value = raw.strip()
        if len(value) > limit:
            raise InvalidRequestError(
                f"{field} exceeds the maximum length",
                details={"field": field, "limit": limit, "length": len(value)},
            )
        # Control characters in a header field are an injection primitive.
        if field == "subject" and any(ord(char) < 32 for char in value):
            raise InvalidRequestError(
                "subject must not contain control characters",
                details={"field": field},
            )
        return value


def build_email_tool(
    *,
    tool_id: str,
    name: str,
    tenant_id: str,
    backend: EmailBackend,
    config: EmailToolConfig,
    description: str | None = None,
    version: int = 1,
) -> tuple[ToolDefinition, EmailTool]:
    """Builds a definition/handler pair ready for registration.

    The definition is always :attr:`ToolSafety.SENSITIVE` and never idempotent.
    Both are features, not defaults: a mail tool that could be retried would
    send twice, and one that was not sensitive would send unapproved.
    """
    if not config.require_approval:
        raise InvalidRequestError(
            "the email tool cannot be configured to skip approval",
            details={"field": "require_approval"},
        )
    definition = ToolDefinition(
        tool_id=tool_id,
        name=name,
        description=description or "Send an email to an allowlisted recipient.",
        version=ToolVersion(version),
        kind=ToolKind.CONNECTOR,
        safety=ToolSafety.SENSITIVE,
        tenant_id=tenant_id,
        idempotent=False,
    )
    return definition, EmailTool(backend, config=config)
