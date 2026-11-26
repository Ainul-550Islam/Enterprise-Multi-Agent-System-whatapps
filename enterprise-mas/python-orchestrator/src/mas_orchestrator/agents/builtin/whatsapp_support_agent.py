"""Product-owned WhatsApp support triage agent.

Read-only over the tenant-scoped conversation context: it classifies the
inbound intent, drafts a reply, and segments that reply into WhatsApp-sized
sends through :func:`~mas_orchestrator.channels.whatsapp_media.split_by_limit`.
It never calls data tools, never writes to the message log (the WhatsApp
service owns storage), and never invents order facts — anything requiring
account data is routed to a human handoff instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from mas_orchestrator.channels.whatsapp_format import (
    format_for_whatsapp,
    reject_tables_and_html,
)
from mas_orchestrator.channels.whatsapp_media import WHATSAPP_TEXT_LIMIT, split_by_limit

WHATSAPP_SUPPORT_AGENT_ID: Final[str] = "whatsapp_support"
WHATSAPP_SUPPORT_AGENT_VERSION: Final[str] = "1"


class SupportIntent(StrEnum):
    """The triage classes the agent can assign."""

    ORDER_STATUS = "order_status"
    REFUND = "refund"
    ACCOUNT = "account"
    HUMAN_HANDOFF = "human_handoff"
    OTHER = "other"


#: Real keyword triage. Order matters: the most specific intent wins.
_INTENT_KEYWORDS: Final[tuple[tuple[SupportIntent, tuple[str, ...]], ...]] = (
    (SupportIntent.HUMAN_HANDOFF, ("human", "agent", "manager", "representative")),
    (SupportIntent.REFUND, ("refund", "money back", "chargeback", "return")),
    (SupportIntent.ORDER_STATUS, ("order", "shipping", "delivery", "shipped", "tracking")),
    (SupportIntent.ACCOUNT, ("password", "login", "account", "email change", "verify")),
)


@dataclass(frozen=True, slots=True)
class TriageResult:
    """The agent's verdict for one inbound message."""

    intent: SupportIntent
    reply_segments: tuple[str, ...]
    needs_human: bool


class WhatsappSupportAgent:
    """Deterministic WhatsApp support triage agent (product-owned)."""

    agent_id: Final[str] = WHATSAPP_SUPPORT_AGENT_ID

    def __init__(self, *, max_segment_chars: int = WHATSAPP_TEXT_LIMIT) -> None:
        if max_segment_chars < 1:
            raise ValueError("max_segment_chars must be >= 1")
        self._max_segment_chars = max_segment_chars

    def classify(self, text: str) -> SupportIntent:
        """Classifies the inbound text into a :class:`SupportIntent`."""
        lowered = text.strip().lower()
        if not lowered:
            return SupportIntent.OTHER
        for intent, keywords in _INTENT_KEYWORDS:
            if any(keyword in lowered for keyword in keywords):
                return intent
        return SupportIntent.OTHER

    def draft_reply(self, intent: SupportIntent, *, customer_name: str = "there") -> str:
        """Drafts the reply body for an intent (facts stay generic on purpose)."""
        greeting = f"Hi {customer_name},"
        if intent is SupportIntent.ORDER_STATUS:
            return (
                f"{greeting} I can look up your order status. "
                "Please share your order number (format ORD-XXXX) and I will check it."
            )
        if intent is SupportIntent.REFUND:
            return (
                f"{greeting} I'm sorry about the trouble. Refunds are handled by a "
                "specialist — I'm connecting you to one now. Please keep your order number handy."
            )
        if intent is SupportIntent.ACCOUNT:
            return (
                f"{greeting} for account security I can't change credentials over chat. "
                "I'm sending you the secure account recovery link from our help center."
            )
        if intent is SupportIntent.HUMAN_HANDOFF:
            return f"{greeting} I'm connecting you to a human teammate right away."
        return (
            f"{greeting} thanks for writing in! Tell me a bit more about what you need "
            "and I'll point you in the right direction."
        )

    def handle_message(self, text: str, *, customer_name: str = "there") -> TriageResult:
        """Classifies, drafts and segments one inbound message."""
        intent = self.classify(text)
        reply = self.draft_reply(intent, customer_name=customer_name)
        safe_reply = format_for_whatsapp(reject_tables_and_html(reply))
        segments = tuple(split_by_limit(safe_reply, self._max_segment_chars))
        return TriageResult(
            intent=intent,
            reply_segments=segments,
            needs_human=intent in (SupportIntent.REFUND, SupportIntent.HUMAN_HANDOFF),
        )
