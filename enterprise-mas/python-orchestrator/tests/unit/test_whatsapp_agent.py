"""WhatsappSupportAgent triage behaviour (real assertions)."""

from __future__ import annotations

import pytest

from mas_orchestrator.agents.builtin.whatsapp_support_agent import (
    SupportIntent,
    WhatsappSupportAgent,
)


class TestWhatsappSupportAgent:
    def setup_method(self) -> None:
        self.agent = WhatsappSupportAgent()

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("where is my order?", SupportIntent.ORDER_STATUS),
            ("I want a refund for ORD-1", SupportIntent.REFUND),
            ("please talk to a human", SupportIntent.HUMAN_HANDOFF),
            ("I can't login to my account", SupportIntent.ACCOUNT),
            ("what is the weather?", SupportIntent.OTHER),
            ("", SupportIntent.OTHER),
        ],
    )
    def test_classifies_intents(self, text: str, expected: SupportIntent) -> None:
        assert self.agent.classify(text) is expected

    def test_refund_and_handoff_need_a_human(self) -> None:
        result = self.agent.handle_message("I want my money back")
        assert result.intent is SupportIntent.REFUND
        assert result.needs_human is True
        result = self.agent.handle_message("get me a manager")
        assert result.needs_human is True

    def test_replies_stay_within_whatsapp_segments(self) -> None:
        result = self.agent.handle_message("tell me about my shipping")
        assert result.intent is SupportIntent.ORDER_STATUS
        assert result.needs_human is False
        assert len(result.reply_segments) >= 1
        assert all(len(segment) <= 4096 for segment in result.reply_segments)
        # the reply asks for the order number — no invented facts
        joined = " ".join(result.reply_segments)
        assert "order number" in joined

    def test_segments_respect_a_custom_limit(self) -> None:
        agent = WhatsappSupportAgent(max_segment_chars=20)
        result = agent.agent_id, agent.handle_message("hello there friend")
        _, triage = result
        assert all(len(segment) <= 20 for segment in triage.reply_segments)

    def test_reply_is_sanitized_and_formatted_for_whatsapp(self) -> None:
        self.agent.draft_reply = lambda intent, customer_name="there": "<b>**Hello**</b>"

        result = self.agent.handle_message("hello")

        assert result.reply_segments == ("*Hello*",)
        assert "<b>" not in result.reply_segments[0]

    def test_invalid_limits_are_refused(self) -> None:
        with pytest.raises(ValueError):
            WhatsappSupportAgent(max_segment_chars=0)

    def test_agent_id_is_stable(self) -> None:
        assert self.agent.agent_id == "whatsapp_support"
