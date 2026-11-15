"""Built-in WhatsApp support agent (platform_assistant.py-rateløkka).

This agent uses the WhatsApp system prompt and help text to assist users
with WhatsApp-related queries within the Enterprise MAS platform.
"""
from __future__ import annotations

from mas_orchestrator.agents.base import BaseAgent
from mas_orchestrator.prompts.registry import PromptRegistry, PromptTemplate
from mas_orchestrator.prompts.whatsapp_assistant import WHATSAPP_SYSTEM_PROMPT, WHATSAPP_HELP_TEXT, WHATSAPP_ESCALATION


class WhatsAppSupportAgent(BaseAgent):
    """A built-in agent that assists with WhatsApp platform queries."""

    def __init__(self, registry: PromptRegistry | None = None) -> None:
        # Register the WhatsApp system prompt if not already registered
        if registry is None:
            registry = PromptRegistry()
        # The prompt is already registered in registry.py at module level,
        # but we ensure it's available.
        current_version = registry.current_version("whatsapp_assistant")
        if current_version is None:
            _ = registry.create_prompt(
                name="whatsapp_assistant",
                template=WHATSAPP_SYSTEM_PROMPT,
                variables=frozenset(),
                description="System prompt for the WhatsApp support agent",
                version=1,
            )

        super().__init__(
            name="whatsapp_support",
            prompt_template_name="whatsapp_assistant",
            help_text=WHATSAPP_HELP_TEXT,
            escalation=WHATSAPP_ESCALATION,
        )

    def handle(self, user_input: str, language: str = "en") -> str:
        """Handle a user query in the given language.

        Args:
            user_input: The user's message.
            language: Detected language code ("en" or "bn").

        Returns:
            A WhatsApp‑compatible response string.
        """
        # Simple routing based on keywords
        lower = user_input.lower().strip()

        if not lower:
            return "Hello! How can I help you with WhatsApp today?"

        if any(kw in lower for kw in ["help", "what can you"]):
            return WHATSAPP_HELP_TEXT

        if any(kw in lower for kw in ["account", "register", "waba", "phone"]):
            return self._format(_("I can help you register a WhatsApp Business Account and check its status. Just provide the WABA ID and phone number ID, and I'll guide you through the process."))

        if any(kw in lower for kw in ["template", "message", "send"]):
            return self._format(_("I can help you send an approved template message. You'll need the template name, language code, and variable values. The template must be approved by Meta — I can validate the variable mapping for you."))

        if any(kw in lower for kw in ["status", "sent", "delivered", "read"]):
            return self._format(_("I can check the status of a message if you provide the WhatsApp message ID (wamid). Just give me the ID and I'll look up its current state."))

        if any(kw in lower for kw in ["handoff", "human", "agent"]):
            return self._format(_("I'll connect you with a human agent who can assist further."))

        # Default: ask for clarification
        return self._format(_("I'm not sure what you need — would you like help with account setup, sending a message, checking status, or something else?"))

    def _format(self, text: str) -> str:
        """Ensure the output is WhatsApp‑safe (no HTML, limited markdown)."""
        # Strip any remaining HTML
        import re
        text = re.sub(r"<[^>]+>", "", text)
        # Ensure it's not too long (WhatsApp has practical limits)
        return text[:1500]  # crude limit


# Mock gettext-like translation for demo purposes
# In a production deployment this would be a proper i18n system.
_ = lambda s: s  # identity for now; replace with real i18n


# Export a singleton for the orchestrator to use
whatsapp_support_agent = WhatsAppSupportAgent()