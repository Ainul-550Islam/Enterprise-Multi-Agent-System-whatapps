"""Unit tests for deterministic redaction.

Redaction is a security control, so these tests are deliberately adversarial:
they try to smuggle secrets in through key names, through prose, and through
nested containers.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.security.redaction import (
    REDACTED,
    is_sensitive_key,
    redact_key,
    redact_mapping,
    redact_text,
    redact_value,
)


class TestSensitiveKeyDetection:
    """Key-shaped detection is the primary defence."""

    @pytest.mark.parametrize(
        "key",
        [
            "password",
            "PASSWORD",
            "api_key",
            "API-Key",
            "X-Api-Key",
            "apikey",
            "authorization",
            "Authorization",
            "bearer_token",
            "access_token",
            "refresh_token",
            "client_secret",
            "private_key",
            "user_password",
            "credentials",
        ],
    )
    def test_sensitive_keys_are_detected(self, key: str) -> None:
        assert is_sensitive_key(key) is True

    @pytest.mark.parametrize(
        "key",
        ["prompt", "model", "tenant_id", "messages", "temperature", "content", "name"],
    )
    def test_ordinary_keys_are_not_detected(self, key: str) -> None:
        assert is_sensitive_key(key) is False

    def test_redact_key_replaces_sensitive_only(self) -> None:
        assert redact_key("api_key") == REDACTED
        assert redact_key("model") == "model"


class TestRedactMapping:
    """Structured redaction."""

    def test_sensitive_value_is_replaced(self) -> None:
        redacted = redact_mapping({"api_key": "sk-abc123", "model": "gpt-4"})
        assert redacted["api_key"] == REDACTED
        assert redacted["model"] == "gpt-4"

    def test_nested_mapping_is_redacted(self) -> None:
        payload = {"outer": {"authorization": "Bearer tok", "keep": "value"}}
        redacted = redact_mapping(payload)
        assert redacted["outer"]["authorization"] == REDACTED
        assert redacted["outer"]["keep"] == "value"

    def test_sequence_elements_are_redacted(self) -> None:
        redacted = redact_mapping({"items": [{"password": "hunter2"}, "plain"]})
        assert redacted["items"][0]["password"] == REDACTED
        assert redacted["items"][1] == "plain"

    def test_input_mapping_is_not_mutated(self) -> None:
        original = {"token": "abc"}
        redact_mapping(original)
        assert original["token"] == "abc"

    def test_scalars_pass_through(self) -> None:
        redacted = redact_mapping({"count": 3, "ratio": 0.5, "ok": True, "none": None})
        assert redacted == {"count": 3, "ratio": 0.5, "ok": True, "none": None}

    def test_string_values_are_scrubbed_even_under_safe_keys(self) -> None:
        """A secret pasted into a 'message' field must still be caught."""
        redacted = redact_mapping({"message": "use sk-abcdefghijklmnopqrstuvwxyz012345"})
        assert "sk-abcdefghijklmnopqrstuvwxyz012345" not in str(redacted["message"])

    def test_unknown_object_is_reported_by_type(self) -> None:
        """Never call str() on an unknown object; it may dump secrets."""

        class Leaky:
            def __str__(self) -> str:
                return "sk-abcdefghijklmnopqrstuvwxyz012345"

        redacted = redact_mapping({"obj": Leaky()})
        assert "sk-" not in str(redacted["obj"])
        assert "Leaky" in str(redacted["obj"])

    def test_cyclic_structure_does_not_explode(self) -> None:
        """Redaction runs inside logging; it must never raise."""
        cyclic: dict[str, object] = {}
        cyclic["self"] = cyclic
        assert redact_mapping(cyclic) is not None


class TestRedactText:
    """Value-shaped scrubbing of free-form text."""

    def test_bearer_token_is_redacted(self) -> None:
        scrubbed = redact_text("Authorization: Bearer abcdefghijklmnop")
        assert "abcdefghijklmnop" not in scrubbed
        assert REDACTED in scrubbed

    def test_openai_style_key_is_redacted(self) -> None:
        scrubbed = redact_text("key=sk-abcdefghijklmnopqrstuvwxyz0123456789")
        assert "sk-abcdefghijklmnopqrstuvwxyz0123456789" not in scrubbed

    def test_assignment_keeps_key_but_drops_value(self) -> None:
        scrubbed = redact_text('api_key: "supersecretvalue"')
        assert "supersecretvalue" not in scrubbed
        assert "api_key" in scrubbed

    def test_pem_private_key_block_is_redacted(self) -> None:
        pem = (
            "-----BEGIN RSA PRIVATE KEY-----\n"
            "MIIBOgIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu\n"
            "-----END RSA PRIVATE KEY-----"
        )
        scrubbed = redact_text(pem)
        assert "MIIBOgIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu" not in scrubbed
        assert REDACTED in scrubbed

    def test_high_entropy_run_is_redacted(self) -> None:
        scrubbed = redact_text("blob: aB3dEf7hIj9kLm1NoP3qRs5Tu7vW9xY1zA2bC4dE6fG8h")
        assert "aB3dEf7hIj9kLm1NoP3qRs5Tu7vW9xY1zA2bC4dE6fG8h" not in scrubbed

    def test_ordinary_text_is_left_alone(self) -> None:
        """Over-redaction would make logs useless; normal text must survive."""
        text = "the model returned a summary of the quarterly report"
        assert redact_text(text) == text

    def test_empty_text_is_returned_unchanged(self) -> None:
        assert redact_text("") == ""

    def test_short_alphanumeric_is_not_treated_as_secret(self) -> None:
        """Identifiers like 'gpt4' must not be redacted."""
        assert redact_text("using model gpt4") == "using model gpt4"


class TestRedactValueTopLevel:
    """The general-purpose entry point."""

    def test_scalars_are_returned_unchanged(self) -> None:
        assert redact_value(None) is None
        assert redact_value(42) == 42
        assert redact_value(1.5) == 1.5
        assert redact_value(True) is True

    def test_tuple_becomes_list(self) -> None:
        assert redact_value((1, 2)) == [1, 2]

    def test_set_is_redacted(self) -> None:
        redacted = redact_value({"plain"})
        assert redacted == ["plain"]


class TestDeterminism:
    """Redaction must be reproducible so it can be asserted on."""

    def test_same_input_yields_same_output(self) -> None:
        payload = {"token": "abc", "message": "Bearer zzzzzzzzzzzz"}
        assert redact_mapping(payload) == redact_mapping(payload)

    def test_redaction_is_idempotent(self) -> None:
        once = redact_mapping({"password": "hunter2"})
        twice = redact_mapping(once)
        assert once == twice
