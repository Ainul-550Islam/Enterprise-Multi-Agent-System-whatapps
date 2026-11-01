"""The guardrail pipeline.

Spec §13 defines three stages — before model execution, during execution, and
after model execution — and one rule that governs all of them:

    Guardrail failures must return typed denial/error results and must not
    become successful executions.

That rule is why every method here returns a typed result or raises a typed
error, and why none of them has a "but proceed anyway" path. A guardrail whose
failure can be skipped is not a guardrail; it is a log line.

The stages:

* **before** — input validation, size limits, injection analysis, tool
  permission checks, policy bridge.
* **during** — tool output stays untrusted, delegation limits, budget limits,
  timeouts, cancellation.
* **after** — schema validation, output safety, PII handling, citation
  validation, policy result handling.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.contracts.errors import (
    OutputValidationFailedError,
    PolicyDeniedError,
    PromptInjectionDetectedError,
)
from mas_orchestrator.guardrails.input import (
    InputLimits,
    enforce_input,
    validate_input,
)
from mas_orchestrator.guardrails.output import (
    OutputConfig,
    enforce_output,
    validate_output,
)
from mas_orchestrator.guardrails.pii import (
    PiiPolicy,
    RedactSecretsOnly,
    redact,
    scan,
)
from mas_orchestrator.guardrails.policy import (
    PolicyBridge,
    PolicyStage,
    allowed_or_denied,
)
from mas_orchestrator.guardrails.prompt_injection import (
    RiskLevel,
    analyse,
)

if TYPE_CHECKING:
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.tools.types import ToolDefinition, ToolInvocationContext

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class GuardrailConfig:
    """Thresholds for the pipeline.

    Attributes:
        input_limits: Size ceilings for input.
        injection_refusal_level: Risk at or above which input is refused.
        injection_redact_level: Risk at or above which input is fenced rather
            than merely logged. Fencing is cheaper than refusing and is the
            right first response to a low-confidence signal.
        pii_policy: What to do with detected personal data.
        output: Output validation settings.
        redact_output: Whether to apply the PII policy to model output before
            it is returned.
    """

    input_limits: InputLimits = field(default_factory=InputLimits)
    injection_refusal_level: RiskLevel = RiskLevel.HIGH
    injection_redact_level: RiskLevel = RiskLevel.LOW
    pii_policy: PiiPolicy | None = None
    output: OutputConfig = field(default_factory=OutputConfig)
    redact_output: bool = True

    def __post_init__(self) -> None:
        if self.injection_redact_level.ordinal > self.injection_refusal_level.ordinal:
            raise ValueError("injection_redact_level must not exceed injection_refusal_level")


@dataclass(frozen=True, slots=True)
class BeforeModelResult:
    """Outcome of the before-model stage.

    Attributes:
        text: The (possibly sanitised or fenced) input to use.
        risk: Injection risk detected.
        fenced: Whether the input was fenced because of risk.
        sanitised: What input sanitisation removed.
    """

    text: str
    risk: RiskLevel
    fenced: bool = False
    sanitised: tuple[str, ...] = ()


class GuardrailManager:
    """Runs the three guardrail stages."""

    def __init__(
        self,
        *,
        policy: PolicyBridge | None = None,
        config: GuardrailConfig | None = None,
    ) -> None:
        from mas_orchestrator.guardrails.policy import DenyAllPolicy

        self._policy = policy or DenyAllPolicy()
        self._config = config or GuardrailConfig()

    @property
    def config(self) -> GuardrailConfig:
        """The configuration in force."""
        return self._config

    # --- Stage 1: before model execution ---------------------------------

    async def before_model(
        self,
        text: object,
        context: ExecutionContext,
        *,
        refuse_on_injection: bool = True,
    ) -> BeforeModelResult:
        """Validates input, analyses injection risk, and consults policy.

        Args:
            text: The user's input.
            context: Authoritative execution context.
            refuse_on_injection: Whether to refuse at the configured risk
                level. When false, risky input is fenced rather than refused,
                which is the right behaviour for a human reviewing an
                execution rather than an automated one.

        Returns:
            A :class:`BeforeModelResult`.

        Raises:
            InvalidRequestError: If the input is rejected on structure or size.
            PromptInjectionDetectedError: If risk reaches the refusal level.
            PolicyDeniedError: If the policy bridge denies.
        """
        checked = validate_input(text, limits=self._config.input_limits)
        if not checked.accepted:
            from mas_orchestrator.contracts.errors import InvalidRequestError

            raise InvalidRequestError(
                f"input rejected: {checked.reason}",
                details={"reason": checked.reason, "stage": "before_model"},
            )

        report = analyse(checked.text)
        fenced = False
        content = checked.text

        if report.level.ordinal >= self._config.injection_refusal_level.ordinal:
            if refuse_on_injection:
                _LOGGER.warning(
                    "input refused for injection risk",
                    extra={
                        "event": "guardrail.injection_refused",
                        "execution_id": context.execution_id,
                        "level": report.level.value,
                        "score": round(report.score, 2),
                    },
                )
                raise PromptInjectionDetectedError(
                    "input was refused because it appears to contain an injection attempt",
                    details={
                        "level": report.level.value,
                        "score": round(report.score, 2),
                        "signals": [signal.name for signal in report.signals],
                    },
                )
            content = _fence(content)
            fenced = True
        elif report.level.ordinal >= self._config.injection_redact_level.ordinal:
            content = _fence(content)
            fenced = True

        await allowed_or_denied(
            self._policy,
            PolicyStage.BEFORE_MODEL,
            "model_request",
            {
                "tenant_id": context.tenant_id,
                "execution_id": context.execution_id,
                "agent_id": context.agent_id,
                "risk_level": report.level.value,
                "input_chars": len(content),
            },
        )

        return BeforeModelResult(
            text=content,
            risk=report.level,
            fenced=fenced,
            sanitised=checked.removed,
        )

    async def check_tool_permission(
        self,
        definition: ToolDefinition,
        invocation: ToolInvocationContext,
    ) -> None:
        """Consults the policy bridge before a tool call.

        Raises:
            PolicyDeniedError: If the bridge denies.
        """
        await allowed_or_denied(
            self._policy,
            PolicyStage.BEFORE_MODEL,
            f"tool:{definition.tool_id}",
            {
                "tenant_id": invocation.tenant_id,
                "execution_id": invocation.execution_id,
                "agent_id": invocation.agent_id,
                "tool_version": definition.version.number,
                "safety": definition.safety.value,
            },
        )

    # --- Stage 3: after model execution ----------------------------------

    async def after_model(
        self,
        content: str,
        context: ExecutionContext,
        *,
        config: OutputConfig | None = None,
    ) -> str:
        """Validates model output.

        Args:
            content: The model's output.
            context: Authoritative execution context.
            config: Overrides the configured output validation.

        Returns:
            The validated content, redacted if configured to be.

        Raises:
            OutputValidationFailedError: If schema, size or citation checks
                fail.
            PolicyDeniedError: If the policy bridge denies the result.
        """
        effective = config or self._config.output
        validated = enforce_output(content, effective)

        result = validated
        if self._config.redact_output:
            result, acted = redact(validated, policy=self._config.pii_policy)
            if acted:
                _LOGGER.info(
                    "output redacted",
                    extra={
                        "event": "guardrail.output_redacted",
                        "execution_id": context.execution_id,
                        "count": len(acted),
                    },
                )

        report = scan(result)
        if report.detected:
            _LOGGER.info(
                "output contains personal data",
                extra={
                    "event": "guardrail.output_pii",
                    "execution_id": context.execution_id,
                    "kinds": [kind.value for kind in report.kinds()],
                },
            )

        await allowed_or_denied(
            self._policy,
            PolicyStage.AFTER_MODEL,
            "model_response",
            {
                "tenant_id": context.tenant_id,
                "execution_id": context.execution_id,
                "agent_id": context.agent_id,
                "output_chars": len(result),
            },
        )
        return result

    # --- Convenience ------------------------------------------------------

    def validate_input_only(self, text: object) -> str:
        """Runs input validation alone, raising on rejection."""
        return enforce_input(text, limits=self._config.input_limits)

    def classify_output(self, content: str, config: OutputConfig | None = None) -> Any:
        """Returns the output check without raising.

        Useful when a caller wants to decide what to do with a bad output
        rather than have it raise.
        """
        return validate_output(content, config or self._config.output)


def _fence(text: str) -> str:
    """Marks risky input as data before it reaches a model.

    Fencing is the cheap response to a low-confidence signal: the content is
    still available to the model, but the boundary around it is explicit.
    """
    from mas_orchestrator.prompts.renderer import fence as fence_text

    return fence_text(text, label="user_input")


def gate_output(
    content: str,
    *,
    schema: dict[str, Any] | None = None,
    available_citations: int = 0,
) -> str:
    """Validates output against a schema and citation range.

    A standalone helper for callers that do not need the whole pipeline.

    Raises:
        OutputValidationFailedError: On any failure.
    """
    config = OutputConfig(
        schema=schema,
        require_citations_in_range=available_citations > 0,
        available_citations=available_citations,
    )
    return enforce_output(content, config)


__all__ = [
    "BeforeModelResult",
    "GuardrailConfig",
    "GuardrailManager",
    "OutputValidationFailedError",
    "PolicyDeniedError",
    "PromptInjectionDetectedError",
    "RedactSecretsOnly",
    "RiskLevel",
    "gate_output",
]
