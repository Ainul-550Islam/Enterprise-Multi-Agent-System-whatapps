"""Guardrails.

Implements spec §13. Three stages — before model execution, during execution,
after model execution — and one governing rule:

    Guardrail failures must return typed denial/error results and must not
    become successful executions.

Several components here are deliberately conservative in the same way:

* :class:`~.policy.DenyAllPolicy` denies everything, so an unconfigured system
  grants nothing.
* :class:`~.pii.RedactSecretsOnly` redacts credentials and leaves personal data
  alone, because deciding that an email address must be suppressed is a policy
  decision this layer has no standing to make.
* :class:`~.prompt_injection.analyse` returns a risk *signal*, not a verdict. Injection
  detection cannot be made sound, and pretending otherwise would produce false
  confidence, which is worse than a false positive.

The ``during execution`` stage is enforced by the agent and tool layers rather
than by this package: tool output stays untrusted, delegation is depth-bounded,
budgets and timeouts are checked before the work they limit.
"""

from __future__ import annotations

from mas_orchestrator.guardrails.citation_check import (
    CitationReport,
    check_citations,
    extract_citations,
    has_unknown_citations,
    strip_citations,
)
from mas_orchestrator.guardrails.input import (
    MAX_ATTACHMENT_BYTES,
    MAX_ATTACHMENTS,
    MAX_INPUT_CHARS,
    InputCheck,
    InputLimits,
    InputVerdict,
    enforce_input,
    validate_input,
)
from mas_orchestrator.guardrails.manager import (
    BeforeModelResult,
    GuardrailConfig,
    GuardrailManager,
    gate_output,
)
from mas_orchestrator.guardrails.models import (
    GuardrailFinding,
    GuardrailOutcome,
    GuardrailStage,
)
from mas_orchestrator.guardrails.output import (
    MAX_OUTPUT_CHARS,
    OutputCheck,
    OutputConfig,
    OutputVerdict,
    enforce_output,
    validate_output,
)
from mas_orchestrator.guardrails.pii import (
    Action,
    PiiFinding,
    PiiKind,
    PiiPolicy,
    PiiReport,
    RedactAll,
    RedactSecretsOnly,
    contains_secrets,
    redact,
    scan,
)
from mas_orchestrator.guardrails.policy import (
    AllowAllPolicy,
    DenyAllPolicy,
    PolicyBridge,
    PolicyDecision,
    PolicyResult,
    PolicyStage,
    allowed_or_denied,
    require_allowed,
)
from mas_orchestrator.guardrails.policy_bridge import (
    BridgeConfig,
    CachingPolicyBridge,
    PolicyAuthority,
    StaticPolicyBridge,
    enforce,
    validate_bridge_config,
)
from mas_orchestrator.guardrails.prompt_injection import (
    InjectionReport,
    InjectionSignal,
    RiskLevel,
    analyse,
    exceeds,
    is_suspicious,
)
from mas_orchestrator.guardrails.tool_safety import (
    SafetyAssessment,
    SafetyClass,
    classify,
    is_repeatable,
    ordinal_of,
    parse_safety,
    requires_approval,
    strictest,
)

__all__ = [
    "MAX_ATTACHMENTS",
    "MAX_ATTACHMENT_BYTES",
    "MAX_INPUT_CHARS",
    "MAX_OUTPUT_CHARS",
    "Action",
    "AllowAllPolicy",
    "BeforeModelResult",
    "BridgeConfig",
    "CachingPolicyBridge",
    "CitationReport",
    "DenyAllPolicy",
    "GuardrailConfig",
    "GuardrailFinding",
    "GuardrailManager",
    "GuardrailOutcome",
    "GuardrailStage",
    "InjectionReport",
    "InjectionSignal",
    "InputCheck",
    "InputLimits",
    "InputVerdict",
    "OutputCheck",
    "OutputConfig",
    "OutputVerdict",
    "PiiFinding",
    "PiiKind",
    "PiiPolicy",
    "PiiReport",
    "PolicyAuthority",
    "PolicyBridge",
    "PolicyDecision",
    "PolicyResult",
    "PolicyStage",
    "RedactAll",
    "RedactSecretsOnly",
    "RiskLevel",
    "SafetyAssessment",
    "SafetyClass",
    "StaticPolicyBridge",
    "allowed_or_denied",
    "analyse",
    "check_citations",
    "classify",
    "contains_secrets",
    "enforce",
    "enforce_input",
    "enforce_output",
    "exceeds",
    "extract_citations",
    "gate_output",
    "has_unknown_citations",
    "is_repeatable",
    "is_suspicious",
    "ordinal_of",
    "parse_safety",
    "redact",
    "require_allowed",
    "requires_approval",
    "scan",
    "strictest",
    "strip_citations",
    "validate_bridge_config",
    "validate_input",
    "validate_output",
]
