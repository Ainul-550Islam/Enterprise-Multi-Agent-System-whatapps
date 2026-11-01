"""Read-only platform assistant with validated structured responses.

This product-owned agent has no data tools, never creates or publishes an
agent, and can only reason over the tenant-scoped snapshot Rust attaches to
the execution input. It provides four dashboard quick actions and returns a
validated ``AgentDraftProposal`` for Create New Agent rather than performing
any write itself.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.agents.base import AgentOutcome, AgentSpec, AgentState, Termination
from mas_orchestrator.contracts.errors import InvalidRequestError, OutputValidationFailedError
from mas_orchestrator.models.types import Message, Role
from mas_orchestrator.prompts.builtin_assistant import (
    PLATFORM_ASSISTANT_PROMPT,
    PLATFORM_ASSISTANT_VERSION,
    register_builtin_assistant_prompt,
)
from mas_orchestrator.prompts.registry import PromptRegistry
from mas_orchestrator.prompts.renderer import render, trusted

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Iterator

    from mas_orchestrator.agents.executor import AgentExecutor
    from mas_orchestrator.context import ExecutionContext


PLATFORM_ASSISTANT_AGENT_ID: Final[str] = "platform_assistant"
MAX_ASSISTANT_OUTPUT_BYTES: Final[int] = 32 * 1024
MAX_EVIDENCE_ITEMS: Final[int] = 50
MAX_EVIDENCE_TEXT_CHARS: Final[int] = 2_000
MAX_SNAPSHOT_NODES: Final[int] = 100_000
MAX_SNAPSHOT_METRIC_REFS: Final[int] = 10_000
_SLUG: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_NUMBER: Final[re.Pattern[str]] = re.compile(
    r"(?<![\w.])[-+]?(?:\d{1,3}(?:,\d{3})+|\d+|\.\d+)"
    r"(?:\.\d+)?(?:[eE][-+]?\d+)?%?(?![\d.])"
)
_NUMERIC_ID_KEYS: Final[frozenset[str]] = frozenset(
    {"id", "agent_id", "document_id", "execution_id", "metric_ref", "metric_refs", "request_id"}
)


class AssistantQuickAction(StrEnum):
    """Supported read-only dashboard actions."""

    PERFORMANCE_INSIGHTS = "performance_insights"
    OPTIMIZE_WORKFLOWS = "optimize_workflows"
    CREATE_NEW_AGENT = "create_new_agent"
    VIEW_RECOMMENDATIONS = "view_recommendations"


@dataclass(frozen=True, slots=True)
class AgentDraftProposal:
    """A validated suggestion for Rust's separate agent-creation workflow."""

    name: str
    slug: str
    description: str
    objective: str
    system_prompt: str
    kind: str = "standard"
    model_hint: str | None = None
    suggested_tools: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not 3 <= len(self.name.strip()) <= 80:
            raise InvalidRequestError("draft agent name must be between 3 and 80 characters")
        if not _SLUG.fullmatch(self.slug):
            raise InvalidRequestError("draft agent slug must be lowercase kebab-case")
        for field_name, value, maximum in (
            ("description", self.description, 2_000),
            ("objective", self.objective, 2_000),
            ("system_prompt", self.system_prompt, 8_000),
        ):
            if not value.strip() or len(value) > maximum:
                raise InvalidRequestError(
                    "draft agent proposal field is empty or exceeds its bound",
                    details={"field": field_name, "maximum": maximum},
                )
        if self.kind != "standard":
            raise InvalidRequestError(
                "platform assistant may propose only a standard no-tool agent"
            )
        if self.model_hint is not None and len(self.model_hint) > 128:
            raise InvalidRequestError("draft agent model hint exceeds its bound")
        if self.suggested_tools:
            raise InvalidRequestError(
                "platform assistant cannot grant or recommend data-tool capabilities",
                details={"field": "suggested_tools"},
            )

    @classmethod
    def from_mapping(cls, value: object) -> AgentDraftProposal:
        """Validates untrusted model output as a new-agent proposal."""
        if not isinstance(value, dict):
            raise InvalidRequestError("agent draft proposal must be a JSON object")
        allowed = {
            "name",
            "slug",
            "description",
            "objective",
            "system_prompt",
            "kind",
            "model_hint",
            "suggested_tools",
        }
        if set(value) - allowed:
            raise InvalidRequestError("agent draft proposal contains unknown fields")
        strings = ("name", "slug", "description", "objective", "system_prompt")
        if any(not isinstance(value.get(key), str) for key in strings):
            raise InvalidRequestError("agent draft proposal is missing required text fields")
        kind = value.get("kind", "standard")
        model_hint = value.get("model_hint")
        tools = value.get("suggested_tools", [])
        if not isinstance(kind, str) or (
            model_hint is not None and not isinstance(model_hint, str)
        ):
            raise InvalidRequestError("agent draft proposal has invalid kind/model fields")
        if not isinstance(tools, list) or any(not isinstance(item, str) for item in tools):
            raise InvalidRequestError("agent draft proposal suggested_tools must be an array")
        return cls(
            name=value["name"],
            slug=value["slug"],
            description=value["description"],
            objective=value["objective"],
            system_prompt=value["system_prompt"],
            kind=kind,
            model_hint=model_hint,
            suggested_tools=tuple(tools),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Returns the stable proposal JSON shape used by the dashboard."""
        return {
            "name": self.name,
            "slug": self.slug,
            "description": self.description,
            "objective": self.objective,
            "system_prompt": self.system_prompt,
            "kind": self.kind,
            "model_hint": self.model_hint,
            "suggested_tools": list(self.suggested_tools),
        }


@dataclass(frozen=True, slots=True)
class AssistantEvidence:
    """One claim linked to a metric reference supplied in the tenant snapshot."""

    metric_ref: str
    claim: str

    @classmethod
    def from_mapping(cls, value: object, allowed_refs: frozenset[str]) -> AssistantEvidence:
        """Validates one evidence reference against Rust-attached snapshot refs."""
        if not isinstance(value, dict):
            raise InvalidRequestError("assistant evidence must be a JSON object")
        if set(value) != {"metric_ref", "claim"}:
            raise InvalidRequestError("assistant evidence must contain only metric_ref and claim")
        metric_ref = value.get("metric_ref")
        claim = value.get("claim")
        if not isinstance(metric_ref, str) or metric_ref not in allowed_refs:
            raise InvalidRequestError(
                "assistant evidence cites a metric reference absent from the supplied snapshot",
                details={"field": "evidence.metric_ref"},
            )
        if not isinstance(claim, str) or not claim.strip() or len(claim) > MAX_EVIDENCE_TEXT_CHARS:
            raise InvalidRequestError("assistant evidence claim is empty or too long")
        return cls(metric_ref=metric_ref, claim=claim.strip())

    def to_mapping(self) -> dict[str, str]:
        """Returns the stable evidence JSON shape."""
        return {"metric_ref": self.metric_ref, "claim": self.claim}


@dataclass(frozen=True, slots=True)
class PlatformAssistantResponse:
    """Structured, bounded response for the four dashboard quick actions."""

    action: AssistantQuickAction
    summary: str
    evidence: tuple[AssistantEvidence, ...] = ()
    proposal: AgentDraftProposal | None = None

    def __post_init__(self) -> None:
        if not self.summary.strip() or len(self.summary) > 8_000:
            raise InvalidRequestError("assistant summary is empty or exceeds 8000 characters")
        if len(self.evidence) > MAX_EVIDENCE_ITEMS:
            raise InvalidRequestError(
                "assistant evidence count exceeds its bound",
                details={"limit": MAX_EVIDENCE_ITEMS},
            )
        if self.action is AssistantQuickAction.CREATE_NEW_AGENT and self.proposal is None:
            raise InvalidRequestError("Create New Agent response requires a draft proposal")
        if self.action is not AssistantQuickAction.CREATE_NEW_AGENT and self.proposal is not None:
            raise InvalidRequestError("only Create New Agent may return a draft proposal")

    def to_json(self) -> str:
        """Serializes the validated result without custom object hooks."""
        encoded = json.dumps(
            {
                "action": self.action.value,
                "summary": self.summary,
                "evidence": [item.to_mapping() for item in self.evidence],
                "proposal": self.proposal.to_mapping() if self.proposal is not None else None,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        try:
            encoded_size = len(encoded.encode("utf-8"))
        except UnicodeEncodeError as error:
            raise InvalidRequestError("assistant response contains invalid Unicode") from error
        if encoded_size > MAX_ASSISTANT_OUTPUT_BYTES:
            raise InvalidRequestError("assistant structured output exceeds its byte limit")
        return encoded

    @classmethod
    def parse(
        cls,
        raw: str,
        *,
        expected_action: AssistantQuickAction,
        allowed_metric_refs: frozenset[str],
    ) -> PlatformAssistantResponse:
        """Parses, schema-checks, and validates one model-generated response."""
        try:
            raw_size = len(raw.encode("utf-8"))
        except UnicodeEncodeError as error:
            raise InvalidRequestError(
                "platform assistant output contains invalid Unicode"
            ) from error
        if raw_size > MAX_ASSISTANT_OUTPUT_BYTES:
            raise InvalidRequestError("assistant structured output exceeds its byte limit")
        try:
            payload: object = json.loads(
                raw,
                object_pairs_hook=_unique_json_object,
                parse_constant=_reject_json_constant,
            )
        except (json.JSONDecodeError, ValueError, RecursionError) as error:
            raise InvalidRequestError("platform assistant did not return valid JSON") from error
        if not isinstance(payload, dict):
            raise InvalidRequestError("platform assistant output must be a JSON object")
        allowed = {"action", "summary", "evidence", "proposal"}
        if set(payload) - allowed:
            raise InvalidRequestError("platform assistant output has unknown fields")
        raw_action = payload.get("action")
        if not isinstance(raw_action, str):
            raise InvalidRequestError("platform assistant action must be text")
        try:
            action = AssistantQuickAction(raw_action)
        except ValueError as error:
            raise InvalidRequestError(
                "platform assistant returned an unknown quick action"
            ) from error
        if action is not expected_action:
            raise InvalidRequestError("platform assistant changed the requested quick action")
        summary = payload.get("summary")
        if not isinstance(summary, str):
            raise InvalidRequestError("platform assistant summary must be text")
        raw_evidence = payload.get("evidence", [])
        if not isinstance(raw_evidence, list) or len(raw_evidence) > MAX_EVIDENCE_ITEMS:
            raise InvalidRequestError("platform assistant evidence must be a bounded array")
        evidence = tuple(
            AssistantEvidence.from_mapping(item, allowed_metric_refs) for item in raw_evidence
        )
        raw_proposal = payload.get("proposal")
        proposal = (
            AgentDraftProposal.from_mapping(raw_proposal) if raw_proposal is not None else None
        )
        return cls(action=action, summary=summary.strip(), evidence=evidence, proposal=proposal)


class PlatformAssistantAgent:
    """Agent implementation conforming to ``BaseAgent`` with no data tools."""

    def __init__(
        self,
        spec: AgentSpec,
        executor: AgentExecutor,
        *,
        prompts: PromptRegistry | None = None,
    ) -> None:
        if spec.agent_id != PLATFORM_ASSISTANT_AGENT_ID:
            raise InvalidRequestError("PlatformAssistantAgent received the wrong spec")
        if spec.allowed_tools or spec.kind.value != "standard" or not spec.read_only:
            raise InvalidRequestError(
                "platform assistant must be a read-only standard no-tool agent"
            )
        prompt_registry = prompts if prompts is not None else PromptRegistry()
        if prompt_registry.current_version(PLATFORM_ASSISTANT_PROMPT) is None:
            register_builtin_assistant_prompt(prompt_registry)
        # Pin both the system prompt and action templates to v1. Future prompt
        # changes require an explicit code/version update, never a silent lookup
        # of whichever wording happens to be newest.
        prompt_registry.get(PLATFORM_ASSISTANT_PROMPT, version=PLATFORM_ASSISTANT_VERSION)
        for action in AssistantQuickAction:
            prompt_registry.get(
                f"{PLATFORM_ASSISTANT_PROMPT}.{action.value}",
                version=PLATFORM_ASSISTANT_VERSION,
            )
        self._spec = spec
        self._executor = executor
        self._prompts = prompt_registry

    @property
    def spec(self) -> AgentSpec:
        """The read-only product-owned agent definition."""
        return self._spec

    async def run(
        self,
        state: AgentState,
        context: ExecutionContext,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> AgentOutcome:
        """Runs the model over the Rust-supplied tenant snapshot and validates JSON."""
        user_messages = tuple(
            message for message in state.messages if getattr(message, "role", None) is Role.USER
        )
        if not user_messages:
            return _failed_outcome(
                self._spec,
                OutputValidationFailedError("platform assistant request has no user snapshot"),
            )

        try:
            action = _requested_action(state, context)
        except InvalidRequestError as error:
            return _failed_outcome(self._spec, OutputValidationFailedError(str(error)))

        system_template = self._prompts.get(
            PLATFORM_ASSISTANT_PROMPT,
            version=PLATFORM_ASSISTANT_VERSION,
        )
        action_template = self._prompts.get(
            f"{PLATFORM_ASSISTANT_PROMPT}.{action.value}",
            version=PLATFORM_ASSISTANT_VERSION,
        )
        system_prompt = render(
            system_template.template,
            {
                "quick_action": trusted(action.value),
                "action_instructions": trusted(action_template.template),
            },
            allowed=system_template.variables,
        )
        non_system_messages = tuple(
            message
            for message in state.messages
            if getattr(message, "role", None) is not Role.SYSTEM
        )
        prepared_state = state.with_messages((Message.system(system_prompt), *non_system_messages))
        outcome = await self._executor.run(prepared_state, context, cancel_event=cancel_event)
        if not outcome.succeeded:
            return outcome
        if outcome.tool_calls_made != 0:
            return _failed_outcome(
                self._spec,
                OutputValidationFailedError("platform assistant attempted to invoke a tool"),
                prior=outcome,
            )
        try:
            response = PlatformAssistantResponse.parse(
                outcome.content,
                expected_action=action,
                allowed_metric_refs=_metric_refs(state, context),
            )
            _validate_snapshot_numbers(response, _snapshot_numbers(state))
            structured = response.to_json()
        except InvalidRequestError as error:
            return _failed_outcome(
                self._spec,
                OutputValidationFailedError(
                    "platform assistant output failed schema/provenance validation",
                    details={"reason": str(error)},
                ),
                prior=outcome,
            )
        return replace(outcome, content=structured)


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Rejects duplicate keys rather than silently trusting the last value."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    """Rejects JavaScript-style NaN and Infinity tokens in JSON output."""
    raise ValueError(f"invalid JSON constant: {value}")


def _requested_action(state: AgentState, context: ExecutionContext) -> AssistantQuickAction:
    """Resolves a quick action from Rust attributes or explicit user input."""
    raw = (context.attributes or {}).get("assistant_action")
    if raw is not None:
        if not isinstance(raw, str):
            raise InvalidRequestError("assistant action attribute must be text")
        try:
            return AssistantQuickAction(raw)
        except ValueError as error:
            raise InvalidRequestError("assistant action attribute is unsupported") from error

    for message in reversed(state.messages):
        if getattr(message, "role", None) is not Role.USER:
            continue
        try:
            payload: object = json.loads(message.content)
        except (json.JSONDecodeError, RecursionError):
            payload = None
        if isinstance(payload, dict):
            candidate = payload.get("quick_action") or payload.get("action")
            if candidate is not None:
                if not isinstance(candidate, str):
                    raise InvalidRequestError("assistant quick action must be text")
                try:
                    return AssistantQuickAction(candidate)
                except ValueError as error:
                    raise InvalidRequestError("assistant quick action is unsupported") from error
        normalized = message.content.lower().replace("-", " ").replace("_", " ")
        for action, phrases in (
            (
                AssistantQuickAction.PERFORMANCE_INSIGHTS,
                ("performance insights", "performance insight"),
            ),
            (
                AssistantQuickAction.OPTIMIZE_WORKFLOWS,
                ("optimize workflows", "optimize workflow"),
            ),
            (
                AssistantQuickAction.CREATE_NEW_AGENT,
                ("create new agent", "new agent proposal"),
            ),
            (
                AssistantQuickAction.VIEW_RECOMMENDATIONS,
                ("view recommendations", "recommendations"),
            ),
        ):
            if any(phrase in normalized for phrase in phrases):
                return action
        break
    return AssistantQuickAction.VIEW_RECOMMENDATIONS


def _snapshot_roots(state: AgentState) -> tuple[object, ...]:
    """Returns bounded JSON snapshot roots carried by user messages."""
    roots: list[object] = []
    for message in state.messages[-100:]:
        if getattr(message, "role", None) is not Role.USER:
            continue
        try:
            payload: object = json.loads(message.content)
        except (json.JSONDecodeError, RecursionError):
            continue
        if isinstance(payload, dict) and "snapshot" in payload:
            roots.append(payload["snapshot"])
        elif isinstance(payload, dict) and "tenant_snapshot" in payload:
            roots.append(payload["tenant_snapshot"])
        else:
            # The full user JSON is itself the snapshot when no wrapper key is
            # present (the canonical bridge currently transports input_json).
            roots.append(payload)
    return tuple(roots)


def _walk_snapshot(value: object) -> Iterator[tuple[object, str | None]]:
    """Walks one JSON-compatible snapshot under a strict node ceiling."""
    stack: list[tuple[object, str | None]] = [(value, None)]
    visited = 0
    while stack and visited < MAX_SNAPSHOT_NODES:
        current, parent_key = stack.pop()
        visited += 1
        yield current, parent_key
        if isinstance(current, dict):
            children = list(current.items())[:MAX_SNAPSHOT_NODES]
            stack.extend((child, str(key)) for key, child in children)
        elif isinstance(current, list):
            stack.extend((child, parent_key) for child in current[:MAX_SNAPSHOT_NODES])


def _metric_refs(state: AgentState, context: ExecutionContext) -> frozenset[str]:
    """Collects bounded metric references from Rust metadata and input snapshots."""
    attributes = context.attributes or {}
    refs: set[str] = set()

    def add_ref(value: object) -> None:
        if (
            isinstance(value, str)
            and value.strip()
            and (value.strip() in refs or len(refs) < MAX_SNAPSHOT_METRIC_REFS)
        ):
            refs.add(value.strip())

    raw_refs = attributes.get("metric_refs")
    if isinstance(raw_refs, (list, tuple, set, frozenset)):
        for value in raw_refs:
            add_ref(value)
            if len(refs) >= MAX_SNAPSHOT_METRIC_REFS:
                break

    for root in _snapshot_roots(state):
        for value, _ in _walk_snapshot(root):
            if isinstance(value, dict):
                add_ref(value.get("metric_ref"))
                raw_nested_refs = value.get("metric_refs")
                if isinstance(raw_nested_refs, list):
                    for item in raw_nested_refs:
                        add_ref(item)
                        if len(refs) >= MAX_SNAPSHOT_METRIC_REFS:
                            break
            if len(refs) >= MAX_SNAPSHOT_METRIC_REFS:
                break
        if len(refs) >= MAX_SNAPSHOT_METRIC_REFS:
            break
    return frozenset(refs)


def _decimal_value(value: object) -> Decimal | None:
    """Converts one finite JSON numeric value to Decimal without float drift."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    candidate = (
        value.strip().removesuffix("%").replace(",", "") if isinstance(value, str) else str(value)
    )
    try:
        parsed = Decimal(candidate)
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _number_tokens(text: str) -> frozenset[Decimal]:
    """Finds numeric claims in prose and normalizes them for exact comparison."""
    numbers: set[Decimal] = set()
    for match in _NUMBER.finditer(text):
        value = _decimal_value(match.group(0))
        if value is not None:
            numbers.add(value)
    return frozenset(numbers)


def _snapshot_numbers(state: AgentState) -> frozenset[Decimal]:
    """Collects numbers from supplied snapshots, excluding identifier fields."""
    numbers: set[Decimal] = set()
    for root in _snapshot_roots(state):
        for value, parent_key in _walk_snapshot(root):
            if parent_key is not None and parent_key.lower() in _NUMERIC_ID_KEYS:
                continue
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                number = _decimal_value(value)
                if number is not None:
                    numbers.add(number)
            elif isinstance(value, str):
                numbers.update(_number_tokens(value))
    return frozenset(numbers)


def _validate_snapshot_numbers(
    response: PlatformAssistantResponse, allowed: frozenset[Decimal]
) -> None:
    """Refuses numeric claims not present in the supplied tenant snapshot."""
    statements = [response.summary, *(item.claim for item in response.evidence)]
    unsupported = set().union(*(_number_tokens(statement) for statement in statements)) - allowed
    if unsupported:
        raise InvalidRequestError(
            "assistant output contains numeric claims absent from the supplied snapshot",
            details={"unsupported_number_count": len(unsupported)},
        )


def _failed_outcome(
    spec: AgentSpec,
    error: BaseException,
    *,
    prior: AgentOutcome | None = None,
) -> AgentOutcome:
    """Creates a typed failed outcome without discarding prior model usage."""
    return AgentOutcome(
        agent_id=spec.agent_id,
        termination=Termination.FAILED,
        messages=prior.messages if prior is not None else (),
        steps_taken=prior.steps_taken if prior is not None else 0,
        tool_calls_made=prior.tool_calls_made if prior is not None else 0,
        cost=prior.cost if prior is not None else None,
        error=error,
        duration_ms=prior.duration_ms if prior is not None else 0,
    )
