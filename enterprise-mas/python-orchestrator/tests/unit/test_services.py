"""Tests for the service layer.

The safety-critical assertions are about *authority*. A service must take
tenancy from the execution context and never from a caller argument, and it must
re-check permission at its own boundary rather than trusting that an earlier
layer already checked.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging

import pytest

from mas_orchestrator.agents.base import AgentKind, AgentSpec, AgentStatus
from mas_orchestrator.agents.registry import AgentRegistry
from mas_orchestrator.context.execution_context import ExecutionContext
from mas_orchestrator.contracts.errors import (
    AgentNotFoundError,
    InvalidRequestError,
    PolicyDeniedError,
    ToolPermissionDeniedError,
)
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.guardrails.policy import (
    AllowAllPolicy,
    DenyAllPolicy,
    PolicyDecision,
    PolicyStage,
)
from mas_orchestrator.models.cost import CostLedger, PricingTable
from mas_orchestrator.security.identity import ServiceIdentity
from mas_orchestrator.services.agent_service import AgentService, classify_status
from mas_orchestrator.services.policy_service import PolicyService, PolicyVerdict
from mas_orchestrator.services.usage_service import UsageService

TENANT = "tenant-alpha"
OTHER = "tenant-beta"


def _identity() -> ServiceIdentity:
    return ServiceIdentity(subject="rust-core", issuer="static")


def _context(agent_id: str = "worker", tenant_id: str = TENANT) -> ExecutionContext:
    return ExecutionContext(
        tenant_id=tenant_id,
        organization_id="org-1",
        execution_id="exec-1",
        agent_id=agent_id,
        correlation_id="corr-1",
        request_id="req-1",
        protocol_version=CURRENT_PROTOCOL_VERSION,
        caller=_identity(),
    )


def _spec(
    agent_id: str = "worker",
    *,
    status: AgentStatus = AgentStatus.PUBLISHED,
    kind: AgentKind = AgentKind.STANDARD,
    tools: frozenset[str] = frozenset(),
) -> AgentSpec:
    return AgentSpec(
        agent_id=agent_id,
        agent_version_id="v1",
        name=agent_id,
        slug=agent_id,
        kind=kind,
        tenant_id=TENANT,
        status=status,
        allowed_tools=tools,
    )


class _Agent:
    def __init__(self, spec: AgentSpec) -> None:
        self.spec = spec

    async def run(self, state, context, *, cancel_event=None):
        raise AssertionError("not used in these tests")


class TestAgentService:
    def test_resolve_returns_a_registered_agent(self) -> None:
        service = AgentService(AgentRegistry())
        service.register(_spec(), lambda: _Agent(_spec()))
        assert service.resolve(_context()).agent_id == "worker"

    def test_resolve_takes_tenancy_from_the_context(self) -> None:
        # The whole point: another tenant's context must not find this agent.
        service = AgentService(AgentRegistry())
        service.register(_spec(), lambda: _Agent(_spec()))
        with pytest.raises(AgentNotFoundError):
            service.resolve(_context(tenant_id=OTHER))

    def test_resolve_unknown_agent_raises(self) -> None:
        with pytest.raises(AgentNotFoundError):
            AgentService(AgentRegistry()).resolve(_context("nope"))

    def test_resolve_refuses_a_non_runnable_agent(self) -> None:
        # Refused at resolve time rather than at the first model call, which
        # is the worst place to discover it.
        service = AgentService(AgentRegistry())
        service.register(_spec(status=AgentStatus.DISABLED), lambda: _Agent(_spec()))
        with pytest.raises(InvalidRequestError):
            service.resolve(_context())

    def test_duplicate_registration_refused(self) -> None:
        from mas_orchestrator.contracts.errors import ConfigurationError

        service = AgentService(AgentRegistry())
        service.register(_spec(), lambda: _Agent(_spec()))
        with pytest.raises(ConfigurationError):
            service.register(_spec(), lambda: _Agent(_spec()))

    def test_list_runnable_excludes_disabled(self) -> None:
        service = AgentService(AgentRegistry())
        service.register(_spec("a"), lambda: _Agent(_spec("a")))
        service.register(_spec("b", status=AgentStatus.DISABLED), lambda: _Agent(_spec("b")))
        assert [spec.agent_id for spec in service.list_runnable(TENANT)] == ["a"]

    def test_list_requires_a_tenant(self) -> None:
        with pytest.raises(InvalidRequestError):
            AgentService(AgentRegistry()).list_agents(" ")

    def test_summary_marks_runnable(self) -> None:
        from mas_orchestrator.services.agent_service import AgentSummary

        summary = AgentSummary(
            agent_id="worker",
            agent_version_id="v1",
            name="worker",
            kind=AgentKind.STANDARD,
            runnable=True,
            status="published",
            tool_count=0,
        )
        assert summary.runnable

    async def test_background_lookup_telemetry_failure_is_observed(self, caplog) -> None:
        class _FailingEmitter:
            async def emit(self, event: object) -> bool:
                raise InvalidRequestError("telemetry was rejected")

        service = AgentService(AgentRegistry(), emitter=_FailingEmitter())  # type: ignore[arg-type]
        service.register(_spec(), lambda: _Agent(_spec()))

        with caplog.at_level(
            logging.WARNING,
            logger="mas_orchestrator.services.agent_service",
        ):
            assert service.spec(_context()).agent_id == "worker"
            # The first yield runs the emission task; the second runs its
            # done callback that retrieves the exception.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        failures = [
            record
            for record in caplog.records
            if record.getMessage() == "agent lookup telemetry emission failed"
        ]
        assert len(failures) == 1
        assert failures[0].event == "agent.lookup_emit_failed"
        assert failures[0].error_type == "InvalidRequestError"


class TestClassifyStatus:
    def test_published_reports_its_own_status(self) -> None:
        assert classify_status(AgentStatus.PUBLISHED) == "published"

    def test_disabled_reports_its_own_status(self) -> None:
        assert classify_status(AgentStatus.DISABLED) == "disabled"


class TestPolicyService:
    async def test_default_policy_denies(self) -> None:
        # Default is deny, so nothing runs merely because nobody said no.
        verdict = await PolicyService(DenyAllPolicy()).ask(
            PolicyStage.BEFORE_MODEL, "tool.invoke", {"tenant_id": TENANT}
        )
        assert verdict.blocked
        assert not verdict.needs_approval

    async def test_ask_never_raises(self) -> None:
        verdict = await PolicyService(DenyAllPolicy()).ask(PolicyStage.BEFORE_MODEL, "x", {})
        assert isinstance(verdict, PolicyVerdict)

    async def test_require_raises_on_denial(self) -> None:
        with pytest.raises(PolicyDeniedError):
            await PolicyService(DenyAllPolicy()).require(PolicyStage.BEFORE_MODEL, "x", {})

    async def test_may_call_tool_denied_by_default(self) -> None:
        assert (await PolicyService(DenyAllPolicy()).may_call_tool("builtin.http", {})).blocked

    async def test_may_return_output_denied_by_default(self) -> None:
        assert (await PolicyService(DenyAllPolicy()).may_return_output("agent", {})).blocked

    async def test_allow_all_permits(self) -> None:
        bridge = AllowAllPolicy(environment="development")
        service = PolicyService(bridge)
        assert not (await service.may_call_tool("builtin.http", {})).blocked

    async def test_deny_all_bridge_denies(self) -> None:
        result = await DenyAllPolicy().check(PolicyStage.BEFORE_MODEL, "s", {})
        assert result.decision is PolicyDecision.DENY

    async def test_needs_approval_is_not_a_plain_denial(self) -> None:
        # "Not yet, a human can unblock" must be distinguishable from "never".
        verdict = PolicyVerdict(
            allowed=False,
            decision=PolicyDecision.REQUIRE_APPROVAL,
            subject="tool",
            needs_approval=True,
        )
        assert verdict.blocked
        # The distinction matters: "not yet, a human can unblock" must not be
        # reported the same way as "never".
        assert verdict.needs_approval
        assert not verdict.allowed


def _ledger() -> CostLedger:
    return CostLedger(PricingTable())


class TestUsageService:
    def test_report_without_ledger_reports_nothing_measured(self) -> None:
        # An unmeasured execution is not a zero-cost execution, so the report
        # carries no measured calls rather than a confident zero.
        report = UsageService().report("exec-1")
        assert report.total_cost == 0.0
        assert report.totals.call_count == 0

    def test_ledger_binding_refuses_rebind(self) -> None:
        service = UsageService()
        service.ledger_for("exec-1", _ledger())
        with pytest.raises(ValueError):
            service.ledger_for("exec-1", _ledger())

    def test_release_unbinds(self) -> None:
        service = UsageService()
        service.ledger_for("exec-1", _ledger())
        service.release("exec-1")
        assert service.report("exec-1").totals.call_count == 0

    def test_enforce_quota_treats_none_as_unenforced(self) -> None:
        # None means unenforced, not a ceiling of zero: a zero ceiling would
        # refuse every execution that recorded anything at all.
        service = UsageService()
        service.ledger_for("exec-1", _ledger())
        assert service.enforce_quota("exec-1") is None

    def test_enforce_quota_refuses_an_exceeded_budget(self) -> None:

        from mas_orchestrator.contracts.errors import QuotaExceededError
        from mas_orchestrator.models.types import ModelRef, Usage

        ledger = _ledger()
        ledger.record(
            ModelRef(provider="openai", model="gpt-4o"),
            Usage(input_tokens=10, output_tokens=10),
        )
        service = UsageService()
        service.ledger_for("exec-1", ledger)
        # Unpriced calls are never counted as free: tokens yes, money no. So
        # the token ceiling is the one that bites here.
        with pytest.raises(QuotaExceededError):
            service.enforce_quota("exec-1", max_tokens=1)

    def test_describe_is_nonempty(self) -> None:
        assert UsageService().report("exec-1").describe()


class TestToolServicePermission:
    async def test_invoke_rechecks_permission_at_the_boundary(self) -> None:
        # The point of the re-check: an earlier layer may have been bypassed,
        # and a tool invocation is the one thing that must never happen by
        # accident.
        from mas_orchestrator.services.tool_service import ToolService
        from mas_orchestrator.tools.executor import ToolExecutor
        from mas_orchestrator.tools.permissions import DenyAllPermissionBridge
        from mas_orchestrator.tools.registry import ToolRegistry

        service = ToolService(
            ToolRegistry(), ToolExecutor(ToolRegistry(), DenyAllPermissionBridge())
        )
        from mas_orchestrator.tools.types import ToolInvocationContext

        agent = _spec(tools=frozenset())
        with pytest.raises(ToolPermissionDeniedError):
            await service.invoke(
                agent=agent,
                tenant_id=TENANT,
                tool_id="builtin.http",
                arguments={},
                context=ToolInvocationContext(
                    tenant_id=TENANT,
                    execution_id="exec-1",
                    agent_id="worker",
                    invocation_id="inv-1",
                ),
            )


class TestTenancyIsNotCallerSupplied:
    def test_resolve_takes_no_tenant_argument(self) -> None:
        import inspect

        # Tenancy arrives via the context, so there is no argument to misuse.
        assert "tenant_id" not in inspect.signature(AgentService.resolve).parameters

    def test_context_agent_id_is_not_overridable(self) -> None:
        # The agent comes from the context too: a caller must not be able to
        # name an arbitrary agent for an execution Rust scoped elsewhere.
        context = _context("worker")
        assert context.agent_id == "worker"
        with pytest.raises(dataclasses.FrozenInstanceError):
            context.agent_id = "other"  # type: ignore[misc]
