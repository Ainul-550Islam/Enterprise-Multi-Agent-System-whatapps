# PURPOSE:
# Tests the structured error mapping the bridge reports to Rust.
#
# Covers spec §12: every failure class must carry a stable machine-readable
# code, a retryable/terminal classification, a safe message, correlation and
# request identity — and must never expose a secret, key, token, or sensitive
# prompt.

"""Error-mapping tests for the bridge.

The contract under test is the ``AgentFailed`` verdict: Rust branches on
``code``, decides whether to retry from ``retryable``, and joins the failure to
its own logs via ``correlation_id`` and ``request_id``. If any of those is
missing or wrong the platform cannot operate the run, so each is asserted
separately rather than as one blob.

The second half of this file is the security contract. A failure report is the
most likely place for a secret to escape, because it is built at the exact
moment something went wrong — often while holding a credential. These tests put
a realistic-looking key, token and prompt through the failure path and assert
that none of them appears in anything the caller can see.
"""

from __future__ import annotations

import pytest
from tests.bridge.conftest import (
    RecordingExecutionService,
    failed_result,
    metadata,
    request,
    resolver,
    success_result,
)

from mas_orchestrator.agents.base import Termination
from mas_orchestrator.contracts.errors import (
    AgentNotFoundError,
    AuthenticationError,
    AuthorizationError,
    CancellationRequestedError,
    ConfigurationError,
    DependencyUnavailableError,
    DuplicateRequestError,
    ErrorCode,
    ExecutionTimeoutError,
    InternalError,
    InvalidRequestError,
    MemoryBackendError,
    ModelNotAvailableError,
    ModelProviderError,
    ModelRateLimitedError,
    ModelTimeoutError,
    OrchestratorError,
    OutputValidationFailedError,
    PiiPolicyDeniedError,
    PolicyDeniedError,
    PromptInjectionDetectedError,
    PromptRenderError,
    ProtocolError,
    QuotaExceededError,
    RetrievalError,
    TenantContextError,
    ToolFailedError,
    ToolNotFoundError,
    ToolPermissionDeniedError,
    ToolTimeoutError,
)
from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
    finish_reason_for,
    model_key_from_result,
)
from mas_orchestrator.grpc.errors import CODE_TO_STATUS

pytestmark = pytest.mark.asyncio(loop_scope="function")

SECRET = "sk-live-AAAAAAAAAAAAAAAAAAAAAAAA"
BEARER = "Bearer eyJhbGciOiJIUzI1NiJ9.SECRET-PAYLOAD"
SENSITIVE_PROMPT = "My card is 4111 1111 1111 1111 and the code is 123"


def _servicer(service: RecordingExecutionService) -> AgentBridgeServicer:
    """Builds a servicer over a double.

    Args:
        service: The execution double.

    Returns:
        The servicer.
    """
    return AgentBridgeServicer(
        execution_service=service,  # type: ignore[arg-type]
        identity_resolver=resolver(),
        config=AgentBridgeServicerConfig(),
    )


async def _reply_for(service: RecordingExecutionService):
    """Drives one run and returns the reply the servicer produced.

    Args:
        service: The execution double, preconfigured with a result or error.

    Returns:
        The ``AgentRunReply``.
    """
    servicer = _servicer(service)
    from tests.bridge.conftest import FakeContext

    context = FakeContext(metadata())
    return await servicer.RunAgent(request(), context)  # type: ignore[arg-type]


class TestEveryFailureClassIsMapped:
    """Each named failure class produces a stable, classified code."""

    async def test_all_error_codes_have_a_grpc_status(self) -> None:
        """No code falls through to an unmapped default."""
        unmapped = [code for code in ErrorCode if code not in CODE_TO_STATUS]
        assert unmapped == []

    async def test_the_platform_defines_the_expected_failure_classes(self) -> None:
        """The fifteen named families from spec §12 all exist."""
        families = {
            ErrorCode.AUTHENTICATION_ERROR,
            ErrorCode.AUTHORIZATION_ERROR,
            ErrorCode.TENANT_CONTEXT_ERROR,
            ErrorCode.POLICY_DENIED,
            ErrorCode.PROTOCOL_ERROR,
            ErrorCode.INVALID_REQUEST,
            ErrorCode.AGENT_NOT_FOUND,
            ErrorCode.MODEL_NOT_AVAILABLE,
            ErrorCode.MODEL_RATE_LIMITED,
            ErrorCode.MODEL_TIMEOUT,
            ErrorCode.MODEL_PROVIDER_ERROR,
            ErrorCode.TOOL_FAILED,
            ErrorCode.TOOL_TIMEOUT,
            ErrorCode.EXECUTION_TIMEOUT,
            ErrorCode.CANCELLATION_REQUESTED,
        }
        missing = families - set(ErrorCode)
        assert missing == set()

    async def test_a_typed_error_reports_its_own_code(self) -> None:
        """The code is the typed one, not the termination reason."""
        service = RecordingExecutionService(
            result=failed_result(error=ModelTimeoutError("the provider timed out"))
        )

        reply = await _reply_for(service)

        assert reply.failed.code == "model_timeout"

    async def test_a_typed_error_reports_its_retryable_flag(self) -> None:
        """A provider timeout is retryable; the caller may try again."""
        service = RecordingExecutionService(
            result=failed_result(error=ModelTimeoutError("timed out"))
        )

        reply = await _reply_for(service)

        assert reply.failed.retryable is True

    async def test_a_terminal_error_is_not_retryable(self) -> None:
        """A rejected prompt is terminal: retrying reproduces the rejection."""
        service = RecordingExecutionService(
            result=failed_result(
                error=PromptInjectionDetectedError("instruction override detected")
            )
        )

        reply = await _reply_for(service)

        assert reply.failed.retryable is False

    async def test_an_untyped_failure_falls_back_to_the_termination(self) -> None:
        """A stop with no error object still names why it stopped."""
        service = RecordingExecutionService(result=failed_result(Termination.STEP_LIMIT))

        reply = await _reply_for(service)

        assert reply.failed.code == "step_limit"
        assert reply.failed.retryable is False

    @pytest.mark.parametrize(
        ("error", "expected_code"),
        [
            (AuthenticationError("no credential"), "authentication_error"),
            (AuthorizationError("not permitted"), "authorization_error"),
            (TenantContextError("tenant mismatch"), "tenant_context_error"),
            (PolicyDeniedError("policy refused"), "policy_denied"),
            (ProtocolError("bad version"), "protocol_error"),
            (InvalidRequestError("malformed"), "invalid_request"),
            (DuplicateRequestError("already seen"), "duplicate_request"),
            (AgentNotFoundError("no such agent"), "agent_not_found"),
            (ModelNotAvailableError("no model"), "model_not_available"),
            (ModelRateLimitedError("slow down"), "model_rate_limited"),
            (ModelTimeoutError("too slow"), "model_timeout"),
            (ModelProviderError("upstream failed"), "model_provider_error"),
            (ToolNotFoundError("no such tool"), "tool_not_found"),
            (ToolPermissionDeniedError("tool refused"), "tool_permission_denied"),
            (ToolTimeoutError("tool too slow"), "tool_timeout"),
            (ToolFailedError("tool broke"), "tool_failed"),
            (PromptRenderError("template failed"), "prompt_render_error"),
            (PromptInjectionDetectedError("injection"), "prompt_injection_detected"),
            (OutputValidationFailedError("bad output"), "output_validation_failed"),
            (PiiPolicyDeniedError("pii found"), "pii_policy_denied"),
            (MemoryBackendError("memory down"), "memory_error"),
            (RetrievalError("retrieval failed"), "retrieval_error"),
            (QuotaExceededError("over quota"), "quota_exceeded"),
            (CancellationRequestedError("cancelled"), "cancellation_requested"),
            (ExecutionTimeoutError("expired"), "execution_timeout"),
            (ConfigurationError("misconfigured"), "configuration_error"),
            (DependencyUnavailableError("down"), "dependency_unavailable"),
            (InternalError("boom"), "internal_error"),
        ],
    )
    async def test_each_class_maps_to_its_stable_code(
        self, error: OrchestratorError, expected_code: str
    ) -> None:
        """The code Rust sees is the domain code, for every class."""
        service = RecordingExecutionService(result=failed_result(error=error))

        reply = await _reply_for(service)

        assert reply.failed.code == expected_code

    @pytest.mark.parametrize(
        "error",
        [
            ModelTimeoutError("t"),
            ModelRateLimitedError("r"),
            ModelProviderError("p"),
            ModelNotAvailableError("n"),
            DependencyUnavailableError("d"),
            ToolTimeoutError("tt"),
            RetrievalError("rt"),
            MemoryBackendError("m"),
            InternalError("i"),
        ],
    )
    async def test_transient_classes_are_retryable(self, error: OrchestratorError) -> None:
        """A transient failure may succeed on a later attempt."""
        service = RecordingExecutionService(result=failed_result(error=error))

        reply = await _reply_for(service)

        assert reply.failed.retryable is True

    @pytest.mark.parametrize(
        "error",
        [
            AuthenticationError("a"),
            AuthorizationError("z"),
            TenantContextError("t"),
            InvalidRequestError("i"),
            PromptInjectionDetectedError("p"),
            PiiPolicyDeniedError("pii"),
            PolicyDeniedError("pd"),
            AgentNotFoundError("anf"),
            ToolPermissionDeniedError("tpd"),
            QuotaExceededError("q"),
        ],
    )
    async def test_rejection_classes_are_terminal(self, error: OrchestratorError) -> None:
        """A rejection would reproduce identically; retrying only delays it."""
        service = RecordingExecutionService(result=failed_result(error=error))

        reply = await _reply_for(service)

        assert reply.failed.retryable is False


class TestFailureIdentity:
    """A failure carries the identity Rust needs to join its own logs."""

    async def test_a_failure_reports_the_correlation_id(self) -> None:
        """Correlation joins the Rust log line to the AI failure."""
        service = RecordingExecutionService(
            result=failed_result(error=ModelTimeoutError("timed out"))
        )
        servicer = _servicer(service)
        from tests.bridge.conftest import FakeContext

        reply = await servicer.RunAgent(request(correlation_id="corr-99"), FakeContext(metadata()))  # type: ignore[arg-type]

        assert reply.failed.correlation_id == "corr-99"

    async def test_a_failure_reports_the_request_id(self) -> None:
        """The platform request id travels alongside the correlation id."""
        service = RecordingExecutionService(
            result=failed_result(error=ModelTimeoutError("timed out"))
        )
        servicer = _servicer(service)
        from tests.bridge.conftest import FakeContext

        reply = await servicer.RunAgent(
            request(correlation_id="corr-99", request_id="req-77"), FakeContext(metadata())
        )  # type: ignore[arg-type]

        assert reply.failed.request_id == "req-77"

    async def test_a_raised_typed_error_becomes_a_verdict(self) -> None:
        """An error raised by the runtime is a verdict, not a transport failure.

        The run was accepted and then failed. Reporting that as an aborted RPC
        would bypass Rust's verdict handling and make a business failure look
        like a broken bridge.
        """
        service = RecordingExecutionService(error=ModelTimeoutError("timed out"))

        reply = await _reply_for(service)

        assert reply.WhichOneof("verdict") == "failed"
        assert reply.failed.code == "model_timeout"


class TestSecretsAreNeverExposed:
    """A failure report must not leak credentials or sensitive prompts."""

    async def test_an_api_key_in_the_error_never_reaches_the_message(self) -> None:
        """A key held when the run failed must not be reported."""
        service = RecordingExecutionService(
            result=failed_result(error=ModelProviderError(f"call with {SECRET} was refused"))
        )

        reply = await _reply_for(service)

        assert SECRET not in reply.failed.message

    async def test_a_bearer_token_in_the_error_never_reaches_the_message(self) -> None:
        """A token in the failure text must not be echoed."""
        service = RecordingExecutionService(
            result=failed_result(error=AuthenticationError(f"rejected {BEARER}"))
        )

        reply = await _reply_for(service)

        assert "SECRET-PAYLOAD" not in reply.failed.message

    async def test_a_sensitive_prompt_is_not_reported_in_a_failure(self) -> None:
        """The prompt that failed must not be quoted back to the caller."""
        service = RecordingExecutionService(
            result=failed_result(error=PromptInjectionDetectedError(SENSITIVE_PROMPT))
        )

        reply = await _reply_for(service)

        assert "4111 1111 1111 1111" not in reply.failed.message

    async def test_the_failure_code_is_machine_readable_not_prose(self) -> None:
        """The code is a stable token, so Rust can branch on it."""
        service = RecordingExecutionService(
            result=failed_result(error=ModelTimeoutError("timed out"))
        )

        reply = await _reply_for(service)

        assert reply.failed.code.isidentifier() or "_" in reply.failed.code
        assert " " not in reply.failed.code

    async def test_the_servicer_never_renders_its_resolver_secret(self) -> None:
        """The resolver's repr is safe to log, by construction."""
        from tests.bridge.conftest import CREDENTIAL

        rendered = repr(resolver())
        assert CREDENTIAL not in rendered
        assert "<redacted>" in rendered or "token=" not in rendered


class TestSuccessfulReplies:
    """A success carries output, usage and a finish state."""

    async def test_a_success_reports_the_finish_reason(self) -> None:
        """A completed run reports ``completed``."""
        service = RecordingExecutionService(result=success_result(content="done"))

        reply = await _reply_for(service)

        assert reply.succeeded.finish_reason == "completed"

    async def test_a_success_encodes_the_output_as_json(self) -> None:
        """``output_json`` must parse, so a consumer never hits a parse error."""
        import json

        service = RecordingExecutionService(result=success_result(content="an answer"))

        reply = await _reply_for(service)

        assert json.loads(reply.succeeded.output_json) == "an answer"

    async def test_a_success_with_no_ledger_reports_no_usage(self) -> None:
        """Absent usage means unreported, and is not fabricated as zero."""
        service = RecordingExecutionService(result=success_result())

        reply = await _reply_for(service)

        assert reply.succeeded.HasField("usage") is False

    async def test_a_success_with_no_model_reported_is_empty(self) -> None:
        """An unknown model is empty, not a guessed default."""
        service = RecordingExecutionService(result=success_result())

        reply = await _reply_for(service)

        assert reply.succeeded.model_key == ""


class TestModelKeyReporting:
    """``model_key`` is reported only when it can be named unambiguously."""

    async def test_no_ledger_means_no_model_key(self) -> None:
        """Without a ledger there is nothing to read."""
        assert model_key_from_result(success_result()) == ""

    async def test_a_single_recorded_model_is_reported(self) -> None:
        """One model means the answer is attributable to it."""
        result = success_result()
        object.__setattr__(result, "cost", _ledger_with("openai", "gpt-4o"))

        assert model_key_from_result(result) == "openai/gpt-4o"

    async def test_several_models_are_not_reported(self) -> None:
        """Fan-out cannot be attributed to one model, so it reports nothing.

        Naming one of several would credit the answer to a model that may not
        have produced it, which is worse than reporting unknown.
        """
        # One ledger, two priced models: the same run genuinely called both.
        result = success_result()
        object.__setattr__(result, "cost", _ledger_with("openai", "gpt-4o", "anthropic/claude"))

        assert model_key_from_result(result) == ""

    async def test_a_ledger_that_cannot_summarise_reports_nothing(self) -> None:
        """A malformed ledger is unreported, not a crash."""

        class _Broken:
            def cost_by_model(self) -> dict[str, float]:
                raise RuntimeError("corrupt ledger")

        result = success_result()
        object.__setattr__(result, "cost", _Broken())

        assert model_key_from_result(result) == ""


def _ledger_with(provider: str, model: str, *also: str):
    """A cost ledger that has recorded calls to one or more priced models.

    Args:
        provider: Provider id of the first model.
        model: Model name of the first model.
        *also: Additional ``provider/model`` keys the run also called.

    Returns:
        A ledger whose ``cost_by_model`` names exactly those models.

    A ledger only attributes cost to models it can price, so every model under
    test needs a rate. An unpriced call is not recorded at all, which would
    make a fan-out look like a single-model run.
    """
    from mas_orchestrator.models.cost import CostLedger, PricingTable
    from mas_orchestrator.models.types import ModelRef, Usage

    keys = [f"{provider}/{model}", *also]
    ledger = CostLedger(PricingTable(rates={key: {"input": 1.0, "output": 1.0} for key in keys}))
    for key in keys:
        head, _, tail = key.partition("/")
        ledger.record(ModelRef(provider=head, model=tail), Usage(1, 1))
    return ledger


class TestFinishReasonCoverage:
    """Every termination has a stable wire string."""

    async def test_each_termination_has_a_distinct_reason(self) -> None:
        """Two terminations never collapse onto the same string."""
        reasons = {finish_reason_for(item) for item in Termination}
        assert len(reasons) == len(Termination)

    async def test_cancelled_is_never_completed(self) -> None:
        """The invariant behind spec §11: a stop is never reported as done."""
        assert finish_reason_for(Termination.CANCELLED) != finish_reason_for(Termination.COMPLETED)
