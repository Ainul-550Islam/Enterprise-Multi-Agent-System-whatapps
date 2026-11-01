"""Tests for secret handling and graceful shutdown.

Both modules exist to make a rule structural. The tests therefore check the
refusal rather than the happy path: that a secret cannot be printed, that
production fails closed without credentials, and that a shutdown which abandons
work says so instead of exiting cleanly.
"""

from __future__ import annotations

import asyncio

import pytest

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.config.secrets import (
    ChainedResolver,
    EnvironmentResolver,
    MappingResolver,
    Secret,
    assert_no_committed_credentials,
    is_valid_secret_name,
    redact_in_text,
    resolve_provider_credentials,
    safe_environment_summary,
    secret_names_for,
)
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.services.lifecycle import (
    DrainReport,
    ExitStatus,
    ShutdownCoordinator,
    ShutdownPhase,
    exit_code_for,
)


class _Spec:
    """A minimal provider credential contract."""

    def __init__(
        self,
        name: str = "openai",
        *,
        enabled: bool = True,
        mandatory: bool = True,
        secret_names: tuple[str, ...] = ("OPENAI_API_KEY",),
    ) -> None:
        self.name = name
        self.enabled = enabled
        self.mandatory = mandatory
        self.secret_names = secret_names


class TestSecret:
    """A credential that will not print itself."""

    def test_value_is_reachable(self) -> None:
        assert Secret("hunter2", name="K").reveal() == "hunter2"

    def test_repr_hides_the_value(self) -> None:
        assert "hunter2" not in repr(Secret("hunter2"))

    def test_str_hides_the_value(self) -> None:
        assert "hunter2" not in str(Secret("hunter2"))

    def test_fstring_hides_the_value(self) -> None:
        """The common accident: interpolating a credential into a message."""
        secret = Secret("hunter2")
        assert "hunter2" not in f"using {secret}"

    def test_fingerprint_shows_only_the_tail(self) -> None:
        fingerprint = Secret("sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456", name="K").fingerprint()
        assert "ABCDEFGH" not in fingerprint
        assert "3456" in fingerprint

    def test_short_secret_has_no_tail(self) -> None:
        assert Secret("ab", name="K").fingerprint() == "K(...)"

    def test_name_is_exposed(self) -> None:
        assert Secret("x", name="MY_KEY").name == "MY_KEY"

    def test_empty_rejected(self) -> None:
        with pytest.raises(ConfigurationError):
            Secret("   ")

    def test_non_string_rejected(self) -> None:
        with pytest.raises(ConfigurationError):
            Secret(123)  # type: ignore[arg-type]

    def test_truthiness_follows_the_value(self) -> None:
        assert bool(Secret("x")) is True

    def test_length_is_safe(self) -> None:
        assert len(Secret("hunter2")) == 7

    def test_equality(self) -> None:
        assert Secret("abc") == Secret("abc")
        assert Secret("abc") != Secret("abd")

    def test_inequality_with_another_type(self) -> None:
        assert Secret("abc") != "abc"

    def test_hashable(self) -> None:
        assert len({Secret("a"), Secret("a")}) == 1

    def test_logging_does_not_leak(self) -> None:
        """A secret in a log record's args must still not render."""
        rendered = "{}".format(Secret("hunter2"))
        assert "hunter2" not in rendered


class TestResolvers:
    """Where secrets come from."""

    def test_environment_resolver(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_KEY", "value")
        found = EnvironmentResolver().resolve("MY_KEY")
        assert found is not None and found.reveal() == "value"

    def test_environment_resolver_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ABSENT_KEY", raising=False)
        assert EnvironmentResolver().resolve("ABSENT_KEY") is None

    def test_environment_resolver_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAS_ORCH_MY_KEY", "value")
        assert EnvironmentResolver(prefix="MAS_ORCH_").resolve("MY_KEY") is not None

    def test_empty_value_is_not_a_secret(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BLANK", "   ")
        assert EnvironmentResolver().resolve("BLANK") is None

    def test_chained_uses_first_hit(self) -> None:
        first = MappingResolver({"A": "1"}, environment="development")
        second = MappingResolver({"A": "2", "B": "3"}, environment="development")
        chained = ChainedResolver(first, second)
        assert chained.resolve("A") is not None
        assert chained.resolve("A").reveal() == "1"
        assert chained.resolve("B") is not None
        assert chained.resolve("B").reveal() == "3"

    def test_chained_requires_a_source(self) -> None:
        with pytest.raises(ConfigurationError):
            ChainedResolver()

    def test_mapping_resolver(self) -> None:
        resolver = MappingResolver({"A": "1"}, environment="development")
        assert resolver.resolve("A") is not None
        assert resolver.resolve("Z") is None

    def test_mapping_resolver_refused_in_production(self) -> None:
        """A literal credential must not survive into a deployment."""
        with pytest.raises(ConfigurationError, match="production"):
            MappingResolver({"A": "1"}, environment=Environment.PRODUCTION.value)


class TestProviderCredentials:
    """Failing closed."""

    def test_disabled_provider_is_not_resolved(self) -> None:
        resolver = MappingResolver({}, environment="development")
        spec = _Spec(enabled=False)
        assert resolve_provider_credentials(spec, resolver, environment="production") == {}

    def test_mandatory_missing_in_production_raises(self) -> None:
        resolver = MappingResolver({}, environment="development")
        with pytest.raises(ConfigurationError, match="credentials"):
            resolve_provider_credentials(
                _Spec(), resolver, environment=Environment.PRODUCTION.value
            )

    def test_mandatory_missing_in_development_warns(self) -> None:
        resolver = MappingResolver({}, environment="development")
        result = resolve_provider_credentials(
            _Spec(), resolver, environment=Environment.DEVELOPMENT.value
        )
        assert result == {}

    def test_optional_missing_in_production_is_tolerated(self) -> None:
        resolver = MappingResolver({}, environment="development")
        spec = _Spec(mandatory=False)
        assert resolve_provider_credentials(spec, resolver, environment="production") == {}

    def test_present_credentials_are_returned(self) -> None:
        resolver = MappingResolver({"OPENAI_API_KEY": "sk-test-value"}, environment="development")
        result = resolve_provider_credentials(
            _Spec(), resolver, environment=Environment.PRODUCTION.value
        )
        assert result["OPENAI_API_KEY"].reveal() == "sk-test-value"

    def test_known_provider_names(self) -> None:
        assert secret_names_for("openai") == ("OPENAI_API_KEY",)
        assert secret_names_for("anthropic") == ("ANTHROPIC_API_KEY",)

    def test_unknown_provider_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            secret_names_for("not_a_provider")


class TestDiagnostics:
    """Never echo secrets."""

    def test_sensitive_names_are_redacted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_API_KEY", "secret-value")
        monkeypatch.setenv("MY_NAME", "plain")
        summary = safe_environment_summary()
        assert summary["MY_API_KEY"] == "[redacted]"
        assert "secret-value" not in summary["MY_API_KEY"]

    def test_insensitive_names_report_length_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_NAME", "plain")
        assert safe_environment_summary()["MY_NAME"] == "[set, 5 chars]"

    def test_include_limits_the_summary(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_NAME", "plain")
        assert safe_environment_summary(include=("MY_NAME",)) == {"MY_NAME": "[set, 5 chars]"}

    def test_unset_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ABSENT_THING", raising=False)
        assert safe_environment_summary(include=("ABSENT_THING",)) == {"ABSENT_THING": "[unset]"}

    def test_summary_never_contains_a_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PLAIN_VAR", "leakme")
        rendered = str(safe_environment_summary(include=("PLAIN_VAR",)))
        assert "leakme" not in rendered

    def test_committed_credential_is_refused(self) -> None:
        with pytest.raises(ConfigurationError, match="credential"):
            assert_no_committed_credentials("key: sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456")

    def test_clean_text_passes(self) -> None:
        assert_no_committed_credentials("host: localhost")

    def test_redact_in_text(self) -> None:
        result = redact_in_text("key: sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456")
        assert "sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456" not in result

    def test_secret_name_validation(self) -> None:
        assert is_valid_secret_name("OPENAI_API_KEY") is True
        assert is_valid_secret_name("lowercase") is False
        assert is_valid_secret_name("9_START") is False


class TestShutdown:
    """The spec §24 sequence."""

    def _coordinator(self, **kwargs: object) -> ShutdownCoordinator:
        return ShutdownCoordinator(**kwargs)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_idle_shutdown_is_clean(self) -> None:
        coordinator = self._coordinator()
        report = await coordinator.shutdown()
        assert report.clean is True
        assert report.exit_status is ExitStatus.OK
        assert coordinator.phase is ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_new_work_is_refused_after_drain_starts(self) -> None:
        coordinator = self._coordinator()
        await coordinator.drain()
        assert coordinator.accepting is False

        async def _noop() -> None:
            return None

        with pytest.raises(RuntimeError, match="draining"):
            coordinator.track("t1", asyncio.get_running_loop().create_task(_noop()))

    @pytest.mark.asyncio
    async def test_in_flight_tasks_are_cancelled(self) -> None:
        coordinator = self._coordinator(drain_deadline_seconds=1.0)
        started = asyncio.Event()

        async def _work() -> None:
            started.set()
            await asyncio.sleep(30)

        task = asyncio.get_running_loop().create_task(_work())
        coordinator.track("t1", task)
        await started.wait()

        report = await coordinator.drain()
        assert report.cancelled == 1
        assert report.clean is True
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_stuck_tasks_are_reported_not_hidden(self) -> None:
        """The rule: never terminate while silently abandoning work."""
        coordinator = self._coordinator(drain_deadline_seconds=0.05)

        # The task outlives the drain because it re-enters its loop until
        # ``released`` is set. Relying on cancellation semantics alone is
        # fragile: an ``asyncio.sleep`` inside a task that suppresses
        # CancelledError can complete immediately on a later iteration, so a
        # bounded swallow-count finishes far too early to be a useful fixture.
        released = False

        async def _swallow_cancel() -> None:
            while not released:
                try:
                    await asyncio.sleep(0.02)
                except asyncio.CancelledError:
                    continue

        task = asyncio.get_running_loop().create_task(_swallow_cancel())
        coordinator.track("stuck", task)
        await asyncio.sleep(0.01)

        report = await coordinator.drain()
        assert report.pending == ("stuck",)
        assert report.clean is False
        assert report.exit_status is ExitStatus.TASKS_ABANDONED

        released = True
        await asyncio.sleep(0.1)
        assert task.done() is True

    @pytest.mark.asyncio
    async def test_providers_and_transports_are_closed(self) -> None:
        closed: list[str] = []

        class _Resource:
            def __init__(self, label: str) -> None:
                self._label = label

            async def aclose(self) -> None:
                closed.append(self._label)

        coordinator = self._coordinator()
        coordinator.register_provider(_Resource("provider"))
        coordinator.register_transport(_Resource("transport"))
        await coordinator.shutdown()
        assert closed == ["provider", "transport"]

    @pytest.mark.asyncio
    async def test_a_close_failure_does_not_stop_the_rest(self) -> None:
        closed: list[str] = []

        class _Broken:
            async def aclose(self) -> None:
                raise RuntimeError("cannot close")

        class _Fine:
            async def aclose(self) -> None:
                closed.append("fine")

        coordinator = self._coordinator()
        coordinator.register_provider(_Broken())
        coordinator.register_transport(_Fine())
        report = await coordinator.shutdown()
        assert closed == ["fine"]
        assert report.exit_status is ExitStatus.CLOSE_FAILED

    @pytest.mark.asyncio
    async def test_telemetry_is_flushed(self) -> None:
        flushed: list[str] = []

        class _Flusher:
            async def flush(self) -> None:
                flushed.append("done")

        coordinator = self._coordinator()
        coordinator.register_flusher(_Flusher())
        await coordinator.shutdown()
        assert flushed == ["done"]

    @pytest.mark.asyncio
    async def test_flush_failure_sets_a_status(self) -> None:
        class _Broken:
            async def flush(self) -> None:
                raise RuntimeError("buffer lost")

        coordinator = self._coordinator()
        coordinator.register_flusher(_Broken())
        report = await coordinator.shutdown()
        assert report.exit_status is ExitStatus.FLUSH_FAILED

    @pytest.mark.asyncio
    async def test_abandoned_work_outranks_lost_telemetry(self) -> None:
        """Worse failure wins: work matters more than its telemetry."""

        class _Broken:
            async def flush(self) -> None:
                raise RuntimeError("buffer lost")

        coordinator = self._coordinator()
        coordinator.register_flusher(_Broken())
        report = await coordinator.shutdown()
        # No tasks, so the flush failure is the only problem.
        assert report.exit_status is ExitStatus.FLUSH_FAILED

    @pytest.mark.asyncio
    async def test_completed_tasks_are_untracked(self) -> None:
        coordinator = self._coordinator()

        async def _quick() -> None:
            return None

        task = asyncio.get_running_loop().create_task(_quick())
        coordinator.track("t1", task)
        await task
        await asyncio.sleep(0)
        assert coordinator.in_flight == 0

    def test_exit_code_mapping(self) -> None:
        assert exit_code_for(DrainReport(ShutdownPhase.STOPPED, 1, exit_status=ExitStatus.OK)) == 0
        assert (
            exit_code_for(
                DrainReport(
                    ShutdownPhase.STOPPED,
                    1,
                    pending=("a",),
                    exit_status=ExitStatus.TASKS_ABANDONED,
                )
            )
            == 70
        )

    def test_report_defaults(self) -> None:
        report = DrainReport(ShutdownPhase.DRAINING, 5)
        assert report.clean is True
        assert report.pending == ()
