"""Failure-path tests for gRPC server startup and resource ownership."""

from __future__ import annotations

from typing import Any

import grpc
import pytest

from mas_orchestrator.grpc.server import GrpcServer, GrpcServerConfig


class _Resource:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class _DataHubServicer:
    def __init__(self, *, fail_startup: bool = False) -> None:
        self.fail_startup = fail_startup
        self.started = False

    async def startup(self) -> None:
        if self.fail_startup:
            raise RuntimeError("data hub startup failed")
        self.started = True


class _RecordingGrpcServer:
    def __init__(self, executor: Any) -> None:
        self.executor = executor
        self.stop_calls: list[float] = []

    def add_generic_rpc_handlers(self, _handlers: object) -> None:
        return None

    def add_registered_method_handlers(self, _name: str, _handlers: object) -> None:
        return None

    def add_insecure_port(self, _address: str) -> int:
        return 49152

    async def start(self) -> None:
        raise RuntimeError("aio server start failed")

    async def stop(self, grace: float) -> None:
        self.stop_calls.append(grace)
        self.executor.shutdown(wait=False)


class _StartedGrpcServer(_RecordingGrpcServer):
    async def start(self) -> None:
        return None


class _RecordingLogger:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def info(self, _message: str, *, extra: dict[str, object]) -> None:
        self.records.append(extra)

    def warning(self, _message: str, *, extra: dict[str, object]) -> None:
        self.records.append(extra)

    def exception(self, _message: str, *, extra: dict[str, object]) -> None:
        self.records.append(extra)


def _patch_noop_registrars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "mas_orchestrator.grpc.generated.mas_pb2_grpc.add_AgentBridgeApiServicer_to_server",
        lambda _servicer, _server: None,
    )
    monkeypatch.setattr(
        "mas_orchestrator.grpc.generated.mas_pb2_grpc.add_DataHubBridgeApiServicer_to_server",
        lambda _servicer, _server: None,
    )
    monkeypatch.setattr(
        "mas_orchestrator.security.transport.build_server_credentials",
        lambda _config: None,
    )


def _patch_server_factory(
    monkeypatch: pytest.MonkeyPatch,
    recording: list[_RecordingGrpcServer],
) -> None:
    def make_server(**options: Any) -> _RecordingGrpcServer:
        server = _RecordingGrpcServer(options["migration_thread_pool"])
        recording.append(server)
        return server

    monkeypatch.setattr(grpc.aio, "server", make_server)


def _patch_successful_server_factory(
    monkeypatch: pytest.MonkeyPatch,
    recording: list[_StartedGrpcServer],
) -> None:
    def make_server(**options: Any) -> _StartedGrpcServer:
        server = _StartedGrpcServer(options["migration_thread_pool"])
        recording.append(server)
        return server

    monkeypatch.setattr(grpc.aio, "server", make_server)


class TestGrpcStartCleanup:
    @pytest.mark.asyncio
    async def test_aio_start_failure_stops_listener_and_closes_runtime(self, monkeypatch) -> None:
        _patch_noop_registrars(monkeypatch)
        recording: list[_RecordingGrpcServer] = []
        _patch_server_factory(monkeypatch, recording)
        resource = _Resource()
        server = GrpcServer(
            servicer=object(),
            closeables=(resource,),
            config=GrpcServerConfig(host="127.0.0.1", port=0),
        )

        with pytest.raises(RuntimeError, match="aio server start failed"):
            await server.start()

        assert len(recording) == 1
        assert recording[0].stop_calls == [0]
        assert resource.closed is True
        assert server.running is False
        assert server.bound_port is None

    @pytest.mark.asyncio
    async def test_data_hub_startup_failure_closes_runtime_before_bind(self, monkeypatch) -> None:
        _patch_noop_registrars(monkeypatch)
        recording: list[_RecordingGrpcServer] = []
        _patch_server_factory(monkeypatch, recording)
        resource = _Resource()
        data_hub = _DataHubServicer(fail_startup=True)
        server = GrpcServer(
            servicer=object(),
            data_hub_servicer=data_hub,  # type: ignore[arg-type]
            closeables=(resource,),
            config=GrpcServerConfig(host="127.0.0.1", port=0),
        )

        with pytest.raises(RuntimeError, match="data hub startup failed"):
            await server.start()

        assert recording == []
        assert resource.closed is True
        assert server.running is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("with_data_hub", [False, True])
    async def test_startup_diagnostics_report_registered_services_as_an_array(
        self, monkeypatch, with_data_hub: bool
    ) -> None:
        _patch_noop_registrars(monkeypatch)
        recording: list[_StartedGrpcServer] = []
        _patch_successful_server_factory(monkeypatch, recording)
        logger = _RecordingLogger()
        data_hub = _DataHubServicer() if with_data_hub else None
        server = GrpcServer(
            servicer=object(),
            data_hub_servicer=data_hub,  # type: ignore[arg-type]
            config=GrpcServerConfig(host="127.0.0.1", port=0),
            logger=logger,  # type: ignore[arg-type]
        )

        assert await server.start() == 49152
        listening = next(
            record for record in logger.records if record.get("event") == "bridge.listening"
        )
        expected = ["mas.api.v1.AgentBridgeApi"]
        if with_data_hub:
            expected.append("mas.api.v1.DataHubBridgeApi")
        assert listening["services"] == expected
        assert "service" not in listening
        if data_hub is not None:
            assert data_hub.started is True
        await server.stop()
