# PURPOSE:
# Serves the REAL AgentBridgeApi over a real port for cross-language tests.
#
# Exists so a Rust `GrpcAgentBridge` client can talk to a real Python server in
# an end-to-end test. Everything on the wire is production code: the servicer,
# the identity resolver, scope reconciliation, deadline parsing, cancellation
# mirroring and error mapping are the real implementations. Only the Agent
# Runtime behind the servicer is a double, because reaching a live model
# provider would make the test bill per run and test the provider rather than
# the bridge.
#
# Run as a script; it prints the chosen port so the caller can connect.

"""Standalone bridge server for the Rust <-> Python end-to-end test.

Invoked as a subprocess by ``crates/ai-bridge/tests/e2e_python.rs``. It binds
an ephemeral port (or one given with ``--port``), prints ``PORT=<n>``, and
serves until the process is terminated.

This is a test harness, not a production entrypoint: it hard-codes a static
bearer token and a canned agent answer, both of which production derives from
the secret manager and the real runtime respectively.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, field
from typing import Any

from mas_orchestrator.agents.base import Termination
from mas_orchestrator.agents.runtime import ExecutionResult
from mas_orchestrator.contracts.errors import ModelTimeoutError
from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.grpc.server import GrpcServer, GrpcServerConfig
from mas_orchestrator.security.identity import (
    ServiceScope,
    StaticTokenServiceIdentityResolver,
)

#: Credential the Rust side must present. Matches the Rust test constant.
DEFAULT_TOKEN = "bridge-service-token"
#: Subject the Rust side must assert. Matches the Rust test constant.
DEFAULT_SUBJECT = "rust-core"
#: Shared secret the Rust side signs each call body with. Matches the Rust
#: test constant. A literal is acceptable only here, in a test harness: the
#: production value comes from ``security.internal_service_secret_ref``.
DEFAULT_INTERNAL_SECRET = "internal-shared-secret"
#: The service id the Rust side signs as. On the default allowlist.
DEFAULT_INTERNAL_SERVICE = "mas-worker"

#: Credential-shaped values planted in the "leak" failure text. They are inert
#: inventions; what matters is their shape, because the redaction pass matches
#: on shape. Mirrored by the Rust test, which asserts neither appears.
LEAKED_KEY = "sk-" + "A" * 32
LEAKED_BEARER = "Bearer eyJhbGciOiJIUzI1NiJ9.ZmFrZS1wYXlsb2Fk.c2lnbmF0dXJl"


@dataclass(slots=True)
class StubExecutionService:
    """Stands in for the Agent Runtime.

    Attributes:
        behaviour: ``success`` returns a completed run, ``failure`` raises a
            typed model error, ``hang`` never answers on its own so the caller
            can exercise deadline and cancellation propagation, and ``leak``
            raises an error whose text deliberately carries credential-shaped
            values so the caller can prove they never cross the wire; and
            ``echo_scope`` answers with the tenant scope *it* observed, so the
            caller can prove the scope that arrived is the scope it sent.
        delay_seconds: How long to wait before answering.
        runs: Every run the bridge dispatched, so the harness can assert the
            prompt and context arrived intact.
    """

    behaviour: str = "success"
    delay_seconds: float = 0.0
    runs: list[Any] = field(default_factory=list)

    async def execute(self, request: Any, *, cancel_event: Any = None) -> ExecutionResult:
        """Answers according to the configured behaviour.

        Args:
            request: The execution request, carrying ``.request`` (the prompt)
                and ``.context`` (the built execution context).
            cancel_event: The bridge's cancellation signal. ``hang`` waits on
                it so a cancellation is observable as a stopped run rather
                than as a run that simply never finished.

        Returns:
            The canned completed result.

        Raises:
            ModelTimeoutError: When the behaviour is ``failure``.
        """
        self.runs.append(request)

        if self.behaviour == "hang":
            event = cancel_event if cancel_event is not None else asyncio.Event()
            await event.wait()
            return ExecutionResult(
                execution_id=request.context.execution_id,
                agent_id=request.context.agent_id,
                termination=Termination.CANCELLED,
                content="",
            )

        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)

        if self.behaviour == "failure":
            raise ModelTimeoutError("the provider did not answer in time")

        if self.behaviour == "echo_scope":
            # Answering with the observed scope is what makes tenant isolation
            # testable across the boundary. The caller sends two different
            # tenants and asserts each reply names its own -- a run executed
            # under the wrong scope, or under a scope left over from a previous
            # call, would answer with the wrong value.
            return ExecutionResult(
                execution_id=request.context.execution_id,
                agent_id=request.context.agent_id,
                termination=Termination.COMPLETED,
                content=json.dumps(
                    {
                        "tenant_id": request.context.tenant_id,
                        "organization_id": request.context.organization_id,
                        "project_id": request.context.project_id,
                        "agent_id": request.context.agent_id,
                    },
                    sort_keys=True,
                ),
            )

        if self.behaviour == "leak":
            # Credential-shaped on purpose. A real provider error raised while a
            # key is in scope can carry that key into its own text; the far side
            # must never see it, so the test asserts on shapes a scanner should
            # recognise rather than on one known literal.
            raise ModelTimeoutError(
                f"provider rejected the call for key {LEAKED_KEY} with bearer {LEAKED_BEARER}"
            )

        return ExecutionResult(
            execution_id=request.context.execution_id,
            agent_id=request.context.agent_id,
            termination=Termination.COMPLETED,
            content="the answer",
        )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parses command-line options.

    Args:
        argv: Arguments; defaults to ``sys.argv[1:]``.

    Returns:
        The parsed options.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0, help="port to bind; 0 picks a free one")
    parser.add_argument("--host", default="127.0.0.1", help="interface to bind")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="accepted bearer credential")
    parser.add_argument("--subject", default=DEFAULT_SUBJECT, help="required caller subject")
    parser.add_argument(
        "--internal-secret",
        default=DEFAULT_INTERNAL_SECRET,
        help="shared secret the caller must sign each request body with; empty disables it",
    )
    parser.add_argument(
        "--internal-service",
        default=DEFAULT_INTERNAL_SERVICE,
        help="service id the caller is permitted to sign as",
    )
    parser.add_argument(
        "--behaviour",
        default="success",
        choices=("success", "failure", "hang", "leak", "echo_scope"),
        help="what the stubbed runtime does",
    )
    parser.add_argument("--delay", type=float, default=0.0, help="seconds to wait before answering")
    return parser.parse_args(argv)


async def _serve(options: argparse.Namespace) -> None:
    """Starts the server and blocks until cancelled.

    Args:
        options: The parsed command-line options.
    """
    resolver = StaticTokenServiceIdentityResolver(
        token=options.token,
        subject=options.subject,
        scopes=frozenset(
            {
                ServiceScope.EXECUTE_AGENT,
                ServiceScope.INVOKE_API,
                ServiceScope.READ_HEALTH,
            }
        ),
    )
    servicer = AgentBridgeServicer(
        execution_service=StubExecutionService(
            behaviour=options.behaviour, delay_seconds=options.delay
        ),
        identity_resolver=resolver,
        config=AgentBridgeServicerConfig(
            require_scope=ServiceScope.EXECUTE_AGENT,
            # A configured secret makes the signature mandatory, which is the
            # production posture. The cross-language test therefore proves the
            # two implementations agree on the exact bytes of a request: sign
            # on one side, verify on the other, over a real socket.
            internal_service_secret=options.internal_secret or None,
            allowed_internal_callers=frozenset({options.internal_service}),
        ),
    )
    server = GrpcServer(
        servicer=servicer, config=GrpcServerConfig(host=options.host, port=options.port)
    )
    port = await server.start()

    # Flushed explicitly: the Rust test blocks on reading this line, and a
    # buffered write would leave it waiting until the process exits.
    print(f"PORT={port}", flush=True)

    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()


def main(argv: list[str] | None = None) -> int:
    """Entry point.

    Args:
        argv: Arguments; defaults to ``sys.argv[1:]``.

    Returns:
        ``0`` on clean shutdown, ``1`` on error.
    """
    options = _parse_args(argv)
    try:
        asyncio.run(_serve(options))
    except KeyboardInterrupt:
        return 0
    except Exception as error:  # a harness reports the failure, it does not hide it
        print(f"stub server failed: {error}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
