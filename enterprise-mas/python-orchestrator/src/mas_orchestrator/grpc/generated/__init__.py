"""Generated protobuf bindings — DO NOT EDIT.

Produced by ``scripts/generate_proto.sh`` from the authoritative Rust
protobuf contract at ``crates/api/proto/mas/api/v1/mas.proto``.

Rust owns the wire contract. To change these definitions, change the ``.proto``
in the Rust workspace and re-run the generator; hand edits will be lost on the
next regeneration and will silently break protocol compatibility.

Lint and type-check rules are relaxed for this package because the code is
machine-generated and does not follow project style.
"""

from __future__ import annotations

from . import mas_pb2 as mas_api_pb2
from . import mas_pb2_grpc as mas_api_pb2_grpc

__all__ = ["mas_api_pb2", "mas_api_pb2_grpc"]
