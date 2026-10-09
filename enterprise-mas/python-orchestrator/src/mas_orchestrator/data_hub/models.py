"""Typed records at the Rust-owned Data Hub ↔ Python ingestion boundary.

Rust owns the source/document catalogue and authorizes the tenant, project,
access policy, and secret references before it signs a bridge request. These
records are immutable request snapshots; Python never looks up a caller's
tenant by itself and never writes catalogue metadata back to Rust tables.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime

from mas_orchestrator.config.enums import DataSourceKind
from mas_orchestrator.contracts.errors import InvalidRequestError


@dataclass(frozen=True, slots=True)
class DataHubSource:
    """A Rust-authorized, signed snapshot of one configured source."""

    source_id: str
    tenant_id: str
    organization_id: str
    project_id: str
    name: str
    kind: DataSourceKind
    config: Mapping[str, Any] = field(default_factory=dict)
    credential_secret_ref: str | None = None
    cursor: str | None = None
    required_roles: frozenset[str] = frozenset()
    denied_principals: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        for field_name, value in (
            ("source_id", self.source_id),
            ("tenant_id", self.tenant_id),
            ("organization_id", self.organization_id),
            ("project_id", self.project_id),
            ("name", self.name),
        ):
            if not value.strip():
                raise InvalidRequestError(
                    "Data Hub source snapshot is missing a required field",
                    details={"field": field_name},
                )
        if len(self.name) > 200:
            raise InvalidRequestError("Data Hub source name exceeds 200 characters")
        if self.cursor is not None and len(self.cursor.encode("utf-8")) > 64 * 1024:
            raise InvalidRequestError("Data Hub source cursor exceeds its size limit")
        _validate_uuid(self.source_id, "source_id")
        _validate_uuid(self.tenant_id, "tenant_id")
        _validate_uuid(self.organization_id, "organization_id")
        _validate_uuid(self.project_id, "project_id")
        _validate_config_secrets(self.config)


@dataclass(frozen=True, slots=True)
class DataHubDocument:
    """A Rust-authorized upload or discovered document to be indexed."""

    source_id: str
    document_id: str
    tenant_id: str
    organization_id: str
    project_id: str
    title: str
    mime_type: str
    source_kind: DataSourceKind
    content: bytes
    source_uri: str | None = None
    expected_sha256: str | None = None
    required_roles: frozenset[str] = frozenset()
    denied_principals: frozenset[str] = frozenset()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name, value in (
            ("source_id", self.source_id),
            ("document_id", self.document_id),
            ("tenant_id", self.tenant_id),
            ("organization_id", self.organization_id),
            ("project_id", self.project_id),
            ("title", self.title),
            ("mime_type", self.mime_type),
        ):
            if not value.strip():
                raise InvalidRequestError(
                    "Data Hub document is missing a required field",
                    details={"field": field_name},
                )
        for field_name, value in (
            ("source_id", self.source_id),
            ("document_id", self.document_id),
            ("tenant_id", self.tenant_id),
            ("organization_id", self.organization_id),
            ("project_id", self.project_id),
        ):
            _validate_uuid(value, field_name)
        if not isinstance(self.source_kind, DataSourceKind):
            raise InvalidRequestError("Data Hub document source kind is invalid")
        if not self.content:
            raise InvalidRequestError("Data Hub document content is empty")
        if len(self.title) > 512 or len(self.mime_type) > 255:
            raise InvalidRequestError("Data Hub document metadata exceeds its bound")
        if self.source_uri is not None and len(self.source_uri) > 2048:
            raise InvalidRequestError("Data Hub source URI exceeds its bound")
        if self.expected_sha256 is not None and (
            len(self.expected_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.expected_sha256)
        ):
            raise InvalidRequestError("Data Hub expected SHA-256 must be lowercase hex")
        _validate_config_secrets(self.metadata)


@dataclass(frozen=True, slots=True)
class DiscoveredDocument:
    """One changed document discovered by a bounded source adapter."""

    locator: str
    title: str
    mime_type: str
    content: bytes
    etag: str | None = None
    last_modified: datetime | None = None
    #: Opaque adapter cursor immediately before this item.  It lets the
    #: service retry a failed item without advancing past it.
    cursor_before: str | None = None
    #: Opaque adapter cursor immediately after this item was discovered.
    cursor_after: str | None = None

    def __post_init__(self) -> None:
        if not self.locator.strip() or not self.title.strip() or not self.mime_type.strip():
            raise InvalidRequestError("discovered document is missing source metadata")
        if len(self.locator) > 2048 or len(self.title) > 512 or len(self.mime_type) > 255:
            raise InvalidRequestError("discovered document metadata exceeds its bound")
        if not self.content:
            raise InvalidRequestError("discovered document content is empty")
        for field_name, cursor in (
            ("cursor_before", self.cursor_before),
            ("cursor_after", self.cursor_after),
        ):
            if cursor is not None and len(cursor.encode("utf-8")) > 64 * 1024:
                raise InvalidRequestError(
                    "discovered document checkpoint exceeds its size limit",
                    details={"field": field_name},
                )


def _validate_uuid(value: str, field_name: str) -> None:
    """Requires canonical UUID identifiers at the signed bridge boundary."""
    import uuid

    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError) as error:
        raise InvalidRequestError(
            f"Data Hub {field_name} must be a UUID",
            details={"field": field_name},
        ) from error
    if str(parsed) != value.lower():
        raise InvalidRequestError(
            f"Data Hub {field_name} must use canonical UUID form",
            details={"field": field_name},
        )


def _validate_config_secrets(config: Mapping[str, Any]) -> None:
    """Rejects raw secret-like values from signed source metadata.

    Rust may send a secret reference in a field ending with ``_secret_ref``;
    it must not serialize a password/token/key value into the bridge request.
    """
    forbidden = {"password", "secret", "token", "api_key", "access_key", "secret_key"}
    for key, value in config.items():
        normalized = str(key).strip().lower().replace("-", "_")
        if normalized in forbidden:
            raise InvalidRequestError(
                "raw credentials are forbidden in Data Hub source configuration",
                details={"field": str(key)},
            )
        if isinstance(value, Mapping):
            _validate_config_secrets(value)
