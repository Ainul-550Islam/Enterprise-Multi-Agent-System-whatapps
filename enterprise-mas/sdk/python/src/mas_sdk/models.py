"""mas SDK - Data models."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypeVar, Generic
from enum import Enum
from pydantic import BaseModel, Field

T = TypeVar("T")


class ExecutionResponse(BaseModel):
    """Response from an execution endpoint."""
    id: str
    status: ExecutionStatus
    input: Optional[Any] = None
    output: Optional[Any] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    idempotent_replay: Optional[bool] = False


execution_response_example = {
    "id": "exec-123",
    "status": "completed",
    "output": {"result": "hello"},
}


class KeySetPage(BaseModel, Generic[T]):
    """A page of results from keyset pagination."""
    items: List[T]
    meta: PaginationMeta


class PaginationMeta(BaseModel):
    """Metadata for keyset pagination."""
    has_more: bool = False
    next_key: Optional[str] = None
    prev_key: Optional[str] = None


class ApiEnvelope(BaseModel):
    """Standard API response envelope."""
    data: Any
    envelope: Optional[dict] = None


class ExecutionStatus(str, Enum):
    """Status of an execution."""
    Completed = "completed"
    Cancelled = "cancelled"
    Failed = "failed"
    Disabled = "disabled"


# Example for documentation
execution_response_example = {
    "id": "exec-123",
    "status": "completed",
    "output": {"result": "hello"},
}