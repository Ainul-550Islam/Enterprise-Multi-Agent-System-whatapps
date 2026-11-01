"""Retrieval service.

This is the boundary between the RAG pipeline and an agent. Everything that
matters about RAG security happens here or in
:mod:`~mas_orchestrator.rag.security`, because there are exactly two ways a RAG
system goes wrong and both are decided at this boundary:

1. **It returns something the caller may not see.** Prevented by filtering
   before ranking, not after.
2. **It lets retrieved text act as instruction.** Prevented by fencing every
   passage as untrusted data.

The service returns a :class:`RetrievedContext` carrying both the fenced text
and the numbered citations that go with it. The citation numbers are the only
way an answer may refer to a source, and
:meth:`RetrievedContext.assert_supported` checks an answer against them — so a
citation that names a source the model was never given is caught rather than
shown to a user.

Source URIs are withheld from the model by default. A URI in retrieved context
is not merely a label; it is an instruction about where to send something.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import (
    CitationValidationError,
    InvalidRequestError,
)
from mas_orchestrator.rag.citations import (
    ContextConfig,
    assert_supported,
    construct_context,
    validate_citations,
)

if TYPE_CHECKING:
    from mas_orchestrator.rag.models import (
        Citation,
        RagContext,
        RetrievalHit,
        SecurityContext,
    )
from mas_orchestrator.rag.security import filter_chunks

if TYPE_CHECKING:
    from mas_orchestrator.events.emitter import EventEmitter
    from mas_orchestrator.rag.retrieval import Retriever

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RetrievedContext:
    """What retrieval produced, ready for a prompt.

    Attributes:
        context: The fenced, numbered context.
        hits: The underlying hits, in citation order.
        citations: Numbered citations, matching the context.
        duration_ms: How long retrieval took.
        filtered: How many hits were withheld by the access filter.
    """

    context: RagContext
    hits: tuple[RetrievalHit, ...]
    citations: tuple[Citation, ...]
    duration_ms: int = 0
    filtered: int = 0

    @property
    def empty(self) -> bool:
        """Whether anything was retrieved."""
        return not self.hits

    @property
    def available(self) -> int:
        """How many citations an answer may reference."""
        return len(self.citations)

    def assert_supported(self) -> None:
        """Refuses a citation that does not match the passage it quotes.

        Raises:
            CitationValidationError: If any citation is unsupported.
        """
        sources = {hit.chunk.chunk_id: hit.chunk.text for hit in self.hits}
        checks = validate_citations(list(self.citations), sources)
        assert_supported(checks)

    def as_prompt_text(self) -> str:
        """The context text, already fenced."""
        return str(self.context.text)


class RagService:
    """Tenant-scoped retrieval."""

    def __init__(
        self,
        retriever: Retriever,
        *,
        emitter: EventEmitter | None = None,
        config: ContextConfig | None = None,
    ) -> None:
        self._retriever = retriever
        self._emitter = emitter
        self._config = config or ContextConfig()

    @property
    def config(self) -> ContextConfig:
        """Context-construction settings."""
        return self._config

    async def retrieve(
        self,
        query: str,
        security: SecurityContext,
        *,
        top_k: int | None = None,
    ) -> RetrievedContext:
        """Retrieves passages the caller is permitted to see.

        Args:
            query: The retrieval query.
            security: Who is asking.
            top_k: Override for the retriever's configured result count.

        Returns:
            A :class:`RetrievedContext`.

        Raises:
            InvalidRequestError: If the query is empty or the security context
                has no tenant.
        """
        if not query.strip():
            raise InvalidRequestError("a retrieval query is required", details={"field": "query"})
        if not security.tenant_id.strip():
            raise InvalidRequestError(
                "a security context must name a tenant",
                details={"field": "tenant_id"},
            )

        started = time.monotonic()
        hits = await self._retriever.retrieve(query, security, top_k=top_k)
        duration = int((time.monotonic() - started) * 1000)

        # Defence in depth: the retriever filters, and so do we. Filtering
        # twice is cheap; a single missed filter is a disclosure.
        permitted = filter_chunks([hit.chunk for hit in hits], security)
        permitted_ids = {chunk.chunk_id for chunk in permitted}
        visible = tuple(hit for hit in hits if hit.chunk.chunk_id in permitted_ids)

        assert_tenant_isolated_documents(visible, security.tenant_id)

        context = construct_context(visible, query, self._config)
        citations = tuple(context.citations)

        if self._emitter is not None:
            from mas_orchestrator.events.types import EventName, duration_event

            await self._emitter.emit(
                duration_event(
                    EventName.RAG_RETRIEVAL_DURATION,
                    tenant_id=security.tenant_id,
                    execution_id="",
                    correlation_id="",
                    duration_ms=duration,
                    top_k=len(visible),
                )
            )

        return RetrievedContext(
            context=context,
            hits=visible,
            citations=citations,
            duration_ms=duration,
            filtered=len(hits) - len(visible),
        )

    async def retrieve_and_check(
        self,
        query: str,
        answer: str,
        security: SecurityContext,
        *,
        top_k: int | None = None,
    ) -> RetrievedContext:
        """Retrieves, then validates an answer against the citations.

        Two independent checks, because they catch different failures:
        ``require_citations`` catches a citation index that names a source
        never supplied, and ``assert_supported`` catches a citation that names
        a real source but quotes something it does not contain.
        """
        retrieved = await self.retrieve(query, security, top_k=top_k)
        if retrieved.empty:
            return retrieved
        require_citations(answer, retrieved.available)
        retrieved.assert_supported()
        return retrieved


def assert_tenant_isolated_documents(
    hits: tuple[RetrievalHit, ...],
    tenant_id: str,
) -> None:
    """Refuses a result set containing another tenant's documents.

    Raises:
        InvalidRequestError: If any hit belongs to another tenant.
    """
    foreign = sorted(
        {hit.chunk.access.tenant_id for hit in hits if hit.chunk.access.tenant_id != tenant_id}
    )
    if foreign:
        raise InvalidRequestError(
            "retrieval returned another tenant's documents: a filter was skipped upstream",
            details={"expected_tenant": tenant_id, "found": foreign},
        )


def require_citations(answer: str, available: int) -> None:
    """Refuses an answer whose citations are out of range.

    Raises:
        CitationValidationError: If any citation index exceeds ``available``.
    """
    from mas_orchestrator.rag.citations import references_are_in_range

    if not references_are_in_range(answer, available):
        raise CitationValidationError(
            "answer cites a source that was not supplied",
            details={"available": available},
        )
