"""Retrieval security boundary.

Retrieval is where a RAG system most often leaks. The model sees whatever
retrieval returns, so anything retrieval can return is effectively readable by
whoever is talking to the model — and "whoever is talking to the model" is not
necessarily the person who was authorised.

The boundary rule enforced here is that filtering happens **before ranking**,
not after. Post-filtering a ranked list is the common shortcut, and it is unsafe
for a subtle reason: the filter only removes what *surfaced*, so a document that
was not retrieved cannot leak, but a document whose *existence* is inferable
from the shape of the results still can. More practically, post-filtering tends
to be applied to the top-k only, which means a lower-ranked document from
another tenant can slip through whenever the implementation is sloppy.

The functions here are pure and take no I/O, so they can be applied at every
stage and tested exhaustively.

The second rule: **a denial is silent**. :func:`filter_documents` returns only
permitted documents and does not explain what was withheld — telling a caller
"there is a document you may not see about this" is itself a disclosure.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Final, TypeVar

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.rag.models import (
    Chunk,
    DocumentAccess,
    SecurityContext,
    SourceDocument,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class AccessDecision:
    """Why a document was or was not visible.

    Returned by :func:`explain`, which exists for auditing. It is never used to
    build a response to the caller, for the reason in the module docstring.

    Attributes:
        visible: Whether the reader may see it.
        reason: A stable machine-readable reason.
    """

    visible: bool
    reason: str


#: Reasons, as stable strings, so an audit log can be queried.
REASON_OK: Final[str] = "ok"
REASON_TENANT_MISMATCH: Final[str] = "tenant_mismatch"
REASON_DENIED_PRINCIPAL: Final[str] = "denied_principal"
REASON_MISSING_ROLE: Final[str] = "missing_role"


def permits(access: DocumentAccess, context: SecurityContext) -> bool:
    """Whether a reader may see a document."""
    return access.permits(context)


def explain(access: DocumentAccess, context: SecurityContext) -> AccessDecision:
    """Explains a visibility decision, for auditing.

    The tenant check is reported generically. Distinguishing "wrong tenant"
    from "does not exist" would let a caller enumerate tenants by probing.
    """
    if context.tenant_id != access.tenant_id:
        return AccessDecision(False, REASON_TENANT_MISMATCH)
    if context.allowed_principals & access.denied_principals:
        return AccessDecision(False, REASON_DENIED_PRINCIPAL)
    if access.required_roles and not (access.required_roles & context.allowed_roles):
        return AccessDecision(False, REASON_MISSING_ROLE)
    return AccessDecision(True, REASON_OK)


def filter_documents(
    documents: list[SourceDocument],
    context: SecurityContext,
) -> list[SourceDocument]:
    """Keeps only documents the reader may see.

    Applied before any ranking, so a withheld document is never scored.
    """
    if not context.tenant_id.strip():
        raise InvalidRequestError(
            "a security context must name a tenant", details={"field": "tenant_id"}
        )
    visible = [doc for doc in documents if doc.access.permits(context)]
    withheld = len(documents) - len(visible)
    if withheld:
        # Counted, not itemised. The count is safe to log; the identities are not.
        _LOGGER.debug(
            "documents withheld from retrieval",
            extra={"event": "rag.access_filtered", "withheld": withheld},
        )
    return visible


def filter_chunks(chunks: list[Chunk], context: SecurityContext) -> list[Chunk]:
    """Keeps only chunks the reader may see.

    Chunks inherit their document's access rules at chunk time, so filtering
    them again here is a second gate rather than the only one. Redundant
    filtering is cheap; a missed filter is a disclosure.
    """
    return [chunk for chunk in chunks if chunk.access.permits(context)]


def assert_tenant_isolated(
    documents: list[SourceDocument],
    tenant_id: str,
) -> None:
    """Refuses a mixed-tenant result set.

    A result set containing documents from more than one tenant means a filter
    was skipped somewhere upstream. Refusing loudly is better than returning a
    filtered subset and leaving the bug in place.

    Raises:
        InvalidRequestError: If any document belongs to another tenant.
    """
    if not tenant_id.strip():
        raise InvalidRequestError("tenant_id must not be empty", details={"field": "tenant_id"})
    foreign = sorted(
        {doc.access.tenant_id for doc in documents if doc.access.tenant_id != tenant_id}
    )
    if foreign:
        raise InvalidRequestError(
            "retrieval returned documents from another tenant: a filter was skipped upstream",
            details={"expected_tenant": tenant_id, "found": foreign},
        )


def visible_fraction(
    documents: list[SourceDocument],
    context: SecurityContext,
) -> float:
    """What fraction of a set a reader may see.

    Used to detect a systematic misconfiguration: a reader who can see almost
    nothing of a large corpus is probably missing a role grant, which is a
    wiring bug rather than a policy decision. Reports ``1.0`` for an empty set,
    because seeing nothing of nothing is not a failure.
    """
    if not documents:
        return 1.0
    visible = sum(1 for doc in documents if doc.access.permits(context))
    return visible / len(documents)


def union_access(rules: list[DocumentAccess]) -> DocumentAccess:
    """Combines several documents' rules into the strictest.

    Used when a chunk derives from more than one source. The combination is
    restrictive in both directions: roles are intersected, so the reader must
    satisfy every source; denials are unioned, so any source can veto.

    Raises:
        InvalidRequestError: If the rules disagree on tenant, or the list is
            empty. Combining across tenants would produce a rule that grants
            access to neither, which is not the same as a safe one.
    """
    if not rules:
        raise InvalidRequestError(
            "cannot combine an empty set of access rules", details={"field": "rules"}
        )
    tenants = {rule.tenant_id for rule in rules}
    if len(tenants) > 1:
        raise InvalidRequestError(
            "cannot combine access rules from different tenants",
            details={"tenants": sorted(tenants)},
        )

    required: set[str] | None = None
    denied: set[str] = set()
    for rule in rules:
        denied |= rule.denied_principals
        if required is None:
            required = set(rule.required_roles)
        else:
            required &= rule.required_roles

    return DocumentAccess(
        tenant_id=rules[0].tenant_id,
        required_roles=frozenset(required or set()),
        denied_principals=frozenset(denied),
    )
