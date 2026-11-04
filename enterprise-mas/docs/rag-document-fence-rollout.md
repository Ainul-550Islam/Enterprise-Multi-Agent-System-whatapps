# Data Hub RAG document fence rollout

Migration 0054 introduces durable Data Hub owner records and deletion fences in the RAG plane. It backfills ownership from Rust's document catalogue, including deleted records. The migration login must be able to bypass the catalogue's forced RLS: the migration explicitly fails rather than silently backfilling only some tenants. The RAG login remains restricted to the RAG tables.

**Do not roll this out while old RAG writers are serving traffic.** Older binaries do not consult the fence. Stop all ingestion, source sync, and purge writers; apply the migration; deploy the new orchestrator to every writer; verify readiness and only then resume traffic. Do not roll back to an older writer without first disabling traffic: it could recreate vectors after deletion. A down migration is intentionally not provided because removing tombstones can resurrect deleted documents.

The migration can identify ownership only for documents already present in the Rust catalogue. Existing vector rows with no catalogue document are not automatically claimed. New Data Hub ingest or purge for such an identifier fails closed with an operator-reconciliation error. Inventory these rows before enabling writers and reconcile their ownership through an audited administrative process; do not guess ownership from vector metadata or delete tenant data as an automatic migration step. The following privileged read-only query identifies such rows:

```sql
SELECT c.tenant_id, c.document_id, count(*) AS chunks
FROM document_chunks AS c
LEFT JOIN rag_document_fences AS f
  ON f.tenant_id = c.tenant_id AND f.document_id = c.document_id
WHERE f.document_id IS NULL
GROUP BY c.tenant_id, c.document_id;
```

This query also finds vectors belonging to non-Data-Hub RAG workloads; determine ownership before taking action. In particular, source-sync objects not represented in the Rust catalogue may require reconciliation. New Data Hub claims and purge requests for an identifier with no fence and no existing vectors establish the owner from the authenticated, signed Rust request. The first such operation wins; this is not an independent catalogue lookup. Fence enforcement is implemented by the current RAG adapter and assumes its database credential is not used to issue arbitrary SQL directly.

## Retrieval scope

The pgvector search path filters fenced Data Hub documents by both organization and project **before** ranking and the result limit. Callers must provide both IDs in `SecurityContext` from an already-authorized Rust request scope; missing or mismatched scope hides fenced rows. These IDs are selectors, not authentication: a caller must not take them directly from untrusted input without authorization. Unowned vectors are hidden by default and cannot be returned by Data Hub composition. A separate generic RAG caller may explicitly opt into tenant/role-scoped unowned vectors, but must not use that mode to serve Data Hub data. Reconcile legacy Data Hub rows before expecting them to be searchable. `Retriever` rejects active hybrid search when given a `PgVectorIndex` and a separate lexical index, because that index could return cross-project or deleted documents without consulting the fence. Hybrid retrieval needs an ownership- and tombstone-aware design before it can be enabled for these documents; bypassing this composition guard with a custom wrapper is not safe. Wiring authoritative project scope into every production retrieval caller remains a separate integration requirement.

### S3 list/read consistency

Object synchronization pins each nonempty, size-bounded GET to the ETag from
its listing via a SigV4-signed `If-Match` and also requires the response ETag to
match. A 412, absent/malformed/mismatched ETag, incomplete range response, or
empty object fails the batch instead of checkpointing uncertain content.
Compatible gateways must support strong ETags, conditional GET and the response
ETag; verify these features before enabling the adapter. ETags are opaque
version identifiers, not necessarily content hashes. A dishonest or broken
provider that reuses an ETag for different bytes is not detected by this check.

This does **not** reconcile deletions or safely remove old vectors when an
object becomes oversized or unsupported; older indexed content can remain
searchable. An ETag checkpoint also does not immediately revoke old indexed
ACLs if source policy changes. Until source-wide policy fencing and complete
delete reconciliation exist, operators must explicitly purge affected
Data Hub documents or disable affected retrieval, rather than treating a
successful incremental sync as proof that upstream deletions or ACL changes
have been applied.

### Sync failure and checkpoint handling

S3 listings with empty or out-of-prefix keys, dot-segment keys, or backslash
keys fail before a GET or checkpoint: URL clients and intermediaries can
normalize these into a different object path. Existing buckets containing
such keys must be cleaned or excluded from the configured prefix. The adapter
uses StartAfter for per-item retries and therefore requires strict key ordering
across listing pages; duplicates or out-of-order pages fail instead of silently
skipping objects. AWS general-purpose buckets have that ordering, but S3
directory buckets do not support the required ordering/StartAfter semantics
and are unsupported. See AWS ListObjectsV2 documentation:
https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html.

An oversized S3 object now fails the sync instead of being silently skipped
and checkpointed. Remove, resize, or otherwise remediate the offending object
before retrying; preceding discoveries in the same adapter batch are not
committed when the adapter fails. This alerts operators to a potential stale
index, but does **not** itself remove previously indexed content. Explicitly
purge it or disable retrieval when stale content is unsafe.

When some documents fail indexing, the API returns `partial`, persists the
retry checkpoint, marks the source `error` with `partial_sync`, and records a
failure audit event. `last_synced_at` remains the time of the last fully
successful sync. Explicit retry reactivates the source. Rust and Python now
accept the same 64 KiB maximum encoded cursor; a 64 KiB-plus cursor is rejected
without advancing state. To keep large sources within this bound, the adapter
may evict complete ETag cache entries (never truncate them or resume tokens);
eviction causes safe re-fetching and can increase provider traffic. Bridge counts are checked for consistency. A
successful bounded sync still does not establish complete upstream coverage or
revocation; the other limitations in this document continue to apply.

### Source checkpoint policy binding

Web and S3 sync cursors now bind ETags and resume positions to a SHA-256
fingerprint of the signed source scope, configuration, name and effective
source ACL. A changed snapshot or an old v1/v2 cursor resets the checkpoint,
so a subsequent sync fetches reachable unchanged-ETag documents again under
the current policy. Per-document checkpoints retain the new fingerprint so
failed indexing is retried. Stop old orchestrator writers before rollout;
they cannot read v3 cursors. The first sync after upgrade can re-fetch and
re-index substantially more data.

This is **not immediate revocation**, a source-wide fence, or a complete
reconciliation: old vectors remain readable until successfully replaced or
explicitly purged, out-of-order concurrent syncs can still write stale policy,
and deleted, oversized, unsupported or unreachable content can remain
indexed. Web crawl caps can also prevent visiting all documents. Disable
retrieval or purge affected documents during policy-sensitive changes, and do
not interpret a successful incremental sync as proof of full revocation.

### Retrieval identity and policy handling

Chunk identity in the pgvector schema is `(tenant_id, document_id, chunk_id)`;
the in-memory indexes and fusion now use the same composite identity. Citation
validation now requires source maps keyed by `(document_id, chunk_id)`, not
bare chunk IDs; callers of `validate_citations` must update their maps. The
service checks each returned hit's access independently even when IDs collide.
Combining disjoint nonempty role restrictions fails rather than producing an
open role rule; because a single any-role set cannot express conjunction,
intersection can conservatively reject readers who hold different roles.

These checks do not supply authoritative project ownership to custom indexes,
or can a post-search filter undo scoring or timing leaks caused by a backend
that failed to apply access checks before ranking. Every production backend
must enforce the Data Hub owner/tombstone fence and ACLs before scoring.
`Retriever` allows hybrid lexical search only for the exact in-memory vector
and lexical pair (development/testing); wrapped or custom backends are refused
when hybrid is active. Direct lexical calls and callers bypassing `Retriever`
remain trusted boundaries and are not protected by this guard.


### Data Hub policy validation

New uploads reject unrecognized access-policy fields rather than silently
indexing a document with an unintended open ACL. Source registration and sync
reject unsupported configuration keys and validate the role/denial arrays,
including for previously registered source rows. Uploads also inherit the
source's role and denial restrictions: denial lists are unioned, overlapping
nonempty role lists are conservatively intersected, and disjoint restrictions
are rejected because the current index cannot represent their conjunction.
This can deny a reader who holds different roles satisfying each original
policy. Check existing rows for unknown keys before rollout. This prevents new
misconfigured ingestion but
does not repair historical vectors already indexed under an ignored policy;
those require explicit purge or reindex under a verified policy. See the
OpenAPI Data Hub request bodies for the accepted fields.
