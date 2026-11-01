-- PURPOSE: Make the approval schema lossless for the canonical Rust policy
-- aggregate while preserving the queue indexes and append-only vote evidence
-- introduced by 0027.
--
-- 0027 predated the policy aggregate and could only represent UUID user
-- approvers, mandatory expiries, and a single denial spelling. The policy
-- engine deliberately supports service actors, optional expiry, role-gated
-- votes and provenance. `request_payload` stores that complete validated
-- aggregate; indexed legacy columns remain the query/RLS projection.

ALTER TABLE approval_requests
    ADD COLUMN request_payload JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(request_payload) = 'object');

ALTER TABLE approval_requests
    ALTER COLUMN expires_at DROP NOT NULL;

ALTER TABLE approval_votes
    ADD COLUMN approver_actor TEXT NULL,
    ADD COLUMN approver_role TEXT NULL,
    ADD COLUMN vote_at TIMESTAMPTZ NULL;

-- The canonical aggregate permits non-user/service actors. Keep the old UUID
-- column for historical rows, but use `approver_actor` for new policy votes.
ALTER TABLE approval_votes
    ALTER COLUMN approver_id DROP NOT NULL,
    ALTER COLUMN reason DROP NOT NULL;

CREATE UNIQUE INDEX approval_votes_actor_once_idx
    ON approval_votes (tenant_id, request_id, approver_actor)
    WHERE approver_actor IS NOT NULL;

CREATE INDEX approval_requests_payload_status_idx
    ON approval_requests (tenant_id, ((request_payload ->> 'status')), updated_at DESC);
