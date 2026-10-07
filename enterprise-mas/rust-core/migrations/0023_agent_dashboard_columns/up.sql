-- PURPOSE: Add the dashboard baseline and immutable system-agent origin to agents.
--
-- These are Rust-owned agent attributes. The baseline is optional: analytics
-- must show an unknown time-saved value rather than inventing a default.
-- Existing agents are ordinary user-managed agents and backfill to origin=user.

ALTER TABLE agents
    ADD COLUMN baseline_minutes_per_task integer NULL
        CHECK (baseline_minutes_per_task IS NULL OR baseline_minutes_per_task > 0),
    ADD COLUMN origin text NOT NULL DEFAULT 'user'
        CHECK (origin IN ('user', 'system'));

COMMENT ON COLUMN agents.baseline_minutes_per_task IS
    'Operator-provided time-saved baseline; NULL means unknown and is never guessed.';
COMMENT ON COLUMN agents.origin IS
    'Ownership class; system agents are immutable through tenant-facing API routes.';
