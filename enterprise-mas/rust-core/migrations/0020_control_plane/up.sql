-- PURPOSE: Migration 0020 — the enterprise control plane: durable plan
-- definitions, subscription lifecycle history, entitlement and quota
-- overrides, distributed quota counters and reservations, the billing-webhook
-- receipt log, and the idempotency dimension that makes usage exactly-once.
--
-- Owning layer: database schema.
--
-- Production responsibility: move every number that decides whether a tenant
-- may consume a resource out of process memory and into rows the database can
-- enforce. In particular `quota_counters` and `quota_reservations` exist so
-- two replicas cannot each authorise half of an over-limit spend, and
-- `billing_webhook_receipts` exists so a replayed provider callback cannot
-- apply a subscription change twice.
--
-- Data ownership: these tables are Rust-owned platform state. The Python
-- orchestrator never writes them; it reports model/tool usage back through the
-- bridge and Rust performs the accounting.
--
-- It must NOT:
--   • create a counter or reservation table without tenant scope — every
--     table here carries `tenant_id` and sits under FORCE RLS;
--   • allow a partial unique index that would let the same logical work be
--     metered twice;
--   • store a provider secret or a raw card/customer credential; only opaque
--     provider identifiers (`external_customer_id`, `external_subscription_id`)
--     are stored, never keys;
--   • be edited after it has been applied anywhere.

-- ---------------------------------------------------------------------------
-- 1. Plans: durable plan definitions (Phase 13)
--
-- Before this table a plan was a `plan_code` string compared against a
-- `StaticPlanMatrix` compiled into the process. Adding a tier, or negotiating
-- a different token ceiling for one customer, meant a code change and a
-- deploy. The plan is data now.
-- ---------------------------------------------------------------------------

CREATE TABLE plans (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code        text NOT NULL UNIQUE,
    name        text NOT NULL,
    description text NOT NULL DEFAULT '',
    -- Marketing/billing tier metadata. Never consulted by the enforcement
    -- path: enforcement reads plan_entitlements and plan_quotas only.
    spec        jsonb NOT NULL DEFAULT '{}'::jsonb,
    active      boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER plans_updated_at BEFORE UPDATE ON plans
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

-- Plans are platform-wide reference data, not tenant data: a tenant reads the
-- plan its subscription points at, and a tenant that could edit plan
-- definitions could grant itself any limit. There is therefore no tenant_id
-- and no RLS policy — reads go through the subscription, writes are
-- platform-admin only. The absence of a policy is deliberate; do not "fix" it
-- by adding a tenant column.
COMMENT ON TABLE plans IS
    'Platform-wide plan catalogue. Not tenant-scoped by design: enforcement '
    'resolves a plan through the subscription, and tenant-writable plans would '
    'let a tenant mint its own limits.';

-- What a plan unlocks (Phase 13). `numeric_limit NULL` = the feature is on and
-- unbounded; a row absent = the feature is off. Deny-by-default.
CREATE TABLE plan_entitlements (
    plan_id        uuid NOT NULL REFERENCES plans (id) ON DELETE CASCADE,
    feature        text NOT NULL,
    enabled        boolean NOT NULL DEFAULT true,
    numeric_limit  bigint NULL CHECK (numeric_limit IS NULL OR numeric_limit > 0),
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (plan_id, feature)
);

-- What a plan meters (Phase 13). One row per (plan, dimension, period): the
-- plan's ceiling for that meter. The tenant's effective ceiling is this row
-- unless a quota_overrides row says otherwise.
CREATE TABLE plan_quotas (
    plan_id     uuid NOT NULL REFERENCES plans (id) ON DELETE CASCADE,
    dimension   text NOT NULL,
    period      text NOT NULL CHECK (period IN ('per_minute', 'hourly', 'daily', 'monthly', 'unbounded')),
    max_value   bigint NOT NULL CHECK (max_value >= 0),
    enforcement text NOT NULL DEFAULT 'enforce'
                 CHECK (enforcement IN ('enforce', 'warn_only', 'allow_with_overage')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (plan_id, dimension, period)
);

-- ---------------------------------------------------------------------------
-- 2. Subscription lifecycle (Phase 14)
--
-- 0008 created `subscriptions` with a free-text `plan` column and a partial
-- unique index that permits exactly one non-terminal subscription per tenant.
-- This migration is additive: it adds the columns a real lifecycle needs and
-- an append-only history, and it does not rewrite 0008.
-- ---------------------------------------------------------------------------

ALTER TABLE subscriptions
    ADD COLUMN IF NOT EXISTS plan_id                 uuid NULL REFERENCES plans (id),
    ADD COLUMN IF NOT EXISTS state_reason            text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS version                  bigint NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS effective_from           timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS effective_to             timestamptz NULL,
    ADD COLUMN IF NOT EXISTS current_period_start     timestamptz NULL,
    ADD COLUMN IF NOT EXISTS current_period_end       timestamptz NULL,
    ADD COLUMN IF NOT EXISTS renews_at                timestamptz NULL,
    ADD COLUMN IF NOT EXISTS trial_ends_at            timestamptz NULL,
    ADD COLUMN IF NOT EXISTS cancelled_at             timestamptz NULL,
    -- Opaque provider identifiers only. Never a secret, never a card number:
    -- these are the handles used to reconcile against the provider, and
    -- leaking a customer id is survivable where leaking an API key is not.
    ADD COLUMN IF NOT EXISTS external_customer_id     text NULL,
    ADD COLUMN IF NOT EXISTS external_subscription_id text NULL;

-- A tenant must not be able to end the month on two active subscriptions.
-- 0008 already created `subscriptions_tenant_active_key` over
-- (tenant_id) WHERE state IN ('trialing','active','past_due'); this adds the
-- equivalent guard for the one-live-subscription invariant including the
-- external reference, so a duplicated provider callback cannot attach a second
-- live row.
CREATE UNIQUE INDEX IF NOT EXISTS subscriptions_external_sub_key
    ON subscriptions (external_subscription_id)
    WHERE external_subscription_id IS NOT NULL;

-- Append-only lifecycle history (Phase 14 "audit history", Phase 26).
-- Every state change writes a row here inside the same transaction that
-- changes the subscription, so the history cannot drift from the state.
CREATE TABLE subscription_events (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id uuid NOT NULL,
    tenant_id      uuid NOT NULL,
    from_state     text NULL,
    to_state       text NOT NULL,
    reason         text NOT NULL DEFAULT '',
    actor_id       text NOT NULL DEFAULT 'system',
    actor_kind     text NOT NULL DEFAULT 'system',
    -- Set when the change originated from a provider callback, so an operator
    -- can tell "support cancelled this" from "Stripe told us to".
    external_event_id text NULL,
    correlation_id text NOT NULL DEFAULT '',
    occurred_at    timestamptz NOT NULL DEFAULT now(),
    -- Monotonic within the table. Two transitions can share an `occurred_at`
    -- (the same request, or a clock coarser than the gap between them); when
    -- they do, the history's order must still be the order they happened, not
    -- an artefact of physical row order.
    seq            bigserial NOT NULL
);
CREATE INDEX IF NOT EXISTS subscription_events_tenant_idx
    ON subscription_events (tenant_id, occurred_at DESC, seq DESC);
CREATE INDEX IF NOT EXISTS subscription_events_subscription_idx
    ON subscription_events (subscription_id, occurred_at DESC);

-- The history is evidence, not state: it is never amended, only appended.
CREATE TRIGGER subscription_events_no_update BEFORE UPDATE ON subscription_events
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();
CREATE TRIGGER subscription_events_no_delete BEFORE DELETE ON subscription_events
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();

ALTER TABLE subscription_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_events FORCE ROW LEVEL SECURITY;
CREATE POLICY subscription_events_isolation ON subscription_events
    USING (tenant_id::text = current_setting('app.current_tenant', true));

-- ---------------------------------------------------------------------------
-- 3. Enterprise overrides (Phase 20)
--
-- A negotiated limit must never be applied by editing the plan: the plan is
-- shared by every tenant on that tier, and a row changed for one customer
-- silently re-prices all the others. Overrides are separate rows that name
-- the tenant, the value, the reason, the actor and the effective period.
-- ---------------------------------------------------------------------------

CREATE TABLE entitlement_overrides (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    feature        text NOT NULL,
    enabled        boolean NOT NULL DEFAULT true,
    numeric_limit  bigint NULL CHECK (numeric_limit IS NULL OR numeric_limit > 0),
    -- Why this override exists. Mandatory: an override nobody can justify is
    -- an override nobody can remove.
    reason         text NOT NULL,
    actor_id       text NOT NULL,
    actor_kind     text NOT NULL DEFAULT 'user',
    effective_from timestamptz NOT NULL DEFAULT now(),
    effective_to   timestamptz NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT entitlement_overrides_period_check
        CHECK (effective_to IS NULL OR effective_to > effective_from)
);
-- One live override per (tenant, feature). Two concurrent grants of the same
-- override would make "which limit applies?" depend on row order.
CREATE UNIQUE INDEX IF NOT EXISTS entitlement_overrides_live_key
    ON entitlement_overrides (tenant_id, feature)
    WHERE effective_to IS NULL;
CREATE INDEX IF NOT EXISTS entitlement_overrides_effective_idx
    ON entitlement_overrides (tenant_id, feature, effective_from DESC);
CREATE TRIGGER entitlement_overrides_updated_at BEFORE UPDATE ON entitlement_overrides
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE entitlement_overrides ENABLE ROW LEVEL SECURITY;
ALTER TABLE entitlement_overrides FORCE ROW LEVEL SECURITY;
CREATE POLICY entitlement_overrides_isolation ON entitlement_overrides
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE quota_overrides (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    dimension      text NOT NULL,
    period         text NOT NULL CHECK (period IN ('per_minute', 'hourly', 'daily', 'monthly', 'unbounded')),
    max_value      bigint NOT NULL CHECK (max_value >= 0),
    enforcement    text NOT NULL DEFAULT 'enforce'
                   CHECK (enforcement IN ('enforce', 'warn_only', 'allow_with_overage')),
    reason         text NOT NULL,
    actor_id       text NOT NULL,
    actor_kind     text NOT NULL DEFAULT 'user',
    effective_from timestamptz NOT NULL DEFAULT now(),
    effective_to   timestamptz NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT quota_overrides_effective_period_check
        CHECK (effective_to IS NULL OR effective_to > effective_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS quota_overrides_live_key
    ON quota_overrides (tenant_id, dimension, period)
    WHERE effective_to IS NULL;
CREATE INDEX IF NOT EXISTS quota_overrides_effective_idx
    ON quota_overrides (tenant_id, dimension, period, effective_from DESC);
CREATE TRIGGER quota_overrides_updated_at BEFORE UPDATE ON quota_overrides
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE quota_overrides ENABLE ROW LEVEL SECURITY;
ALTER TABLE quota_overrides FORCE ROW LEVEL SECURITY;
CREATE POLICY quota_overrides_isolation ON quota_overrides
    USING (tenant_id::text = current_setting('app.current_tenant', true));

-- ---------------------------------------------------------------------------
-- 4. Distributed quota counters (Phase 6, Phase 7)
--
-- This is the table that replaces the process-local `Mutex<BTreeMap<..>>`.
-- Two replicas reserving 70 and 50 against a limit of 100 must not both
-- succeed; that guarantee comes from the UPSERT ... RETURNING on this row,
-- which lets Postgres arbitrate in a single statement.
--
-- `bucket` is the fixed-window index (`unix_seconds / window_seconds`);
-- bucket 0 is the gauge used by non-resetting dimensions (concurrent runs,
-- storage bytes).
-- ---------------------------------------------------------------------------

CREATE TABLE quota_counters (
    tenant_id   uuid NOT NULL,
    dimension   text NOT NULL,
    bucket      bigint NOT NULL,
    value       bigint NOT NULL DEFAULT 0 CHECK (value >= 0),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, dimension, bucket)
);
CREATE INDEX IF NOT EXISTS quota_counters_gc_idx
    ON quota_counters (bucket)
    WHERE bucket > 0;

COMMENT ON CONSTRAINT quota_counters_value_check ON quota_counters IS
    'A counter may never go negative. This is the last line of defence behind '
    'the saturating decrement: if application logic ever produced a negative '
    'release, the transaction aborts rather than silently minting headroom.';

ALTER TABLE quota_counters ENABLE ROW LEVEL SECURITY;
ALTER TABLE quota_counters FORCE ROW LEVEL SECURITY;
CREATE POLICY quota_counters_isolation ON quota_counters
    USING (tenant_id::text = current_setting('app.current_tenant', true));

-- ---------------------------------------------------------------------------
-- 5. Quota reservations (Phase 8, Phase 31)
--
-- check → reserve → execute → settle actual → release the unused remainder.
-- A reservation that outlives its lease is reclaimed by the sweeper, so a
-- process that dies mid-execution does not consume quota forever.
--
-- `reservation_key` is the idempotency dimension: a caller that retries with
-- the same key gets the same reservation back rather than a second reservation
-- for the same logical work.
-- ---------------------------------------------------------------------------

CREATE TABLE quota_reservations (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid NOT NULL,
    dimension       text NOT NULL,
    bucket          bigint NOT NULL,
    reserved_amount bigint NOT NULL CHECK (reserved_amount > 0),
    settled_amount  bigint NOT NULL DEFAULT 0 CHECK (settled_amount >= 0),
    state           text NOT NULL DEFAULT 'held'
                    CHECK (state IN ('held', 'settled', 'released', 'expired')),
    -- Caller-supplied idempotency key. Unique per tenant so a replay returns
    -- the original reservation instead of reserving twice.
    reservation_key text NOT NULL,
    execution_id    uuid NULL,
    task_id         uuid NULL,
    -- When an un-settled reservation becomes reclaimable. A held reservation
    -- with a lease in the past is invisible to the accounting and is returned
    -- to the tenant by the sweeper.
    lease_expires_at timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    settled_at      timestamptz NULL,
    CONSTRAINT quota_reservations_settle_bounded_check
        CHECK (settled_amount <= reserved_amount)
);
CREATE UNIQUE INDEX IF NOT EXISTS quota_reservations_key_idx
    ON quota_reservations (tenant_id, reservation_key);
CREATE INDEX IF NOT EXISTS quota_reservations_reclaim_idx
    ON quota_reservations (state, lease_expires_at)
    WHERE state = 'held';
CREATE INDEX IF NOT EXISTS quota_reservations_execution_idx
    ON quota_reservations (execution_id)
    WHERE execution_id IS NOT NULL;
CREATE TRIGGER quota_reservations_updated_at BEFORE UPDATE ON quota_reservations
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE quota_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE quota_reservations FORCE ROW LEVEL SECURITY;
CREATE POLICY quota_reservations_isolation ON quota_reservations
    USING (tenant_id::text = current_setting('app.current_tenant', true));

-- ---------------------------------------------------------------------------
-- 6. Billing boundary (Phase 17, Phase 19)
--
-- The platform execution ledger stays authoritative for whether an execution
-- happened. This table records what the *provider* told us, so the two can be
-- reconciled — and so a replayed callback is recognised as a replay.
-- ---------------------------------------------------------------------------

CREATE TABLE billing_webhook_receipts (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider       text NOT NULL,
    -- The provider's event id, preserved verbatim. This is what makes the
    -- receipt idempotent: the same event delivered twice collides here.
    event_id       text NOT NULL,
    event_type     text NOT NULL DEFAULT '',
    -- SHA-256 of the raw body, so a replay with a mutated payload is
    -- detected rather than trusted.
    payload_hash   text NOT NULL,
    signature_valid boolean NOT NULL DEFAULT false,
    state          text NOT NULL DEFAULT 'received'
                   CHECK (state IN ('received', 'applied', 'ignored', 'rejected')),
    tenant_id      uuid NULL,
    -- Set when the tenant could not be resolved from server-side truth. An
    -- unsigned callback that names a tenant must never be believed.
    rejection_reason text NULL,
    received_at    timestamptz NOT NULL DEFAULT now(),
    processed_at   timestamptz NULL,
    CONSTRAINT billing_webhook_receipts_hash_len
        CHECK (length(payload_hash) = 64)
);
-- Exactly one receipt per (provider, event_id). This is the anti-replay
-- constraint: a duplicate delivery fails the insert and the caller treats that
-- as "already applied", not as a new event.
CREATE UNIQUE INDEX IF NOT EXISTS billing_webhook_receipts_event_key
    ON billing_webhook_receipts (provider, event_id);
CREATE INDEX IF NOT EXISTS billing_webhook_receipts_received_idx
    ON billing_webhook_receipts (received_at DESC);

-- Receipts are evidence, so the *verdict* is append-only: a receipt that says
-- the signature was invalid can never be edited into one that says it was
-- valid. But a receipt also tracks processing, which does move — received →
-- applied or ignored — and a blanket append-only trigger would forbid the one
-- transition the design requires.
--
-- `mas_forbid_mutation()` is therefore not used here. This trigger admits the
-- single legal forward step and refuses everything else, which is the actual
-- guarantee: no receipt is ever edited into a different verdict, and none is
-- re-opened once it has been decided.
CREATE OR REPLACE FUNCTION mas_billing_receipt_forward_only() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    -- The verdict columns are immutable once written.
    IF NEW.signature_valid IS DISTINCT FROM OLD.signature_valid
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.provider IS DISTINCT FROM OLD.provider
       OR NEW.event_id IS DISTINCT FROM OLD.event_id THEN
        RAISE EXCEPTION
            'billing_webhook_receipts: the verdict (provider, event_id, payload_hash, signature_valid) is immutable';
    END IF;

    -- Only received/ignored may move, and only to a terminal state. A
    -- duplicate finish is a no-op, not a second state change.
    IF OLD.state IN ('applied', 'rejected') AND NEW.state IS DISTINCT FROM OLD.state THEN
        RAISE EXCEPTION
            'billing_webhook_receipts: state % is terminal and cannot become %',
            OLD.state, NEW.state;
    END IF;

    IF OLD.state = 'received' AND NEW.state NOT IN ('received', 'applied', 'ignored', 'rejected') THEN
        RAISE EXCEPTION
            'billing_webhook_receipts: unknown state %', NEW.state;
    END IF;

    -- processed_at is stamped once, on the transition out of `received`.
    IF NEW.state <> 'received' AND OLD.processed_at IS NULL THEN
        NEW.processed_at := now();
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER billing_webhook_receipts_forward_only
    BEFORE UPDATE ON billing_webhook_receipts
    FOR EACH ROW EXECUTE FUNCTION mas_billing_receipt_forward_only();

-- Deletion is still forbidden outright: destroying the evidence that a
-- provider event arrived is how a replay becomes undetectable.
CREATE TRIGGER billing_webhook_receipts_no_delete BEFORE DELETE ON billing_webhook_receipts
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();

-- Deliberately no RLS: the webhook is processed before a tenant is known, and
-- the whole point of the receipt log is to survive a callback that names a
-- tenant it is not entitled to name. Reads are platform-admin only.
COMMENT ON TABLE billing_webhook_receipts IS
    'Provider callback log. Not tenant-scoped by design: a receipt is written '
    'before the tenant is resolved, and a rejected receipt for the wrong '
    'tenant is exactly the evidence an incident review needs.';

-- ---------------------------------------------------------------------------
-- 7. Exactly-once metering (Phase 11)
--
-- 0014 already created `usage_records_task_dimension_idx`, UNIQUE
-- (task_id, resource) WHERE task_id IS NOT NULL. That covers the retry path:
-- the same task settled twice writes one row.
--
-- What it does not cover is usage that has no task — platform-initiated
-- metering, and usage attributed to an execution before a task exists. This
-- adds the explicit metering key for that case, plus the attribution columns
-- Phase 10 requires (organization, project, attempt, model/provider, cost).
-- ---------------------------------------------------------------------------

ALTER TABLE usage_records
    ADD COLUMN IF NOT EXISTS organization_id uuid NULL,
    ADD COLUMN IF NOT EXISTS project_id      uuid NULL,
    ADD COLUMN IF NOT EXISTS task_attempt_id uuid NULL,
    ADD COLUMN IF NOT EXISTS model           text NULL,
    ADD COLUMN IF NOT EXISTS provider        text NULL,
    ADD COLUMN IF NOT EXISTS input_quantity  bigint NULL CHECK (input_quantity IS NULL OR input_quantity >= 0),
    ADD COLUMN IF NOT EXISTS output_quantity bigint NULL CHECK (output_quantity IS NULL OR output_quantity >= 0),
    ADD COLUMN IF NOT EXISTS duration_ms     bigint NULL CHECK (duration_ms IS NULL OR duration_ms >= 0),
    -- Cost is stored as integer micro-units of the billing currency. Never a
    -- float: 0.1 + 0.2 is not 0.3, and an invoice that is wrong in the third
    -- decimal place is a support ticket.
    ADD COLUMN IF NOT EXISTS cost_micros     bigint NULL CHECK (cost_micros IS NULL OR cost_micros >= 0),
    ADD COLUMN IF NOT EXISTS cost_currency   text NULL,
    ADD COLUMN IF NOT EXISTS estimated       boolean NOT NULL DEFAULT false,
    -- Caller-supplied idempotency key for usage with no task to key on.
    ADD COLUMN IF NOT EXISTS metering_key    text NULL;

CREATE UNIQUE INDEX IF NOT EXISTS usage_records_metering_key_idx
    ON usage_records (tenant_id, metering_key)
    WHERE metering_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS usage_records_project_idx
    ON usage_records (tenant_id, project_id, occurred_at DESC)
    WHERE project_id IS NOT NULL;

COMMENT ON COLUMN usage_records.estimated IS
    'TRUE when the quantity is a model-reported figure the platform has not '
    'independently verified. Estimated usage is metered and billed, but it is '
    'never silently presented as measured.';

-- ---------------------------------------------------------------------------
-- 7b. Plan lifecycle (Phase 13)
--
-- 0008 gave `plans` a boolean `active`. A boolean cannot express the lifecycle
-- the domain model carries — a retired plan is still referenced by every
-- subscription that ever used it, so it must remain readable while being
-- unsellable, and "archived" is a third state again. Replacing the boolean
-- with the status column keeps one representation; leaving both would let a
-- row be `active = true, status = 'retired'` with no way to say which wins.
-- ---------------------------------------------------------------------------

ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'retired', 'archived'));

-- Carry the existing rows across, so the column is never briefly all-'active'
-- for plans that were already withdrawn.
-- Idempotent: on a re-run the status column already exists with real values,
-- and re-deriving it from `active` is impossible once `active` is gone, so the
-- backfill is scoped to rows still sitting on the default.
UPDATE plans SET status = CASE WHEN active THEN 'active' ELSE 'retired' END
 WHERE status = 'active' AND active IS DISTINCT FROM NULL AND NOT active IS NULL;

-- `active` is deliberately NOT dropped. This codebase forbids destructive
-- migrations, and dropping a column is exactly the thing that rule exists to
-- stop. Instead the legacy column becomes a derived view of the new one: a
-- trigger keeps it equal to `status = 'active'`, so the two can never
-- disagree, and any code still reading `active` keeps working correctly while
-- `status` is the single source of truth.
CREATE OR REPLACE FUNCTION mas_plans_sync_active() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    NEW.active := (NEW.status = 'active');
    RETURN NEW;
END;
$$;

CREATE TRIGGER plans_sync_active BEFORE INSERT OR UPDATE ON plans
    FOR EACH ROW EXECUTE FUNCTION mas_plans_sync_active();

-- Anything already in the table is brought into line, so no row is left with
-- the two columns contradicting each other.
UPDATE plans SET status = status WHERE true;

-- The domain models `description` as optional. 0008 declared it NOT NULL with
-- a '' default, which forced a caller with no description to write a value it
-- did not have and read back `Some("")` for the `None` it passed in.
ALTER TABLE plans ALTER COLUMN description DROP NOT NULL;

CREATE INDEX IF NOT EXISTS plans_status_idx ON plans (status, code);

-- The plan a newly-activated tenant lands on. It is seeded here rather than
-- being a constant the Rust code special-cases: what it grants is commercial
-- data, and an operator must be able to change it without a release.
--
-- It grants the capabilities a tenant needs to do ordinary work and nothing
-- from the premium surface (advanced models, high token budgets, premium
-- connectors, RAG, long audit retention, high concurrency). A tenant that has
-- bought those has an override or a different plan.
INSERT INTO plans (code, name, description, status)
VALUES ('developer', 'Developer', 'Base plan granted on tenant activation', 'active')
    ON CONFLICT (code) DO NOTHING;

INSERT INTO plan_entitlements (plan_id, feature, enabled)
SELECT p.id, f.feature, true
  FROM plans p
  CROSS JOIN (VALUES ('workflows'), ('tool_invocation'), ('scheduled_execution')) AS f(feature)
 WHERE p.code = 'developer'
   AND NOT EXISTS (
       SELECT 1 FROM plan_entitlements pe
        WHERE pe.plan_id = p.id AND pe.feature = f.feature
   );

-- ---------------------------------------------------------------------------
-- 8. Policy provenance (Phase 3, Phase 4)
--
-- `policies` from 0007 holds the authored definition but has no organisation
-- or project column, so a policy could only ever be tenant-scoped. The
-- evaluator already understands PolicyScope::Organization and
-- PolicyScope::Project; the table could not store them.
-- ---------------------------------------------------------------------------

ALTER TABLE policies
    ADD COLUMN IF NOT EXISTS description    text NULL,
    ADD COLUMN IF NOT EXISTS organization_id uuid NULL,
    ADD COLUMN IF NOT EXISTS project_id      uuid NULL,
    ADD COLUMN IF NOT EXISTS published_at    timestamptz NULL,
    ADD COLUMN IF NOT EXISTS published_by    text NULL,
    ADD COLUMN IF NOT EXISTS actor_id        text NOT NULL DEFAULT 'system',
    -- Evaluation priority (higher wins on conflict). 0007 stored the policy
    -- spec as opaque jsonb and left ordering implicit, which made the result
    -- of two conflicting rules depend on row order. The domain model has
    -- carried `priority` since the engine was written; the table did not.
    ADD COLUMN IF NOT EXISTS priority        integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS version         bigint NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS scope           text NOT NULL DEFAULT 'tenant',
    ADD COLUMN IF NOT EXISTS compiled_checksum text NULL;

-- The policy *type* and *body* are 0007's `kind` and `spec`. They are not
-- re-homed: one column per concept, or the two copies disagree and nobody can
-- say which one the evaluator read. `active` is likewise not a column — it is
-- `status = 'active'`, so a policy can never be active and disabled at once.

-- One published version per (tenant, name): an operator renaming a policy
-- must not be able to leave two active policies with the same identity.
CREATE UNIQUE INDEX IF NOT EXISTS policies_tenant_name_key
    ON policies (COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::uuid), name)
    WHERE status = 'active';
CREATE INDEX IF NOT EXISTS policies_applicable_idx
    ON policies (tenant_id, organization_id, project_id, status, priority DESC);

-- ---------------------------------------------------------------------------
-- 9. Quota definitions, tenant-scoped (Phase 6)
--
-- `quotas` from 0008 keys on (tenant_id, scope, resource, window) using free
-- text. The runtime engine keys on (tenant_id, dimension, period). This table
-- is the runtime shape; quota_overrides layers on top of it.
-- ---------------------------------------------------------------------------

CREATE TABLE quota_definitions (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    dimension   text NOT NULL,
    period      text NOT NULL CHECK (period IN ('per_minute', 'hourly', 'daily', 'monthly', 'unbounded')),
    max_value   bigint NOT NULL CHECK (max_value >= 0),
    enforcement text NOT NULL DEFAULT 'enforce'
                 CHECK (enforcement IN ('enforce', 'warn_only', 'allow_with_overage')),
    -- Where this definition came from: derived from the plan, or set
    -- directly for this tenant.
    source      text NOT NULL DEFAULT 'tenant'
                 CHECK (source IN ('plan', 'tenant')),
    actor_id    text NOT NULL DEFAULT 'system',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, dimension, period)
);
CREATE TRIGGER quota_definitions_updated_at BEFORE UPDATE ON quota_definitions
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE quota_definitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE quota_definitions FORCE ROW LEVEL SECURITY;
CREATE POLICY quota_definitions_isolation ON quota_definitions
    USING (tenant_id::text = current_setting('app.current_tenant', true));
