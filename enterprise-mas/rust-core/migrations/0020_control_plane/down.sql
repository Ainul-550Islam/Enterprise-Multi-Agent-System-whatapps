-- PURPOSE: Migration 0020 (down) — reverses the control-plane schema added by
-- `up.sql`.
--
-- Owning layer: database schema.
--
-- Production responsibility: leave the database exactly as it was before
-- 0020 applied, so a failed rollout can be reverted without manual surgery.
-- Objects are dropped in reverse dependency order; the additive ALTER TABLE
-- columns are dropped explicitly because `DROP TABLE` alone would leave them
-- behind on 0008's tables.
--
-- It must NOT:
--   • be run against a database that has already metered billable usage —
--     dropping `usage_records` columns destroys accounting evidence. This
--     down-migration exists for pre-production rollback and integration
--     tests, not for unwinding a live billing period.

DROP TRIGGER IF EXISTS quota_definitions_updated_at ON quota_definitions;
DROP POLICY IF EXISTS quota_definitions_isolation ON quota_definitions;
DROP TABLE IF EXISTS quota_definitions;

DROP INDEX IF EXISTS policies_applicable_idx;
DROP INDEX IF EXISTS policies_tenant_name_key;
ALTER TABLE policies
    DROP COLUMN IF EXISTS compiled_checksum,
    DROP COLUMN IF EXISTS definition,
    DROP COLUMN IF EXISTS scope,
    DROP COLUMN IF EXISTS policy_type,
    DROP COLUMN IF EXISTS version,
    DROP COLUMN IF EXISTS priority,
    DROP COLUMN IF EXISTS actor_id,
    DROP COLUMN IF EXISTS published_by,
    DROP COLUMN IF EXISTS published_at,
    DROP COLUMN IF EXISTS project_id,
    DROP COLUMN IF EXISTS organization_id;

DROP INDEX IF EXISTS usage_records_project_idx;
DROP INDEX IF EXISTS usage_records_metering_key_idx;
ALTER TABLE usage_records
    DROP COLUMN IF EXISTS metering_key,
    DROP COLUMN IF EXISTS estimated,
    DROP COLUMN IF EXISTS cost_currency,
    DROP COLUMN IF EXISTS cost_micros,
    DROP COLUMN IF EXISTS duration_ms,
    DROP COLUMN IF EXISTS output_quantity,
    DROP COLUMN IF EXISTS input_quantity,
    DROP COLUMN IF EXISTS provider,
    DROP COLUMN IF EXISTS model,
    DROP COLUMN IF EXISTS task_attempt_id,
    DROP COLUMN IF EXISTS project_id,
    DROP COLUMN IF EXISTS organization_id;

DROP TRIGGER IF EXISTS billing_webhook_receipts_no_delete ON billing_webhook_receipts;
DROP TRIGGER IF EXISTS billing_webhook_receipts_no_update ON billing_webhook_receipts;
DROP INDEX IF EXISTS billing_webhook_receipts_received_idx;
DROP INDEX IF EXISTS billing_webhook_receipts_event_key;
DROP TABLE IF EXISTS billing_webhook_receipts;

DROP TRIGGER IF EXISTS quota_reservations_updated_at ON quota_reservations;
DROP POLICY IF EXISTS quota_reservations_isolation ON quota_reservations;
DROP INDEX IF EXISTS quota_reservations_execution_idx;
DROP INDEX IF EXISTS quota_reservations_reclaim_idx;
DROP INDEX IF EXISTS quota_reservations_key_idx;
DROP TABLE IF EXISTS quota_reservations;

DROP POLICY IF EXISTS quota_counters_isolation ON quota_counters;
DROP INDEX IF EXISTS quota_counters_gc_idx;
DROP TABLE IF EXISTS quota_counters;

DROP TRIGGER IF EXISTS quota_overrides_updated_at ON quota_overrides;
DROP POLICY IF EXISTS quota_overrides_isolation ON quota_overrides;
DROP INDEX IF EXISTS quota_overrides_effective_idx;
DROP INDEX IF EXISTS quota_overrides_live_key;
DROP TABLE IF EXISTS quota_overrides;

DROP TRIGGER IF EXISTS entitlement_overrides_updated_at ON entitlement_overrides;
DROP POLICY IF EXISTS entitlement_overrides_isolation ON entitlement_overrides;
DROP INDEX IF EXISTS entitlement_overrides_effective_idx;
DROP INDEX IF EXISTS entitlement_overrides_live_key;
DROP TABLE IF EXISTS entitlement_overrides;

DROP TRIGGER IF EXISTS subscription_events_no_delete ON subscription_events;
DROP TRIGGER IF EXISTS subscription_events_no_update ON subscription_events;
DROP POLICY IF EXISTS subscription_events_isolation ON subscription_events;
DROP INDEX IF EXISTS subscription_events_subscription_idx;
DROP INDEX IF EXISTS subscription_events_tenant_idx;
DROP TABLE IF EXISTS subscription_events;

DROP INDEX IF EXISTS subscriptions_external_sub_key;
ALTER TABLE subscriptions
    DROP COLUMN IF EXISTS external_subscription_id,
    DROP COLUMN IF EXISTS external_customer_id,
    DROP COLUMN IF EXISTS cancelled_at,
    DROP COLUMN IF EXISTS trial_ends_at,
    DROP COLUMN IF EXISTS renews_at,
    DROP COLUMN IF EXISTS current_period_end,
    DROP COLUMN IF EXISTS current_period_start,
    DROP COLUMN IF EXISTS effective_to,
    DROP COLUMN IF EXISTS effective_from,
    DROP COLUMN IF EXISTS version,
    DROP COLUMN IF EXISTS state_reason,
    DROP COLUMN IF EXISTS plan_id;

DROP TABLE IF EXISTS plan_quotas;
DROP TABLE IF EXISTS plan_entitlements;
DROP TRIGGER IF EXISTS plans_updated_at ON plans;
DROP TABLE IF EXISTS plans;

DROP FUNCTION IF EXISTS mas_billing_receipt_forward_only() CASCADE;

DROP TRIGGER IF EXISTS plans_sync_active ON plans;
DROP FUNCTION IF EXISTS mas_plans_sync_active();
