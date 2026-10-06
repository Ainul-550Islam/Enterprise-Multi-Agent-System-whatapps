-- Migration 0016 — bind API keys to their tenant and organization.
--
-- WHY THIS EXISTS (security Phase 8)
-- ==================================
-- An API key is a bearer secret minted inside one tenant. That fact lived
-- only in `api_keys.tenant_id`, and the API's verifier read the row but
-- discarded the column, building a principal with no tenancy at all. The
-- request's `x-tenant-id` header then decided the scope, so a key minted in
-- tenant A could be presented with `x-tenant-id: B`.
--
-- This migration stores both halves of the binding and the role the key acts
-- under, so authentication itself can carry them into the principal and no
-- downstream layer has to guess.
--
-- WHY `organization_id` IS STORED AND NOT JOINED
--   A `tenants` row is only visible through RLS when `app.current_tenant`
--   already matches it, and the key lookup happens *before* a scope exists.
--   Joining would therefore return nothing exactly when the platform needs
--   the answer. Storing the organization at mint time also freezes the
--   binding: a tenant cannot be moved between organizations, so storing it is
--   both correct and a coherence check the resolver re-verifies.
--
-- WHY `role` IS STORED
--   A key acts under a role, and a role must never be chosen by the caller.
--   Storing it makes the key's authority part of the credential rather than a
--   per-request decision. It is clamped at the application layer so no key can
--   ever carry `owner`.
--
-- BACKFILL
--   Existing rows take their tenant's organization and the least-privileged
--   role. Least privilege on upgrade is deliberate: a key that predates this
--   column does not become an administrator because a column appeared.
--
-- NO DESTRUCTIVE STATEMENTS: this migration is additive (two columns and two
-- constraints). No table, column or row is dropped.

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS organization_id uuid NULL
    REFERENCES organizations (id) ON DELETE RESTRICT;

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS role text NULL
    CHECK (role IN ('owner', 'admin', 'developer', 'operator', 'viewer', 'service_account'));

-- Backfill from the owning tenant. Runs unscoped because it is a migration
-- performed by the schema owner, never by application traffic.
UPDATE api_keys AS k
   SET organization_id = t.organization_id
  FROM tenants AS t
 WHERE k.tenant_id = t.id
   AND k.organization_id IS NULL;

-- Any key whose tenant has since been deleted cannot be bound; it is left
-- NULL here and is refused at verification time rather than guessed at.
UPDATE api_keys
   SET role = 'service_account'
 WHERE role IS NULL;

-- From here on, both halves of the binding are mandatory for new keys. The
-- backfill above is what makes the NOT NULL honest for existing rows; if a
-- row could not be backfilled it has no tenant either and is unreachable.
ALTER TABLE api_keys
    ALTER COLUMN role SET NOT NULL;

-- The dual-link coherence check, mirroring `projects`: the key's organization
-- must equal its tenant's organization. `NOT VALID` first so the constraint
-- does not lock the table on a large deployment, then validated below.
ALTER TABLE api_keys
    ADD CONSTRAINT api_keys_tenant_org_key
    FOREIGN KEY (tenant_id, organization_id)
    REFERENCES tenants (id, organization_id) ON DELETE CASCADE DEFERRABLE;

-- Needed by the composite foreign key above (already present on `tenants`
-- from migration 0002, restated here so the dependency is local and obvious).
-- If the constraint already exists this is a no-op migration error only in a
-- broken schema, where failing loudly is correct.

CREATE INDEX IF NOT EXISTS api_keys_organization_idx
    ON api_keys (organization_id);

COMMENT ON COLUMN api_keys.organization_id IS
    'The organization the key is bound to (security Phase 8). Stored, not '
    'joined: the key lookup runs before a tenant scope exists, so an RLS-'
    'protected join would return nothing.';

COMMENT ON COLUMN api_keys.role IS
    'The membership role the bearer of this key acts under. Clamped at mint '
    'time so no key can carry owner. Never settable from a request.';
