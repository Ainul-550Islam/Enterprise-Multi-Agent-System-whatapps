-- 0003_identity: users, memberships, api keys, sessions.

CREATE TABLE users (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email              citext NOT NULL UNIQUE,
    display_name       text   NOT NULL CHECK (length(display_name) BETWEEN 1 AND 200),
    status             text   NOT NULL DEFAULT 'pending_activation'
                       CHECK (status IN ('pending_activation', 'active', 'disabled', 'locked')),
    external_subject   text   NULL,
    identity_provider  text   NULL,
    last_login_at      timestamptz NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

-- Users are global identities: app traffic only sees `app.current_user`
-- (self-service); cross-user catalog access goes through BYPASSRLS admin
-- service accounts, never through mas_app.
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE users FORCE ROW LEVEL SECURITY;
CREATE POLICY users_self ON users
    USING (id::text = current_setting('app.current_user', true));

CREATE TABLE memberships (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    organization_id uuid NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    tenant_id       uuid NULL REFERENCES tenants (id) ON DELETE CASCADE,
    roles           jsonb  NOT NULL, -- non-empty JSON array of role strings
    status          text   NOT NULL DEFAULT 'invited'
                    CHECK (status IN ('invited', 'active', 'suspended')),
    invited_at      timestamptz NOT NULL DEFAULT now(),
    joined_at       timestamptz NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK (jsonb_typeof(roles) = 'array' AND jsonb_array_length(roles) > 0),
    CHECK (roles <@ '["owner","admin","developer","operator","viewer","service_account"]'::jsonb),
    -- Exactly one of org-wide / tenant-scoped semantics; org-wide has NULL tenant.
    UNIQUE (user_id, organization_id, tenant_id)
);
CREATE UNIQUE INDEX memberships_org_wide_key ON memberships (user_id, organization_id)
    WHERE tenant_id IS NULL;
CREATE INDEX memberships_user_idx ON memberships (user_id);
CREATE INDEX memberships_tenant_idx ON memberships (tenant_id) WHERE tenant_id IS NOT NULL;
CREATE TRIGGER memberships_updated_at BEFORE UPDATE ON memberships
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE memberships ENABLE ROW LEVEL SECURITY;
ALTER TABLE memberships FORCE ROW LEVEL SECURITY;
CREATE POLICY memberships_isolation ON memberships
    USING (tenant_id::text = current_setting('app.current_tenant', true)
        OR organization_id::text = current_setting('app.current_org', true));

CREATE TABLE api_keys (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    name         text NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    prefix       text NOT NULL, -- public lookup prefix (first 8 key chars)
    secret_hash  text NOT NULL, -- argon2/blake3 hash of the secret; never the secret
    scopes       jsonb NOT NULL DEFAULT '[]'::jsonb,
    status       text NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active', 'revoked', 'expired')),
    expires_at   timestamptz NULL,
    last_used_at timestamptz NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);
CREATE INDEX api_keys_prefix_idx ON api_keys (prefix);
CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id);
CREATE TRIGGER api_keys_updated_at BEFORE UPDATE ON api_keys
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE api_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY api_keys_isolation ON api_keys
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE sessions (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    tenant_id    uuid NULL REFERENCES tenants (id) ON DELETE CASCADE,
    refresh_hash text NOT NULL UNIQUE, -- hash of the refresh token, never the token
    user_agent   text NULL,
    ip_address   inet NULL,
    status       text NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active', 'revoked', 'expired')),
    expires_at   timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sessions_user_idx ON sessions (user_id);
CREATE TRIGGER sessions_updated_at BEFORE UPDATE ON sessions
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions FORCE ROW LEVEL SECURITY;
CREATE POLICY sessions_isolation ON sessions
    USING (user_id::text = current_setting('app.current_user', true)
        OR tenant_id::text = current_setting('app.current_tenant', true));
