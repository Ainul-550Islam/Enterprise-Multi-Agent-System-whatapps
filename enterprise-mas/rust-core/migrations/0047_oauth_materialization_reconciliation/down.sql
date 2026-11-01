-- PURPOSE: Reverse migration 0047 materialization-reconciliation queue artifacts.
--
-- Owning layer: database OAuth callback/background-lifecycle security boundary.
--
-- Production role: remove only the reconciliation queue/dead-letter tables and
-- grants during a coordinated rollback after every pending intent is examined.
--
-- Security boundary: this rollback does not infer/delete Vault material or
-- alter credential bindings because that would turn an uncertain outcome into
-- an unsafe destructive action.
--
-- Data ownership: external material and credentials remain in their original
-- owners; only durable reconciliation evidence is removed.
--
-- Prohibited responsibilities: deleting tokens, restoring legacy Vault paths,
-- weakening RLS, or revoking unrelated OAuth lifecycle grants.

REVOKE INSERT ON oauth_token_materialization_dead_letters FROM mas_oauth_refresh;
REVOKE SELECT, UPDATE ON oauth_token_materialization_jobs FROM mas_oauth_refresh;
REVOKE UPDATE (credential_id, state, next_attempt_at, completed_at, updated_at)
    ON oauth_token_materialization_jobs FROM mas_oauth;
REVOKE SELECT (secret_reference_id, tenant_id, connector_id, state)
    ON oauth_token_materialization_jobs FROM mas_oauth;
REVOKE INSERT ON oauth_token_materialization_jobs FROM mas_oauth;

DROP POLICY IF EXISTS oauth_token_materialization_dead_letters_isolation
    ON oauth_token_materialization_dead_letters;
DROP TABLE IF EXISTS oauth_token_materialization_dead_letters;
DROP POLICY IF EXISTS oauth_token_materialization_jobs_isolation
    ON oauth_token_materialization_jobs;
DROP TRIGGER IF EXISTS oauth_token_materialization_jobs_updated_at
    ON oauth_token_materialization_jobs;
DROP TABLE IF EXISTS oauth_token_materialization_jobs;
