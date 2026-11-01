-- PURPOSE: Reverse migration 0045's OAuth token-cleanup job artifacts and grants.
--
-- Owning layer: database OAuth background-lifecycle security boundary.
--
-- Production role: remove only the durable cleanup queue/dead-letter objects
-- during a coordinated rollback after outstanding token material has been
-- explicitly reconciled.
--
-- Security boundary: this rollback does not delete secret-manager material,
-- alter OAuth credentials, or change canonical secret-reference availability.
--
-- Data ownership: queue evidence is removed only; credential/revocation and
-- secret-manager state remain owned by their original boundaries.
--
-- Prohibited responsibilities: silently deleting live external tokens, dropping
-- shared credential/reference tables, weakening RLS, or revoking unrelated
-- OAuth callback/refresh grants.

REVOKE INSERT ON oauth_token_cleanup_dead_letters FROM mas_oauth_refresh;
REVOKE SELECT, UPDATE ON oauth_token_cleanup_jobs FROM mas_oauth_refresh;
REVOKE INSERT ON oauth_token_cleanup_jobs FROM mas_oauth;

DROP POLICY IF EXISTS oauth_token_cleanup_dead_letters_isolation ON oauth_token_cleanup_dead_letters;
DROP TABLE IF EXISTS oauth_token_cleanup_dead_letters;
DROP POLICY IF EXISTS oauth_token_cleanup_jobs_isolation ON oauth_token_cleanup_jobs;
DROP TRIGGER IF EXISTS oauth_token_cleanup_jobs_updated_at ON oauth_token_cleanup_jobs;
DROP TABLE IF EXISTS oauth_token_cleanup_jobs;
