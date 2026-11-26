-- An external subject is meaningful only together with its exact validated
-- issuer. Duplicate existing links make safe login ambiguous, so fail the
-- migration rather than choosing an arbitrary account. Operators must resolve
-- duplicates before rollout. NULL pairs remain unlinked.
CREATE UNIQUE INDEX users_oidc_issuer_subject_unique
    ON users (identity_provider, external_subject)
    WHERE identity_provider IS NOT NULL AND external_subject IS NOT NULL;

-- Pre-scope identity lookup needs no email, profile or write permission.
-- BYPASSRLS remains confined to the separately configured directory role.
GRANT SELECT (id, identity_provider, external_subject, status) ON users TO mas_directory;
