-- Stop new OIDC verifiers before rollback; they require this column grant.
REVOKE SELECT (id, identity_provider, external_subject, status) ON users FROM mas_directory;
DROP INDEX users_oidc_issuer_subject_unique;
