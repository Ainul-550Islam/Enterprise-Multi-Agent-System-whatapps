-- PURPOSE: Enforce Data Hub parent links inside the same tenant/project.
--
-- UUID primary keys alone prove existence, not ownership. The Rust service
-- checks these links, but a database constraint is required so a mistaken
-- repository or future worker cannot attach a source/document to another
-- tenant's project or credential.

ALTER TABLE projects
    ADD CONSTRAINT projects_tenant_id_unique UNIQUE (tenant_id, id);

ALTER TABLE credentials
    ADD CONSTRAINT credentials_tenant_id_unique UNIQUE (tenant_id, id);

ALTER TABLE data_sources
    ADD CONSTRAINT data_sources_tenant_project_id_unique UNIQUE (tenant_id, project_id, id),
    ADD CONSTRAINT data_sources_tenant_project_fk
        FOREIGN KEY (tenant_id, project_id)
        REFERENCES projects (tenant_id, id)
        ON DELETE CASCADE,
    ADD CONSTRAINT data_sources_tenant_credential_fk
        FOREIGN KEY (tenant_id, credential_id)
        REFERENCES credentials (tenant_id, id)
        ON DELETE RESTRICT;

ALTER TABLE documents
    ADD CONSTRAINT documents_tenant_project_fk
        FOREIGN KEY (tenant_id, project_id)
        REFERENCES projects (tenant_id, id)
        ON DELETE CASCADE,
    ADD CONSTRAINT documents_tenant_project_source_fk
        FOREIGN KEY (tenant_id, project_id, source_id)
        REFERENCES data_sources (tenant_id, project_id, id)
        ON DELETE CASCADE;
