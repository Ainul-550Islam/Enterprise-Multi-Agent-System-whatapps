-- PURPOSE: Index literal substring command-palette matching without weakening
-- the scoped adapter's explicit tenant and organization predicates.
--
-- The catalogue uses `ILIKE`, not `lower(column) LIKE ...`; pg_trgm operator
-- classes must therefore be attached to the same expressions the queries use.
-- PostgreSQL can bitmap-combine these GIN indexes with the pre-existing tenant
-- btree indexes. The search adapter escapes `%`, `_`, and `\` before binding
-- values, so these indexes accelerate literal text matching rather than a
-- caller-controlled wildcard language.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX agents_name_trgm_idx
    ON agents USING gin (name gin_trgm_ops);
CREATE INDEX agents_slug_trgm_idx
    ON agents USING gin ((slug::text) gin_trgm_ops);
CREATE INDEX workflows_name_trgm_idx
    ON workflows USING gin (name gin_trgm_ops);
CREATE INDEX executions_correlation_trgm_idx
    ON executions USING gin (correlation_id gin_trgm_ops);
CREATE INDEX executions_search_title_trgm_idx
    ON executions USING gin
    ((COALESCE('workflow ' || workflow_id::text,
               'agent ' || agent_id::text,
               'execution')) gin_trgm_ops);
CREATE INDEX connectors_name_trgm_idx
    ON connectors USING gin (name gin_trgm_ops);
CREATE INDEX connectors_provider_trgm_idx
    ON connectors USING gin (provider gin_trgm_ops);
