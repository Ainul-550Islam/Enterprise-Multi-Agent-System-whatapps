#!/usr/bin/env bash
set -euo pipefail

# Provision the mas_memory role for the RAG memory plane (plane 0028)
# This script ensures the confined role exists and has the correct permissions.

PSQL="psql -U mas_user -d enterprise_mas -h /var/run/postgres"

# Create the mas_memory role if it doesn't exist
${PSQL} -c "DO \$\$ \
BEGIN \
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_memory') THEN \
        CREATE ROLE mas_memory; \
    END IF;
END \$\$;"

# Grant CONNECT on database if not already granted
${PSQL} -c "GRANT mas_memory CONNECT ON DATABASE enterprise_mas;" || true

# Grant USAGE on the memory schema if it exists
${PSQL} -c "GRANT USAGE ON SCHEMA memory TO mas_memory;" || true

# Grant SELECT on agent_memory_records for the mas_memory role
${PSQL} -c "GRANT SELECT ON agent_memory_records TO mas_memory;" || true

# Grant usage on the memory_grants table
${PSQL} -c "GRANT SELECT ON memory_grants TO mas_memory;" || true

echo "mas_memory role provisioned successfully"
