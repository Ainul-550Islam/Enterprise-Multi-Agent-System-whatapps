#!/usr/bin/env bash
# PURPOSE:
# Provisions the machine-plane database roles with credentials, so a deployed
# cluster can actually authenticate as them. Migrations 0015, 0017 and 0019
# create `mas_directory`, `mas_worker`, `mas_outbox`, `mas_audit` and
# `mas_scheduler` as NOLOGIN on purpose: an unconfigured deployment must not
# be able to authenticate as a plane role by accident. Something has to grant
# them LOGIN, and this is that thing.
#
# Owning layer: operations. Run by a human or by the provisioning pipeline,
# never by the application at boot.
#
# Production responsibility: read each role's password from its own
# environment variable, refuse to invent one, and apply the grant inside a
# single psql session so a partial provisioning cannot leave one plane able to
# log in and another not.
#
# Security boundary: this script handles live credentials. It therefore
#   • never accepts a password as an argument (the process table is readable);
#   • never echoes one — `set +x` is forced and every value is passed through
#     psql variables rather than interpolated into logged SQL;
#   • refuses to run when a required variable is empty, and refuses to use
#     `--dev` against anything other than a loopback host;
#   • grants LOGIN and nothing else. The privilege surface of each role was
#     decided in the migration that created it and is not widened here.
#
# It must NOT:
#   • be invoked by service code, a container entrypoint, or a migration;
#   • grant a role any privilege beyond LOGIN;
#   • fall back to a default password.

set -euo pipefail
set +x  # credentials are in the environment; never trace them

usage() {
    cat <<'USAGE'
provision-plane-roles.sh — grant LOGIN to the machine-plane roles.

Usage:
    MAS_DATABASE_ADMIN_URL=postgres://migrator:...@host:5432/mas \
    MAS_APP_PASSWORD=...            \
    MAS_DIRECTORY_PASSWORD=...      \
    MAS_WORKER_PASSWORD=...         \
    MAS_OUTBOX_PASSWORD=...         \
    MAS_AUDIT_PASSWORD=...          \
    MAS_SCHEDULER_PASSWORD=...      \
    scripts/provision-plane-roles.sh

Options:
    --dev       Generate random passwords and provision a local test cluster.
                Refuses to run against a non-loopback host.
    --dry-run   Print the statements that would run, with every password
                replaced by ********.
    -h, --help  This message.

Every password is read from the environment. None has a default.
USAGE
}

DEV=0
DRY_RUN=0
while [ $# -gt 0 ]; do
    case "$1" in
        --dev) DEV=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

ADMIN_URL="${MAS_DATABASE_ADMIN_URL:-}"
[ -n "$ADMIN_URL" ] || { echo "MAS_DATABASE_ADMIN_URL is required" >&2; exit 2; }

# Refuse --dev against anything that is not this host.
if [ "$DEV" -eq 1 ]; then
    host="${ADMIN_URL#*@}"; host="${host%%/*}"; host="${host%%:*}"
    case "$host" in
        127.0.0.1|localhost|"") : ;;
        *) echo "--dev may only target a loopback host (got '$host')" >&2; exit 2 ;;
    esac
fi

random_password() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -base64 32 | tr -d '\n'
    elif [ -r /dev/urandom ]; then
        LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 40
    else
        echo "no source of randomness available" >&2
        exit 1
    fi
}

if [ "$DEV" -eq 1 ]; then
    MAS_APP_PASSWORD="$(random_password)"
    MAS_DIRECTORY_PASSWORD="$(random_password)"
    MAS_WORKER_PASSWORD="$(random_password)"
    MAS_OUTBOX_PASSWORD="$(random_password)"
    MAS_AUDIT_PASSWORD="$(random_password)"
    MAS_SCHEDULER_PASSWORD="$(random_password)"
    echo "generated random passwords for a local cluster (not printed)" >&2
fi

# Every remaining password must come from the environment. An empty one is a
# hard error: a role that gets LOGIN with an empty password is a role anyone
# can become.
MISSING=""
for var in MAS_APP_PASSWORD MAS_DIRECTORY_PASSWORD MAS_WORKER_PASSWORD \
           MAS_OUTBOX_PASSWORD MAS_AUDIT_PASSWORD MAS_SCHEDULER_PASSWORD; do
    eval "value=\"\${$var:-}\""
    [ -n "$value" ] || MISSING="$MISSING $var"
done
if [ -n "$MISSING" ]; then
    echo "missing required password environment variables:$MISSING" >&2
    echo "no role was modified" >&2
    exit 2
fi

# `ON_ERROR_STOP` plus one session: either every role is provisioned or none
# is. Half a provisioning is worse than none, because the planes that failed
# look identical to planes that were never configured.
PSQL=(psql "$ADMIN_URL" --no-psqlrc -v ON_ERROR_STOP=1 --quiet)

run_sql() {
    if [ "$DRY_RUN" -eq 1 ]; then
        sed -E "s/(PASSWORD ')[^']*(')/\1********\2/g" <<<"$1"
        return 0
    fi
    printf '%s\n' "$1" | "${PSQL[@]}" \
        -v app_password="$MAS_APP_PASSWORD" \
        -v directory_password="$MAS_DIRECTORY_PASSWORD" \
        -v worker_password="$MAS_WORKER_PASSWORD" \
        -v outbox_password="$MAS_OUTBOX_PASSWORD" \
        -v audit_password="$MAS_AUDIT_PASSWORD" \
        -v scheduler_password="$MAS_SCHEDULER_PASSWORD"
}

read -r -d '' SQL <<'SQL' || true
-- The runtime role. No BYPASSRLS, and this script does not give it one:
-- request traffic is confined by row-level security, which is the whole point.
ALTER ROLE mas_app       LOGIN PASSWORD :'app_password';

-- The pre-scope directory reader.
ALTER ROLE mas_directory LOGIN PASSWORD :'directory_password';

-- The machine planes. Each holds BYPASSRLS already, granted by the migration
-- that created it and confined there by table and operation.
ALTER ROLE mas_worker    LOGIN PASSWORD :'worker_password';
ALTER ROLE mas_outbox    LOGIN PASSWORD :'outbox_password';
ALTER ROLE mas_audit     LOGIN PASSWORD :'audit_password';
ALTER ROLE mas_scheduler LOGIN PASSWORD :'scheduler_password';
SQL

run_sql "$SQL"

if [ "$DRY_RUN" -eq 0 ]; then
    echo "provisioned: mas_app mas_directory mas_worker mas_outbox mas_audit mas_scheduler"
    echo "no privilege beyond LOGIN was granted; the grant surface lives in the migrations"
fi
