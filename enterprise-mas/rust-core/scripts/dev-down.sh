#!/usr/bin/env bash
# Stops every process started by dev-up.sh (SIGTERM first, then SIGKILL after
# the documented drain budget), even across reboots of the shell.
set -uo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .dev-logs/pids ]; then
    echo "nothing started (no .dev-logs/pids)"
    exit 0
fi

while read -r pid name; do
    [ -z "${pid:-}" ] && continue
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null
        for _ in 1 2 3 4 5 6; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done
        kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null
        echo "stopped $name ($pid)"
    else
        echo "$name ($pid) already gone"
    fi
done < .dev-logs/pids
rm -f .dev-logs/pids
