#!/usr/bin/env bash
# Runs every workspace bench (fast-to-slow). Criterion writes .html reports
# into target/criterion/; baselines can be pinned with --save-baseline main.
#
#   scripts/bench.sh [--quick]
#
# --quick runs one measurement pass per case (criterion's
# --sample-size-minimum --quick flag set) so smoke/pr checks stay under a
# couple of minutes.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .cargo-env ]; then source .cargo-env; fi

ARGS=()
if [ "${1:-}" = "--quick" ]; then
    ARGS=(-- --warm-up-time 1 --measurement-time 3)
fi

cargo bench -p mas-worker --bench root_backoff_bench "${ARGS[@]}"
cargo bench -p mas-messaging --bench root_codec_bench "${ARGS[@]}"
cargo bench -p mas-application --bench root_topological_bench "${ARGS[@]}"

echo "reports: target/criterion/**/report/index.html"
