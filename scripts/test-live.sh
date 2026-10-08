#!/usr/bin/env bash
# Run the appliance-backed (LIVE) test suites against a real Netezza.
#
# GitHub CI never runs these; they need a developer machine with access to an
# appliance. Configuration comes from the environment only (never commit
# credentials):
#
#   NZ_DEV_HOST, NZ_DEV_USER, NZ_DEV_PASSWORD   required
#   NZ_DEV_DB or NZ_DEV_DATABASE                required
#   NZ_DEV_PORT                                 optional (default 5480)
#
# Usage: scripts/test-live.sh [mode] [-- extra libtest args]
#   (default)        functional suite: live_qualification, live_driver,
#                    live_integration (the latter two partly read the
#                    JUST_DATA sample schema)
#   --qualification  only live_qualification (self-contained; works on an
#                    empty test database)
#   --stress         live_stress (pool stress and connection cycling; slow).
#                    Tune with NZ_STRESS_QUERIES / NZ_STRESS_CYCLES.
#   --capture        regenerate tests/fixtures/wire/*.bin from the appliance
#   --all            functional + stress
#
# Every suite runs serially (--test-threads=1) so results are deterministic.
set -euo pipefail

cd "$(dirname "$0")/.."

mode="${1:-}"
if [[ "$mode" == --* && "$mode" != "--" ]]; then
  shift
else
  mode=""
fi
if [[ "${1:-}" == "--" ]]; then
  shift
fi

missing=()
[[ -n "${NZ_DEV_HOST:-}" ]] || missing+=("NZ_DEV_HOST")
[[ -n "${NZ_DEV_USER:-}" ]] || missing+=("NZ_DEV_USER")
[[ -n "${NZ_DEV_PASSWORD:-}" ]] || missing+=("NZ_DEV_PASSWORD")
[[ -n "${NZ_DEV_DB:-}${NZ_DEV_DATABASE:-}" ]] || missing+=("NZ_DEV_DB or NZ_DEV_DATABASE")
if ((${#missing[@]})); then
  echo "test-live: missing configuration: ${missing[*]}" >&2
  exit 2
fi

# Older tooling still checks this flag.
export NZ_RUN_LIVE_TESTS=1
features="compat,chrono"

run_suite() {
  echo "==> cargo test --test $1"
  cargo test -p nz_rust --features "$features" --test "$1" -- \
    --ignored --nocapture --test-threads=1 "${@:2}"
}

case "$mode" in
  "")
    run_suite live_qualification "$@"
    run_suite live_driver "$@"
    run_suite live_integration "$@"
    ;;
  --qualification)
    run_suite live_qualification "$@"
    ;;
  --stress)
    run_suite live_stress "$@"
    ;;
  --capture)
    cargo run -p nz_rust --features "$features" --example capture_wire_fixtures -- \
      --out tests/fixtures/wire "$@"
    ;;
  --all)
    run_suite live_qualification "$@"
    run_suite live_driver "$@"
    run_suite live_integration "$@"
    run_suite live_stress "$@"
    ;;
  *)
    echo "test-live: unknown mode $mode" >&2
    exit 2
    ;;
esac
