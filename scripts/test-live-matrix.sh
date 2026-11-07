#!/usr/bin/env bash
# Run the self-contained LIVE qualification suite against one or two appliances.
#
# Profiles (environment only; credentials are never written to disk):
#   NZ_DEV_*    primary appliance (required)
#   NZ_DEV2_*   second appliance or server version (optional)
#
# Each profile needs HOST, USER, PASSWORD and DB or DATABASE. PORT and SCHEMA
# are optional and are forwarded unchanged. The profile variables are copied to
# the NZ_DEV_* names expected by scripts/test-live.sh.
#
# Usage:
#   scripts/test-live-matrix.sh [--require-two] [-- extra libtest args]
#
# Results: target/live-matrix/report.txt (never committed).
set -u

cd "$(dirname "$0")/.."
root="$(pwd)"
out="$root/target/live-matrix"
report="$out/report.txt"
require_two=0

if [[ "${1:-}" == "--require-two" ]]; then
  require_two=1
  shift
fi
if [[ "${1:-}" == "--" ]]; then
  shift
fi

mkdir -p "$out"
{
  echo "# nz_rust live matrix"
  echo "# date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "# host: $(hostname 2>/dev/null || echo unknown)"
  echo
} > "$report"

env_value() {
  local name="$1"
  printf '%s' "${!name:-}"
}

missing_for() {
  local prefix="$1"
  local missing=()
  [[ -n "$(env_value "${prefix}HOST")" ]] || missing+=("${prefix}HOST")
  [[ -n "$(env_value "${prefix}USER")" ]] || missing+=("${prefix}USER")
  [[ -n "$(env_value "${prefix}PASSWORD")" ]] || missing+=("${prefix}PASSWORD")
  if [[ -z "$(env_value "${prefix}DB")$(env_value "${prefix}DATABASE")" ]]; then
    missing+=("${prefix}DB or ${prefix}DATABASE")
  fi
  printf '%s\n' "${missing[@]}"
}

run_profile() {
  local prefix="$1" label="$2" extra=("${@:3}")
  local missing
  missing="$(missing_for "$prefix")"
  if [[ -n "$missing" ]]; then
    echo "SKIP  $label: missing $(echo "$missing" | paste -sd ', ' -)" | tee -a "$report"
    return 2
  fi

  local host port user password db database schema
  host="$(env_value "${prefix}HOST")"
  port="$(env_value "${prefix}PORT")"
  user="$(env_value "${prefix}USER")"
  password="$(env_value "${prefix}PASSWORD")"
  db="$(env_value "${prefix}DB")"
  database="$(env_value "${prefix}DATABASE")"
  schema="$(env_value "${prefix}SCHEMA")"

  export NZ_DEV_HOST="$host"
  export NZ_DEV_PORT="${port:-5480}"
  export NZ_DEV_USER="$user"
  export NZ_DEV_PASSWORD="$password"
  if [[ -n "$db" ]]; then
    export NZ_DEV_DB="$db"
    unset NZ_DEV_DATABASE
  else
    export NZ_DEV_DATABASE="$database"
    unset NZ_DEV_DB
  fi
  if [[ -n "$schema" ]]; then
    export NZ_DEV_SCHEMA="$schema"
  else
    unset NZ_DEV_SCHEMA
  fi

  echo "==> $label (${NZ_DEV_HOST}:${NZ_DEV_PORT}/${NZ_DEV_DB:-$NZ_DEV_DATABASE})" \
    | tee -a "$report"
  if "$root/scripts/test-live.sh" --qualification "${extra[@]}" \
    >>"$out/$label.log" 2>&1; then
    echo "PASS  $label" | tee -a "$report"
    return 0
  fi
  echo "FAIL  $label (see $out/$label.log)" | tee -a "$report"
  return 1
}

status=0
run_profile "NZ_DEV_" "primary" "$@" || status=$?
if [[ -n "$(env_value NZ_DEV2_HOST)$(env_value NZ_DEV2_USER)$(env_value NZ_DEV2_PASSWORD)$(env_value NZ_DEV2_DB)$(env_value NZ_DEV2_DATABASE)" ]]; then
  second_status=0
  run_profile "NZ_DEV2_" "secondary" "$@" || second_status=$?
  if ((second_status != 0)); then
    status=1
  fi
elif ((require_two)); then
  echo "FAIL  secondary: NZ_DEV2_* is required but not configured" | tee -a "$report"
  status=1
else
  echo "SKIP  secondary: NZ_DEV2_* is not configured" | tee -a "$report"
fi

echo | tee -a "$report"
if ((status == 0)); then
  echo "matrix passed" | tee -a "$report"
else
  echo "matrix failed" | tee -a "$report"
fi
exit "$status"
