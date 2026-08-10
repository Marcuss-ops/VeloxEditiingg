#!/usr/bin/env bash
# Non-destructive checks for the modular pilot entrypoint.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PILOT="$ROOT/scripts/pilot.sh"
MODULE_DIR="$ROOT/scripts/pilot"
TMP="$(mktemp -d /tmp/velox-pilot-offline.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

for f in "$PILOT" "$MODULE_DIR"/*.sh; do
  bash -n "$f"
  if command -v shellcheck >/dev/null 2>&1; then
    shellcheck -x "$f"
  fi
done

PILOT_DIR="$TMP/pilot" "$PILOT" --help >"$TMP/help.out"
grep -q 'Usage:' "$TMP/help.out"
grep -q 'all' "$TMP/help.out"

set +e
"$PILOT" definitely-not-a-command >"$TMP/unknown.out" 2>&1
rc=$?
set -e
[[ "$rc" -eq 2 ]]

PILOT_DIR="$TMP/pilot" SKIP_CLEANUP=1 "$PILOT" status >"$TMP/status.out" 2>&1
grep -q 'STATUS' "$TMP/status.out"
PILOT_DIR="$TMP/pilot" SKIP_CLEANUP=1 "$PILOT" stop >"$TMP/stop.out" 2>&1
grep -q 'processes stopped' "$TMP/stop.out"

# Preserve the public command table and documented exit-code contract without
# invoking the destructive/building commands themselves.
for command in all build start submit work stop status log; do
  grep -q "^[[:space:]]*${command})" "$PILOT"
done
for exit_code in 1 2 3 126; do
  grep -q "${exit_code}" "$PILOT" "$MODULE_DIR"/*.sh
done

grep -q "source \"\${REPO_ROOT}/scripts/pilot/build.sh\"" "$PILOT"
grep -q "source \"\${REPO_ROOT}/scripts/pilot/lifecycle.sh\"" "$PILOT"
grep -q "source \"\${REPO_ROOT}/scripts/pilot/job.sh\"" "$PILOT"
grep -q "source \"\${REPO_ROOT}/scripts/pilot/poll.sh\"" "$PILOT"
grep -q "source \"\${REPO_ROOT}/scripts/pilot/cleanup.sh\"" "$PILOT"
[[ "$(grep -Rhc '^trap cleanup EXIT' "$PILOT" "$MODULE_DIR"/*.sh | awk '{s+=$1} END {print s}')" -eq 1 ]]

# Sourcing each module alone must not execute any command or emit output.
for module in "$MODULE_DIR"/*.sh; do
  output="$(bash -c 'set -euo pipefail; source "$1"' _ "$module")"
  [[ -z "$output" ]]
done

echo 'pilot offline checks passed'
