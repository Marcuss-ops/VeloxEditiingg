#!/usr/bin/env bash
# scripts/ci/run-split-regression.sh
#
# Purpose:
#   Regression check for the 8 split orchestrators + the 2 full Go
#   modules. Pinned run command per group:
#       go test -race -count=1 -timeout=15m <pkg>
#   Records wall-clock via `date +%s%N` deltas and an overall rc
#   (any group rc != 0 escalates the wrapper exit to 2).
#
# Exit codes:
#   0   all groups rc=0
#   1   runtime error (missing module directory; tooling not on PATH)
#   2   at least one group rc != 0
#
# This is the canonical reproducer for
#   docs/2026-07-19-post-0d2158d-regression-check.md
# Do NOT move or rename this script without updating that report's
# `Reproducer` section in the same atomic commit.
#
# Usage:
#   bash scripts/ci/run-split-regression.sh
#
# Outputs:
#   /tmp/velox-regression-results.txt   CSV-style per-group lines + the
#                                       tail of each go test invocation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RESULTS_TMP="/tmp/velox-regression-results.txt"
: > "$RESULTS_TMP"

log()  { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; exit "${2:-1}"; }

OVERALL_RC=0

# run_group <label> <rel-cwd-under-repo> <pkg>
#
# rc propagated is from `go test` (the leftmost element of the pipe),
# not from `tee` or `tail`. We briefly disable `pipefail` so that
# PIPESTATUS[0] survives the surrounding `set -euo pipefail`.
run_group() {
  local label="$1"
  local rel_cwd="$2"
  local pkg="$3"
  local cwd="${REPO_ROOT}/${rel_cwd}"
  local start_ns end_ns elapsed_s rc_go

  if [[ ! -d "$cwd" ]]; then
    log "SKIP $label (directory missing: $cwd)"
    echo "${label}|${cwd}|${pkg}|SKIP|0" >> "$RESULTS_TMP"
    return 0
  fi

  log "=== $label ==="
  log "  cwd: $cwd"
  log "  pkg: $pkg"
  start_ns=$(date +%s%N)
  set +o pipefail
  ( cd "$cwd" && go test -race -count=1 -timeout=15m "$pkg" 2>&1 ) \
    | tail -200 | tee -a "$RESULTS_TMP" >/dev/null
  rc_go=${PIPESTATUS[0]}
  set -o pipefail
  end_ns=$(date +%s%N)
  elapsed_s=$(( (end_ns - start_ns) / 1000000000 ))

  log "  rc: ${rc_go}  elapsed_s: ${elapsed_s}"
  echo "${label}|${cwd}|${pkg}|${rc_go}|${elapsed_s}" >> "$RESULTS_TMP"
  if [[ "$rc_go" -ne 0 ]]; then
    OVERALL_RC=2
  fi
}

# 5 unique Go packages housing the 8 split orchestrators.
# Order chosen so the leaf-most-dependencies run first; this isolates a
# regression to whichever leaf surfaces red first on a future re-run.
run_group "split-data-store"        "DataServer"                         "./internal/store/..."
run_group "split-data-assets"       "DataServer"                         "./internal/assets/..."
run_group "split-data-handlers-api" "DataServer"                         "./internal/handlers/server/api/..."
run_group "split-worker-core"       "RemoteCodex/native/worker-agent-go" "./internal/worker/..."
run_group "split-worker-video"      "RemoteCodex/native/worker-agent-go" "./pkg/video/services/native/..."

# Full modules (authoritative rc pass/fail; targeted runs above give
# per-package go-test `ok X.Xs` granularity when blame-attributing).
run_group "full-velox-server"       "DataServer"                         "./..."
run_group "full-velox-worker-agent" "RemoteCodex/native/worker-agent-go" "./..."

log "=== summary (label | rc | elapsed_s) ==="
awk -F'|' 'NF==5 {printf "  %-30s  rc=%-4s  elapsed=%ss\n", $1, $4, $5}' \
  "$RESULTS_TMP"

if [[ "$OVERALL_RC" -ne 0 ]]; then
  log "OVERALL_RC=${OVERALL_RC} (at least one group rc != 0); see ${RESULTS_TMP}"
  exit "${OVERALL_RC}"
fi

log "OVERALL_RC=0 (all 7 groups green under -race detection)"
log "per-group output archived at: ${RESULTS_TMP}"
