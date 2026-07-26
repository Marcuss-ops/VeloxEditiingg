#!/usr/bin/env bash
# scripts/ci/pre-removal-verify.sh
#
# Purpose:
#   Mandatory verification gate for commits that retire code (removal
#   commits). Runs full-module `go vet ./...`, `go build ./...`, and
#   `go test ./...` on the `DataServer` module. Scoped verification
#   (e.g. `go vet ./internal/foo/...`) is insufficient: orphan _test.go
#   references to a now-removed helper, plus unused imports in sibling
#   files, are not caught by the scoped check.
#
# Empirical evidence (2026-07-25, VeloxEditiingg main):
#   The `c322182` fixup commit on `main` is the canonical case study.
#   Scoped `go vet ./internal/handlers/server/pipeline/...` +
#   `go build ./internal/handlers/server/pipeline/...` had passed at the
#   boundary of `c9c2ae4` and `716426a`, but full-module
#   `go test ./...` later surfaced two compile residuals:
#     1. orphan reference to `normalizeRemoteEngineIntake` in
#        `creator_push_test.go` (helper removed in `c9c2ae4`);
#     2. unused `enqueue` import in `pipeline_run_actions.go`
#        (sync-forward branch removed in `c9c2ae4`).
#   Without this gate, both residuals would have shipped to `main` and
#   broken downstream consumers; the `c322182` fixup commit repaired
#   them after the fact. This script prevents the next removal from
#   producing a similar fixup.
#
# Exit codes:
#   0   all three checks green
#   1   runtime error (missing DataServer/go.mod, missing tooling)
#   2   at least one check failed (read the per-check output to see which)
#
# Usage:
#   bash scripts/ci/pre-removal-verify.sh
#
# The script does NOT detect removal itself (the caller knows the commit
# shape); it enforces the gate unconditionally because the cost is
# bounded (one full-module test run per push) and the rule is simple.
#
# Cross-references:
#   - docs/adr/0008-soft-deprecate-vs-remove-pivot.md §(b) point 2
#     (the codified verification gate this script enforces).
#   - AGENTS.md (the operational note pointing at this script).
#   - scripts/ci/run-split-regression.sh (broader CI regression runner
#     that includes a `full-velox-server` group for the same gate at
#     release time).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DATASERVER_DIR="${REPO_ROOT}/DataServer"
RESULTS_TMP="/tmp/velox-pre-removal-verify.txt"
: > "$RESULTS_TMP"

log()  { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; exit "${2:-1}"; }

[[ -d "$DATASERVER_DIR" ]] \
  || fail "DataServer/ not found at ${DATASERVER_DIR} (run from repo root)"

[[ -f "${DATASERVER_DIR}/go.mod" ]] \
  || fail "DataServer/go.mod not found (run from repo root)"

command -v go >/dev/null 2>&1 \
  || fail "go not on PATH; install Go 1.25.8+ (DataServer/go.mod requirement)"

OVERALL_RC=0

# run_full_module_check <label> <go-cmd-and-args...>
#
# Runs the given `go` command from DataServer/ with full-module scope
# (./...). Records wall-clock via `date +%s%N` deltas and propagates
# the go command's exit code (not tee/tail's) into OVERALL_RC.
#
# Pipefail handling: PIPESTATUS[0] carries `go`'s exit code across the
# pipe to `tee` and `tail`. We briefly disable `pipefail` so that
# PIPESTATUS[0] survives the surrounding `set -euo pipefail` without
# `tee`/`tail` non-zero exit codes short-circuiting the wrapper.
run_full_module_check() {
  local label="$1"
  shift
  local start_ns end_ns elapsed_s rc_go

  log "=== ${label} ==="
  log "  cwd: ${DATASERVER_DIR}"
  log "  cmd: go $*"
  start_ns=$(date +%s%N)
  set +o pipefail
  ( cd "$DATASERVER_DIR" && go "$@" ) 2>&1 \
    | tee -a "$RESULTS_TMP" >/dev/null
  rc_go=${PIPESTATUS[0]}
  set -o pipefail
  end_ns=$(date +%s%N)
  elapsed_s=$(( (end_ns - start_ns) / 1000000000 ))

  log "  rc: ${rc_go}  elapsed_s: ${elapsed_s}"
  if [[ "$rc_go" -ne 0 ]]; then
    OVERALL_RC=2
  fi
}

run_full_module_check "go vet ./..."   vet ./...
run_full_module_check "go build ./..." build ./...
run_full_module_check "go test -count=1 ./..." test -count=1 -timeout=15m ./...

log "=== summary ==="
log "  full output archived at: ${RESULTS_TMP}"
if [[ "$OVERALL_RC" -ne 0 ]]; then
  log "OVERALL_RC=${OVERALL_RC} (at least one check failed; see ${RESULTS_TMP})"
  exit "${OVERALL_RC}"
fi

log "OVERALL_RC=0 (all 3 full-module checks green; safe to push removal commit)"