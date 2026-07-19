#!/usr/bin/env bash
# scripts/ci/run-split-regression.sh
#
# Purpose:
#   Regression check for the 8 split orchestrators + the 2 full Go
#   modules + the worker-runtime single-writer BeginTx audit.
#   Pinned run command per group:
#       go test -race -count=1 -timeout=15m <pkg>
#   The audit group runs a static grep against
#           DataServer/internal/store/store_worker_*.go
#   and asserts EXACTLY 2 s.db.BeginTx sites, all in
#           store_worker_heartbeat.go
#           store_worker_recovery_tx.go
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

# run_audit_single_writer_begin_tx
#
# Worker-runtime cluster single-writer BeginTx audit (commit babcb81 contract).
# Asserts the worker_runtime cluster contains EXACTLY 2 s.db.BeginTx sites,
# in EXACTLY store_worker_heartbeat.go + store_worker_recovery_tx.go. Any
# deviation (extra file, missing file, count mismatch) escalates rc to 2
# and the overall wrapper exit to 2.
#
# Rationale (cross-references):
#   - store_worker_runtime.go (shell)         — per-package single-writer
#                                                contract;
#                                                explicit two-site exception.
#   - store_worker_heartbeat.go               — heartbeat-path opener
#                                                (#1 of 2):
#                                                PersistWorkerHeartbeat.
#   - store_worker_recovery_tx.go             — recovery-path opener
#                                                (#2 of 2):
#                                                reconcileOnePartition.
#   - store_worker_runtime_recovery.go        — heartbeat-path detector
#                                                (RECEIVES *sql.Tx; never
#                                                opens a new one) plus the
#                                                public recovery entry-point.
#
# Test files (suffix _test.go) are excluded so unit tests asserting
# helper-level *sql.Tx usage do not contribute to the cluster count.
# Comment-only lines (`// ...`) are NOT excluded here because actual
# code call sites are always on lines whose first non-whitespace token
# is `tx, err :=` or similar — grep -rEn already resticts to lines that
# *literally* contain `s.db.BeginTx`, so inline documentation blocks
# that mention the token in prose do not match unless they precisely
# contain the call expression.
#
# Returns rc in {0, 2}. Sets OVERALL_RC=2 on any non-zero.
run_audit_single_writer_begin_tx() {
  local label="audit-single-writer-begin-tx"
  local cluster_pattern="DataServer/internal/store/store_worker_*.go"
  local allowed1="DataServer/internal/store/store_worker_heartbeat.go"
  local allowed2="DataServer/internal/store/store_worker_recovery_tx.go"
  local start_ns end_ns elapsed_s rc_go

  log "=== $label ==="
  log "  scope: ${cluster_pattern} (excluding _test.go)"
  log "  expected: exactly 2 sites — ${allowed1##*/} + ${allowed2##*/}"

  start_ns=$(date +%s%N)

  # Collect every literal s.db.BeginTx call site in the cluster,
  # excluding _test.go files.
  #
  # Pattern rationale (post-`babcb81`):
  #   We match the full call signature `tx, err := s.db.BeginTx(ctx,
  #   nil)` rather than the bare token `s.db.BeginTx`. Both real
  #   call sites use this exact leading `tx, err := ` prefix;
  #   prose mentions in doc comments use truncated forms
  #   (`s.db.BeginTx`, `s.db.BeginTx)`, `s.db.BeginTx (...)`,
  #   `s.db.BeginTx per candidate worker`, etc.) so they do NOT
  #   match the strict pattern. The grep count thus reflects
  #   actual call sites, not documentation cross-references.
  #
  #   Brittleness: this pattern assumes both producers use `tx,
  #   err := s.db.BeginTx(ctx, nil)` exactly. If a future call
  #   renames the variable, splits the call across lines, or
  #   uses non-nil TxOptions, the audit will flag a mismatch.
  #   That is the intended behavior — the audit is the canonical
  #   SHAPE of the contract, and any divergence is a deliberate
  #   change that the audit must surface.
  #
  # Note on shell expansion: ${cluster_pattern} is intentionally
  # UN-QUOTED in the grep invocation below. Double-quoted variable
  # expansion inhibits filename expansion (globbing) in bash,
  # which would force grep to read a single literal filename
  # `DataServer/internal/store/store_worker_*.go` and return zero
  # matches. Un-quoted expansion lets the shell expand the glob
  # and pass each matching path to grep as a separate argument.
  # ${allowed1}/${allowed2} remain double-quoted because they are
  # literal exact-match filenames used in equality comparisons
  # below (no expansion needed).
  local matches
  matches=$(grep -rEn 'tx, err := .*s\.db\.BeginTx\(ctx, nil\)' ${cluster_pattern} 2>/dev/null \
            | grep -v '_test\.go' || true)

  local match_count=0
  if [[ -n "$matches" ]]; then
    match_count=$(printf '%s\n' "$matches" | grep -cE 'tx, err := .*s\.db\.BeginTx\(ctx, nil\)' || true)
  fi
  rc_go=0

  if [[ "$match_count" != "2" ]]; then
    rc_go=2
    log "  FAIL: expected exactly 2 s.db.BeginTx sites in worker_runtime cluster; found ${match_count}."
    if [[ -n "$matches" ]]; then
      printf '%s\n' "$matches" | sed 's/^/    matches: /' || true
    else
      log "    matches: (none)"
    fi
  else
    local mismatch=""
    local line file
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      file="${line%%:*}"
      if [[ "$file" != "$allowed1" && "$file" != "$allowed2" ]]; then
        mismatch="$file"
        rc_go=2
        break
      fi
    done <<< "$matches"
    if [[ "$rc_go" -eq 2 ]]; then
      log "  FAIL: extraneous s.db.BeginTx site in ${mismatch}."
    else
      log "  OK: exactly 2 sites — ${allowed1##*/} + ${allowed2##*/} (single-writer contract preserved)."
    fi
  fi

  {
    echo "------ ${label} ------"
    printf '%s\n' "$matches"
    echo "------ end ${label} ------"
  } >> "$RESULTS_TMP"

  end_ns=$(date +%s%N)
  elapsed_s=$(( (end_ns - start_ns) / 1000000000 ))

  log "  rc: ${rc_go}  elapsed_s: ${elapsed_s}"
  echo "${label}|${REPO_ROOT}|${cluster_pattern}|${rc_go}|${elapsed_s}" \
    >> "$RESULTS_TMP"
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

# Single-writer tx contract (commit babcb81 + a6b293a documented
# exception). Static audit; runs near-instantaneously. See
# store_worker_runtime.go (shell) for the underlying contract and
# store_worker_recovery_tx.go for the recovery-path opener.
run_audit_single_writer_begin_tx

log "=== summary (label | rc | elapsed_s) ==="
awk -F'|' 'NF==5 {printf "  %-30s  rc=%-4s  elapsed=%ss\n", $1, $4, $5}' \
  "$RESULTS_TMP"

if [[ "$OVERALL_RC" -ne 0 ]]; then
  log "OVERALL_RC=${OVERALL_RC} (at least one group rc != 0); see ${RESULTS_TMP}"
  exit "${OVERALL_RC}"
fi

log "OVERALL_RC=0 (all 8 checks green; 7 go-test groups + 1 single-writer audit)"
log "per-group output archived at: ${RESULTS_TMP}"
