#!/usr/bin/env bash
# scripts/ci/check-completion-protocol-invariants.sh
#
# Phase 1.5 of the Artifact Commit Protocol — CI gate.
#
# Runs five SQL invariant queries against a SQLite database. Each
# query asserts that the desired post-condition of the protocol
# holds; any non-empty result set fires a CI failure with the
# offending rows surfaced in the log.
#
# INVARIANTS
# ──────────
#   Q1 (job_SucceededWithoutReadyArtifact)
#       A job with status='SUCCEEDED' must have at least one
#       artifact with status='READY'. Pre-Phase 1 this is the most
#       common desync mode ("render finished, but TaskResult
#       wrote SUCCEEDED before the artifact bytes landed in
#       BlobStore").
#
#   Q2 (task_SucceededWithoutReadyArtifact)
#       A task with status='SUCCEEDED' must have ALL the
#       declarations it advertised linked to a READY artifact.
#       The literal completion-protocol.md §1.5 query asserts
#       `d.required=1`; that column is not yet on
#       task_output_declarations (landing in migration 064
#       alongside the Phase 2 coordinator). The form here uses
#       `task_output_declarations.artifact_id → artifacts.status`
#       as the structural proxy: any task marked SUCCEEDED with
#       zero READY artifacts via declarations is a desync. When
#       migration 064 lands, this query will be replaced by the
#       literal `d.required=1` form (see completion-protocol.md
#       §2.5 and §1.5).
#
#   Q3 (multipleReadyArtifactsPerJobKind)
#       At most one READY artifact per (job_id, output_kind).
#       Two READY 'final_video' artifacts on the same job would
#       mean the master finalized the same logical artifact
#       twice (the canonical Phase 2 atomic-commit bug).
#
#   Q4 (deliveryOnArtifactNotReady)
#       A job_delivery MUST NOT reference an artifact whose
#       status != 'READY'. A delivery that points to a STAGING
#       or FAILED artifact is a Phase 5 cross-link bug — Drive
#       landing on bytes that don't exist.
#
#   Q5 (job_SucceededWithTaskStillRunning)
#       A job with status='SUCCEEDED' MUST NOT have any
#       associated task still in status 'RUNNING' or 'LEASED'.
#       The desync surfaced when the closure tx (sqlite_finalize_writer.go
#       Step 2) flipped jobs.status='SUCCEEDED' while the canonical
#       tasks row stayed at RUNNING/LEASED/PENDING. Step 2.5
#       (markTaskSucceededTx) sweeps that row inside the same
#       tx; this query is the post-commit gate that catches any
#       future regressions. PENDING is excluded intentionally
#       because Step 2.5 accepts it (fast-abort-finalization can
#       promote a job to SUCCEEDED before the claimant flip runs).
#
# USAGE
# ─────
#   ./scripts/ci/check-completion-protocol-invariants.sh [DB_PATH]
#   DB_PATH=/path/to/velox.db ./scripts/ci/check-completion-protocol-invariants.sh
#
# BUDGET
# ──────
# Each query is wrapped in `timeout 1`. Worst-case total ≈ 5 s (5
# queries × 1 s) which is the safety ceiling — on an empty CI
# fixture the actual runtime is sub-100 ms, on a populated
# production DB it's typically <500 ms. sqlite3 buffers row output
# so even large result sets finish in <1 s on modern hardware; if
# any single query exceeds 1 s the operator is alerted via rc=124
# (timeout) and the script exits 1.
#
# Exit codes:
#   0 — all five queries returned 0 rows
#   1 — at least one query returned ≥1 offending rows OR a query
#       timed out (>1 s) OR a query errored
#   2 — DB_PATH not provided or unreadable
#   3 — sqlite3 binary missing
set -euo pipefail

# ─── Paths & sanity ───────────────────────────────────────────────────────
DB_PATH="${1:-${DB_PATH:-${VELOX_DB_PATH:-}}}"

if [[ -z "$DB_PATH" ]]; then
  printf 'FATAL: DB_PATH (or arg 1, or VELOX_DB_PATH) required.\n' >&2
  printf '       e.g. %s /path/to/velox.db\n' "$0" >&2
  exit 2
fi
if [[ ! -f "$DB_PATH" ]]; then
  printf 'FATAL: DB_PATH=%s does not exist or is not a regular file.\n' "$DB_PATH" >&2
  exit 2
fi
if ! command -v sqlite3 >/dev/null 2>&1; then
  printf 'FATAL: sqlite3 CLI not found on $PATH. Install sqlite3 (>= 3.40) to run this gate.\n' >&2
  exit 3
fi

# ─── Query runner ─────────────────────────────────────────────────────────
# Single sqlite3 invocation per query — stdout and stderr are
# captured together. On sqlite3 failures, ONLY stderr is written
# (stdout is empty). On success, ONLY stdout contains rows. So we
# can disambiguate by exit code:
#   rc == 0  → $combined is pure rows (possibly empty)
#   rc == 124 → `timeout 1` killed the query (treat as a violation)
#   rc != 0  → $combined IS the error message
# This avoids the 2× runtime penalty of running each query twice
# (one for stdout, one for stderr).
violations=0
declare -a FAILURES=()

run_query() {
  local label="$1" sql="$2"
  local combined rc
  combined="$(timeout 1 sqlite3 "$DB_PATH" "$sql" 2>&1)"
  rc=$?
  if (( rc == 124 )); then
    printf 'FAIL [%s]: query exceeded 1s timeout (sqlite3 not responsive on a fresh-DB or row-count blowup)\n' "$label" >&2
    violations=$((violations + 1))
    FAILURES+=("$label: timeout (>1s)")
    return
  fi
  if (( rc != 0 )); then
    printf 'FAIL [%s]: sqlite3 exited with code %d (output: %s)\n' "$label" "$rc" "$combined" >&2
    violations=$((violations + 1))
    FAILURES+=("$label: sqlite error (rc=$rc)")
    return
  fi
  # rc==0 → $combined is pure rows. Compute line count, stripping
  # any trailing empty line sqlite3 may emit.
  local count
  if [[ -z "$combined" ]]; then
    count=0
  else
    count="$(printf '%s\n' "$combined" | sed '/^$/d' | wc -l | tr -d ' ')"
  fi
  if (( count == 0 )); then
    printf 'OK   [%s]: 0 offending rows\n' "$label"
  else
    printf 'FAIL [%s]: %d offending row(s)\n' "$label" "$count" >&2
    printf '       -- offending rows --\n' >&2
    printf '%s\n' "$combined" | sed 's/^/       /' >&2
    printf '       -- end --\n' >&2
    violations=$((violations + 1))
    FAILURES+=("$label: $count row(s)")
  fi
}

# ─── Run the five invariants ──────────────────────────────────────────────
run_query "Q1 job_SucceededWithoutReadyArtifact" "
SELECT j.job_id
FROM jobs j
WHERE j.status='SUCCEEDED'
  AND NOT EXISTS (
    SELECT 1 FROM artifacts a
    WHERE a.job_id = j.job_id AND a.status='READY'
  );
"

run_query "Q2 task_SucceededWithoutReadyArtifact" "
SELECT t.task_id
FROM tasks t
LEFT JOIN task_output_declarations d ON d.task_id = t.task_id
LEFT JOIN artifacts a
  ON a.id = d.artifact_id AND a.status='READY'
WHERE t.status='SUCCEEDED'
GROUP BY t.task_id
HAVING COUNT(a.id) = 0;
"

run_query "Q3 multipleReadyArtifactsPerJobKind" "
SELECT job_id, output_kind, COUNT(*) AS n
FROM artifacts
WHERE status='READY'
GROUP BY job_id, output_kind
HAVING COUNT(*) > 1;
"

run_query "Q4 deliveryOnArtifactNotReady" "
SELECT d.delivery_id
FROM job_deliveries d
JOIN artifacts a ON a.id = d.artifact_id
WHERE a.status != 'READY';
"

run_query "Q5 job_SucceededWithTaskStillRunning" "
SELECT t.task_id, j.job_id
FROM tasks t
JOIN jobs  j ON j.job_id = t.job_id
WHERE j.status = 'SUCCEEDED'
  AND t.status IN ('RUNNING', 'LEASED');
"

# ─── Summary ─────────────────────────────────────────────────────────────
if (( violations > 0 )); then
  printf '\nFAIL: %d invariant violation(s):\n' "$violations" >&2
  for f in "${FAILURES[@]}"; do
    printf '  - %s\n' "$f" >&2
  done
  exit 1
fi

printf '\nOK   all 5 completion-protocol invariants hold on %s\n' "$DB_PATH"
exit 0
