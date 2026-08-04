#!/usr/bin/env bash
# scripts/ci/check-ac-taskresult-convergence.sh
#
# Convergence gate for the Artifact Commit / TaskResult / delivery protocol.
# This is intentionally read-only: the reconciler and delivery runner own
# repairs; this script proves that a completed E2E batch has converged.
#
# Required evidence for a job:
#   jobs SUCCEEDED
#   tasks SUCCEEDED
#   winning task_attempts SUCCEEDED
#   matching attempt_commits COMMITTED
#   READY final artifact with valid bytes and declaration identity
#   terminal delivery with a non-empty remote_id (Drive file ID for Drive)
#   worker TaskResultAck and TaskCommitAck received in the worker log
#
# Global zero-state checks:
#   no RUNNING jobs/tasks
#   no expired active task leases
#   no open artifact uploads
#   no non-terminal deliveries
#   no old non-terminal worker spool rows
#   no pending worker TaskResult outbox rows
#
# Usage:
#   ./scripts/ci/check-ac-taskresult-convergence.sh \
#     --db /path/to/velox.db --job-id JOB \
#     --worker-spool-db /path/to/worker_output_spool.sqlite3 \
#     --worker-log /path/to/worker.log
#
# Exit codes:
#   0 — all convergence and zero-state checks pass
#   1 — invariant violation or missing evidence
#   2 — invalid usage / missing input
#   3 — missing sqlite3
set -euo pipefail

DB_PATH=""
JOB_ID=""
WORKER_SPOOL_DB=""
WORKER_LOG=""
STALE_AFTER_SECONDS=600

usage() {
  cat >&2 <<'USAGE'
usage: check-ac-taskresult-convergence.sh --db PATH --job-id ID \
  --worker-spool-db PATH --worker-log PATH [--stale-after-seconds N]
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db) [[ $# -ge 2 ]] || { usage; exit 2; }; DB_PATH="$2"; shift 2 ;;
    --job-id) [[ $# -ge 2 ]] || { usage; exit 2; }; JOB_ID="$2"; shift 2 ;;
    --worker-spool-db) [[ $# -ge 2 ]] || { usage; exit 2; }; WORKER_SPOOL_DB="$2"; shift 2 ;;
    --worker-log) [[ $# -ge 2 ]] || { usage; exit 2; }; WORKER_LOG="$2"; shift 2 ;;
    --stale-after-seconds) [[ $# -ge 2 ]] || { usage; exit 2; }; STALE_AFTER_SECONDS="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'FATAL: unknown argument %q\n' "$1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$DB_PATH" || -z "$JOB_ID" || -z "$WORKER_SPOOL_DB" || -z "$WORKER_LOG" ]]; then
  usage
  exit 2
fi
if [[ ! -f "$DB_PATH" || ! -f "$WORKER_SPOOL_DB" || ! -f "$WORKER_LOG" ]]; then
  printf 'FATAL: DB, worker spool DB, and worker log must be regular files.\n' >&2
  exit 2
fi
if ! [[ "$STALE_AFTER_SECONDS" =~ ^[0-9]+$ ]] || (( STALE_AFTER_SECONDS <= 0 )); then
  printf 'FATAL: --stale-after-seconds must be a positive integer.\n' >&2
  exit 2
fi
if ! command -v sqlite3 >/dev/null 2>&1; then
  printf 'FATAL: sqlite3 CLI not found on $PATH.\n' >&2
  exit 3
fi

# IDs originate from the database/job API. Quote them before embedding in the
# sqlite3 CLI query; no shell command is constructed from their contents.
sql_quote() {
  local value="$1"
  value=${value//\'/\'\'}
  printf "'%s'" "$value"
}
JOB_SQL="$(sql_quote "$JOB_ID")"
violations=0
declare -a FAILURES=()

run_zero_query() {
  local label="$1" db="$2" sql="$3" output rc count
  set +e
  output="$(timeout 5 sqlite3 -noheader -batch "$db" "$sql" 2>&1)"
  rc=$?
  set -e
  if (( rc != 0 )); then
    printf 'FAIL [%s]: sqlite3 rc=%d: %s\n' "$label" "$rc" "$output" >&2
    violations=$((violations + 1))
    FAILURES+=("$label: sqlite error")
    return
  fi
  if [[ -z "$output" ]]; then
    count=0
  else
    count="$(printf '%s\n' "$output" | sed '/^$/d' | wc -l | tr -d ' ')"
  fi
  if (( count == 0 )); then
    printf 'OK   [%s]: 0 offending rows\n' "$label"
  else
    printf 'FAIL [%s]: %d offending row(s)\n' "$label" "$count" >&2
    printf '%s\n' "$output" | sed 's/^/       /' >&2
    violations=$((violations + 1))
    FAILURES+=("$label: $count row(s)")
  fi
}

run_scalar_expect() {
  local label="$1" db="$2" expected="$3" sql="$4" output rc
  set +e
  output="$(timeout 5 sqlite3 -noheader -batch "$db" "$sql" 2>&1)"
  rc=$?
  set -e
  output="$(printf '%s' "$output" | tr -d '\r\n')"
  if (( rc != 0 )); then
    printf 'FAIL [%s]: sqlite3 rc=%d: %s\n' "$label" "$rc" "$output" >&2
    violations=$((violations + 1))
    FAILURES+=("$label: sqlite error")
  elif [[ "$output" == "$expected" ]]; then
    printf 'OK   [%s]: %s\n' "$label" "$output"
  else
    printf 'FAIL [%s]: got %q, want %q\n' "$label" "$output" "$expected" >&2
    violations=$((violations + 1))
    FAILURES+=("$label: got $output, want $expected")
  fi
}

# ── Job-specific convergence chain ─────────────────────────────────────────
run_scalar_expect "AC job exists" "$DB_PATH" "1" "SELECT COUNT(*) FROM jobs WHERE job_id=$JOB_SQL;"
run_scalar_expect "AC job SUCCEEDED" "$DB_PATH" "1" "SELECT COUNT(*) FROM jobs WHERE job_id=$JOB_SQL AND status='SUCCEEDED';"
run_scalar_expect "AC has task" "$DB_PATH" "1" "SELECT COUNT(*) FROM tasks WHERE job_id=$JOB_SQL;"

run_zero_query "AC task not SUCCEEDED" "$DB_PATH" "
SELECT task_id, status FROM tasks
 WHERE job_id=$JOB_SQL AND status <> 'SUCCEEDED';
"
run_zero_query "AC winning attempt/commit mismatch" "$DB_PATH" "
SELECT t.task_id, COALESCE(t.winning_attempt_id,''), COALESCE(a.status,''), COALESCE(c.status,'')
  FROM tasks t
  LEFT JOIN task_attempts a
    ON a.id=t.winning_attempt_id AND a.task_id=t.task_id AND a.status='SUCCEEDED'
  LEFT JOIN attempt_commits c
    ON c.task_id=t.task_id AND c.attempt_id=t.winning_attempt_id
   AND c.job_id=t.job_id AND c.status='COMMITTED'
 WHERE t.job_id=$JOB_SQL
   AND (COALESCE(t.winning_attempt_id,'')='' OR a.id IS NULL OR c.commit_id IS NULL);
"
run_scalar_expect "AC READY final artifact" "$DB_PATH" "1" "
SELECT COUNT(*) FROM artifacts
 WHERE job_id=$JOB_SQL AND output_kind='final_video' AND status='READY';
"
run_zero_query "AC final artifact bytes" "$DB_PATH" "
SELECT a.id, COALESCE(a.sha256,''), a.size_bytes
  FROM artifacts a
 WHERE a.job_id=$JOB_SQL AND a.output_kind='final_video' AND a.status='READY'
   AND (length(trim(COALESCE(a.sha256,''))) <> 64 OR a.size_bytes <= 0);
"
run_zero_query "AC declaration/commit/artifact identity" "$DB_PATH" "
SELECT a.id, d.declaration_id, d.task_id, d.attempt_id
  FROM artifacts a
  LEFT JOIN task_output_declarations d ON d.artifact_id=a.id
  LEFT JOIN tasks t ON t.task_id=d.task_id AND t.job_id=a.job_id
  LEFT JOIN attempt_commits c ON c.commit_id=d.commit_id
       AND c.task_id=d.task_id AND c.attempt_id=d.attempt_id
       AND c.job_id=a.job_id AND c.status='COMMITTED'
 WHERE a.job_id=$JOB_SQL AND a.output_kind='final_video' AND a.status='READY'
   AND (d.declaration_id IS NULL OR t.task_id IS NULL OR c.commit_id IS NULL);
"
run_zero_query "AC open artifact uploads" "$DB_PATH" "
SELECT upload_id, status FROM artifact_uploads
 WHERE job_id=$JOB_SQL
   AND status IN ('CREATED','UPLOADING','RECEIVED','FINALIZING');
"
run_zero_query "AC final delivery not completed or missing remote/Drive file ID" "$DB_PATH" "
SELECT a.id, d.delivery_id, d.status, COALESCE(dd.provider,''), COALESCE(d.remote_id,'')
  FROM artifacts a
  LEFT JOIN job_deliveries d ON d.artifact_id=a.id
  LEFT JOIN delivery_destinations dd ON dd.destination_id=d.destination_id
 WHERE a.job_id=$JOB_SQL AND a.output_kind='final_video' AND a.status='READY'
   AND (d.delivery_id IS NULL
        OR d.status NOT IN ('SUCCEEDED','COMPLETED')
        OR length(trim(COALESCE(dd.provider,'')))=0
        OR length(trim(COALESCE(d.remote_id,'')))=0);
"

# ── Global convergence / zero-state checks ──────────────────────────────────
run_zero_query "ZERO running jobs" "$DB_PATH" "SELECT job_id, status FROM jobs WHERE status='RUNNING';"
run_zero_query "ZERO running tasks" "$DB_PATH" "SELECT task_id, status FROM tasks WHERE status='RUNNING';"
run_zero_query "ZERO expired active task leases" "$DB_PATH" "
SELECT task_id, status, lease_expires_at FROM tasks
 WHERE status IN ('RUNNING','LEASED')
   AND COALESCE(lease_expires_at,'')<>''
   AND julianday(lease_expires_at) < julianday('now');
"
run_zero_query "ZERO non-terminal deliveries" "$DB_PATH" "
SELECT delivery_id, status FROM job_deliveries
 WHERE status NOT IN ('SUCCEEDED','COMPLETED','FAILED','CANCELLED','BLOCKED_AUTH');
"
run_zero_query "ZERO stale artifact uploads" "$DB_PATH" "
SELECT upload_id, status FROM artifact_uploads
 WHERE status IN ('CREATED','UPLOADING','RECEIVED','FINALIZING')
   AND julianday(expires_at) < julianday('now');
"

# ── Worker-side durable evidence ────────────────────────────────────────────
run_zero_query "ZERO old non-terminal worker spool rows" "$WORKER_SPOOL_DB" "
SELECT spool_id, task_id, status FROM worker_output_spool
 WHERE status IN ('RENDERING','OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')
   AND julianday(updated_at) < julianday('now','-${STALE_AFTER_SECONDS} seconds');
"
run_scalar_expect "ZERO pending TaskResult outbox" "$WORKER_SPOOL_DB" "0" "SELECT COUNT(*) FROM task_result_outbox;"

# The log is the worker-side receipt evidence. Master-side ACK sends are not
# enough: convergence requires that the worker consumed both ACKs and advanced
# its local protocol state.
while IFS='|' read -r task_id attempt_id; do
  [[ -n "$task_id" && -n "$attempt_id" ]] || continue
  if ! grep -Fq -- "[TASK_RESULT_OUTBOX] TaskResultAck received task=$task_id attempt=$attempt_id" "$WORKER_LOG"; then
    printf 'FAIL [TaskResultAck received task=%s attempt=%s]: marker missing\n' "$task_id" "$attempt_id" >&2
    violations=$((violations + 1))
    FAILURES+=("TaskResultAck missing: $task_id/$attempt_id")
  else
    printf 'OK   [TaskResultAck received task=%s attempt=%s]\n' "$task_id" "$attempt_id"
  fi
  if ! awk -v task="$task_id" -v attempt="$attempt_id" '
      /TASK_COMMIT_ACK_RECEIVED/ &&
      index($0, "\"task_id\":\"" task "\"") &&
      index($0, "\"attempt_id\":\"" attempt "\"") { found=1 }
      END { exit(found ? 0 : 1) }
    ' "$WORKER_LOG"; then
    printf 'FAIL [TaskCommitAck received task=%s attempt=%s]: marker missing\n' "$task_id" "$attempt_id" >&2
    violations=$((violations + 1))
    FAILURES+=("TaskCommitAck missing: $task_id/$attempt_id")
  else
    printf 'OK   [TaskCommitAck received task=%s attempt=%s]\n' "$task_id" "$attempt_id"
  fi
done < <(sqlite3 -noheader -separator '|' "$DB_PATH" "
  SELECT task_id, winning_attempt_id FROM tasks
   WHERE job_id=$JOB_SQL ORDER BY task_id;
")

if (( violations > 0 )); then
  printf '\nFAIL: %d AC/TaskResult convergence violation(s)\n' "$violations" >&2
  for failure in "${FAILURES[@]}"; do printf '  - %s\n' "$failure" >&2; done
  exit 1
fi

printf '\nOK   AC/TaskResult/delivery convergence gate passed for job %s\n' "$JOB_ID"
exit 0
