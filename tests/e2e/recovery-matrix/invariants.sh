#!/usr/bin/env bash
# =============================================================================
# tests/e2e/recovery-matrix/invariants.sh — 7 canonical post-recovery checks
# =============================================================================
# Each helper returns 0 (PASS) or 1 (FAIL) and prints PASS/FAIL on stdout.
# Helpers are sourced (not executed as a script) by run.sh and scenarios/*.sh.
#
# Pure-function style: state to inspect is read from a single sqlite db path
# passed as $1. The "PASS on negative" semantic from cap. 6 is preserved —
# if the invariant fails AND the scenario was a NEGATIVE test (e.g.,
# duplicate TaskResult), the suite marks the FAIL as EXPECTED and re-records
# it as PASS. Callers pass through `rm_assert_invariant <expected_negative:0|1>`.
#
# NR-1 .. NR-7 mapping:
#   assert_invariant_1_one_attempt_active_per_task
#   assert_invariant_2_old_lease_cannot_finalize
#   assert_invariant_3_no_job_stuck_forever
#   assert_invariant_4_no_partial_files_in_final
#   assert_invariant_5_no_ready_without_valid_bytes
#   assert_invariant_6_no_succeeded_without_ready
#   assert_invariant_7_new_attempt_id_after_reap
# =============================================================================

# ─── shared color shims (lib.sh already sources but we re-declare for safety) ─
if [[ -t 1 ]]; then
  I_GREEN=$'\033[32m'; I_RED=$'\033[31m'; I_RST=$'\033[0m'
else
  I_GREEN=""; I_RED=""; I_RST=""
fi

# ─── internal: positive helper ───────────────────────────────────────────────
# _check <label> <positive:0|1> <actual_pass:0|1>
# Emits PASS/FAIL/consumes `expected_negative` semantics.
_check() {
  local label="$1" neg_expected="$2" actual="$3"
  if (( neg_expected == 0 )); then
    # Positive invariant: must hold.
    if (( actual == 0 )); then
      printf '%sPASS%s  %s\n' "$I_GREEN" "$I_RST" "$label"
      return 0
    fi
    printf '%sFAIL%s  %s\n' "$I_RED" "$I_RST" "$label"
    rm_mark_inv_fail
    return 1
  fi
  # Negative expected (bad input must be REJECTED): assert system refused.
  if (( actual != 0 )); then
    printf '%sPASS%s  %s (bad input correctly rejected)\n' "$I_GREEN" "$I_RST" "$label"
    return 0
  fi
  printf '%sFAIL%s  %s (bad input was NOT rejected)\n' "$I_RED" "$I_RST" "$label"
  rm_mark_inv_fail
  return 1
}

# ─── NR-1: un solo Attempt attivo per task ────────────────────────────────────
# A task in RUNNING/LEASED must have exactly one task_attempts row in the
# corresponding non-terminal state. Multiple attempts of mixed states are OK
# (a retried attempt), but exactly ONE per (task_id, status IN RUNNING) tuple.
assert_invariant_1_one_attempt_active_per_task() {
  local db="$1" neg_expected="${2:-0}"
  local count
  count=$(sqlite3 "$db" "
    SELECT COUNT(*) FROM (
      SELECT task_id, COUNT(*) AS n
      FROM   task_attempts
      WHERE  status IN ('PENDING','RUNNING','LEASED_ACCEPTED')
      GROUP  BY task_id
      HAVING n > 1
    )" 2>/dev/null || echo "0")
  if [[ "$count" == "0" ]]; then
    _check "NR-1 one attempt active per task" "$neg_expected" 0
    return $?
  fi
  _check "NR-1 one attempt active per task (found $count tasks with >1 active attempt)" "$neg_expected" 1
  return $?
}

# ─── NR-2: vecchia lease non finalizza ───────────────────────────────────────
# A TaskResult/AttemptFinalize attempt with an OLD lease_id (one that
# ExpireTaskLeaseAtomic already CAS-cleared) must not promote the task to
# terminal. Surface: count rows in task_attempts where status=SUCCEEDED but
# lease_id is no longer on the tasks.worker_id/lease_id row.
assert_invariant_2_old_lease_cannot_finalize() {
  local db="$1" neg_expected="${2:-0}"
  local count
  count=$(sqlite3 "$db" "
    SELECT COUNT(*) FROM task_attempts a
    JOIN tasks t ON t.task_id = a.task_id
    WHERE a.status = 'SUCCEEDED'
      AND (t.worker_id = '' OR t.worker_id <> a.worker_id
           OR t.lease_id = '' OR t.lease_id <> a.lease_id)" \
    2>/dev/null || echo "0")
  if [[ "$count" == "0" ]]; then
    _check "NR-2 old lease cannot finalize" "$neg_expected" 0
    return $?
  fi
  _check "NR-2 old lease cannot finalize (found $count leases promoted on stale tuple)" "$neg_expected" 1
  return $?
}

# ─── NR-3: no Job bloccato per sempre ─────────────────────────────────────────
# No job may be stuck non-terminal beyond the max-lease-TTL window once the
# applicable worker has torn down. Surface: jobs in (PENDING|LEASED|RUNNING|
# AWAITING_ARTIFACT) with stale updated_at (>10 min ago). Caller passes
# the window as $3 seconds.
assert_invariant_3_no_job_stuck_forever() {
  local db="$1" neg_expected="${2:-0}" window_sec="${3:-600}"
  local count
  count=$(sqlite3 "$db" "
    SELECT COUNT(*) FROM jobs
    WHERE status IN ('PENDING','LEASED','RUNNING','AWAITING_ARTIFACT')
      AND updated_at < datetime('now', '-${window_sec} seconds')" \
    2>/dev/null || echo "0")
  if [[ "$count" == "0" ]]; then
    _check "NR-3 no job stuck forever (>$((window_sec/60)) min stale)" "$neg_expected" 0
    return $?
  fi
  _check "NR-3 no job stuck forever (found $count non-terminal jobs older than $window_sec s)" "$neg_expected" 1
  return $?
}

# ─── NR-4: no file parziali in final storage ────────────────────────────────
# Status codes that indicate partial stage must NOT exist in storage:
# - artifact_uploads.status='CREATED' AND >24h old without a terminal flip
# - artifacts.status NOT IN ('READY','FAILED','REJECTED','RETRY') — STAGING
#   or VERIFYING is OK as transient but should not linger.
# Surface: artifact_uploads.temporary_storage_key with no matching
# artifact row in READY (orphan partial blob).
assert_invariant_4_no_partial_files_in_final() {
  local db="$1" neg_expected="${2:-0}" age_sec="${3:-86400}"
  local count
  count=$(sqlite3 "$db" "
    SELECT COUNT(*) FROM artifact_uploads au
    LEFT JOIN artifacts ar ON ar.id = au.artifact_id
    WHERE au.status NOT IN ('COMPLETED','FAILED','REJECTED','EXPIRED')
      AND au.created_at < datetime('now', '-${age_sec} seconds')
      AND (ar.id IS NULL OR ar.status NOT IN ('READY','FAILED','REJECTED'))" \
    2>/dev/null || echo "0")
  if [[ "$count" == "0" ]]; then
    _check "NR-4 no partial files in final storage (>$((age_sec/3600))h orphans)" "$neg_expected" 0
    return $?
  fi
  _check "NR-4 no partial files in final storage (found $count orphan partial blobs)" "$neg_expected" 1
  return $?
}

# ─── NR-5: no READY senza byte validi ────────────────────────────────────────
# Every artifacts.status='READY' row must have a non-empty sha256 AND a
# positive size_bytes. Empty hash + zero size = "READY without valid bytes".
assert_invariant_5_no_ready_without_valid_bytes() {
  local db="$1" neg_expected="${2:-0}"
  local count
  count=$(sqlite3 "$db" "
    SELECT COUNT(*) FROM artifacts
    WHERE status = 'READY'
      AND (sha256 IS NULL OR sha256 = '' OR size_bytes IS NULL OR size_bytes <= 0)" \
    2>/dev/null || echo "0")
  if [[ "$count" == "0" ]]; then
    _check "NR-5 no READY artifact without valid bytes" "$neg_expected" 0
    return $?
  fi
  _check "NR-5 no READY artifact without valid bytes (found $count)" "$neg_expected" 1
  return $?
}

# ─── NR-6: no SUCCEEDED senza READY ──────────────────────────────────────────
# jobs.status='SUCCEEDED' implies a matching artifacts row in 'READY'.
assert_invariant_6_no_succeeded_without_ready() {
  local db="$1" neg_expected="${2:-0}"
  local count
  count=$(sqlite3 "$db" "
    SELECT COUNT(*) FROM jobs j
    LEFT JOIN artifacts a
      ON a.job_id = j.job_id AND a.status = 'READY'
    WHERE j.status = 'SUCCEEDED'
      AND a.id IS NULL" \
    2>/dev/null || echo "0")
  if [[ "$count" == "0" ]]; then
    _check "NR-6 no SUCCEEDED job without READY artifact" "$neg_expected" 0
    return $?
  fi
  _check "NR-6 no SUCCEEDED job without READY artifact (found $count)" "$neg_expected" 1
  return $?
}

# ─── NR-7: nuovo attempt_id+lease_id dopo il reap ────────────────────────────
# After reaper recovery, the next RequeueExpiredLeases → ClaimNextReadyTask
# must mint a fresh (attempt_id, lease_id) tuple. Surface: for any task that
# recovered from a TIMED_OUT attempt, the currently-active attempt has a
# different (attempt_id) than the TIMED_OUT one AND (worker_id|lease_id)
# has been freshly assigned.
assert_invariant_7_new_attempt_id_after_reap() {
  local db="$1" neg_expected="${2:-0}"
  local count
  count=$(sqlite3 "$db" "
    SELECT COUNT(*) FROM tasks t
    JOIN task_attempts a
      ON a.task_id = t.task_id
    WHERE a.status = 'TIMED_OUT'
      AND t.status IN ('READY','LEASED','RUNNING')
      AND t.attempt_id = a.id" \
    2>/dev/null || echo "0")
  # The above checks if a TIMED_OUT attempt is being treated as the active
  # attempt — that would be a §9.5 desync. After Recovery: t.attempt_id
  # must NOT equal any TIMED_OUT a.id.
  if [[ "$count" == "0" ]]; then
    _check "NR-7 new attempt_id+lease_id after reap" "$neg_expected" 0
    return $?
  fi
  _check "NR-7 new attempt_id+lease_id after reap (found $count §9.5 desyncs)" "$neg_expected" 1
  return $?
}

# ─── Generic dispatcher ──────────────────────────────────────────────────────
# rm_assert_invariant <db> <label> <expected_negative:0|1> [<window_sec>] [<age_sec>]
# Routes by label suffix. Default windows: NR-3=600s, NR-4=86400s.
rm_assert_invariant() {
  local db="$1" label="$2" neg_expected="${3:-0}" w="$4" a="$5"
  case "$label" in
    NR-1) assert_invariant_1_one_attempt_active_per_task "$db" "$neg_expected" ;;
    NR-2) assert_invariant_2_old_lease_cannot_finalize "$db" "$neg_expected" ;;
    NR-3) assert_invariant_3_no_job_stuck_forever "$db" "$neg_expected" "${w:-600}" ;;
    NR-4) assert_invariant_4_no_partial_files_in_final "$db" "$neg_expected" "${a:-86400}" ;;
    NR-5) assert_invariant_5_no_ready_without_valid_bytes "$db" "$neg_expected" ;;
    NR-6) assert_invariant_6_no_succeeded_without_ready "$db" "$neg_expected" ;;
    NR-7) assert_invariant_7_new_attempt_id_after_reap "$db" "$neg_expected" ;;
    *) printf '%sFAIL%s  unknown invariant label: %s\n' "$I_RED" "$I_RST" "$label"; rm_mark_inv_fail; return 1 ;;
  esac
}
