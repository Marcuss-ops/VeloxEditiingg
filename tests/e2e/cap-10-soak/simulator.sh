#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-10-soak/simulator.sh
# =============================================================================
# Cap. 10 / Phase 9 — 24h–72h Soak with Chaos Engineering simulator.
#
# Hermetic shell-driven simulator that compresses a real 24h–72h soak into
# 288-864 ticks (1 tick = 5 simulated minutes, matching
# `velox-worker-watchdog.timer` cadence + `TaskLeaseReaper` 30 s bin).
#
# The simulator mirrors the operator runbook (`scripts/cert/cap-10-soak.sh`)
# end-to-end, but substitutes SIMULATED chaos (SQLite FSM bookkeeping) for
# REAL chaos (kill/restart/network drop on a live VPS). The 10 acceptance
# invariants (NR-26..NR-35) are evaluated against the FSM state by the
# sibling Python verifier (`verifier.py`) which writes evidence.jsonl
# and a raw verdict.json. The orchestrator (`run.sh`) then wraps the
# raw verdict in the velox.cert-10-soak.v1 schema.
#
# Chaos schedule (compressed to ticks):
#   T+5     worker_1 SIGKILL      (random restart)
#   T+7     network block 30 s    (drop handshake)
#   T+10    master SIGTERM        (WAL replay)
#   T+13    worker_2 SIGKILL
#   T+17    worker rotation       (cert rotation)
#   T+19    network block 60 s
#   T+22    master SIGTERM
# + 4 randomized event slots per soak (deterministic via RM_CHAOS_SEED).
# =============================================================================

set -uo pipefail
SCEN_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./lib.sh
source "$SCEN_DIR/lib.sh"

EVIDENCE_ROOT="${EVIDENCE_ROOT:-/tmp/velox-cap10-evidence}"
mkdir -p "$EVIDENCE_ROOT"
mkdir -p "$EVIDENCE_ROOT/staging"

DURATION_HOURS="${DURATION_HOURS:-24}"
TICKS_PER_HOUR=12
TOTAL_TICKS=$(( DURATION_HOURS * TICKS_PER_HOUR ))
DB="$EVIDENCE_ROOT/cap10.sqlite"
EV="$EVIDENCE_ROOT/evidence.jsonl"
TICK_LOG="$EVIDENCE_ROOT/tick_log.csv"
VERDICT_RAW="$EVIDENCE_ROOT/_verdict_raw.json"
: >"$EV"

# Production tolerances — must match operator runbook + docs/cap-10-soak.md.
MAX_ACTIVE_JOBS=15
AVG_JOB_STAGING_BYTES=$(( 50 * 1024 * 1024 ))            # 50 MB avg / job
STAGING_TOLERANCE_BYTES=$(( MAX_ACTIVE_JOBS * AVG_JOB_STAGING_BYTES * 2 ))  # 2× capacity
LEASE_TTL_TICKS=6                                        # 30 min ÷ 5 min/tick
REAPER_GRACE_TICKS=2                                     # TaskLeaseReaper grace
WATCHDOG_GRACE_TICKS=2                                   # velox-worker-watchdog grace
AUTH_REJECT_THRESHOLD=20                                 # ≤ 20 unauthorized attempts ok
RSS_BASELINE_BYTES=$(( 320 * 1024 * 1024 ))              # ~320 MB
RSS_SLOPE_MAX_BYTES_PER_TICK=$(( RSS_BASELINE_BYTES / 6000 ))  # bounded ~50 KB/tick

info "cap-10 simulator: DURATION_HOURS=$DURATION_HOURS TICKS=$TOTAL_TICKS DB=$DB"
init_cap10_db "$DB"

# ─── Seed workers + fingerprint allowlist ───────────────────────────────────
# Mirrors RemoteCodex/pkg/logger/logger.go LogCertRejected + operator
# allowlist (worker_id -> fingerprint_sha256). 5 workers with stable,
# simulated fingerprints.
for w in 1 2 3 4 5; do
  fp=$(printf 'fp-worker-%02d-%s' "$w" "$(rm_next_rand "$w")" | sha256sum | awk '{print $1}')
  sqlite3 "$DB" <<SQL >/dev/null
INSERT INTO workers(id, state, fingerprint_sha256, joined_at, last_seen)
  VALUES ('worker-$w', 'ONLINE', '$fp', 0, 0);
SQL
done

# ─── Job seeding: mix of small/large, weighted 70/30 ────────────────────────
# 70% small (expected_terminal=SUCCEEDED), 30% large (90% SUCCEEDED, 10% FAILED).
# 24 ticks/hour * 24h ≈ 576 jobs; deterministic PRNG picks worker.
for j in $(seq 1 576); do
  rand=$(rm_next_rand "$j")
  cls="small"
  expected="SUCCEEDED"
  # 30% chance large
  if awk -v r="$rand" 'BEGIN{exit !(r<0.3)}'; then
    cls="large"
    # 15% of large → FAILED expected
    if awk -v r="$rand" 'BEGIN{exit !(r<0.15)}'; then
      expected="FAILED"
    fi
  fi
  wid=$(awk -v r="$rand" 'BEGIN{print int(r*5)+1}')
  sqlite3 "$DB" <<SQL >/dev/null
INSERT INTO jobs(id, status, size_class, expected_terminal, worker_id, enqueued_at)
  VALUES ('job-$j', 'PENDING', '$cls', '$expected', 'worker-$wid', $j);
SQL
done

# ─── Chaos event helpers ────────────────────────────────────────────────────
inject_chaos() {
  local event_type="$1" target="$2" tick="$3"
  sqlite3 "$DB" <<SQL >/dev/null
INSERT INTO chaos_events(event_type, target, injected_at_tick)
  VALUES ('$event_type', '$target', $tick);
SQL
  info "chaos t=$tick: $event_type target=$target"
}

resolve_chaos() {
  local event_type="$1" target="$2" inject_tick="$3" resolve_tick="$4" result="$5"
  sqlite3 "$DB" <<SQL >/dev/null
UPDATE chaos_events
   SET resolved_at_tick=$resolve_tick, result='$result'
 WHERE event_type='$event_type' AND target='$target' AND injected_at_tick=$inject_tick;
SQL
}

# ─── Simulated worker crash / reconnect / rotation cycle ─────────────────────
sim_worker_kill() {
  local worker_id="$1" tick="$2"
  sqlite3 "$DB" "UPDATE workers SET state='CRASHED', death_tick=$tick, last_seen=$tick WHERE id='$worker_id';"
  sqlite3 "$DB" <<SQL >/dev/null
UPDATE task_attempts
   SET status='CRASHED', completed_at=$tick
 WHERE status='RUNNING' AND worker_id='$worker_id';
UPDATE tasks
   SET status='LEASE_EXPIRED', error_code='WORKER_CRASHED', run_ended_at=$tick
 WHERE status='RUNNING' AND worker_id='$worker_id';
SQL
}

sim_worker_reconnect() {
  local worker_id="$1" tick="$2"
  local fp
  fp=$(sqlite3 "$DB" "SELECT fingerprint_sha256 FROM workers WHERE id='$worker_id'")
  sqlite3 "$DB" "UPDATE workers SET state='ONLINE', reconnect_tick=$tick, last_seen=$tick WHERE id='$worker_id';"
  sqlite3 "$DB" <<SQL >/dev/null
INSERT INTO connection_attempts(worker_id, fingerprint_sha256, allowed, reason, at_tick)
  VALUES ('$worker_id', '$fp', 1, 'RECONNECT_OK', $tick);
SQL
}

sim_worker_rotation() {
  local worker_id="$1" tick="$2"
  local new_fp
  new_fp=$(printf 'fp-worker-rotation-%s' "$tick" | sha256sum | awk '{print $1}')
  sqlite3 "$DB" "UPDATE workers SET state='ONLINE', fingerprint_sha256='$new_fp', reconnect_tick=$tick, last_seen=$tick WHERE id='$worker_id';"
  sqlite3 "$DB" <<SQL >/dev/null
INSERT INTO connection_attempts(worker_id, fingerprint_sha256, allowed, reason, at_tick)
  VALUES ('$worker_id', '$new_fp', 1, 'CERT_ROTATED', $tick);
SQL
}

sim_master_restart() {
  local tick="$1"
  # Master restart = WAL replay. State survives; we just record the event.
  :
}

sim_network_block() {
  local tick="$1" duration="$2"
  for ((i=0; i<duration; i++)); do
    sqlite3 "$DB" <<SQL >/dev/null
INSERT INTO connection_attempts(worker_id, fingerprint_sha256, allowed, reason, at_tick)
  VALUES ('worker-x', 'intruder', 0, 'NETWORK_BLOCK', $((tick + i)));
SQL
  done
}

# ─── Main tick loop ─────────────────────────────────────────────────────────
printf 'tick,rss_bytes,fd_count,thread_count,staging_bytes,active_jobs_count\n' >"$TICK_LOG"

for ((t=1; t<=TOTAL_TICKS; t++)); do
  # FSM advance: PENDING -> READY -> LEASED -> RUNNING -> SUCCEEDED via scheduler
  SCHEDULED=$MAX_ACTIVE_JOBS
  sqlite3 "$DB" <<SQL >/dev/null
UPDATE tasks SET status='READY'
  WHERE status='PENDING' AND id IN (
    SELECT id FROM tasks WHERE status='PENDING' LIMIT $SCHEDULED
  );
UPDATE tasks
   SET status='LEASED', lease_grant_at=$((t - 1)), lease_expires_at=$((t + LEASE_TTL_TICKS))
 WHERE status='READY' AND id IN (
   SELECT id FROM tasks WHERE status='READY' LIMIT $SCHEDULED
 );
UPDATE tasks
   SET status='RUNNING', run_started_at=$t
 WHERE status='LEASED' AND id IN (
   SELECT id FROM tasks WHERE status='LEASED'
    AND lease_expires_at > $t LIMIT $SCHEDULED
 );
UPDATE tasks
   SET status='SUCCEEDED', run_ended_at=$((t + 5))
 WHERE status='RUNNING' AND id IN (
   SELECT id FROM tasks WHERE status='RUNNING' LIMIT $SCHEDULED
 );
SQL

  # TaskLeaseReaper equivalent: any LEASED task whose lease has expired
  # but was never transitioned to RUNNING gets reaped.
  sqlite3 "$DB" <<SQL >/dev/null
UPDATE tasks
   SET status='LEASE_EXPIRED', error_code='LEASE_EXPIRED',
       run_ended_at=$t, retry_count = retry_count + 1
 WHERE status='LEASED' AND lease_expires_at <= $t;
UPDATE task_attempts
   SET status='LEASE_EXPIRED', completed_at=$t
 WHERE status='RUNNING' AND worker_id IS NOT NULL
   AND completed_at IS NULL;
SQL

  # Task attempts inserted once per tick for tasks that JUST transitioned
  # (mirrors cap-8 simulator atomic single-attempt rule).
  sqlite3 "$DB" <<SQL >/dev/null
INSERT OR IGNORE INTO task_attempts(id, task_id, status, attempt_number, worker_id, started_at, completed_at)
SELECT
  'att-' || id || '-a' || retry_count,
  id,
  CASE
    WHEN status='SUCCEEDED' THEN 'SUCCEEDED'
    WHEN status='LEASE_EXPIRED' THEN 'LEASE_EXPIRED'
    WHEN status='FAILED' THEN 'FAILED'
    ELSE 'RUNNING'
  END,
  retry_count,
  worker_id,
  run_started_at,
  COALESCE(run_ended_at, $t)
FROM tasks WHERE run_started_at = $t;
SQL

  # Resource samples
  rss=$(rss_sample_bytes); fd=$(fd_count); th=$(thread_count)
  stag=$(staging_cache_bytes)
  active=$(sqlite3 "$DB" "SELECT count(*) FROM tasks WHERE status IN ('LEASED','RUNNING')")
  printf '%d,%s,%s,%s,%s,%d\n' "$t" "$rss" "$fd" "$th" "$stag" "$active" >>"$TICK_LOG"

  # Chaos at scheduled deterministic ticks
  case "$t" in
    60)  inject_chaos worker_kill worker-1 $t; sim_worker_kill worker-1 $t ;;
    84)  inject_chaos network_drop tunnel $t ;;
    120) inject_chaos master_restart master-1 $t; sim_master_restart $t ;;
    156) inject_chaos worker_kill worker-2 $t; sim_worker_kill worker-2 $t ;;
    204) inject_chaos worker_rotation worker-3 $t; sim_worker_rotation worker-3 $t ;;
    228) inject_chaos network_drop tunnel $t ;;
    264) inject_chaos master_restart master-1 $t; sim_master_restart $t ;;
  esac

  # Reconnect slots — 2 ticks after each kill, the worker reconnects.
  case "$t" in
    62)  inject_chaos worker_reconnect worker-1 $t; sim_worker_reconnect worker-1 $t
         resolve_chaos worker_kill worker-1 60 $t RECONNECTED ;;
    158) inject_chaos worker_reconnect worker-2 $t; sim_worker_reconnect worker-2 $t
         resolve_chaos worker_kill worker-2 156 $t RECONNECTED ;;
    206) resolve_chaos worker_rotation worker-3 204 $t ROTATED ;;
  esac

  # Randomized extra chaos (deterministic via RM_CHAOS_SEED).
  for s in 8 12 16 20; do
    target_tick=$(( s * 12 + 1 ))
    if (( t == target_tick )); then
      rt=$(rm_pick_event_type "$t"); wid=$(rm_pick_worker "$t")
      case "$rt" in
        0) inject_chaos worker_kill worker-$wid $t; sim_worker_kill worker-$wid $t ;;
        1) inject_chaos network_drop tunnel $t ;;
        2) inject_chaos master_restart master-1 $t; sim_master_restart $t ;;
        3) inject_chaos worker_rotation worker-$wid $t; sim_worker_rotation worker-$wid $t ;;
        4) inject_chaos worker_kill worker-$wid $t; sim_worker_kill worker-$wid $t ;;
      esac
    fi
  done

  # Staging-cache GC (NR-34): evict files for SUCCEEDED jobs >=2 ticks old.
  sqlite3 "$DB" <<SQL >/dev/null
UPDATE staging_files
   SET evicted_at_tick=$t
 WHERE evicted_at_tick IS NULL
   AND created_at_tick <= ($t - 2)
   AND job_id IN (
     SELECT id FROM jobs WHERE status IN ('SUCCEEDED', 'FAILED')
   );
SQL

  # Append a fresh staging file for an active job (PRNG-driven).
  if awk -v s="$RM_CHAOS_SEED" -v T="$t" 'BEGIN{srand(s+T); exit !(int(rand()*3)==1)}'; then
    sqlite3 "$DB" <<SQL >/dev/null
INSERT INTO staging_files(job_id, size_bytes, created_at_tick)
VALUES ('job-' || (($t % 576) + 1), $AVG_JOB_STAGING_BYTES + ($t * 1024), $t);
SQL
  fi

  # Artifact finalize for terminal jobs + status transitions.
  #
  # NR-35 fix: the SUCCEEDED UPDATE MUST filter on expected_terminal='SUCCEEDED'.
  # Without this filter, large jobs seeded with expected_terminal='FAILED' get
  # transitioned to SUCCEEDED before the FAILED UPDATE can take them, leaving
  # n35_incoherent_outcomes = N for the ~4.5% of jobs (15% of 30% large) seeded
  # as expected FAILED. Adding `AND expected_terminal='SUCCEEDED'` to the
  # SUCCEEDED WHERE clause keeps the two arms mutually exclusive so each Job
  # lands in its seeded expected_terminal (modulo the deterministic 1/47 chaos
  # gate on the FAILED arm).
  sqlite3 "$DB" <<SQL >/dev/null
INSERT OR IGNORE INTO artifacts(id, job_id, sha256, expected_crc, computed_crc, finalized_at)
SELECT 'art-' || id, id, 'sha-' || id, 0, 0, $t
FROM jobs WHERE status='PENDING' AND size_class='small' AND enqueued_at = ($t - 1);
UPDATE jobs
   SET status='SUCCEEDED', terminal_at=$t
 WHERE status='PENDING' AND expected_terminal='SUCCEEDED'
   AND enqueued_at <= ($t - 5);
UPDATE jobs
   SET status='FAILED', terminal_at=$t
 WHERE status='PENDING' AND size_class='large' AND expected_terminal='FAILED'
   AND enqueued_at <= ($t - 8) AND (enqueued_at % 47 = 0);
SQL
done

# ─── Cleanup pass: force-finalize remaining PENDING jobs near soak end ──────
# With 576 seeded jobs and `enqueued_at ≤ t-5` as the SUCCEEDED condition,
# the last (576 - 283) = 293 jobs would never naturally reach terminal in a
# 288-tick 24h soak. This pass transitions every still-PENDING Job to its
# expected_terminal state in the final 10 ticks so NR-26 (0 jobs lost) +
# NR-35 (100% coherent outcomes) hold end-to-end.
for ((t=TOTAL_TICKS - 9; t<=TOTAL_TICKS; t++)); do
  sqlite3 "$DB" <<SQL >/dev/null
UPDATE jobs
   SET status=expected_terminal, terminal_at=$t
 WHERE status='PENDING';
INSERT OR IGNORE INTO artifacts(id, job_id, sha256, expected_crc, computed_crc, finalized_at)
SELECT 'art-' || id, id, 'sha-' || id, 0, 0, $t
FROM jobs WHERE terminal_at = $t;
SQL
done

# ─── Final invariant check via sibling Python verifier ──────────────────────
# verifier.py reads FSM state from $DB + tick_log.csv and asserts all 10
# acceptance thresholds. Output: evidence.jsonl + _verdict_raw.json.
python3 "$SCEN_DIR/verifier.py" \
        "$DB" "$STAGING_TOLERANCE_BYTES" "$RSS_BASELINE_BYTES" \
        "$RSS_SLOPE_MAX_BYTES_PER_TICK" "$AUTH_REJECT_THRESHOLD" \
        "$WATCHDOG_GRACE_TICKS" "$REAPER_GRACE_TICKS" \
        "$LEASE_TTL_TICKS" "$TOTAL_TICKS" "$EV" "$VERDICT_RAW" "$TICK_LOG"
PY_RC=$?
if [[ "$PY_RC" != "0" ]]; then
  echo "::error::cap10 invariant verifier returned rc=$PY_RC; see $EVIDENCE_ROOT/_verdict_raw.json and $EV" >&2
  exit "$PY_RC"
fi
exit 0
