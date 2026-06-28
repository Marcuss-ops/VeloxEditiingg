#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-10-soak/lib.sh
# =============================================================================
# Shared bash helpers for the cap. 10 24h–72h soak CI simulator.
#
# Cap. 10 deliverable covers a continuous-job soak with chaos injection
# (worker restart, network interruption, master restart, worker rotation,
# mixed small/large Jobs) and asserts 10 hard acceptance thresholds
# (NR-26 … NR-35).
#
# Provides:
#   - Per-cell accumulator (RM_CELL_FAILED; rm_begin_cell / rm_end_cell).
#   - Chaos PRNG (seeded deterministic schedule; mirrors the operator's
#     random-restart cadence so CI is reproducible across hosts).
#   - Resource samplers: RSS (VmRSS) / fd count / Threads / staging_size.
#     RSS sampler is cached for ~250 ms (matches cap. 9 lib.sh).
#   - SQLite FSM schema seed: jobs, tasks, task_attempts, workers,
#     artifacts, connection_attempts, chaos_events. The `connection_attempts`
#     table mirrors the production path
#     (RemoteCodex/.../pkg/logger/logger.go LogCertRejected) so NR-30 can
#     be asserted without an actual gRPC server — fingerprint allowlist
#     lookups against the simulated allowlist reproduce the
#     `scripts/check-share-cert.sh` dedup logic.
#   - Bump helpers for per_worker_counters + per_executor_counters
#     (mirrors cap. 9 lib.sh semantics).
#
# Chaos schedule horizon (operator runbook + CI simulator share):
#   T+5     worker_1 SIGKILL (random restart)              → 1 event
#   T+7     30 s network block drop                       → 1 event
#   T+10    master SIGTERM → WAL replay → restart          → 1 event
#   T+13    worker_2 SIGKILL                               → 1 event
#   T+17    worker rotation (cert rotation, 7-day overlap) → 1 event
#   T+19    60 s network block drop                       → 1 event
#   T+22    master SIGTERM → WAL replay → restart          → 1 event
# (events compressed to 1 tick = 5 simulated minutes in the CI simulator).
# =============================================================================

set -uo pipefail  # NOT -e: every invariant reports individually

# ─── Color shims ────────────────────────────────────────────────────────────
I_GREEN=$'\033[1;32m'
I_RED=$'\033[1;31m'
I_YELLOW=$'\033[1;33m'
I_BLUE=$'\033[1;34m'
I_RST=$'\033[0m'

# ─── Per-cell accumulator ───────────────────────────────────────────────────
RM_CELL_FAILED=${RM_CELL_FAILED:-0}
rm_mark_inv_fail() { RM_CELL_FAILED=1; }
rm_begin_cell() {
  PROFILE="${1:-?}"
  HOURS="${2:-0}"
  CELL="${PROFILE}@${HOURS}h"
  RM_CELL_FAILED=0
  printf '%s═══ cell %s ═══%s\n' "$I_BLUE" "$CELL" "$I_RST"
}
rm_end_cell() {
  local desc="$1"
  local status
  if (( RM_CELL_FAILED == 0 )); then
    status="PASS"
    printf '%s[PASS]%s  %s — %s\n' "$I_GREEN" "$I_RST" "${PROFILE:-?}@${HOURS:-?}h" "$desc" >&2
  else
    status="FAIL"
    printf '%s[FAIL]%s  %s — %s\n' "$I_RED"   "$I_RST" "${PROFILE:-?}@${HOURS:-?}h" "$desc" >&2
  fi
  printf '%s' "$status"
}

ok()       { printf '%s[OK]%s  %s\n'   "$I_GREEN"  "$I_RST" "$*"; }
warn()     { printf '%s[WARN]%s %s\n'  "$I_YELLOW" "$I_RST" "$*"; }
fail()     { printf '%s[FAIL]%s %s\n'  "$I_RED"    "$I_RST" "$*"; exit 1; }
info()     { printf '%s[i]%s    %s\n'  "$I_BLUE"   "$I_RST" "$*"; }

# ─── Path constants ────────────────────────────────────────────────────────
: "${EVIDENCE_ROOT:=/tmp/velox-cap10-evidence}"
: "${PROFILE:=small}"
: "${HOURS:=0}"
CELL_DIR="$EVIDENCE_ROOT/$PROFILE/${HOURS}h"
mkdir -p "$CELL_DIR"

# ─── Chaos PRNG (deterministic, seedable) ───────────────────────────────────
# Deterministic chaos scheduling is critical: a CI rerun with the same
# SEED must inject the same chaos events at the same ticks, otherwise
# NR-26..NR-35 would race against the schedule and produce non-reproducible
# Pass/Fail decisions.
RM_CHAOS_SEED="${RM_CHAOS_SEED:-42}"
rm_seed_random()    { awk -v s="$RM_CHAOS_SEED" 'BEGIN{srand(s); print rand()}'; }
rm_next_rand()      { awk -v s="$RM_CHAOS_SEED" 'BEGIN{srand(s + ARGV[1]); print rand()}' "$1"; }
rm_pick_worker()    { awk -v s="$RM_CHAOS_SEED" 'BEGIN{srand(s + ARGV[1]); print int(rand()*5)+1}' "$1"; }
rm_pick_event_type(){ awk -v s="$RM_CHAOS_SEED" 'BEGIN{srand(s + ARGV[1]); print int(rand()*5)}' "$1"; }

# ─── Resource samplers ──────────────────────────────────────────────────────
# RSS from /proc/self/status (Linux-only). Returns 0 elsewhere.
_rss_cache=""; _rss_cache_at=0
rss_sample_bytes() {
  local now; now=$(date +%s%3N 2>/dev/null || date +%s)
  if (( now - _rss_cache_at < 250 )); then
    printf '%s\n' "$_rss_cache"; return 0
  fi
  if [[ -r /proc/self/status ]]; then
    _rss_cache="$(awk '/^VmRSS:/ {print $2}' /proc/self/status 2>/dev/null || echo 0)"
    if [[ "$_rss_cache" =~ ^[0-9]+$ ]]; then
      _rss_cache=$(( _rss_cache * 1024 ))
    fi
  else
    _rss_cache=0
  fi
  _rss_cache_at=$now
  printf '%s\n' "$_rss_cache"
}

fd_count() {
  if [[ -d /proc/self/fd ]]; then
    printf '%d\n' "$(find /proc/self/fd -maxdepth 1 -mindepth 1 2>/dev/null | wc -l)"
  else
    printf '0\n'
  fi
}

thread_count() {
  if [[ -r /proc/self/status ]]; then
    awk '/^Threads:/ {print $2}' /proc/self/status 2>/dev/null || echo 0
  else
    printf '0\n'
  fi
}

# Staging cache size. In production this would be the `work_dir` + the
# `staging` subdir under /var/lib/velox. In the simulator we measure
# the SQLite page-byte footprint as a proxy (deterministic, no /var/lib
# dependency).
staging_cache_bytes() {
  local staging_dir="$EVIDENCE_ROOT/staging"
  if [[ -d "$staging_dir" ]]; then
    du -sb "$staging_dir" 2>/dev/null | awk '{print $1}'
  else
    printf '%d\n' 0
  fi
}

# ─── SQLite FSM schema seed ────────────────────────────────────────────────
# Mirrors the canonical schemas in tests/e2e/cap-7-reboot-recovery/lib.sh +
# cap-9 lib.sh + adds tables needed for chaos injection telemetry
# (connection_attempts, chaos_events, kill_log, reconnect_log) plus
# artifact dedup / corruption tracking (artifact_hash, artifact_crc).
init_cap10_db() {
  local db="$1"
  rm -f "$db"
  sqlite3 "$db" <<'SQL'
CREATE TABLE jobs (
  id TEXT PRIMARY KEY,
  status TEXT,
  size_class TEXT,           -- "small" | "large" (mix per profile)
  expected_terminal TEXT,    -- "SUCCEEDED" | "FAILED" (per profile)
  worker_id TEXT,
  enqueued_at INTEGER,
  terminal_at INTEGER
);
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  job_id TEXT, status TEXT, worker_id TEXT,
  queue_ready_at INTEGER,
  lease_grant_at INTEGER,
  lease_expires_at INTEGER,
  run_started_at INTEGER,
  run_ended_at INTEGER,
  retry_count INTEGER DEFAULT 0,
  error_code TEXT
);
CREATE TABLE task_attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT, status TEXT, attempt_number INTEGER,
  worker_id TEXT, started_at INTEGER, completed_at INTEGER
);
CREATE TABLE workers (
  id TEXT PRIMARY KEY,
  state TEXT,                -- "ONLINE" | "CRASHED" | "DRAINING" | "DEAD"
  fingerprint_sha256 TEXT,
  joined_at INTEGER,
  last_seen INTEGER,
  death_tick INTEGER DEFAULT NULL,
  reconnect_tick INTEGER DEFAULT NULL
);
CREATE TABLE artifacts (
  id TEXT PRIMARY KEY,
  job_id TEXT,
  sha256 TEXT,
  expected_crc INTEGER,
  computed_crc INTEGER,
  finalized_at INTEGER
);
CREATE TABLE connection_attempts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id TEXT,
  fingerprint_sha256 TEXT,
  allowed INTEGER,           -- 1=accepted, 0=rejected
  reason TEXT,
  at_tick INTEGER
);
CREATE TABLE chaos_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT,           -- "worker_kill" | "network_drop" | "master_restart" | "worker_rotation"
  target TEXT,
  injected_at_tick INTEGER,
  resolved_at_tick INTEGER DEFAULT NULL,
  result TEXT DEFAULT NULL
);
CREATE TABLE staging_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT,
  size_bytes INTEGER,
  created_at_tick INTEGER,
  evicted_at_tick INTEGER
);
CREATE TABLE per_worker_counters (
  worker_id TEXT PRIMARY KEY,
  succeeded INTEGER DEFAULT 0,
  failed INTEGER DEFAULT 0,
  timed_out INTEGER DEFAULT 0,
  lease_expired INTEGER DEFAULT 0,
  unauthorized_attempts INTEGER DEFAULT 0
);
SQL
}

# ─── Per-cell evidence writers ─────────────────────────────────────────────
write_cell_evidence() {
  local name="$1" content="$2" file="${3:-}"
  : "${file:=$CELL_DIR/$name.json}"
  printf '%s' "$content" >"$file"
}

bump_worker_counter() {
  local db="$1" worker="$2" field="$3"
  sqlite3 "$db" <<SQL >/dev/null
INSERT INTO per_worker_counters(worker_id,$field) VALUES ('$worker', 1)
  ON CONFLICT(worker_id) DO UPDATE SET $field = $field + 1;
SQL
}
