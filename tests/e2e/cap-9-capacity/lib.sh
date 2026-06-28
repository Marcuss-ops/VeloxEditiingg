#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-9-capacity/lib.sh
# =============================================================================
# Shared bash helpers for the cap. 9 capacity-curve CI simulator. The
# cap. 9 deliverable covers 12 cells (3 workload profiles × 4 capacity
# multipliers) + a 150-frame C++ engine benchmark; every cell produces
# hermetic evidence without docker / FFmpeg / live master.
#
# Provides:
#   - Per-cell accumulator (RM_CELL_FAILED; rm_begin_cell / rm_end_cell).
#   - Resource samplers (RSS via /proc/self/status, fd count, threads).
#     These are used to assert bounded-RSS growth for the Large profile.
#   - Deterministic PPM frame generator (Python stdlib only) — feeds
#     Small profile byte-determinism + frame-channel PMF checks.
#   - SQLite FSM seed (jobs, tasks, task_attempts, worker_messages).
#   - Per-executor counter struct (succeeded / failed / timed_out /
#     lease_expired / retries / fallback_full_dirty). Mirrors the
#     executor_id-keyed counter family in
#     RemoteCodex/.../internal/telemetry/metrics.go.
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
  MULTIPLIER="${2:-?}"
  CELL="${PROFILE}@${MULTIPLIER}x"
  RM_CELL_FAILED=0
  printf '%s═══ cell %s ═══%s\n' "$I_BLUE" "$CELL" "$I_RST"
}
rm_end_cell() {
  local desc="$1"
  local status
  if (( RM_CELL_FAILED == 0 )); then
    status="PASS"
    # Human-readable line on STDERR (so callers capturing STDOUT to a file
    # only receive "PASS"/"FAIL" without the multi-line prefix).
    printf '%s[PASS]%s  %s — %s\n' "$I_GREEN" "$I_RST" "${PROFILE:-?}@${MULTIPLIER:-?}x" "$desc" >&2
  else
    status="FAIL"
    printf '%s[FAIL]%s  %s — %s\n' "$I_RED"   "$I_RST" "${PROFILE:-?}@${MULTIPLIER:-?}x" "$desc" >&2
  fi
  printf '%s' "$status"
}

ok()       { printf '%s[OK]%s  %s\n'   "$I_GREEN"  "$I_RST" "$*"; }
warn()     { printf '%s[WARN]%s %s\n'  "$I_YELLOW" "$I_RST" "$*"; }
fail()     { printf '%s[FAIL]%s %s\n'  "$I_RED"    "$I_RST" "$*"; exit 1; }
info()     { printf '%s[i]%s    %s\n'  "$I_BLUE"   "$I_RST" "$*"; }

# ─── Path constants ────────────────────────────────────────────────────────
: "${EVIDENCE_ROOT:=/tmp/velox-cap9-evidence}"
: "${PROFILE:=small}"
: "${MULTIPLIER:=1}"
CELL_DIR="$EVIDENCE_ROOT/$PROFILE/${MULTIPLIER}x"
mkdir -p "$CELL_DIR"

# ─── Resource sampler — RSS, fd, threads ───────────────────────────────────
# RSS from /proc/self/status. Linux-only; returns 0 elsewhere (caller-bound).
# cache_for_ms keeps the script from spamming /proc/self/status for hundreds
# of cells.
_rss_cache=""; _rss_cache_at=0
rss_sample_bytes() {
  local now; now=$(date +%s%3N 2>/dev/null || date +%s)
  if (( now - _rss_cache_at < 250 )); then
    printf '%s\n' "$_rss_cache"; return 0
  fi
  if [[ -r /proc/self/status ]]; then
    _rss_cache="$(awk '/^VmRSS:/ {print $2}' /proc/self/status 2>/dev/null || echo 0)"
    # VmRSS is in kB; convert to bytes for parity with DataServer/procstat.go.
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

# ─── Deterministic PPM generator + PMF extractor ────────────────────────────
# NR-16 + NR-17 hinge on:
#   (a) exact-byte repro: same seed → same bytes.
#   (b) PMF equality: sorted histogram of pixel channels identifies the
#       frame deterministically without depending on byte-level layout.
# We use Python 3 stdlib (random + sys.stdout) — no /dev/urandom, no time.
synthetic_ppm_path=""
synthetic_ppm() {
  local seed="$1" w="$2" h="$3" out="$4"
  python3 - "$seed" "$w" "$h" >"$out" <<'PYEOF' || { fail "synthetic_ppm failed"; }
import sys, random
seed = int(sys.argv[1]); w = int(sys.argv[2]); h = int(sys.argv[3])
random.seed(seed)
# PPM P3 plain ASCII — small frames are byte-exact deterministic.
sys.stdout.write(f"P3\n{w} {h}\n255\n")
for _ in range(w * h):
    sys.stdout.write(f"{random.randint(0, 255)} {random.randint(0, 255)} {random.randint(0, 255)}\n")
PYEOF
  printf '%s\n' "$out"
}

# PMF = (count, value) lex-sorted tuples. Two frames with identical
# generation produce byte-identical PMF files (cmp compares them).
extract_pmf() {
  local input="$1" pmf_out="$2"
  # Drop the 3-line header (P3\nWxH\n255\n), then sort the lines
  # (each is one RGB triple). Sort gives the PMF; uniq -c counts.
  tail -n +4 "$input" | sort | uniq -c >"$pmf_out"
  printf '%s\n' "$pmf_out"
}

# ─── SQLite FSM seed (jobs + tasks + task_attempts + worker_messages) ───────
# The schema mirrors tests/e2e/cap-7-reboot-recovery/lib.sh + adds columns
# needed for queue-latency tracking and per-executor counters.
init_cap9_db() {
  local db="$1"
  rm -f "$db"
  sqlite3 "$db" <<'SQL'
CREATE TABLE jobs (
  id TEXT PRIMARY KEY,
  status TEXT,
  video_name TEXT,
  spec_hash TEXT,
  executor_id TEXT,
  worker_class TEXT
);
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  job_id TEXT, status TEXT, worker_id TEXT,
  queue_ready_at INTEGER,
  lease_grant_at INTEGER,
  run_started_at INTEGER,
  run_ended_at INTEGER,
  retry_count INTEGER DEFAULT 0
);
CREATE TABLE task_attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT, job_id TEXT, attempt_number INTEGER,
  worker_id TEXT, status TEXT, lease_id TEXT,
  error_code TEXT,
  enqueued_at INTEGER,
  leased_at INTEGER,
  started_at INTEGER,
  completed_at INTEGER
);
CREATE TABLE worker_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id TEXT, kind TEXT, payload_json TEXT, at INTEGER
);
CREATE TABLE per_executor_counters (
  executor_id TEXT PRIMARY KEY,
  succeeded INTEGER DEFAULT 0,
  failed INTEGER DEFAULT 0,
  timed_out INTEGER DEFAULT 0,
  lease_expired INTEGER DEFAULT 0,
  retries INTEGER DEFAULT 0,
  fallback_full_dirty INTEGER DEFAULT 0
);
SQL
}

# ─── Per-cell evidence writers ─────────────────────────────────────────────
# Each cell writes its own evidence bundle so 12 cells × 2 evidence files
# don't collide. The orchestrator picks them all up afterwards.
write_cell_evidence() {
  local name="$1" content="$2" file="${3:-}"
  : "${file:=$CELL_DIR/$name.json}"
  printf '%s' "$content" >"$file"
}

bump_executor() {
  local db="$1" executor="$2" field="$3"
  sqlite3 "$db" <<SQL >/dev/null
INSERT INTO per_executor_counters(executor_id,$field) VALUES ('$executor', 1)
  ON CONFLICT(executor_id) DO UPDATE SET $field = $field + 1;
SQL
}
