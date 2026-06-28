#!/usr/bin/env bash
# =============================================================================
# tests/e2e/recovery-matrix/lib.sh — shared helpers for the 15-scenario matrix
# =============================================================================
# Pure-function style: sourced by run.sh + scenarios/*.sh so every scenario
# gets the same master/worker boot contract, kill-timer scaffolding, and
# evidence-dir layout.
#
# Net contract:
#   rm_init <evidence_root> <worker_id>   — create evidence/<date>/fleet/<scenario>/ layout
#   rm_spawn_master <scenario> <env>      — start velox-server, push pid onto CHILD_PIDS[]
#   rm_spawn_worker <scenario> <id> <cfg> — start velox-worker-agent, push pid
#   rm_kill <pid> [<signal>]               — SIGSTOP/SIGTERM/SIGKILL with a budget
#   rm_wait_master_healthy <sec>           — poll /health until UP
#   rm_wait_worker_registered <sec>        — poll /api/v1/workers until CONNECTED
#   rm_wait_terminus <db> <job_id> <sec>   — poll jobs.status until terminal
#   rm_trap_cleanup                        — trap EXIT → kill_all, then keep WORKDIR
#
# Color shims: only emit ANSI when stdout is a TTY (CI logs stay plain text).
# =============================================================================

# ─── ANSI colour shims ───────────────────────────────────────────────────────
if [[ -t 1 ]]; then
  ESC_GREEN=$'\033[32m'; ESC_RED=$'\033[31m'; ESC_CYAN=$'\033[36m'
  ESC_YELLOW=$'\033[33m'; ESC_RST=$'\033[0m'
else
  ESC_GREEN=""; ESC_RED=""; ESC_CYAN=""; ESC_YELLOW=""; ESC_RST=""
fi

rm_pass()  { printf '%sPASS%s  %s\n' "$ESC_GREEN" "$ESC_RST" "$*"; return 0; }
rm_fail()  { printf '%sFAIL%s  %s\n' "$ESC_RED"   "$ESC_RST" "$*"; return 1; }
rm_info()  { printf '%s.. %s%s\n'    "$ESC_CYAN"  "$ESC_RST" "$*"; return 0; }
rm_warn()  { printf '%sWARN%s  %s\n' "$ESC_YELLOW" "$ESC_RST" "$*"; return 0; }

# ─── Bug-compatibility guard: refuse to run as root ─────────────────────────
# Root would mask real ENOSPC / chmod failures; the matrix must surface them.
if [[ "$EUID" -eq 0 ]] && [[ -z "${VELOX_RECOVERY_ALLOW_ROOT:-}" ]]; then
  printf 'FATAL: recovery-matrix refuses to run as root (ENOSPC sim is unreliable).\n' >&2
  printf '       Re-run as a regular user (try: sudo -u velox make recovery-matrix)\n' >&2
  printf '       OR set VELOX_RECOVERY_ALLOW_ROOT=1 to opt in.\n' >&2
  exit 3
fi

# ─── Per-scenario FAIL accumulator ───────────────────────────────────────────
# Each scenario calls `rm_begin_scenario $SCENARIO_ID` at the top. Each
# invariant assertion (rm_assert_invariant) that returns non-zero calls
# `rm_mark_inv_fail` to flip the accumulator. The scenario's tail calls
# `rm_end_scenario $SCENARIO_ID "<summary>"` which routes the verdict:
#   - accumulator=0 => rm_record_verdict PASS
#   - accumulator=1 => rm_record_verdict FAIL
# Without this, the matrix historically hardcoded `PASS` at the tail and
# silently masked all NR-x failures. (B1 fix from cap-6 review pass.)
declare -g RM_CURRENT_SCENARIO=""
declare -g RM_SCEN_FAILED=0   # 0=no failed invariant in current scenario; 1=at least one

rm_begin_scenario() { RM_CURRENT_SCENARIO="$1"; RM_SCEN_FAILED=0; }
rm_mark_inv_fail()  { RM_SCEN_FAILED=1; }

rm_end_scenario() {
  local sid="$1" summary="$2"
  if (( RM_SCEN_FAILED == 0 )); then
    rm_record_verdict "$sid" "PASS" "$summary"
  else
    rm_record_verdict "$sid" "FAIL" "$summary (NR-x invariant violated)"
  fi
}

# ─── PID tracking + trap cleanup ─────────────────────────────────────────────
declare -ga RM_CHILD_PIDS=()
declare -ga RM_CHILD_LABELS=()

rm_push() { RM_CHILD_PIDS+=("$1"); RM_CHILD_LABELS+=("$2"); }

rm_kill_pid() {
  # rm_kill_pid <pid> <signal>
  local pid="$1" sig="${2:-TERM}"
  kill -0 "$pid" 2>/dev/null && kill -"$sig" "$pid" 2>/dev/null || true
}

rm_kill_all() {
  # rm_kill_all [signal]
  local sig="${1:-TERM}"
  if (( ${#RM_CHILD_PIDS[@]} == 0 )); then return 0; fi
  rm_info "trap: sending $sig to ${#RM_CHILD_PIDS[@]} child(ren): ${RM_CHILD_LABELS[*]}"
  for i in "${!RM_CHILD_PIDS[@]}"; do
    rm_kill_pid "${RM_CHILD_PIDS[$i]}" "$sig"
  done
  if [[ "$sig" == "TERM" ]]; then
    sleep 1
    for i in "${!RM_CHILD_PIDS[@]}"; do
      rm_kill_pid "${RM_CHILD_PIDS[$i]}" "KILL"
    done
    for pid in "${RM_CHILD_PIDS[@]:-}"; do wait "$pid" 2>/dev/null || true; done
  fi
}

rm_trap_cleanup() {
  trap 'rm_kill_all TERM; exit 130' INT
  trap 'rm_kill_all TERM; exit 143' TERM
  trap 'rm_kill_all TERM' EXIT
}

# ─── Evidence scaffolding ────────────────────────────────────────────────────
# rm_init_evidence <evidence_root> <scenario_id>
rm_init_evidence() {
  local root="$1" sid="$2"
  mkdir -p "$root/logs" "$root/diffs"
  echo "$root"
}

# ─── Master + worker spawners ────────────────────────────────────────────────
# rm_spawn_master <case_dir> <master_env> <log_path>
# Sets RM_MASTER_PID on success.
rm_spawn_master() {
  local case_dir="$1" envfile="$2" log="$3"
  [[ -z "${VELOX_SERVER_BIN:-}" ]] && { rm_fail "VELOX_SERVER_BIN not set"; return 2; }
  set -a; source "$envfile"; set +a
  "$VELOX_SERVER_BIN" >"$log" 2>&1 &
  local pid=$!
  rm_push "$pid" "master-$(basename "$case_dir")"
  RM_MASTER_PID="$pid"
  sleep 1
}

# rm_spawn_worker <case_dir> <worker_id> <cfg_path> <log_path>
# Sets RM_WORKER_PID on success.
rm_spawn_worker() {
  local case_dir="$1" worker_id="$2" cfg="$3" log="$4"
  [[ -z "${VELOX_WORKER_BIN:-}" ]] && { rm_fail "VELOX_WORKER_BIN not set"; return 2; }
  "$VELOX_WORKER_BIN" --config "$cfg" >"$log" 2>&1 &
  local pid=$!
  rm_push "$pid" "worker-$worker_id"
  RM_WORKER_PID="$pid"
}

# ─── Polling helpers ─────────────────────────────────────────────────────────
# rm_wait_for <grep-pattern> <file> <budget-seconds>
# Returns 0 if the pattern appears within budget, else 1.
rm_wait_for() {
  local pat="$1" file="$2" budget="${3:-15}"
  local deadline=$(( $(date +%s) + budget ))
  while (( $(date +%s) < deadline )); do
    [[ -f "$file" ]] && grep -qE "$pat" "$file" 2>/dev/null && return 0
    sleep 0.5
  done
  return 1
}

# rm_wait_terminus <db_path> <job_id> <budget-seconds>
# Polls jobs.status until terminal (SUCCEEDED|FAILED|TIMED_OUT|CANCELLED|REJECTED).
rm_wait_terminus() {
  local db="$1" job_id="$2" budget="${3:-120}"
  local deadline=$(( $(date +%s) + budget ))
  while (( $(date +%s) < deadline )); do
    local status
    status="$(sqlite3 "$db" "SELECT status FROM jobs WHERE job_id='$job_id'" 2>/dev/null || true)"
    case "$status" in
      SUCCEEDED|FAILED|TIMED_OUT|CANCELLED|REJECTED) echo "$status"; return 0 ;;
    esac
    sleep 1
  done
  echo "${status:-TIMEOUT}"
  return 1
}

# ─── sqlite helpers ──────────────────────────────────────────────────────────
# Re-export EVIDENCE scenario helper: dump a single SQL projection to a JSON-ish file.
rm_dump_sql() {
  # rm_dump_sql <sql> <db> <output_path>
  local sql="$1" db="$2" out="$3"
  sqlite3 -separator '|' -header "$db" "$sql" >"$out" 2>/dev/null
}

# ─── Per-scenario fixture helpers ─────────────────────────────────────────────
# rm_make_worker_config <output_path> <worker_id> <concurrency> <master_url>
rm_make_worker_config() {
  local out="$1" worker_id="$2" concurrency="$3" master_url="$4"
  cat >"$out" <<JSON
{
  "master_url": "$master_url",
  "control_grpc_url": "$master_url",
  "worker_id": "$worker_id",
  "worker_name": "$worker_id",
  "work_dir": "/tmp/velox-recovery-$(date +%s)",
  "max_active_jobs": $concurrency,
  "allow_insecure_grpc_dev": true,
  "data_dir": "/tmp/velox-recovery-$(date +%s)-data",
  "prometheus_port": 0,
  "health_port": 0,
  "protocol_version": "v3"
}
JSON
}

# rm_make_master_env <output_path> <case_dir> <admin_token> <allowed_workers_csv>
rm_make_master_env() {
  local out="$1" case_dir="$2" admin_token="$3" allowed="$4"
  cat >"$out" <<ENV
VELOX_MASTER_PORT=8080
VELOX_GRPC_PORT=50051
VELOX_DB_PATH=$case_dir/velox.db
VELOX_DATA_DIR=$case_dir/data
VELOX_STAGING_DIR=$case_dir/staging
VELOX_STORAGE_DIR=$case_dir/storage
VELOX_ADMIN_TOKEN=$admin_token
VELOX_ALLOWED_WORKERS=$allowed
VELOX_CODE_VERSION=test-recovery
VELOX_GRPC_ALLOW_INSECURE_DEV=true
VELOX_ASSET_REWRITE_DEV_BYPASS=true
GIN_MODE=release
ENV
}

# ─── Scenario dispatch ───────────────────────────────────────────────────────
# rm_record_verdict <scenario_id> <pass|fail|degraded|skip> <detail>
# Appends to RM_CASE_VERDICTS[] for the matrix summary.
rm_record_verdict() {
  RM_CASE_VERDICTS+=("$1: $2 — $3")
  case "$2" in
    PASS)     (( RM_PASS_COUNT++ )) ;;
    FAIL)     (( RM_FAIL_COUNT++ )) ;;
    DEGRADED) (( RM_DEG_COUNT++  )) ;;
  esac
}
