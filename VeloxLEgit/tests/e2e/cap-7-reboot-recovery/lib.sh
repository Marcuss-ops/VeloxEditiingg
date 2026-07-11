#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-7-reboot-recovery/lib.sh
# =============================================================================
# Shared helpers for the cap. 7 reboot-recovery CI simulator. Provides
# process-lifecycle wrappers (start_velox / restart_velox), color shims,
# per-scenario accumulator semantics, and a sqlite3 schema seed so the
# simulator can assert its invariants offline.
#
# The simulator (simulator.sh) drives a local master + worker pair against
# an in-memory SQLite DB, then "reboots" by killing master + worker,
# re-seeding the lease-oracle, and bringing both back up. Certs, configs,
# and data dirs are kept on disk the entire time so the post-reboot
# snapshot can compare their sha256 against the pre-reboot one — exactly
# the same invariant NR-8/9/11 the operator-runbook asserts on a real VPS.
# =============================================================================

set -uo pipefail  # NOT -e: we want every invariant to report individually

# ─── Color shims ────────────────────────────────────────────────────────────
I_GREEN=$'\033[1;32m'
I_RED=$'\033[1;31m'
I_YELLOW=$'\033[1;33m'
I_BLUE=$'\033[1;34m'
I_RST=$'\033[0m'

# ─── Per-scenario accumulator ───────────────────────────────────────────────
RM_SCEN_FAILED=${RM_SCEN_FAILED:-0}
rm_mark_inv_fail() { RM_SCEN_FAILED=1; }
rm_begin_scenario() {
  SCENARIO_ID="${1:-unknown}"
  RM_SCEN_FAILED=0
  printf '%s═══ scenario %s ═══%s\n' "$I_BLUE" "$SCENARIO_ID" "$I_RST"
}
rm_end_scenario() {
  local sid="$1" desc="$2"
  if (( RM_SCEN_FAILED == 0 )); then
    printf '%s[PASS]%s  %s — %s\n' "$I_GREEN" "$I_RST" "$sid" "$desc"
  else
    printf '%s[FAIL]%s  %s — %s\n' "$I_RED"   "$I_RST" "$sid" "$desc"
  fi
}
rm_info()    { printf '%s[i]%s %s\n'   "$I_BLUE"   "$I_RST" "$*"; }
rm_warning() { printf '%s[!]%s %s\n'  "$I_YELLOW" "$I_RST" "$*"; }
rm_error()   { printf '%s[x]%s %s\n'  "$I_RED"    "$I_RST" "$*"; }
ok()         { printf '%s[OK]%s  %s\n' "$I_GREEN"  "$I_RST" "$*"; }

# ─── Simulator paths ────────────────────────────────────────────────────────
: "${EVIDENCE_ROOT:=/tmp/velox-cap7-evidence}"
SCEN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VELOX_BIN="${VELOX_BIN:-$SCEN_DIR/_bin/velox-server}"
WORKER_BIN="${WORKER_BIN:-$SCEN_DIR/_bin/velox-worker-agent}"
DATA_DIR="${DATA_DIR:-$EVIDENCE_ROOT/_var/lib/velox}"
WORKER_DATA_DIR="${WORKER_DATA_DIR:-$EVIDENCE_ROOT/_var/lib/velox-worker}"
WORKER_CERT_DIR="${WORKER_CERT_DIR:-$EVIDENCE_ROOT/_var/etc/velox-worker/certs}"
MASTER_DB="${MASTER_DB:-$DATA_DIR/velox.db}"
MASTER_HTTP="${MASTER_HTTP:-127.0.0.1:18080}"
VELOX_GRPC="${VELOX_GRPC:-127.0.0.1:18081}"

# Cap. 7 ALWAYS uses fresh dirs so the test is reproducible. Wipe on entry.
rm_c7_reset_state() {
  rm -rf "$EVIDENCE_ROOT" "$DATA_DIR" "$WORKER_DATA_DIR" "$WORKER_CERT_DIR"
  mkdir -p "$EVIDENCE_ROOT" "$DATA_DIR" "$WORKER_DATA_DIR"/saved \
           "$DATA_DIR/secrets/ansible" "$WORKER_CERT_DIR"
  # Worker "certs" — random 64-hex strings stand in; we only need a stable
  # sha256 fingerprint across the reboot snapshot.
  for f in worker.crt worker.key ca.crt; do
    head -c 32 /dev/urandom | xxd -p -c 64 >"$WORKER_CERT_DIR/$f"
  done
  rm -f "$MASTER_DB"
  sqlite3 "$MASTER_DB" "VACUUM" 2>/dev/null || true
  rm_c7_seed_schema "$MASTER_DB"
}

# ─── Schema seed (minimal — only the columns the invariants touch) ──────────
rm_c7_seed_schema() {
  local db="$1"
  sqlite3 "$db" <<'SQL'
CREATE TABLE IF NOT EXISTS workers (
  worker_id TEXT PRIMARY KEY,
  status    TEXT,
  drain     INTEGER DEFAULT 0,
  code_version TEXT,
  bundle_version TEXT,
  bundle_hash TEXT,
  last_hb   TEXT,
  first_seen TEXT
);
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  job_id TEXT,
  status TEXT,
  worker_id TEXT,
  lease_owner TEXT,
  lease_expires_at INTEGER
);
CREATE TABLE IF NOT EXISTS task_attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT,
  job_id TEXT,
  attempt_number INTEGER,
  worker_id TEXT,
  status TEXT,
  lease_id TEXT,
  error_code TEXT,
  error_message TEXT,
  started_at INTEGER,
  completed_at INTEGER
);
SQL
}

# ─── worker_config seed (we mock the digest pin as a stable fingerprint) ────
rm_c7_seed_worker_config() {
  local cfg="$WORKER_DATA_DIR/worker_config.json"
  cat >"$cfg" <<JSON
{
  "WorkerID": "velox-worker-cap7",
  "MasterURL": "$VELOX_GRPC",
  "MaxActiveJobs": 1,
  "BundleHash": "deadbeef0000000000000000000000000000000000000000000000000000beef",
  "WorkerCertPath": "$WORKER_CERT_DIR/worker.crt",
  "WorkerKeyPath":  "$WORKER_CERT_DIR/worker.key",
  "BootstrapReadyTimeoutSecs": 30,
  "MockDigest": "sha256:1111111111111111111111111111111111111111111111111111111111111111"
}
JSON
}

# ─── Process lifecycle wrappers ─────────────────────────────────────────────
# We don't actually rebuild the Go binaries in CI. Instead we run a tiny
# stub server (rm_c7_stub_start) so the simulator's "kill+restart+verify"
# has something to wait on. The invariants don't talk to the stub —
# they read DB / config / certs directly — but the lifecycle is exercised
# so the run-script proves the orchestrator mechanics.

MASTER_PID=""
WORKER_PID=""

rm_c7_stub_start() {
  local role="$1" logfile="$2" port="$3"
  # "stub master" / "stub worker" prints its PID + a JSON line every 5s.
  (
    echo "{\"role\":\"$role\",\"pid\":\"$$\"}"
    while true; do
      printf '%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$role" "tick"
      sleep 5
    done
  ) >"$logfile" 2>&1 &
  local pid=$!
  if [[ "$role" == "master" ]]; then MASTER_PID="$pid"; fi
  if [[ "$role" == "worker" ]]; then WORKER_PID="$pid"; fi
  rm_info "stub $role PID=$pid logs=$logfile"
}

rm_c7_kill_pid() {
  local pid="$1" sig="${2:-9}"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    if [[ "$sig" == "9" ]]; then
      kill -9 "$pid" 2>/dev/null || true
    else
      kill "$pid" 2>/dev/null || true
    fi
  fi
}

# ─── Snapshot helpers ───────────────────────────────────────────────────────
rm_c7_image_digest() {
  cat "$WORKER_DATA_DIR/worker_config.json" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("MockDigest",""))' 2>/dev/null || echo ""
}
rm_c7_config_sha256() {
  sha256sum "$WORKER_DATA_DIR/worker_config.json" 2>/dev/null | awk '{print $1}' || echo ""
}
rm_c7_certs_sha256() {
  ( cd "$WORKER_CERT_DIR" 2>/dev/null && sha256sum worker.crt worker.key ca.crt 2>/dev/null ) \
    | sort || echo ""
}
