#!/usr/bin/env bash
# =============================================================================
# tests/e2e/recovery-matrix/run.sh — cap. 6 15-scenario orchestrator
# =============================================================================
# Drives the 15 fault-injection scenarios + 7 invariants. Each scenario is
# a small bash script that pre-recovery mutates the SQLite db to simulate
# the fault surface, then asserts the canonical invariant set.
#
# Why this design: bash + sqlite3 is sufficient for ALL 15 scenarios
# because the canonically vulnerable surface is at the SQL CAS layer
# (TransitionTaskToTerminalAtomic, ExpireTaskLeaseAtomic,
# FinalizeVerified, handleTaskRejected's lease_id check). A custom gRPC
# probe client would only add VALUE to scenarios 11-15 — for those, the
# bash stub mutates the db to simulate a buggy worker and asserts that
# the rejection path is correct (NEGATIVE-pass).
#
# Modes:
#   default      — runs all 15 scenarios, emits evidence/<date>/fleet-recovery/
#   --scenario N — runs scenario N only (1..15)
#   --dry-run    — bash -n + invariants shellcheck syntax; no DB mutations
#   --help       — usage
# =============================================================================

set -uo pipefail  # NOT -e: continue across case failures so the matrix reports all 15 verdicts

ROOT="$(cd "$(dirname "$0")" && pwd)"

# shellcheck disable=SC1091
source "$ROOT/lib.sh"

# ─── Flags ────────────────────────────────────────────────────────────────────
RUN_ALL=1
RUN_SCENARIO=""
DRY_RUN=0
while (( $# > 0 )); do
  case "$1" in
    --scenario) RUN_ALL=0; RUN_SCENARIO="$2"; shift 2 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    --help|-h)
      cat <<HELP
Usage: bash run.sh [--scenario N] [--dry-run] [--help]

  --scenario N     Run scenario N only (1..15). Default: run all 15.
  --dry-run        bash -n + invariants shellcheck syntax; no DB mutations.
  --help           this help.
HELP
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# ─── Globals ─────────────────────────────────────────────────────────────────
RM_PASS_COUNT=0
RM_FAIL_COUNT=0
RM_DEG_COUNT=0
declare -ga RM_CASE_VERDICTS=()

# ─── Dry-run helper ──────────────────────────────────────────────────────────
dry_run_check() {
  local f
  rm_info "dry-run: bash -n on every shell script under $ROOT"
  local rc=0
  while IFS= read -r f; do
    bash -n "$f" 2>&1 | head -10 && [[ ${PIPESTATUS[0]} -eq 0 ]] || rc=1
  done < <(find "$ROOT" -name "*.sh" -type f)
  return $rc
}

if [[ "$DRY_RUN" == "1" ]]; then
  dry_run_check
  exit $?
fi

# ─── DB bootstrap ─────────────────────────────────────────────────────────────
EVIDENCE_ROOT="${EVIDENCE_ROOT:-/tmp/velox-recovery-matrix}"
DATE="$(date -u +%Y-%m-%d)"
EVIDENCE_DIR="${EVIDENCE_ROOT}/${DATE}/fleet-recovery"
mkdir -p "$EVIDENCE_DIR/logs" "$EVIDENCE_DIR/diffs"

# Each scenario operates on a SHARED, fresh in-memory SQLite. We use a
# single db file because interleaving lease/attempt rows across scenarios
# is what makes NR-1/NR-2/NR-7 meaningful (we're checking the CAS
# seralization, not isolated DB states). For convenience we tag every
# scenario row with a unique job_id suffix ($RANDOM) so cross-scenario
# idempotency is preserved.
DB="$EVIDENCE_DIR/recovery.db"
rm -f "$DB"
sqlite3 "$DB" <<'SCHEMA'
-- Minimal subset of the canonical Velox SQLite store. Each scenario
-- populates what its fault requires; the invariants select against it.
CREATE TABLE IF NOT EXISTS jobs (
  job_id        TEXT PRIMARY KEY,
  status        TEXT NOT NULL,
  revision      INTEGER NOT NULL DEFAULT 0,
  video_name    TEXT,
  attempt       INTEGER,
  started_at    TEXT,
  updated_at    TEXT,
  completed_at  TEXT,
  created_at    TEXT
);
CREATE TABLE IF NOT EXISTS tasks (
  task_id           TEXT PRIMARY KEY,
  job_id            TEXT,
  status            TEXT NOT NULL,
  revision          INTEGER NOT NULL DEFAULT 0,
  attempt_count     INTEGER NOT NULL DEFAULT 0,
  worker_id         TEXT,
  lease_id          TEXT,
  lease_expires_at  TEXT,
  attempt_id        TEXT,
  attempt_number    INTEGER,
  started_at        TEXT,
  completed_at      TEXT,
  created_at        TEXT,
  updated_at        TEXT,
  ready_at          TEXT
);
CREATE TABLE IF NOT EXISTS task_attempts (
  id            TEXT PRIMARY KEY,
  task_id       TEXT NOT NULL,
  job_id        TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  worker_id     TEXT NOT NULL,
  lease_id      TEXT NOT NULL,
  status        TEXT NOT NULL,
  revision      INTEGER NOT NULL DEFAULT 0,
  report_version INTEGER NOT NULL DEFAULT 0,
  error_code    TEXT,
  error_message TEXT,
  started_at    TEXT,
  completed_at  TEXT,
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
  id              TEXT PRIMARY KEY,
  job_id          TEXT,
  attempt_id      INTEGER,
  type            TEXT,
  storage_provider TEXT,
  storage_key     TEXT,
  storage_url     TEXT,
  local_path      TEXT,
  sha256          TEXT,
  size_bytes      INTEGER,
  mime_type       TEXT,
  duration_seconds REAL,
  status          TEXT,
  verified_at     TEXT,
  created_at      TEXT
);
CREATE TABLE IF NOT EXISTS artifact_uploads (
  upload_id          TEXT PRIMARY KEY,
  artifact_id        TEXT,
  job_id             TEXT,
  attempt_number     INTEGER,
  worker_id          TEXT,
  lease_id           TEXT,
  status             TEXT,
  expected_size_bytes INTEGER,
  expected_sha256   TEXT,
  expected_revision  INTEGER,
  received_size_bytes INTEGER,
  received_sha256   TEXT,
  temporary_storage_key TEXT,
  created_at         TEXT,
  expires_at         TEXT,
  completed_at       TEXT
);
CREATE TABLE IF NOT EXISTS worker_flags (
  worker_id    TEXT PRIMARY KEY,
  revoked      INTEGER,
  quarantined  INTEGER,
  raw_json     TEXT,
  migrated_at  TEXT
);
CREATE TABLE IF NOT EXISTS worker_sessions (
  session_id  TEXT PRIMARY KEY,
  worker_id   TEXT,
  token_hash  TEXT,
  ip_address  TEXT,
  created_at  TEXT,
  expires_at  TEXT,
  last_seen   TEXT,
  revoked     INTEGER
);
CREATE TABLE IF NOT EXISTS worker_messages (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id     TEXT,
  kind          TEXT,
  refused_reason TEXT,
  created_at    TEXT
);
CREATE TABLE IF NOT EXISTS job_deliveries (
  delivery_id      TEXT PRIMARY KEY,
  artifact_id      TEXT,
  destination_id   TEXT,
  status           TEXT,
  idempotency_key  TEXT,
  remote_id        TEXT,
  remote_url       TEXT,
  created_at       TEXT,
  updated_at       TEXT
);
CREATE TABLE IF NOT EXISTS delivery_destinations (
  destination_id TEXT PRIMARY KEY,
  provider       TEXT,
  name           TEXT,
  enabled        INTEGER DEFAULT 1,
  created_at     TEXT,
  updated_at     TEXT
);
-- worker_messages: forensic log of canonical rejection decisions (lease
-- mismatch, hash mismatch, etc.). Inserted by scenarios that simulate a
-- buggy/malicious worker's bad input.
CREATE TABLE IF NOT EXISTS worker_messages (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id      TEXT,
  kind           TEXT,
  refused_reason TEXT,
  created_at     TEXT
);
-- The two nullable identity columns below are NULL on tasks that have
-- never been claimed / have just been reaped. CRITICAL: clearing them
-- is the audit §9.5 invariant for "no Task RUNNING without an attempt".
-- Both columns tolerate NULL because SQLite TEXT/INTEGER are nullable
-- by default; this comment documents the intentional nullable contract.
INSERT OR IGNORE INTO delivery_destinations
  (destination_id, provider, name, enabled, created_at, updated_at)
VALUES ('primary', 'local', 'primary', 1, '', '');
SCHEMA

rm_info "EVIDENCE_DIR=$EVIDENCE_DIR"
rm_info "DB=$DB"

# ─── Scenario dispatch ──────────────────────────────────────────────────────
run_scenario() {
  local sid="$1"
  local script="$ROOT/scenarios/${sid}-*.sh"
  # shellcheck disable=SC2207
  local matches=( $(compgen -G "$script" 2>/dev/null) )
  if (( ${#matches[@]} == 0 )); then
    rm_fail "no scenario file matches pattern: $script"
    rm_record_verdict "$sid" "FAIL" "missing scenario script"
    return 1
  fi
  local f="${matches[0]}"
  rm_info "--- scenario $sid :: $(basename "$f") ---"
  # Pre-create the per-scenario evidence subdir. Without `mkdir -p` the
  # sqlite3 -header redirects inside scenarios/01..15.sh fail with
  # ENOENT when writing 01-task-state.txt, etc. Operators find this
  # useful because the empty subdir signals "scenario didn't run yet".
  mkdir -p "$EVIDENCE_DIR/scenarios/$sid"
  EVIDENCE_DIR="$EVIDENCE_DIR/scenarios/$sid" \
    DB="$DB" \
    bash "$f"
}

if [[ "$RUN_ALL" == "1" ]]; then
  for n in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15; do
    # run_scenario returns 0 when the script completed (PASS or FAIL — both
    # are valid terminal states for the matrix). The per-scenario script
    # itself records its own verdict via rm_record_verdict; a return code
    # of 0 here just means "the script didn't crash".
    run_scenario "$n" || true
  done
else
  zero_padded="$(printf '%02d' "$RUN_SCENARIO" 2>/dev/null || echo "$RUN_SCENARIO")"
  run_scenario "$zero_padded" || true
fi

# ─── Verdict aggregator ──────────────────────────────────────────────────────
# Preflight: python3 is required for the JSON emitter. Minimal hosts without
# python3 must install it before running the matrix; clear fail-closed here
# rather than blowing up later with a confusing traceback.
command -v python3 >/dev/null 2>&1 || {
  printf 'FATAL: python3 required for verdict.json emission (install python3 or json-emit fallback).\n' >&2
  exit 3
}

VERDICT_FILE="$EVIDENCE_DIR/verdict.json"
python3 - "$VERDICT_FILE" "$RM_PASS_COUNT" "$RM_FAIL_COUNT" "$RM_DEG_COUNT" "$EVIDENCE_DIR" \
  <<'PYEOF'
import json, os, sys, datetime
out_path, p, fa, de, ev_root = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
verdict = "CERTIFIED" if fa == 0 and de == 0 else "REJECTED"
schema = "velox.cert-6-recovery-matrix.v1"
# enumerate scenario files for evidence listing
scen_dir = os.path.join(ev_root, "scenarios")
scenarios = []
if os.path.isdir(scen_dir):
    for sd in sorted(os.listdir(scen_dir)):
        scenarios.append(sd)
result = {
  "schema": schema,
  "final_verdict": verdict,
  "matrix_summary": {
    "PASS":   p,
    "FAIL":   fa,
    "DEGRADED": de,
    "total":  p + fa + de,
  },
  "scenarios_emitted": scenarios,
  "evidence_root": ev_root,
  "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
}
with open(out_path, "w") as fh:
    json.dump(result, fh, indent=2)
print(json.dumps(result, indent=2))
PYEOF

rm_info "verdict.json: $VERDICT_FILE"
rm_info "matrix summary: $RM_PASS_COUNT PASS / $RM_FAIL_COUNT FAIL / $RM_DEG_COUNT DEGRADED"

# Exit non-zero if any FAIL — DEGRADED is informational only (operator review).
exit $((RM_FAIL_COUNT > 0))
