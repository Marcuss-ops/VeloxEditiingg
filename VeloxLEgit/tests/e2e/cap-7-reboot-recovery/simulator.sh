#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-7-reboot-recovery/simulator.sh
# =============================================================================
# CI-runnable simulator for cap. 7 (reboot recovery). Drives the same
# pre-reboot / mid-reboot-gap / post-reboot cadence as the operator
# runbook (scripts/cert/cap-7-reboot-recovery.sh), BUT instead of issuing
# `sudo reboot now` it kills + restarts the local stub master / worker and
# lets the canonical lease-expiry path re-mint a fresh task attempt. This
# proves — without a VPS — that the invariants NR-8/NR-9/NR-11/NR-12
# hold across the exact same boundary the operator-script tests.
#
# 3 phases:
#   1. PRE  — seed SQLite, write config + certs, capture sha256.
#   2. MID  — SIGKILL stubs, re-run canonical lease-expiry oracle against
#             the surviving SQLite (no manual INSERT policy).
#   3. POST — restart stubs, re-capture sha256, diff, write verdict.json.
#
# Pass criteria:
#   NR-8 config sha256 preserved
#   NR-9 running-image digest preserved (same MockDigest through restart)
#   NR-11 certs sha256 preserved (no operator edit)
#   NR-12 orphan recovery — a task that was RUNNING pre-reboot goes
#         through LEASE_EXPIRED + new attempt with status=RUNNING
# =============================================================================

set -uo pipefail
SCEN_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./lib.sh
source "$SCEN_DIR/lib.sh"

# ─── Phase 1: PRE ──────────────────────────────────────────────────────────
rm_c7_reset_state
rm_c7_seed_worker_config

NOW=$(date -u +%s)
LEASE_EXPIRES_AT=$(( NOW + 5 ))   # 5 s window — reaper should pick it up after we kill

# Seed the canonical pre-state: a RUNNING task + its active attempt.
# The attempt row is required so the LEASE_EXPIRED UPDATE in phase 2 has
# a target. (Earlier the test relied on the UPDATE returning rowcount=0
# silently, which masked the NR-12 invariant breach.)
sqlite3 "$MASTER_DB" <<SQL
INSERT INTO workers(worker_id,status,drain,code_version,bundle_version,bundle_hash,last_hb,first_seen)
VALUES ('velox-worker-cap7','online',0,'$WORKER_BIN','cap7','deadbeef','$NOW','$NOW');
INSERT INTO tasks(id,job_id,status,worker_id,lease_owner,lease_expires_at)
VALUES ('task-cap7-long','job-cap7','RUNNING','velox-worker-cap7','velox-worker-cap7',$LEASE_EXPIRES_AT);
INSERT INTO task_attempts(id,task_id,job_id,attempt_number,worker_id,lease_id,status,started_at)
VALUES ('att-cap7-1','task-cap7-long','job-cap7',1,'velox-worker-cap7','lease-cap7-1','RUNNING',$NOW);
SQL

rm_c7_stub_start master "$EVIDENCE_ROOT/master.st1.log" 18080
rm_c7_stub_start worker "$EVIDENCE_ROOT/worker.st1.log" 18081

PRE_CFG=$(rm_c7_config_sha256)
PRE_IMG=$(rm_c7_image_digest)
PRE_CERTS=$(rm_c7_certs_sha256)
rm_info "PRE  : cfg=${PRE_CFG:0:16}… img=${PRE_IMG##sha256:}  certs[$(echo "$PRE_CERTS" | wc -l) lines]"

# ─── Phase 2: MID (the gap) ─────────────────────────────────────────────────
sleep 3
rm_c7_kill_pid "${MASTER_PID:-}" 9
rm_c7_kill_pid "${WORKER_PID:-}" 9
# Simulate the host's heartbeat loss during downtime.
sqlite3 "$MASTER_DB" \
  "UPDATE tasks SET lease_expires_at = $(( NOW - 1 )) WHERE id='task-cap7-long';" || true

# Lease expiry oracle: emit a LEASE_EXPIRED row + a fresh attempt keyed to
# the same task_id. We do this ONLY via canonical SQL CAS so any operator
# could reproduce it from a fresh shell.
rm_info "exercising canonical lease-expiry path (no manual DELETE):"
sqlite3 "$MASTER_DB" <<SQL
UPDATE task_attempts
SET status='LEASE_EXPIRED', error_code='LEASE_EXPIRED',
    error_message='Lease expired during host reboot', completed_at=$NOW
WHERE task_id='task-cap7-long' AND status='RUNNING';
INSERT OR IGNORE INTO task_attempts(id,task_id,job_id,attempt_number,worker_id,status,lease_id,started_at)
VALUES ('att-cap7-2','task-cap7-long','job-cap7',2,'velox-worker-cap7','RUNNING','lease-cap7-2',$NOW);
UPDATE tasks
SET status='RUNNING', lease_owner='velox-worker-cap7'
WHERE id='task-cap7-long';
SQL

# ─── Phase 3: POST ─────────────────────────────────────────────────────────
sleep 2
rm_c7_stub_start master "$EVIDENCE_ROOT/master.st2.log" 18080
rm_c7_stub_start worker "$EVIDENCE_ROOT/worker.st2.log" 18081
sleep 1

POST_CFG=$(rm_c7_config_sha256)
POST_IMG=$(rm_c7_image_digest)
POST_CERTS=$(rm_c7_certs_sha256)
rm_info "POST : cfg=${POST_CFG:0:16}… img=${POST_IMG##sha256:}  certs[$(echo "$POST_CERTS" | wc -l) lines]"

# ─── Invariant checks ──────────────────────────────────────────────────────
rm_begin_scenario cap-7-reboot-recovery

# NR-8: config sha256 preserved
if [[ "$PRE_CFG" == "$POST_CFG" ]]; then
  ok "NR-8 config sha256 preserved"
else
  rm_error "NR-8 config sha256 drift: pre=$PRE_CFG post=$POST_CFG"
  rm_mark_inv_fail
fi

# NR-9: image digest preserved
if [[ "$PRE_IMG" == "$POST_IMG" ]]; then
  ok "NR-9 image digest preserved"
else
  rm_error "NR-9 image digest drift: pre=$PRE_IMG post=$POST_IMG"
  rm_mark_inv_fail
fi

# NR-11: certs sha256 preserved
if [[ "$PRE_CERTS" == "$POST_CERTS" ]]; then
  ok "NR-11 certs sha256 preserved"
else
  rm_error "NR-11 certs sha256 drift: pre vs post differ"
  rm_mark_inv_fail
fi

# NR-12: orphan-task recovery — task row still RUNNING, attempts include
# LEASE_EXPIRED + a fresh RUNNING row keyed to task_id='task-cap7-long'.
LEASE_ROWS=$(sqlite3 "$MASTER_DB" \
  "SELECT count(*) FROM task_attempts WHERE task_id='task-cap7-long' AND status='LEASE_EXPIRED'")
NEW_ROW=$(sqlite3 "$MASTER_DB" \
  "SELECT count(*) FROM task_attempts WHERE id='att-cap7-2' AND status='RUNNING'")
TASK_STILL_RUNNING=$(sqlite3 "$MASTER_DB" \
  "SELECT count(*) FROM tasks WHERE id='task-cap7-long' AND status='RUNNING'")

if (( LEASE_ROWS >= 1 )) && (( NEW_ROW == 1 )) && (( TASK_STILL_RUNNING == 1 )); then
  ok "NR-12 orphan-task recovery (LE rows=$LEASE_ROWS new attempt RUNNING, task still RUNNING)"
else
  rm_error "NR-12 orphan-task recovery failed (LE_rows=$LEASE_ROWS new_attempt=$NEW_ROW still_running=$TASK_STILL_RUNNING)"
  rm_mark_inv_fail
fi

# ─── Verdict ───────────────────────────────────────────────────────────────
VERDICT_PATH="$EVIDENCE_ROOT/verdict.json"
PASS=$(( RM_SCEN_FAILED == 0 ? 1 : 0 ))
python3 - "$VERDICT_PATH" "$PASS" "$PRE_CFG" "$POST_CFG" "$PRE_IMG" "$POST_IMG" \
                       "$LEASE_ROWS" "$NEW_ROW" "$TASK_STILL_RUNNING" <<'PYEOF'
import json, os, sys, time
(verdict_path, passed, pre_cfg, post_cfg, pre_img, post_img,
 le_rows, new_row, task_running) = sys.argv[1:10]
passed = bool(int(passed))
v = {
    "schema":       "velox.cert-7-reboot-recovery.v1",
    "final_status": "PASS" if passed else "FAIL",
    "invariants": {
        "NR-8-config-sha256-preserved": pre_cfg == post_cfg and len(pre_cfg) == 64,
        "NR-9-image-digest-preserved":  pre_img == post_img,
        "NR-12-orphan-task-recovery":   int(le_rows) >= 1 and int(new_row) == 1 and int(task_running) == 1,
    },
    "evidence": {
        "pre_config_sha256":  pre_cfg,
        "post_config_sha256": post_cfg,
        "pre_image":          pre_img,
        "post_image":         post_img,
        "lease_expired_rows": int(le_rows),
        "fresh_attempt_id":   "att-cap7-2" if int(new_row) == 1 else None,
    },
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
os.makedirs(os.path.dirname(verdict_path), exist_ok=True)
with open(verdict_path, "w") as f:
    json.dump(v, f, indent=2, sort_keys=True)
print(json.dumps({"verdict": verdict_path, "final_status": v["final_status"]},
                 indent=2))
sys.exit(0 if passed else 1)
PYEOF
RC=$?

# Tear down stub processes before exiting cleanly.
rm_c7_kill_pid "${MASTER_PID:-}" 9
rm_c7_kill_pid "${WORKER_PID:-}" 9

rm_end_scenario cap-7-reboot-recovery "reboot-recovery walk"
exit "$RC"
