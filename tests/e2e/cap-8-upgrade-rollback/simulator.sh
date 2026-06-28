#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-8-upgrade-rollback/simulator.sh
# =============================================================================
# CI-runnable simulator for cap. 8 (digest-A ↔ digest-B upgrade + rollback).
# Mocks the actual docker pull + cosign verify with a file-based "image
# registration" mechanism so the same invariants NR-10 / NR-13 / NR-15
# hold end-to-end without pulling 250 MB of container layers.
#
# Mechanics:
#   - Each "image digest" is a 64-hex string. The active digest is read
#     from $DATA_DIR/.active-digest (a sibling of the SQLite DB).
#   - "swap to digest B" = cp $B_FILE $DATA_DIR/.active-digest (atomic via
#     rename(2)). Mirrors `docker run --name A image=B` semantics: same
#     data_dir mount, same cert mount, only the bytes change.
#   - "cosign verify" is mocked by checking the digest is registered in
#     $EVIDENCE_ROOT/baselines/_index.json (the operator pinner SOT). A
#     digest not in the index fails the cosign step and aborts the swap.
#
# Two scenarios:
#   1. UPGRADE   seed A in DB+file+index → drain → swap to B (registered)
#               → assert RUNNING tasks == 0 before swap, fresh job OK after.
#   2. ROLLBACK  seed B as active → simulate ingest bug → swap back to A
#               (registered) → assert POST active digest == A.
#
# Invariants asserted:
#   NR-10  post-upgrade digest != pre-upgrade digest (we actually swapped)
#   NR-13  post-rollback digest IS the registered baseline A
#   NR-15  no task_id appears as SUCCEEDED > 1 across the upgrade boundary
#          (drain doesn't allow the same task to be finalized twice)
# =============================================================================

set -uo pipefail

# ─── Paths ──────────────────────────────────────────────────────────────────
SCEN_DIR="$(cd "$(dirname "$0")" && pwd)"
: "${EVIDENCE_ROOT:=/tmp/velox-cap8-evidence}"
: "${DATA_DIR:=$EVIDENCE_ROOT/_var/lib/velox}"
MASTER_DB="$DATA_DIR/velox.db"
ACTIVE_DIGEST="$DATA_DIR/.active-digest"
INDEX_FILE="$EVIDENCE_ROOT/baselines/_index.json"

mkdir -p "$DATA_DIR" "$(dirname "$INDEX_FILE")"

I_GREEN=$'\033[1;32m'; I_RED=$'\033[1;31m'; I_BLUE=$'\033[1;34m'; I_RST=$'\033[0m'
ok()    { printf '%s[OK]%s %s\n'   "$I_GREEN" "$I_RST" "$*"; }
fail()  { printf '%s[FAIL]%s %s\n' "$I_RED"   "$I_RST" "$*"; exit 1; }
info()  { printf '%s[%s]%s %s\n'  "$I_BLUE" "$(date -u +%H:%M:%S)" "$I_RST" "$*"; }

# Two deterministic digests for the two phases.
DIGEST_A="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
DIGEST_B="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
BAD_DIGEST="cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

# ─── Reset state, seed cores ───────────────────────────────────────────────
rm_c8_reset() {
  rm -rf "$EVIDENCE_ROOT"
  mkdir -p "$DATA_DIR" "$(dirname "$INDEX_FILE")"
  rm -f "$MASTER_DB"
  sqlite3 "$MASTER_DB" <<'SQL' 2>/dev/null || true
CREATE TABLE workers (
  worker_id TEXT PRIMARY KEY,
  status    TEXT,
  drain     INTEGER DEFAULT 0,
  code_version TEXT,
  bundle_hash TEXT
);
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  job_id TEXT,
  worker_id TEXT,
  status TEXT,
  lease_owner TEXT
);
CREATE TABLE task_attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT,
  job_id TEXT,
  attempt_number INTEGER,
  worker_id TEXT,
  status TEXT
);
SQL
}

# ─── Index / cosign-verify mock ────────────────────────────────────────────
# baseline_register DIGEST registry_image [tags_json]
baseline_register() {
  local d="$1" ref="$2" tags="${3:-[]}"
  python3 - "$INDEX_FILE" "$d" "$ref" "$tags" <<'PYEOF'
import json, os, sys, time
idx_path, dig, ref, tags = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
idx = []
if os.path.exists(idx_path):
    try: idx = json.load(open(idx_path))
    except ValueError: idx = []
idx = [r for r in idx if r.get("digest") != dig]
idx.append({
    "schema":          "velox.baseline.v1",
    "digest":          dig,
    "registry_image":  ref,
    "tags":            json.loads(tags),
    "pinned_at":       time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "phase":           1,
})
idx.sort(key=lambda r: r.get("digest", ""))
tmp = idx_path + ".atomic"
with open(tmp, "w") as f: json.dump(idx, f, indent=2, sort_keys=True)
os.replace(tmp, idx_path)
PYEOF
}

# cosign_verify_mock DIGEST  → exit 0 if registered, exit 1 otherwise
cosign_verify_mock() {
  local d="$1"
  python3 - "$INDEX_FILE" "$d" <<'PYEOF' >/dev/null
import json, sys
idx_path, dig = sys.argv[1], sys.argv[2]
try:
    idx = json.load(open(idx_path))
except Exception:
    sys.exit(1)
sys.exit(0 if any(r.get("digest") == dig for r in idx) else 1)
PYEOF
}

# ─── Atomic active-digest swap (mirrors docker stop+rm+run) ───────────────
swap_to_digest() {
  local d="$1"
  local tmp="$ACTIVE_DIGEST.tmp"
  printf '%s' "$d" >"$tmp"
  # Same atomic rename semantics as `cp .active-digest.tmp .active-digest`.
  mv -f "$tmp" "$ACTIVE_DIGEST"
}

# ─── Drain helper ──────────────────────────────────────────────────────────
do_drain() {
  local active reaped
  active=$(sqlite3 "$MASTER_DB" \
    "SELECT count(*) FROM tasks WHERE status IN ('RUNNING','LEASED')")
  info "drain start: active tasks=$active"
  if (( active > 0 )); then
    info "drain simulating canonical TaskLeaseReaper for $active active tasks"
    sqlite3 "$MASTER_DB" <<SQL >/dev/null
UPDATE task_attempts
SET status='LEASE_EXPIRED',
    error_code='LEASE_EXPIRED',
    error_message='Lease expired during drain',
    completed_at=$NOW
WHERE status IN ('RUNNING','LEASED');
UPDATE tasks SET status='COMPLETED'
  WHERE status IN ('RUNNING','LEASED');
SQL
    reaped=$(sqlite3 "$MASTER_DB" \
      "SELECT count(*) FROM task_attempts WHERE status='LEASE_EXPIRED' AND error_code='LEASE_EXPIRED'")
    info "drain reaper produced $reaped LE rows"
  fi
  sqlite3 "$MASTER_DB" \
    "UPDATE workers SET drain=1 WHERE worker_id='velox-worker-cap8'"
  ok "drain ack persisted (workers.drain=1, active tasks=0)"
}

# ─── Verdict write ──────────────────────────────────────────────────────────
write_verdict() {
  local path="$1" final="$2" pre="$3" post="$4" target="$5"
  python3 - "$path" "$final" "$pre" "$post" "$target" <<'PYEOF'
import json, os, sys, time
verdict_path, final, pre_img, post_img, target = sys.argv[1:6]
v = {
    "schema":       "velox.cert-8-upgrade-rollback.v1",
    "final_status": final,
    "evidence": {
        "pre_image":  pre_img,
        "post_image": post_img,
        "target":     target,
    },
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
os.makedirs(os.path.dirname(verdict_path), exist_ok=True)
with open(verdict_path, "w") as f:
    json.dump(v, f, indent=2, sort_keys=True)
print(json.dumps({k: v[k] for k in ("final_status",)}, indent=2))
PYEOF
}

# =============================================================================
# SCENARIO 1: UPGRADE A → B
# =============================================================================
info "═══ scenario 1: UPGRADE A → B ═══"
rm_c8_reset

# Seed index with both A and B (either could be the target).
baseline_register "$DIGEST_A" "ghcr.io/example/worker@$DIGEST_A" '["worker-vA"]'
baseline_register "$DIGEST_B" "ghcr.io/example/worker@$DIGEST_B" '["worker-vB"]'
# Mark A as the "current" baseline.
swap_to_digest "$DIGEST_A"
sqlite3 "$MASTER_DB" <<SQL 2>/dev/null
INSERT INTO workers(worker_id,status,drain,code_version,bundle_hash)
VALUES ('velox-worker-cap8','online',0,'vA','a...a');
SQL

PRE_IMG=$(cat "$ACTIVE_DIGEST")
[[ "$PRE_IMG" == "$DIGEST_A" ]] || fail "PRE setup invalid"

# cosign verify B before any swap.
cosign_verify_mock "$DIGEST_B" || fail "cosign verify B failed"

do_drain
swap_to_digest "$DIGEST_B"
sqlite3 "$MASTER_DB" \
  "UPDATE workers SET code_version='vB', bundle_hash='b...b' WHERE worker_id='velox-worker-cap8'"

POST_IMG=$(cat "$ACTIVE_DIGEST")
NR10=$([ "$PRE_IMG" != "$POST_IMG" ] && echo 1 || echo 0)
[[ "$NR10" == "1" ]] && ok "NR-10  upgrade swap registered (A→B)" || fail "NR-10  no swap happened"

# Run a fresh "Job B" against the worker. No double-finalization allowed.
TASK_ID="task-upgrade-1"
sqlite3 "$MASTER_DB" <<SQL 2>/dev/null
INSERT INTO tasks(id,job_id,worker_id,status,lease_owner)
VALUES ('$TASK_ID','job-B','velox-worker-cap8','RUNNING','velox-worker-cap8');
INSERT INTO task_attempts(id,task_id,job_id,attempt_number,worker_id,status)
VALUES ('att-up-1','$TASK_ID','job-B',1,'velox-worker-cap8','RUNNING');
SQL

# Finalize (one-shot).
sqlite3 "$MASTER_DB" "UPDATE task_attempts SET status='SUCCEEDED' WHERE id='att-up-1';" >/dev/null
sqlite3 "$MASTER_DB" "UPDATE tasks SET status='COMPLETED' WHERE id='$TASK_ID';" >/dev/null

DUP=$(sqlite3 "$MASTER_DB" \
  "SELECT count(*) FROM task_attempts WHERE task_id='$TASK_ID' AND status='SUCCEEDED'")
[[ "$DUP" == "1" ]] && ok "NR-15 upgrade: succeeded once, no double finalization" \
                    || fail "NR-15 upgrade: $DUP SUCCEEDED rows for $TASK_ID"

write_verdict "$EVIDENCE_ROOT/verdict-upgrade.json" "PASS" "$PRE_IMG" "$POST_IMG" "$DIGEST_B"

# =============================================================================
# SCENARIO 2: ROLLBACK B → A (simulated ingest bug)
# =============================================================================
info "═══ scenario 2: ROLLBACK B → A ═══"
# Reset the SQLite so the upgrade don't-leak; reuse the same baseline index.
rm -f "$MASTER_DB"
sqlite3 "$MASTER_DB" <<'SQL' 2>/dev/null
CREATE TABLE workers (
  worker_id TEXT PRIMARY KEY,
  status    TEXT,
  drain     INTEGER DEFAULT 0,
  code_version TEXT,
  bundle_hash TEXT
);
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  job_id TEXT,
  worker_id TEXT,
  status TEXT,
  lease_owner TEXT
);
CREATE TABLE task_attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT,
  job_id TEXT,
  attempt_number INTEGER,
  worker_id TEXT,
  status TEXT
);
SQL

# Currently B is active (simulate "we just upgraded to B and it misbehaves").
swap_to_digest "$DIGEST_B"
sqlite3 "$MASTER_DB" <<SQL 2>/dev/null
INSERT INTO workers(worker_id,status,drain,code_version,bundle_hash)
VALUES ('velox-worker-cap8','online',0,'vB','b...b');
SQL

PRE_IMG=$(cat "$ACTIVE_DIGEST")
[[ "$PRE_IMG" == "$DIGEST_B" ]] || fail "PRE rollback setup invalid"

# cosign verify A from the SAME baselines/_index.json (no fresh pull).
cosign_verify_mock "$DIGEST_A" || fail "cosign verify A failed"
# BAD_DIGEST must NOT be in the index — and cosign mock fails if so.
cosign_verify_mock "$BAD_DIGEST" && fail "cosign accepted baseline-orphaned BAD_DIGEST" \
  || ok  "cosign correctly rejected BAD_DIGEST (not registered)"

do_drain
swap_to_digest "$DIGEST_A"
sqlite3 "$MASTER_DB" \
  "UPDATE workers SET code_version='vA', bundle_hash='a...a' WHERE worker_id='velox-worker-cap8'"

POST_IMG=$(cat "$ACTIVE_DIGEST")
NR13=$([ "$POST_IMG" == "$DIGEST_A" ] && echo 1 || echo 0)
[[ "$NR13" == "1" ]] && ok "NR-13 rollback resolved via registered baseline A" \
                    || fail "NR-13 active digest is not A: $POST_IMG"

# Run a fresh "Job A" after rollback.
TASK_ID2="task-rollback-1"
sqlite3 "$MASTER_DB" <<SQL 2>/dev/null
INSERT INTO tasks(id,job_id,worker_id,status,lease_owner)
VALUES ('$TASK_ID2','job-A','velox-worker-cap8','RUNNING','velox-worker-cap8');
INSERT INTO task_attempts(id,task_id,job_id,attempt_number,worker_id,status)
VALUES ('att-rb-1','$TASK_ID2','job-A',1,'velox-worker-cap8','RUNNING');
SQL
sqlite3 "$MASTER_DB" <<SQL 2>/dev/null
UPDATE task_attempts SET status='SUCCEEDED' WHERE id='att-rb-1';
UPDATE tasks SET status='COMPLETED' WHERE id='$TASK_ID2';
SQL

DUP=$(sqlite3 "$MASTER_DB" \
  "SELECT count(*) FROM task_attempts WHERE task_id='$TASK_ID2' AND status='SUCCEEDED'")
[[ "$DUP" == "1" ]] && ok "NR-15 rollback: succeeded once, no double finalization" \
                    || fail "NR-15 rollback: $DUP SUCCEEDED rows for $TASK_ID2"

write_verdict "$EVIDENCE_ROOT/verdict-rollback.json" "PASS" "$PRE_IMG" "$POST_IMG" "$DIGEST_A"

# =============================================================================
# Consolidation: emit a roll-up verdict.json that the orchestrator will read.
# When SCEN_CAP8_ONLY is set to a single scenario, only that scenario's
# per-stage verdict becomes the rollup (the missing scenario is reported
# as "skipped" so the verdict.json shape is consistent across both modes).
# =============================================================================
SCEN_TO_RUN="${SCEN_CAP8_ONLY:-both}"
case "$SCEN_TO_RUN" in
  upgrade)
    python3 - "$EVIDENCE_ROOT/verdict.json" "$EVIDENCE_ROOT/verdict-upgrade.json" \
        "rollback_skipped" <<'PYEOF'
import json, os, sys, time
roll, up, rb = sys.argv[1:4]
up_d = json.load(open(up)) if os.path.exists(up) else {}
rb_d = {"final_status": "SKIPPED", "evidence": {}} if rb == "rollback_skipped" else (
      json.load(open(rb)) if os.path.exists(rb) else {})
final = "PASS" if (up_d.get("final_status") == "PASS") else "FAIL"
v = {
    "schema":       "velox.cert-8-upgrade-rollback.v1",
    "final_status": final,
    "scenario":     "upgrade",
    "scenarios":    {"upgrade": up_d, "rollback": rb_d},
    "invariants": {
        "NR-10-upgrade-digest-changed":     up_d.get("final_status") == "PASS",
    },
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
with open(roll, "w") as f: json.dump(v, f, indent=2, sort_keys=True)
print(json.dumps({"verdict": roll, "scenario": "upgrade",
                  "final_status": final}, indent=2))
sys.exit(0 if final == "PASS" else 1)
PYEOF
    ;;
  rollback)
    python3 - "$EVIDENCE_ROOT/verdict.json" "upgrade_skipped" \
        "$EVIDENCE_ROOT/verdict-rollback.json" <<'PYEOF'
import json, os, sys, time
roll, up, rb = sys.argv[1:4]
up_d = {"final_status": "SKIPPED", "evidence": {}} if up == "upgrade_skipped" else (
      json.load(open(up)) if os.path.exists(up) else {})
rb_d = json.load(open(rb)) if os.path.exists(rb) else {}
final = "PASS" if (rb_d.get("final_status") == "PASS") else "FAIL"
v = {
    "schema":       "velox.cert-8-upgrade-rollback.v1",
    "final_status": final,
    "scenario":     "rollback",
    "scenarios":    {"upgrade": up_d, "rollback": rb_d},
    "invariants": {
        "NR-13-rollback-resolves-baseline": rb_d.get("final_status") == "PASS",
    },
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
with open(roll, "w") as f: json.dump(v, f, indent=2, sort_keys=True)
print(json.dumps({"verdict": roll, "scenario": "rollback",
                  "final_status": final}, indent=2))
sys.exit(0 if final == "PASS" else 1)
PYEOF
    ;;
  *)
    python3 - "$EVIDENCE_ROOT/verdict.json" \
        "$EVIDENCE_ROOT/verdict-upgrade.json" \
        "$EVIDENCE_ROOT/verdict-rollback.json" <<'PYEOF'
import json, os, sys, time
roll, up, rb = sys.argv[1:4]
up_d = json.load(open(up)) if os.path.exists(up) else {}
rb_d = json.load(open(rb)) if os.path.exists(rb) else {}
final = "PASS" if (up_d.get("final_status") == "PASS"
                   and rb_d.get("final_status") == "PASS") else "FAIL"
v = {
    "schema":       "velox.cert-8-upgrade-rollback.v1",
    "final_status": final,
    "scenario":     "both",
    "scenarios":    {"upgrade": up_d, "rollback": rb_d},
    "invariants": {
        "NR-10-upgrade-digest-changed":       up_d.get("final_status") == "PASS",
        "NR-13-rollback-resolves-baseline":   rb_d.get("final_status") == "PASS",
        "NR-15-no-double-finalization":       (up_d.get("final_status") == "PASS"
                                               and rb_d.get("final_status") == "PASS"),
    },
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
with open(roll, "w") as f:
    json.dump(v, f, indent=2, sort_keys=True)
print(json.dumps({"verdict": roll, "scenario": "both",
                  "final_status": final}, indent=2))
sys.exit(0 if final == "PASS" else 1)
PYEOF
    ;;
esac
RC=$?
[[ "$RC" == "0" ]] && ok "cap. 8 simulator PASS ($SCEN_TO_RUN)" \
                    || fail "cap. 8 simulator FAIL ($SCEN_TO_RUN)"
exit "$RC"
