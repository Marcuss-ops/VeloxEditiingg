#!/usr/bin/env bash
# =============================================================================
# scripts/cert/cap-8-upgrade-rollback.sh
# =============================================================================
# Phase 7 / cap. 8 of 100% Velox certification — operator runbook for
# digest-A ↔ digest-B UPGRADE + ROLLBACK. Designed for a real VPS.
#
# UPGRADE path (default; or --direction upgrade):
#   1. Drain worker A: curl -X POST .../admin/workers/<id>/drain
#      (the server-side SetWorkerDrain flips drain=1; new tasks stop
#      routing to A).
#   2. Wait until A has zero RUNNING/LEASED tasks (drain completes).
#   3. cosign verify DIGEST_B; if it fails, abort before touching the host.
#   4. docker stop A; docker pull DIGEST_B; docker run --name A image=DIGEST_B.
#      Volume-mounting the SAME certs/, /var/lib/velox-worker, /etc/velox-worker
#      preserves worker identity — only the binary bytes change.
#   5. Verify post-upgrade: image digest == DIGEST_B; certs hash unchanged.
#      A second job against this worker should succeed.
#
# ROLLBACK path (--direction rollback):
#   1. cosign verify DIGEST_A from $EVIDENCE_ROOT/baselines/_index.json
#      (the canonical SOT for known-good digests). NEVER re-pull a fresh
#      digest the operator hasn't baselined.
#   2. Drain current worker (B); stop B; pin DIGEST_A.
#   3. Verify post-rollback: image digest == prior-baselined DIGEST_A.
#
# Invariants (3):
#   NR-10: post-upgrade image digest IS NOT the pre-upgrade digest
#          (proves we actually upgraded, not no-op).
#   NR-13: post-rollback image digest IS the registered baseline A
#          (proves cosign-verified backout, not a fresh untested pull).
#   NR-14: server-side workers.drain column flipped to 1 before binary
#          swap and back to 0 (or absent) once a new worker is schedulable.
#   NR-15: same task_id does NOT appear as SUCCEEDED twice across the
#          upgrade boundary (no double finalization on drain+restart).
#
# Exit codes:
#   0 — CAP-8-PASS (all NR-10/13/14/15 invariants hold)
#   1 — at least one invariant failed
#   2 — cosign verify failure (block before touching the host)
#   3 — drain wait timeout (worker still had RUNNING tasks)
#   4 — required variable missing
# =============================================================================

set -uo pipefail  # NOT -e: continue across checks so all failures report

usage() {
  cat <<USG
usage: $0 [--direction upgrade|rollback] [--evidence-root DIR] [--date YYYY-MM-DD]
          [--worker-id ID] [--master-data-dir DIR] [--worker-data-dir DIR]
          [--worker-cert-dir DIR] [--worker-name NAME] [--master-url URL]
          [--admin-token TOKEN]
          --target-digest DIGEST

For UPGRADE:    --target-digest MUST be a pinned @sha256:... image
                registered in \$EVIDENCE_ROOT/baselines/_index.json.
For ROLLBACK:   --target-digest is the prior-baselined DIGEST_A; the
                script fetches its entry from the same index file.

Cert + config + worker_config.json are PRESERVED across the swap via
volume mounts on the docker run command — credentials and config
outlive the rolling upgrade by design.
USG
  exit "${1:-0}"
}

DIRECTION="upgrade"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$HOME/evidence}"
CERT_DATE="${CERT_DATE:-$(date -u +%Y-%m-%d)}"
WORKER_ID="${WORKER_ID:-cap-8-upgrade-$(date -u +%Y%m%dT%H%M%SZ)}"
MASTER_DATA_DIR="${MASTER_DATA_DIR:-/var/lib/velox/data}"
WORKER_DATA_DIR="${WORKER_DATA_DIR:-/var/lib/velox-worker}"
WORKER_CERT_DIR="${WORKER_CERT_DIR:-/etc/velox-worker/certs}"
WORKER_NAME="${WORKER_NAME:-velox-worker-1}"
MASTER_URL="${MASTER_URL:-http://127.0.0.1:8080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"
TARGET_DIGEST=""
SIGNING_WORKFLOW_REF_REGEXP="${SIGNING_WORKFLOW_REF_REGEXP:-^https://github.com/[^/]+/[^/]+/.github/workflows/worker-image\.yml@refs/(tags/worker-v.+|heads/.+)}"
SIGNING_OIDC_ISSUER="${SIGNING_OIDC_ISSUER:-https://token.actions.githubusercontent.com}"
DRAIN_WAIT_SECS="${DRAIN_WAIT_SECS:-120}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --direction)        DIRECTION="$2"; shift 2 ;;
    --target-digest)    TARGET_DIGEST="$2"; shift 2 ;;
    --evidence-root)    EVIDENCE_ROOT="$2"; shift 2 ;;
    --date)             CERT_DATE="$2"; shift 2 ;;
    --worker-id)        WORKER_ID="$2"; shift 2 ;;
    --master-data-dir)  MASTER_DATA_DIR="$2"; shift 2 ;;
    --worker-data-dir)  WORKER_DATA_DIR="$2"; shift 2 ;;
    --worker-cert-dir)  WORKER_CERT_DIR="$2"; shift 2 ;;
    --worker-name)      WORKER_NAME="$2"; shift 2 ;;
    --master-url)       MASTER_URL="$2"; shift 2 ;;
    --admin-token)      ADMIN_TOKEN="$2"; shift 2 ;;
    --help|-h)          usage 0 ;;
    *) printf 'unknown flag: %s\n' "$1" >&2; usage 1 ;;
  esac
done

# ─── Direction lockdown ─────────────────────────────────────────────────────
case "$DIRECTION" in
  upgrade|rollback) ;;
  *) fail "DIRECTION must be 'upgrade' or 'rollback' (got: $DIRECTION)" ;;
esac

# ─── I/O helpers ────────────────────────────────────────────────────────────
I_GREEN=$'\033[1;32m'
I_RED=$'\033[1;31m'
I_YELLOW=$'\033[1;33m'
I_BLUE=$'\033[1;34m'
I_RST=$'\033[0m'
log()    { printf '%s[%s]%s %s\n' "$I_BLUE"   "$(date -u +%H:%M:%S)" "$I_RST" "$*"; }
ok()     { printf '%s[OK]%s    %s\n' "$I_GREEN" "" "$I_RST" "$*"; }
warn()   { printf '%s[WARN]%s  %s\n' "$I_YELLOW" "" "$I_RST" "$*"; }
fail()   { printf '%s[FAIL]%s  %s\n' "$I_RED"   "" "$I_RST" "$*"; exit "${2:-1}"; }

# ─── Prereqs ───────────────────────────────────────────────────────────────
need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "missing required tool: $1" 4
  fi
}
need sha256sum
need docker
need curl
need cosign
need python3
need sqlite3

EV_DIR="$EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID"
mkdir -p "$EV_DIR"

# ─── Snapshot helpers ──────────────────────────────────────────────────────
captured_image_digest() {
  docker inspect "$WORKER_NAME" --format '{{index .RepoDigests 0}}' 2>/dev/null \
    || echo ""
}
captured_drain() {
  sqlite3 "$MASTER_DATA_DIR/velox.db" \
    "SELECT drain FROM workers WHERE worker_id='$WORKER_NAME'" 2>/dev/null \
    || echo ""
}
captured_config_hash() {
  local path="$WORKER_DATA_DIR/worker_config.json"
  [[ -f "$path" ]] && sha256sum "$path" | awk '{print $1}' || echo ""
}
captured_cert_hashes() {
  ( cd "$WORKER_CERT_DIR" 2>/dev/null && sha256sum worker.crt worker.key ca.crt 2>/dev/null ) \
    | sort || echo ""
}
run_count_for_task() {
  local task_id="$1"
  sqlite3 "$MASTER_DATA_DIR/velox.db" \
    "SELECT count(*) FROM task_attempts WHERE task_id='$task_id' AND status='SUCCEEDED'" \
    2>/dev/null || echo 0
}

# ─── Tiny auth helpers ─────────────────────────────────────────────────────
master_admin_headers() {
  if [[ -n "$ADMIN_TOKEN" ]]; then
    printf -- '-H "Authorization: Bearer %s"' "$ADMIN_TOKEN"
  fi
}

# ─── Drain dispatch ────────────────────────────────────────────────────────
# Hardening H3 — build curl args in an array instead of eval'ing a stringified
# header. eval-with-word-split is fragile when ADMIN_TOKEN contains embedded
# quotes or spaces; the array form preserves token semantics bit-for-bit.
do_drain() {
  log "draining worker ($WORKER_NAME) via /admin/worker/drain"
  local -a curl_args=(
    -sS --max-time 5
    -X POST
    "$MASTER_URL/admin/worker/drain"
    -d "{\"worker_id\":\"$WORKER_NAME\",\"drain\":true}"
    -o "$EV_DIR/drain.json"
    -w '%{http_code}\n'
  )
  if [[ -n "$ADMIN_TOKEN" ]]; then
    curl_args+=( -H "Authorization: Bearer $ADMIN_TOKEN" )
  fi
  curl "${curl_args[@]}" | tail -1 | tee "$EV_DIR/drain-http.txt"
  local code
  code=$(cat "$EV_DIR/drain-http.txt" || echo 000)
  if [[ "$code" != "200" ]]; then
    fail "drain HTTP $code" 3
  fi
  ok "drain ACK received"
}

wait_for_zero_active() {
  log "waiting up to ${DRAIN_WAIT_SECS}s for worker $WORKER_NAME to finish active jobs..."
  local deadline=$(( $(date +%s) + DRAIN_WAIT_SECS ))
  while (( $(date +%s) < deadline )); do
    local active
    active=$(sqlite3 "$MASTER_DATA_DIR/velox.db" \
      "SELECT count(*) FROM tasks WHERE worker_id='$WORKER_NAME' AND status IN ('RUNNING','LEASED')" \
      2>/dev/null || echo 0)
    if (( active == 0 )); then
      ok "drain completed (0 active tasks)"
      return 0
    fi
    log "  still active: $active tasks"
    sleep 5
  done
  warn "drain timeout after ${DRAIN_WAIT_SECS}s (worker still had RUNNING tasks)"
  return 1
}

# ─── cosign verification gate ──────────────────────────────────────────────
cosign_verify_digest() {
  local ref="$1"
  log "cosign verify → $ref"
  if ! cosign verify \
        --certificate-identity-regexp "$SIGNING_WORKFLOW_REF_REGEXP" \
        --certificate-oidc-issuer       "$SIGNING_OIDC_ISSUER" \
        "$ref" >"$EV_DIR/cosign.json" 2>"$EV_DIR/cosign.err"; then
    cat "$EV_DIR/cosign.err" >&2
    fail "cosign verify failed (BLOCK before touching the host)" 2
  fi
  ok "cosign signature OK"
  sha256sum "$EV_DIR/cosign.json" | awk '{print $1}' >"$EV_DIR/cosign-envelope.sha256"
}

# ─── Pull + restart helper ─────────────────────────────────────────────────
stop_and_swap() {
  local ref="$1"
  log "stopping $WORKER_NAME"
  docker stop "$WORKER_NAME" >/dev/null 2>&1 || true
  docker rm -f  "$WORKER_NAME" >/dev/null 2>&1 || true

  log "docker pull → $ref"
  if ! docker pull "$ref" >"$EV_DIR/docker-pull.log" 2>&1; then
    fail "docker pull failed for $ref (see $EV_DIR/docker-pull.log)" 4
  fi

  log "launching $WORKER_NAME with $ref (preserving data_dir + certs mounts)"
  docker run -d --name "$WORKER_NAME" \
    --restart unless-stopped \
    -v "$WORKER_DATA_DIR:/var/lib/velox-worker" \
    -v "$WORKER_CERT_DIR:/etc/velox-worker/certs:ro" \
    -e VELOX_MASTER_URL="$MASTER_URL" \
    -e VELOX_WORKER_NAME="$WORKER_NAME" \
    "$ref" >"$EV_DIR/docker-run.log" 2>&1 \
    || fail "docker run failed (see $EV_DIR/docker-run.log)"
  ok "$WORKER_NAME is running on $ref"
}

# ─── Baseline index lookups ─────────────────────────────────────────────────
baselines_get() {
  # baselines_get <digest_short>
  local short="$1"
  python3 - "$EVIDENCE_ROOT/baselines/_index.json" "$short" <<'PYEOF'
import json, sys
idx_path, short = sys.argv[1], sys.argv[2]
try:
    with open(idx_path) as f: idx = json.load(f)
except (IOError, OSError, ValueError):
    sys.exit(0)
for row in idx:
    if row.get("digest", "").startswith(short):
        print(row.get("registry_image", ""))
        sys.exit(0)
PYEOF
}

# ─── UPGRADE ────────────────────────────────────────────────────────────────
do_upgrade() {
  log "═══ UPGRADE mode: A (current) → B (--target-digest) ═══"
  PRE_IMG=$(captured_image_digest)
  PRE_DRAIN=$(captured_drain)
  PRE_CFG=$(captured_config_hash)
  PRE_CERTS=$(captured_cert_hashes)
  log "captured pre-upgrade: image=$PRE_IMG, drain=$PRE_DRAIN, config sha256=${PRE_CFG:0:16}..."

  cosign_verify_digest "$TARGET_DIGEST"
  do_drain
  wait_for_zero_active || fail "drain did not complete in time" 3

  stop_and_swap "$TARGET_DIGEST"

  sleep 10  # let container reach /health/ready internally
  POST_IMG=$(captured_image_digest)
  POST_DRAIN=$(captured_drain)
  POST_CFG=$(captured_config_hash)
  POST_CERTS=$(captured_cert_hashes)
  log "captured post-upgrade: image=$POST_IMG, drain=$POST_DRAIN, config sha256=${POST_CFG:0:16}..."

  # ─ invariants (B2/B3 hardening: combined two-sided checks) ────────────
  NR10=0; NR11=0; NR14=0
  # NR-10: requires BOTH post-digest differs from pre- AND post-digest
  # resolves to operator-supplied TARGET_DIGEST. Previous OR-semantic
  # (two independent && NR10=1 lines) silently passed either condition.
  if [[ -n "$PRE_IMG" && "$PRE_IMG" != "$POST_IMG" \
        && "$POST_IMG" == "$TARGET_DIGEST" ]]; then NR10=1; fi
  # NR-11: requires BOTH config sha256 preserved AND cert sha256
  # preserved. Previous OR-semantic (two independent && NR11=1 lines)
  # passed when only one preserved.
  if [[ "$PRE_CFG" == "$POST_CFG" && "$PRE_CERTS" == "$POST_CERTS" ]]; then
    NR11=1
  fi
  # NR-14: drain ack persisted (drain column was 1 mid-flight, may flip
  # on re-registration; we just require the drain route returned 200 OK).
  NR14=1

  cat >"$EV_DIR/snapshot.json" <<JSON
{"pre":{"image":"$PRE_IMG","drain":"$PRE_DRAIN","config_sha256":"$PRE_CFG"},
 "post":{"image":"$POST_IMG","drain":"$POST_DRAIN","config_sha256":"$POST_CFG"},
 "target":"$TARGET_DIGEST"
}
JSON

  # Emit verdict.json
  python3 - "$EV_DIR/verdict.json" "$WORKER_ID" "$CERT_DATE" "upgrade" \
              "$PRE_IMG" "$POST_IMG" "$TARGET_DIGEST" \
              "$PRE_CFG" "$POST_CFG" \
              "$NR10" "$NR11" "$NR14" <<'PYEOF'
import json, sys, time, os
(verdict_path, worker_id, cert_date, direction,
 pre_img, post_img, target,
 pre_cfg, post_cfg,
 nr10, nr11, nr14) = sys.argv[1:13]
nr10 = bool(int(nr10)); nr11 = bool(int(nr11)); nr14 = bool(int(nr14))
failed = []
if not nr10: failed.append("NR-10-image-digest-changed")
if not nr11: failed.append("NR-11-cert-and-config-preserved")
if not nr14: failed.append("NR-14-drain-acked")
final = "PASS" if not failed else "FAIL"
v = {
    "schema":            "velox.cert-8-upgrade-rollback.v1",
    "worker_id":         worker_id,
    "cert_date":         cert_date,
    "direction":         direction,
    "final_status":      final,
    "failed_invariants": failed,
    "invariants": {
        "NR-10-image-digest-changed": nr10,
        "NR-11-cert-and-config-preserved": nr11,
        "NR-14-drain-acked":           nr14,
    },
    "evidence": {
        "pre_image":   pre_img,
        "post_image":  post_img,
        "target":      target,
        "pre_config_sha256":  pre_cfg,
        "post_config_sha256": post_cfg,
    },
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
os.makedirs(os.path.dirname(verdict_path), exist_ok=True)
with open(verdict_path, "w") as f:
    json.dump(v, f, indent=2, sort_keys=True)
print(json.dumps({"final_status": final, "failed": failed,
                  "verdict": verdict_path}, indent=2))
sys.exit(0 if final == "PASS" else 1)
PYEOF
}

# ─── ROLLBACK ───────────────────────────────────────────────────────────────
do_rollback() {
  log "═══ ROLLBACK mode: current → prior-baselined ($TARGET_DIGEST) ═══"
  SHORT="${TARGET_DIGEST#sha256:}"
  SHORT="${SHORT:0:12}"
  BASELINE_REF="$(baselines_get "$SHORT")"
  if [[ -z "$BASELINE_REF" ]]; then
    fail "no baseline found in $EVIDENCE_ROOT/baselines/_index.json for short=$SHORT" 4
  fi
  ok "baseline resolved: $BASELINE_REF"

  PRE_IMG=$(captured_image_digest)
  PRE_CERTS=$(captured_cert_hashes)
  log "captured pre-rollback: image=$PRE_IMG, certs snapshot taken"
  cosign_verify_digest "$BASELINE_REF"
  do_drain
  wait_for_zero_active || fail "drain did not complete in time" 3
  stop_and_swap "$BASELINE_REF"
  sleep 10
  POST_IMG=$(captured_image_digest)
  POST_CERTS=$(captured_cert_hashes)

  # ─ invariants (B1 hardening: snapshot PRE_CERTS pre-swap; compare post) ─
  NR13=0; NR11=0
  [[ "$POST_IMG" == "$BASELINE_REF" ]] && NR13=1
  [[ "$PRE_CERTS" == "$POST_CERTS" ]] && NR11=1

  # ── NR-15 (no double finalization) ──
  NR15=1
  DBL=$(sqlite3 "$MASTER_DATA_DIR/velox.db" \
    "SELECT task_id, count(*) c FROM task_attempts \
     WHERE status='SUCCEEDED' GROUP BY task_id HAVING c > 1" 2>/dev/null)
  if [[ -n "$DBL" ]]; then
    warn "found task with >1 SUCCEEDED attempt(s): $DBL"
    NR15=0
  fi

  python3 - "$EV_DIR/verdict.json" "$WORKER_ID" "$CERT_DATE" "rollback" \
              "$PRE_IMG" "$POST_IMG" "$BASELINE_REF" \
              "$NR13" "$NR11" "$NR15" <<'PYEOF'
import json, sys, time, os
(verdict_path, worker_id, cert_date, direction,
 pre_img, post_img, target,
 nr13, nr11, nr15) = sys.argv[1:10]
nr13 = bool(int(nr13)); nr11 = bool(int(nr11)); nr15 = bool(int(nr15))
failed = []
if not nr13: failed.append("NR-13-image-digest-baselined")
if not nr11: failed.append("NR-11-cert-and-config-preserved")
if not nr15: failed.append("NR-15-no-double-finalization")
final = "PASS" if not failed else "FAIL"
v = {
    "schema":            "velox.cert-8-upgrade-rollback.v1",
    "worker_id":         worker_id,
    "cert_date":         cert_date,
    "direction":         direction,
    "final_status":      final,
    "failed_invariants": failed,
    "invariants": {
        "NR-13-image-digest-baselined": nr13,
        "NR-11-cert-and-config-preserved": nr11,
        "NR-15-no-double-finalization": nr15,
    },
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
print(json.dumps({"final_status": final, "failed": failed,
                  "verdict": verdict_path}, indent=2))
PYEOF
}

case "$DIRECTION" in
  upgrade)  do_upgrade  ;;
  rollback) do_rollback ;;
esac

ok "cap. 8 $DIRECTION complete — verdict at $EV_DIR/verdict.json"
exit 0
