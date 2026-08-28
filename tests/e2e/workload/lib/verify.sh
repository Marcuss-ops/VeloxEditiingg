# shellcheck shell=bash
# verify.sh — Phases 6 + 6a: poll for SUCCEEDED, verify artifacts, metrics, DB state.

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 6: Poll + verify
# ═══════════════════════════════════════════════════════════════════════════════
phase_poll_and_verify() {
  info "Phase 6: polling for SUCCEEDED (max 5 min)"
  local db="$DATA_DIR/velox.db"
  local status=""

  for i in $(seq 1 60); do
    status="$(sqlite3 "$db" "SELECT status FROM jobs WHERE job_id='${JOB_ID}';" 2>/dev/null || true)"
    case "$status" in
      SUCCEEDED)
        pass "job SUCCEEDED after ~$(( i * 5 ))s"
        break
        ;;
      FAILED|TIMEOUT|REJECTED|CANCELLED)
        fail "job reached terminal status=$status (expected SUCCEEDED)"
        exit 1
        ;;
    esac
    if (( i % 6 == 0 )); then
      info "  poll[$i/60] status=$status (elapsed=$(( i * 5 ))s)"
    fi
    sleep 5
  done

  if [[ "$status" != "SUCCEEDED" ]]; then
    fail "job did not reach SUCCEEDED within 5 min"
    sqlite3 "$db" "SELECT job_id, status, updated_at FROM jobs WHERE job_id='${JOB_ID}';" || true
    exit 1
  fi

  assert_artifact_exists
  assert_video_properties
  assert_artifact_sha256

  info "Verification 4: GET /api/v1/workers"
  local workers_json
  workers_json="$(curl -sS -m 5 -H "Authorization: Bearer ${ADMIN_TOKEN}"     "http://127.0.0.1:${MASTER_PORT}/api/v1/workers" 2>/dev/null || true)"
  if echo "$workers_json" | grep -qF "$WORKER_ID"; then
    pass "worker '$WORKER_ID' visible in /api/v1/workers"
  else
    fail "worker '$WORKER_ID' NOT in /api/v1/workers"
    info "response: $workers_json"
    exit 1
  fi

  assert_master_metrics
  assert_database_state
  assert_worker_metrics
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 6a: live attempt-milestone projection (STEP A)
# ═══════════════════════════════════════════════════════════════════════════════
phase_live_milestones() {
  info "Phase 6a: capturing attempt_milestones from the LIVE projection"
  local db="$DATA_DIR/velox.db"
  local status=""
  local milestones=""
  for i in $(seq 1 90); do
    status="$(sqlite3 "$db" "SELECT status FROM jobs WHERE job_id='${JOB_ID}';" 2>/dev/null || true)"
    case "$status" in
      SUCCEEDED|FAILED|TIMEOUT|REJECTED|CANCELLED) break ;;
    esac
    local live
    live="$(curl -sS -m 5 -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      "http://127.0.0.1:${MASTER_PORT}/api/v1/admin/jobs/${JOB_ID}/live" 2>/dev/null || true)"
    if echo "$live" | grep -q '"attempt_milestones"'; then
      milestones="$(echo "$live" | python3 -c '
import sys, json
d = json.load(sys.stdin)
ms = (d.get("execution") or {}).get("attempt_milestones") or []
for m in ms:
    print("{} @ {}ms".format(m.get("name"), m.get("elapsed_ms")))
' 2>/dev/null || true)"
      if [[ -n "$milestones" ]]; then
        break
      fi
    fi
    sleep 1
  done

  if [[ -z "$milestones" ]]; then
    fail "attempt_milestones never appeared in the /live projection while RUNNING (final status=$status)"
    tail -30 "$WORKER_LOG" 2>/dev/null || true
    exit 1
  fi
  pass "live attempt_milestones captured while job status=$status"
  info "milestone timeline (elapsed_ms since attempt start):"
  while IFS= read -r line; do
    [[ -n "$line" ]] && info "  $line"
  done <<< "$milestones"
}
