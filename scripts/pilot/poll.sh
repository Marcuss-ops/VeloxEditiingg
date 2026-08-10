#!/usr/bin/env bash
# Sourced by scripts/pilot.sh; definitions only.

cmd_all() {
  banner "VELOX PILOT — full pipeline"
  log "version: ${VERSION}"
  log "pilot dir: ${PILOT_DIR}"
  log "dev bypasses:"
  log "  VELOX_GRPC_ALLOW_INSECURE_DEV=true  (master gRPC plaintext)"
  log "  VELOX_ALLOW_INSECURE_GRPC_DEV=true  (worker gRPC plaintext)"
  log "  VELOX_ASSET_REWRITE_DEV_BYPASS=true (asset path allow-all)"
  echo
  warn "These bypasses are PRODUCTION-UNSAFE. See WARNING at top of script."

  cmd_build
  cmd_start
  cmd_submit
  cmd_work

  # Poll for SUCCEEDED
  banner "POLL: waiting for SUCCEEDED"
  local DB="${DATA_DIR}/velox.db"
  local JOB_ID
  JOB_ID="$(sqlite3 "$DB" "SELECT job_id FROM jobs ORDER BY created_at DESC LIMIT 1;" 2>/dev/null || true)"
  if [[ -z "$JOB_ID" ]]; then
    die "no job found in DB — submission may have failed" 1
  fi
  log "polling job_id=${JOB_ID}"

  local MAX_POLLS=42  # 42 × 10s = 7 minutes
  local POLL_INTERVAL=10

  for i in $(seq 1 "$MAX_POLLS"); do
    local STATUS
    STATUS="$(sqlite3 "$DB" "SELECT status FROM jobs WHERE job_id='${JOB_ID}';" 2>/dev/null || true)"

    case "$STATUS" in
      SUCCEEDED)
        ok "job SUCCEEDED after ~$(( i * POLL_INTERVAL ))s"
        verify_completed_job "$DB" "$JOB_ID"
        return 0
        ;;
      FAILED|TIMEOUT|REJECTED|CANCELLED)
        warn "job terminal with status=${STATUS}"
        sqlite3 "$DB" "SELECT job_id, status, updated_at FROM jobs WHERE job_id='${JOB_ID}';" || true
        die "job reached terminal status ${STATUS} (expected SUCCEEDED)" 1
        ;;
      ""|PENDING|RUNNING|LEASED|RENDER_FINISHED|FINALIZING)
        if (( i % 5 == 0 )); then
          log "  poll[${i}/${MAX_POLLS}] status=${STATUS}  (elapsed=$(( i * POLL_INTERVAL ))s)"
        fi
        ;;
      *)
        warn "unknown status: ${STATUS}"
        ;;
    esac
    sleep "$POLL_INTERVAL"
  done

  die "job did not reach SUCCEEDED within $(( MAX_POLLS * POLL_INTERVAL ))s" 126
}

# ═══════════════════════════════════════════════════════════════════════════════
