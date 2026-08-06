#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/cert/remote-worker-cert-config.sh
source "${ROOT_DIR}/scripts/cert/remote-worker-cert-config.sh"

command -v jq >/dev/null 2>&1 || {
  printf 'SKIP: jq is required\n'
  exit 0
}

DIGEST="sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
IMAGE="ghcr.io/acme/velox-worker@${DIGEST}"
WORKER="worker-test"

export WORKER_ID="$WORKER"
export RW_UPDATE_TARGET_IMAGE="$IMAGE"
export RW_UPDATE_TARGET_DIGEST="$DIGEST"
export RW_UPDATE_REASON="offline certification test"
export RW_UPDATE_LEASE_TIMEOUT_S=1
export RW_UPDATE_LEASE_POLL_INTERVAL_S=1
export RW_OPERATION_TIMEOUT_S=1
export RW_OPERATION_POLL_INTERVAL_S=1
export RW_HEARTBEAT_MAX_AGE_S=30

PRE_BODY='{"worker_id":"worker-test","status":"CONNECTED","session_active":true,"last_heartbeat_at":"2026-08-06T12:00:00Z"}'
POST_BODY="$(jq -cn --arg id "$WORKER" --arg digest "$DIGEST" '{worker_id:$id,status:"CONNECTED",session_active:true,last_heartbeat_at:"2026-08-06T12:01:00Z",release_identity:{image_digest:$digest},scheduling_state:"AVAILABLE",health_state:"HEALTHY",active_jobs:0,active_slots:0,max_slots:2}')"
DRAIN_BODY="$(jq -cn --arg id "$WORKER" '{worker_id:$id,status:"DRAINING",session_active:true,scheduling_state:"DRAINING",active_jobs:0,active_slots:0,max_slots:2}')"
HEALTH_BODY='{"level":"D","healthy":true,"checks":{"smoke_ok":{"passed":true,"value":"artifact-test"}}}'
OP_BODY='{"operation_id":"op-update","op":"update","status":"SUCCEEDED","started_at":"2026-08-06T12:00:01Z","finished_at":"2026-08-06T12:00:30Z"}'

resume_required=0
resume_requests=0
resume_completed=0
captured_update_payload=''

rw_worker_snapshot_ok() { return 0; }
rw_worker_release_diagnostic() { return 0; }
rw_update_release_matches() { return 0; }
rw_update_health_smoke_ok() { return 0; }
rw_worker_heartbeat_epoch() {
  [[ "$1" == "2026-08-06T12:00:00Z" ]] && printf '100' || printf '200'
}
rw_worker_poll_connected() { printf '%s' "$POST_BODY"; }
rw_update_poll_idle() {
  RW_UPDATE_IDLE_BODY="$DRAIN_BODY"
  return 0
}
rw_lifecycle_operation_matches() { return 0; }
rw_lifecycle_poll_operation() {
  RW_LIFECYCLE_POLL_BODY="$OP_BODY"
  return 0
}
rw_worker_admin_get() {
  case "$1" in
    "/api/v1/workers/${WORKER_ID}") printf '%s' "$PRE_BODY" ;;
    "/api/v1/admin/workers/${WORKER_ID}")
      if (( resume_required && !resume_completed )); then printf '%s' "$DRAIN_BODY"; else printf '%s' "$POST_BODY"; fi
      ;;
    *) printf '%s' "$POST_BODY" ;;
  esac
}
rw_admin_request() {
  local method="$1" path="$2" body="${3:-}"
  RW_LAST_CURL_RC=0
  RW_LAST_HTTP_STATUS=200
  RW_LAST_BODY="$OP_BODY"
  if [[ "$method" == POST && "$path" == "/api/v1/admin/workers/${WORKER_ID}/update" ]]; then
    captured_update_payload="$body"
    RW_LAST_HTTP_STATUS=202
    RW_LAST_BODY='{"operation_id":"op-update","op":"update","status":"QUEUED"}'
  elif [[ "$method" == POST && "$path" == "/api/v1/admin/workers/${WORKER_ID}/resume" ]]; then
    resume_requests=$((resume_requests + 1))
    resume_completed=1
    RW_LAST_HTTP_STATUS=202
    RW_LAST_BODY='{"operation_id":"op-resume","op":"resume","status":"QUEUED"}'
  elif [[ "$method" == GET && "$path" == "/api/v1/admin/workers/${WORKER_ID}/health?level=D" ]]; then
    RW_LAST_HTTP_STATUS=200
    RW_LAST_BODY="$HEALTH_BODY"
  fi
  return 0
}

run_case() {
  local expected_resume="$1" output overall output_file
  resume_required="$expected_resume"
  resume_requests=0
  resume_completed=0
  captured_update_payload=''
  output_file="$(mktemp)"
  if rw_update_checks >"$output_file"; then
    :
  else
    rm -f "$output_file"
    return 1
  fi
  output="$(cat "$output_file")"
  rm -f "$output_file"
  overall="$(jq -r '.overall' <<<"$output")"
  [[ "$overall" == PASS ]] || { printf 'FAIL: expected PASS, got %s\n%s\n' "$overall" "$output"; return 1; }
  [[ "$(jq -r '.checks[] | select(.id == "U02") | .status' <<<"$output")" == PASS ]] || return 1
  [[ "$(jq -r '.target_image' <<<"$output")" == "$IMAGE" ]] || return 1
  [[ "$(jq -r '.target_digest' <<<"$output")" == "$DIGEST" ]] || return 1
  [[ "$(jq -r '.target_digest' <<<"$captured_update_payload")" == "$IMAGE" ]] || {
    printf 'FAIL: update payload did not carry immutable image reference\n'
    return 1
  }
  if (( expected_resume )); then
    [[ "$resume_requests" -eq 1 ]] || { printf 'FAIL: expected one resume request\n'; return 1; }
    [[ "$(jq -r '.checks[] | select(.id == "U06") | .status' <<<"$output")" == PASS ]] || return 1
  else
    [[ "$resume_requests" -eq 0 ]] || { printf 'FAIL: unexpected resume request\n'; return 1; }
  fi
}

run_case 0
run_case 1
printf 'PASS: update certification offline orchestration\n'
