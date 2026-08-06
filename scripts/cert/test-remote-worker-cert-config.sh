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

# P02/P03 offline harness: exercise fixture substitution and the complete
# required lifecycle without contacting a master or downloading infrastructure.
JOB_TMP_DIR="$(mktemp -d)"
trap 'rm -rf -- "$JOB_TMP_DIR"' EXIT
export WORKER_ID="$WORKER"
export RW_JOB_DESTINATION_ID="cert-destination"
export RW_JOB_FIXTURE_FILE="$ROOT_DIR/tests/worker-cert/fixtures/jobs/minimal-render-job.json"
export RW_JOB_VERIFY_PRE_READY=0
export RW_JOB_PRE_READY_REQUIRED=0
export RW_JOB_VERIFY_FFPROBE=0
export RW_JOB_VERIFY_SHA256=1
export RW_JOB_EXPECTED_SHA256="$(printf artifact-bytes | sha256sum | awk '{print $1}')"
export RW_JOB_ARTIFACT_ID=artifact-test
export RW_JOB_POLL_INTERVAL_S=0
export RW_JOB_HTTP_TIMEOUT_S=1
export RW_JOB_ARTIFACT_DOWNLOAD_TIMEOUT_S=1
export RW_JOB_DOWNLOAD_DIR="$JOB_TMP_DIR"
export RW_ADMIN_TOKEN=test-admin-token
export CERT_POLL_TIMEOUT_S=10
export M2M_TOKEN=test-m2m-token

job_poll_index=0
job_payload=''
job_sequence=(PENDING LEASED RUNNING AWAITING_ARTIFACT SUCCEEDED)
rw_job_request() {
  local method="$1" path="$2"
  if [[ "$method" == POST ]]; then
    job_payload="$3"
    if [[ "${RW_JOB_EXPECTED_SUBMIT_STATUS:-202}" == 422 ]]; then
      RW_JOB_HTTP_STATUS=422
      RW_JOB_BODY='{"ok":false,"error":"validation_failed","details":[{"path":"scenes.0.duration_seconds","issue":"min"}]}'
    else
      RW_JOB_HTTP_STATUS=202
      RW_JOB_BODY='{"job_id":"job-test","status_url":"/api/v1/jobs/job-test"}'
    fi
    return 0
  fi
  local state="${job_sequence[job_poll_index]:-SUCCEEDED}"
  job_poll_index=$((job_poll_index + 1))
  RW_JOB_HTTP_STATUS=200
  RW_JOB_BODY="$(jq -cn --arg state "$state" '{ok:true,created:true,job_id:"job-test",status_url:"/api/v1/jobs/job-test",status:$state,artifact_id:(if $state == "SUCCEEDED" then "artifact-test" else null end),artifact_size_bytes:(if $state == "SUCCEEDED" then 14 else null end)}')"
}
rw_job_download_to_file() {
  printf artifact-bytes >"$2"
  RW_JOB_DOWNLOAD_HTTP_STATUS=200
  RW_JOB_DOWNLOAD_CURL_RC=0
  return 0
}
rw_job_artifact_download_url() { printf 'http://master.example/api/internal/artifacts/%s/download' "${1:-artifact-test}"; }

job_output_file="${JOB_TMP_DIR}/job-output.json"
if ! rw_job_checks >"$job_output_file"; then
  cat "$job_output_file"
  exit 1
fi
job_output="$(cat "$job_output_file")"
[[ "$(jq -r '.overall' <<<"$job_output")" == PASS ]] || { printf 'FAIL: valid job did not pass\n%s\n' "$job_output"; exit 1; }
[[ "$(jq -r '.checks[] | select(.id == "P02-poll") | .status' <<<"$job_output")" == PASS ]] || exit 1
[[ "$(jq -r '.checks[] | select(.id == "P02-poll") | .diagnostic' <<<"$job_output")" == *'PENDING -> LEASED -> RUNNING -> AWAITING_ARTIFACT -> SUCCEEDED'* ]] || exit 1
[[ "$(jq -r '.placement_pin_worker_id' <<<"$job_payload")" == "$WORKER" ]] || exit 1
[[ "$(jq -r '.delivery_plan[0].destination_id' <<<"$job_payload")" == "cert-destination" ]] || exit 1

# The validator must reject a missing required state rather than accepting a
# merely terminal SUCCEEDED response.
job_poll_index=0
job_sequence=(PENDING LEASED RUNNING SUCCEEDED)
if rw_job_checks >"${JOB_TMP_DIR}/missing-state.json"; then
  printf 'FAIL: missing lifecycle state unexpectedly passed\n'
  exit 1
fi
[[ "$(jq -r '.overall' "${JOB_TMP_DIR}/missing-state.json")" == FAIL ]] || exit 1
[[ "$(jq -r '.checks[] | select(.id == "P02-poll") | .status' "${JOB_TMP_DIR}/missing-state.json")" == FAIL ]] || exit 1

# The validator must also reject a lifecycle regression through a valid
# intermediate state, not only a missing required state.
job_poll_index=0
job_sequence=(PENDING LEASED RUNNING AWAITING_ARTIFACT RUNNING SUCCEEDED)
if rw_job_checks >"${JOB_TMP_DIR}/regressed-state.json"; then
  printf 'FAIL: lifecycle regression unexpectedly passed\n'
  exit 1
fi
[[ "$(jq -r '.overall' "${JOB_TMP_DIR}/regressed-state.json")" == FAIL ]] || exit 1
[[ "$(jq -r '.checks[] | select(.id == "P02-poll") | .diagnostic' "${JOB_TMP_DIR}/regressed-state.json")" == *'lifecycle state regressed'* ]] || exit 1

# Invalid fixture: exact HTTP 422 validation rejection, with no job identity
# and no polling/artifact phase.
export RW_JOB_FIXTURE_FILE="$ROOT_DIR/tests/worker-cert/fixtures/jobs/invalid-job.json"
export RW_JOB_DESTINATION_ID="cert-destination"
export RW_JOB_EXPECTED_SUBMIT_STATUS=422
if ! rw_job_checks >"${JOB_TMP_DIR}/invalid-job.json"; then
  cat "${JOB_TMP_DIR}/invalid-job.json"
  exit 1
fi
invalid_output="$(cat "${JOB_TMP_DIR}/invalid-job.json")"
[[ "$(jq -r '.overall' <<<"$invalid_output")" == PASS ]] || exit 1
[[ "$(jq -r '.job_id' <<<"$invalid_output")" == null ]] || exit 1
[[ "$(jq -r '.checks[] | select(.id == "P02-submit") | .evidence' <<<"$invalid_output")" == intake_422 ]] || exit 1
export RW_JOB_EXPECTED_SUBMIT_STATUS=202

# Fixture contracts are deterministic and the shared-stock fixture reuses the
# same stock asset in both scenes.
for fixture in minimal-render-job shared-stock-job invalid-job; do
  jq -e . "$ROOT_DIR/tests/worker-cert/fixtures/jobs/${fixture}.json" >/dev/null
 done
stock_a="$(jq -r '.scenes[0].stock.asset_id' "$ROOT_DIR/tests/worker-cert/fixtures/jobs/shared-stock-job.json")"
stock_b="$(jq -r '.scenes[1].stock.asset_id' "$ROOT_DIR/tests/worker-cert/fixtures/jobs/shared-stock-job.json")"
[[ "$stock_a" == "$stock_b" && -n "$stock_a" ]] || exit 1
[[ "$(jq -r '.scenes[0].duration_seconds' "$ROOT_DIR/tests/worker-cert/fixtures/jobs/invalid-job.json")" == 0.05 ]] || exit 1

# Exercise the canonical artifact verifier with a real deterministic MP4 so
# codec, streams, dimensions, FPS, duration and SHA-256 checks are covered.
if command -v ffmpeg >/dev/null 2>&1 && command -v ffprobe >/dev/null 2>&1; then
  verifier_mp4="${JOB_TMP_DIR}/deterministic.mp4"
  verifier_report="${JOB_TMP_DIR}/deterministic-report.json"
  ffmpeg -v error -y \
    -f lavfi -i color=c=blue:s=640x360:r=30 \
    -f lavfi -i sine=frequency=880:sample_rate=48000 \
    -t 1 -c:v libx264 -pix_fmt yuv420p -c:a aac -shortest "$verifier_mp4"
  bash "$ROOT_DIR/tests/worker-cert/verify_artifact.sh" "$verifier_mp4" \
    --report-json "$verifier_report" >/dev/null
  jq -e '.checks | all(.status == "PASS")' "$verifier_report" >/dev/null
  jq -e '.sha256 | test("^[a-f0-9]{64}$")' "$verifier_report" >/dev/null
else
  printf 'FAIL: ffmpeg and ffprobe are required for artifact verifier coverage\\n' >&2
  exit 1
fi

printf 'PASS: update and job certification offline orchestration\n'
