# remote-worker-cert-lifecycle.sh — worker lifecycle checks.
# Loaded by scripts/cert/remote-worker-cert-config.sh.
# shellcheck shell=bash
# shellcheck disable=SC2034

_RW_LIFECYCLE_LIB_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${_RW_LIFECYCLE_LIB_DIR}/remote-worker-cert-safety.sh"
# shellcheck disable=SC1091
source "${_RW_LIFECYCLE_LIB_DIR}/remote-worker-cert-renewal.sh"
# shellcheck disable=SC1091
source "${_RW_LIFECYCLE_LIB_DIR}/remote-worker-cert-installation.sh"
# shellcheck disable=SC1091
source "${_RW_LIFECYCLE_LIB_DIR}/remote-worker-cert-rollback.sh"
unset _RW_LIFECYCLE_LIB_DIR

rw_lifecycle_checks() {
  local started finished elapsed body operation_id duplicate_body state health scheduling
  local diagnostic overall="PASS" health_body health_report smoke_passed smoke_value
  local resume_queued_at resume_queued_epoch resume_started_at resume_finished_at
  local resume_started_epoch resume_finished_epoch smoke_collected_at smoke_collected_epoch
  local -a RW_LIFECYCLE_RESULTS=()

  for bin in jq curl; do
    command -v "$bin" >/dev/null 2>&1 || {
      rw_lifecycle_record W00 prerequisites FAIL "missing local prerequisite: ${bin}" 0
      overall="FAIL"
    }
  done
  if [[ "$overall" != "PASS" ]]; then
    jq -n --arg worker_id "${WORKER_ID:-}" --arg overall "$overall" \
      --argjson checks "$(printf '%s\n' "${RW_LIFECYCLE_RESULTS[@]}" | jq -s '.')" \
      '{schema:"velox.remote_worker.lifecycle.v1",worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
    return 2
  fi

  # W04 — drain immediately excludes the worker from placement and a
  # second drain is rejected with HTTP 409 without another operation.
  started="$(rw_now_s)"
  diagnostic=""
  if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/drain" \
    "$(jq -nc --arg reason "remote worker W04 drain certification" '{reason:$reason}')"; then
    diagnostic="drain POST transport failed (rc=${RW_LAST_CURL_RC})"
  elif [[ "$RW_LAST_HTTP_STATUS" != "202" ]]; then
    diagnostic="drain POST returned HTTP ${RW_LAST_HTTP_STATUS}: ${RW_LAST_BODY}"
  else
    operation_id="$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
    [[ -n "$operation_id" ]] || diagnostic="drain response omitted operation_id"
    [[ "$(jq -r '.op // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "drain" ]] || diagnostic="drain response op is not drain"
    [[ "$(jq -r '.status // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "QUEUED" ]] || diagnostic="drain response status is not QUEUED"
  fi
  if [[ -z "$diagnostic" ]]; then
    body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
    state="$(rw_lifecycle_worker_state "$body")"
    [[ "${state%%|*}" == "CONNECTED" ]] || diagnostic="worker connection state after drain is ${state} (expected CONNECTED with drain exclusion)"
    [[ "$(jq -r '.drain // false' <<<"$body" 2>/dev/null || true)" == "true" ]] || diagnostic="admin worker drain flag is not true after drain"
    scheduling="${state##*|}"
    [[ "$scheduling" == "DRAINING" ]] || diagnostic="worker scheduling state after drain is ${scheduling:-<empty>} (expected DRAINING)"
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_lifecycle_poll_operation "$operation_id"; then
      diagnostic="${RW_LIFECYCLE_POLL_ERROR}"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_lifecycle_operation_matches "$RW_LIFECYCLE_POLL_BODY" "$operation_id" drain; then
      diagnostic="drain operation identity mismatch in terminal response"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/drain" \
      '{"reason":"duplicate W04 drain"}'; then
      diagnostic="duplicate drain transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "409" ]]; then
      diagnostic="duplicate drain returned HTTP ${RW_LAST_HTTP_STATUS}, expected 409"
    elif [[ "$(jq -r '.error // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" != *DRAINING* ]]; then
      diagnostic="duplicate drain 409 did not explain DRAINING: ${RW_LAST_BODY}"
    elif [[ -n "$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" ]]; then
      diagnostic="duplicate drain 409 unexpectedly returned operation_id"
    fi
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_lifecycle_record W04 drain_placement FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_lifecycle_record W04 drain_placement PASS "HTTP 202 + operation SUCCEEDED; CONNECTED with drain=true/scheduling=DRAINING; duplicate drain HTTP 409" "$elapsed"
  fi

  # W05 — resume is asynchronous and the worker may become HEALTHY only
  # after a fresh Level D smoke is green. Health D is checked explicitly.
  started="$(rw_now_s)"
  diagnostic=""
  if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/resume" \
    "$(jq -nc --arg reason "remote worker W05 resume certification" '{reason:$reason}')"; then
    diagnostic="resume POST transport failed (rc=${RW_LAST_CURL_RC})"
  elif [[ "$RW_LAST_HTTP_STATUS" != "202" ]]; then
    diagnostic="resume POST returned HTTP ${RW_LAST_HTTP_STATUS}: ${RW_LAST_BODY}"
  else
    operation_id="$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
    resume_queued_at="$(jq -r '.queued_at // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
    [[ -n "$operation_id" ]] || diagnostic="resume response omitted operation_id"
    [[ -n "$resume_queued_at" ]] || diagnostic="resume response omitted queued_at"
    resume_queued_epoch="$(rw_worker_heartbeat_epoch "$resume_queued_at" 2>/dev/null || true)"
    [[ -n "$resume_queued_epoch" ]] || diagnostic="resume queued_at is not valid RFC3339"
    [[ "$(jq -r '.op // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "resume" ]] || diagnostic="resume response op is not resume"
    [[ "$(jq -r '.status // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "QUEUED" ]] || diagnostic="resume response status is not QUEUED"
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_lifecycle_poll_operation "$operation_id"; then
      diagnostic="${RW_LIFECYCLE_POLL_ERROR}"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_lifecycle_operation_matches "$RW_LIFECYCLE_POLL_BODY" "$operation_id" resume; then
      diagnostic="resume operation identity mismatch in terminal response"
    fi
  fi
  # Require the successful resume operation's fresh Level D gate and then
  # observe a healthy Level D report after the operation completed.
  if [[ -z "$diagnostic" ]]; then
    # A successful resume is the authoritative correlation point: the real
    # ResumeExecutor runs a fresh Level D smoke synchronously inside the
    # operation and clears drain only after that smoke succeeds. Require the
    # terminal operation timestamps so a fake/partial 202 cannot be mistaken
    # for a completed smoke gate.
    resume_started_at="$(jq -r '.started_at // empty' <<<"$RW_LIFECYCLE_POLL_BODY" 2>/dev/null || true)"
    resume_finished_at="$(jq -r '.finished_at // empty' <<<"$RW_LIFECYCLE_POLL_BODY" 2>/dev/null || true)"
    resume_started_epoch="$(rw_worker_heartbeat_epoch "$resume_started_at" 2>/dev/null || true)"
    resume_finished_epoch="$(rw_worker_heartbeat_epoch "$resume_finished_at" 2>/dev/null || true)"
    [[ -n "$resume_started_epoch" ]] || diagnostic="resume operation SUCCEEDED without valid started_at"
    [[ -n "$resume_finished_epoch" ]] || diagnostic="resume operation SUCCEEDED without valid finished_at"
    if [[ -z "$diagnostic" ]]; then
      (( resume_started_epoch >= resume_queued_epoch )) || diagnostic="resume operation started before it was queued (started_at=${resume_started_at}, queued_at=${resume_queued_at})"
      (( resume_finished_epoch >= resume_started_epoch )) || diagnostic="resume operation finished before it started (finished_at=${resume_finished_at}, started_at=${resume_started_at})"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_admin_request GET "/api/v1/admin/workers/${WORKER_ID}/health?level=D"; then
      diagnostic="Level D health GET transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "200" ]]; then
      diagnostic="Level D health GET returned HTTP ${RW_LAST_HTTP_STATUS}"
    else
      health_body="$RW_LAST_BODY"
      [[ "$(jq -r '.level // empty' <<<"$health_body" 2>/dev/null || true)" == "D" ]] || diagnostic="health report level is not D"
      [[ "$(jq -r '.healthy // false' <<<"$health_body" 2>/dev/null || true)" == "true" ]] || diagnostic="Level D smoke report is not healthy: ${health_body}"
      smoke_passed="$(jq -r '.checks.smoke_ok.passed // false' <<<"$health_body" 2>/dev/null || true)"
      smoke_value="$(jq -r '.checks.smoke_ok.value // empty' <<<"$health_body" 2>/dev/null || true)"
      [[ "$smoke_passed" == "true" && -n "$smoke_value" ]] || diagnostic="Level D smoke gate lacks passed smoke_ok/artifact evidence"
      smoke_collected_at="$(jq -r '.collected_at // empty' <<<"$health_body" 2>/dev/null || true)"
      smoke_collected_epoch="$(rw_worker_heartbeat_epoch "$smoke_collected_at" 2>/dev/null || true)"
      # `collected_at` is the health-probe timestamp, not the smoke_runs
      # timestamp. Do not present it as direct smoke-run evidence. The
      # authoritative freshness proof is the successful resume operation:
      # ResumeExecutor runs a new smoke within that operation and only then
      # clears drain. The D probe is sampled after operation completion and
      # must still be healthy with a non-empty artifact value.
      [[ -n "$smoke_collected_epoch" && -n "$resume_finished_epoch" && "$smoke_collected_epoch" -ge "$resume_finished_epoch" ]] || diagnostic="Level D probe was not collected after the successful resume operation (collected_at=${smoke_collected_at:-<empty>}, finished_at=${resume_finished_at:-<empty>})"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
    state="$(rw_lifecycle_worker_state "$body")"
    [[ "${state%%|*}" == "CONNECTED" ]] || diagnostic="worker connection state after resume is ${state}"
    health="${state#*|}"; health="${health%%|*}"
    [[ "$health" == "HEALTHY" ]] || diagnostic="worker health after resume is ${health:-<empty>} (expected HEALTHY)"
    [[ "$(jq -r '.drain // false' <<<"$body" 2>/dev/null || true)" == "false" ]] || diagnostic="worker drain flag remains true after successful resume"
    scheduling="${state##*|}"
    [[ "$scheduling" != "DRAINING" && "$scheduling" != "QUARANTINED" ]] || diagnostic="worker scheduling state remains excluded: ${scheduling}"
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/resume" \
      '{"reason":"duplicate W05 resume"}'; then
      diagnostic="duplicate resume transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "409" ]]; then
      diagnostic="duplicate resume returned HTTP ${RW_LAST_HTTP_STATUS}, expected 409"
    elif [[ "$(jq -r '.error // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" != *HEALTHY* ]]; then
      diagnostic="duplicate resume 409 did not explain HEALTHY no-op: ${RW_LAST_BODY}"
    elif [[ -n "$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" ]]; then
      diagnostic="duplicate resume 409 unexpectedly returned operation_id"
    fi
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_lifecycle_record W05 resume_smoke_gate FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_lifecycle_record W05 resume_smoke_gate PASS "HTTP 202 + operation SUCCEEDED with started_at/finished_at; fresh Level D smoke gate PASS; worker CONNECTED/HEALTHY; duplicate resume HTTP 409" "$elapsed"
  fi

  jq -n --arg schema 'velox.remote_worker.lifecycle.v1' \
    --arg worker_id "$WORKER_ID" --arg overall "$overall" \
    --argjson checks "$(printf '%s\n' "${RW_LIFECYCLE_RESULTS[@]}" | jq -s '.')" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{schema:$schema,worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:$generated_at}'
  [[ "$overall" == "PASS" ]]
}
