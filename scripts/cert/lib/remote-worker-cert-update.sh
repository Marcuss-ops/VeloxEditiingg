# remote-worker-cert-update.sh — worker update checks.
# Loaded by scripts/cert/remote-worker-cert-config.sh.

rw_update_record() {
  local id="$1" name="$2" status="$3" diagnostic="$4" elapsed_ms="$5" evidence="${6:-}"
  RW_UPDATE_RESULTS+=("$(jq -cn \
    --arg id "$id" --arg name "$name" --arg status "$status" \
    --arg diagnostic "$diagnostic" --arg evidence "$evidence" --argjson elapsed_ms "$elapsed_ms" \
    '{id:$id,name:$name,status:$status,elapsed_ms:$elapsed_ms,diagnostic:$diagnostic,evidence:(if $evidence == "" then null else $evidence end)}')")
}

rw_update_active_lease_count() {
  local body="$1"
  jq -er '
    if (.active_tasks? != null) then (.active_tasks | tonumber)
    elif (.active_slots? != null) then (.active_slots | tonumber)
    elif (.active_jobs? | type) == "number" then .active_jobs
    elif (.active_jobs? | type) == "array" then (.active_jobs | length)
    else empty
    end
  ' <<<"$body" 2>/dev/null
}

rw_update_poll_idle() {
  local deadline=$(( $(date +%s) + RW_UPDATE_LEASE_TIMEOUT_S )) body="" active="" detail=""
  RW_UPDATE_IDLE_BODY=""
  RW_UPDATE_IDLE_ERROR=""
  while (( $(date +%s) < deadline )); do      # The admin card's active_jobs/active_slots values are hydrated from
      # the authoritative lease projection; the diagnostic endpoint's
      # active_tasks value is heartbeat telemetry only.
      body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
      active="$(rw_update_active_lease_count "$body" 2>/dev/null || true)"
    if [[ "$active" =~ ^[0-9]+$ && "$active" == "0" ]]; then
      RW_UPDATE_IDLE_BODY="$body"
      return 0
    fi
    sleep "$RW_UPDATE_LEASE_POLL_INTERVAL_S"
  done
  detail="$(rw_update_active_lease_count "$body" 2>&1 || true)"
  RW_UPDATE_IDLE_ERROR="active lease check timed out after ${RW_UPDATE_LEASE_TIMEOUT_S}s: active_tasks=${detail:-<missing>}${body:+; last worker snapshot received}"
  export RW_UPDATE_IDLE_ERROR
  printf '%s' "$RW_UPDATE_IDLE_ERROR"
  return 1
}

rw_update_release_matches() {
  local body="$1" expected_digest="$2" actual_digest
  actual_digest="$(jq -r '.release_identity.image_digest // empty' <<<"$body" 2>/dev/null || true)"
  [[ "$actual_digest" == "$expected_digest" ]] || {
    printf 'ReleaseIdentity.image_digest=%s (expected %s)' "${actual_digest:-<empty>}" "$expected_digest"
    return 1
  }
  rw_worker_release_diagnostic "$body" || return 1
}

rw_update_health_smoke_ok() {
  local body="$1" smoke_passed smoke_value
  [[ "$(jq -r '.level // empty' <<<"$body" 2>/dev/null || true)" == "D" ]] || {
    printf 'health report level is not D'
    return 1
  }
  [[ "$(jq -r '.healthy // false' <<<"$body" 2>/dev/null || true)" == "true" ]] || {
    printf 'Level D report is not healthy'
    return 1
  }
  smoke_passed="$(jq -r '.checks.smoke_ok.passed // false' <<<"$body" 2>/dev/null || true)"
  smoke_value="$(jq -r '.checks.smoke_ok.value // empty' <<<"$body" 2>/dev/null || true)"
  [[ "$smoke_passed" == "true" && -n "$smoke_value" ]] || {
    printf 'Level D smoke_ok evidence is missing or not passed'
    return 1
  }
}

rw_update_checks() {
  local started finished elapsed body pre_body post_body health_body operation_id
  local diagnostic overall="PASS" target_digest target_image old_digest old_hb new_hb
  local old_hb_epoch new_hb_epoch active_count state scheduling health_state
  local -a RW_UPDATE_RESULTS=()

  for bin in jq curl; do
    command -v "$bin" >/dev/null 2>&1 || {
      rw_update_record U00 prerequisites FAIL "missing local prerequisite: ${bin}" 0
      overall="FAIL"
    }
  done
  target_image="${RW_UPDATE_TARGET_IMAGE:-}"
  target_digest="${RW_UPDATE_TARGET_DIGEST:-}"
  if [[ -z "$target_image" || -z "$target_digest" ]]; then
    rw_update_record U00 configuration FAIL 'RW_UPDATE_TARGET_IMAGE and RW_UPDATE_TARGET_DIGEST are required for update certification' 0
    overall="FAIL"
  fi
  if [[ "$overall" != "PASS" ]]; then
    jq -n --arg worker_id "${WORKER_ID:-}" --arg overall "$overall" \
      --argjson checks "$(printf '%s\n' "${RW_UPDATE_RESULTS[@]}" | jq -s '.')" \
      '{schema:"velox.remote_worker.update.v1",worker_id:$worker_id,target_digest:(env.RW_UPDATE_TARGET_DIGEST // ""),checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
    return 2
  fi

  # U01 — capture the pre-update identity and heartbeat. The worker must be
  # connected before the mutating operation is published.
  started="$(rw_now_s)"
  pre_body="$(rw_worker_admin_get "/api/v1/workers/${WORKER_ID}" 2>/dev/null || true)"
  diagnostic=""
  if [[ -z "$pre_body" ]]; then
    diagnostic='pre-update worker snapshot request failed'
  else
    rw_worker_snapshot_ok "$pre_body" "$WORKER_ID" || diagnostic="$(rw_worker_snapshot_ok "$pre_body" "$WORKER_ID" 2>&1)"
  fi
  old_digest="$(jq -r '.release_identity.image_digest // empty' <<<"$pre_body" 2>/dev/null || true)"
  old_hb="$(jq -r '.last_heartbeat_at // empty' <<<"$pre_body" 2>/dev/null || true)"
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U01 pre_update_identity FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U01 pre_update_identity PASS "CONNECTED worker with complete ReleaseIdentity; previous_digest=${old_digest}" "$elapsed"
  fi

  # U02 — publish the canonical update operation. The Master/UpdateExecutor
  # owns the automatic drain, idle wait, digest verification, restart and
  # forward Level-D smoke; this harness only observes its public contract.
  started="$(rw_now_s)"
  diagnostic=""
  if [[ "$overall" == "PASS" ]]; then
    local update_payload
    # The admin API calls the immutable image reference target_digest;
    # ReleaseIdentity.image_digest is the bare sha256 suffix checked later.
    update_payload="$(jq -cn --arg target_digest "$target_image" --arg reason "${RW_UPDATE_REASON}" '{target_digest:$target_digest,reason:$reason}')"
    if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/update" "$update_payload"; then
      diagnostic="update POST transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "202" ]]; then
      diagnostic="update POST returned HTTP ${RW_LAST_HTTP_STATUS}: ${RW_LAST_BODY}"
    else
      operation_id="$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
      [[ -n "$operation_id" ]] || diagnostic='update response omitted operation_id'
      [[ "$(jq -r '.op // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "update" ]] || diagnostic='update response op is not update'
      [[ "$(jq -r '.status // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "QUEUED" ]] || diagnostic='update response status is not QUEUED'
    fi
  else
    diagnostic='previous update-certification check failed'
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U02 update_queued FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U02 update_queued PASS "HTTP 202; operation_id=${operation_id}; target_digest=${target_digest}" "$elapsed" "$operation_id"
  fi

  # U03 — observe the executor's automatic drain and canonical active-task
  # signal. Do this before waiting for terminal status so a stuck lease is
  # reported as its own certification failure, not as an opaque timeout.
  started="$(rw_now_s)"
  diagnostic=""
  if [[ "$overall" == "PASS" ]]; then
    if rw_update_poll_idle; then
      active_count="$(rw_update_active_lease_count "$RW_UPDATE_IDLE_BODY" 2>/dev/null || true)"
      [[ "$active_count" == "0" ]] || diagnostic="active lease count after drain is ${active_count}, expected 0"
      scheduling="$(jq -r '.scheduling // .scheduling_state // empty' <<<"$RW_UPDATE_IDLE_BODY" 2>/dev/null || true)"
      state="$(jq -r '.status // empty' <<<"$RW_UPDATE_IDLE_BODY" 2>/dev/null || true)"
      if [[ "$scheduling" != "DRAINING" && "$state" != "DRAINING" && "$(jq -r '.drain // false' <<<"$RW_UPDATE_IDLE_BODY" 2>/dev/null || true)" != "true" ]]; then
        diagnostic="automatic drain was not observable before idle: status=${state:-<empty>}; scheduling=${scheduling:-<empty>}"
      fi
    else
      diagnostic="${RW_UPDATE_IDLE_ERROR:-active lease check failed}"
    fi
  else
    diagnostic='update operation was not queued'
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U03 drain_idle FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U03 drain_idle PASS 'automatic drain observed; active_tasks/active_slots=0 before update completion' "$elapsed"
  fi

  # U04 — wait for the UpdateExecutor's complete terminal cascade. It
  # includes restart, reconnect/readiness, target digest validation, and a
  # fresh Level-D smoke before SUCCEEDED is written.
  started="$(rw_now_s)"
  diagnostic=""
  if [[ "$overall" == "PASS" ]]; then
    if ! rw_lifecycle_poll_operation "$operation_id"; then
      diagnostic="$RW_LIFECYCLE_POLL_ERROR"
    elif ! rw_lifecycle_operation_matches "$RW_LIFECYCLE_POLL_BODY" "$operation_id" update; then
      diagnostic='update operation identity mismatch in terminal response'
    fi
    if [[ -z "$diagnostic" ]]; then
      [[ "$(jq -r '.status // empty' <<<"$RW_LIFECYCLE_POLL_BODY")" == "SUCCEEDED" ]] || diagnostic='update operation did not reach SUCCEEDED'
    fi
  else
    diagnostic='update operation was not eligible for polling'
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U04 update_cascade FAIL "$diagnostic" "$elapsed" "${operation_id:-}"
    overall="FAIL"
  else
    rw_update_record U04 update_cascade PASS 'UpdateExecutor reached SUCCEEDED after digest/restart/readiness/master/smoke cascade' "$elapsed" "$operation_id"
  fi

  # U05 — verify post-restart identity and smoke evidence from public reads.
  started="$(rw_now_s)"
  diagnostic=""
  post_body="$(rw_worker_poll_connected 2>/dev/null || true)"
  if [[ "$overall" == "PASS" && -z "$post_body" ]]; then
    diagnostic='post-update worker did not reconnect with a valid heartbeat'
  elif [[ "$overall" == "PASS" ]]; then
    rw_update_release_matches "$post_body" "$target_digest" || diagnostic="$(rw_update_release_matches "$post_body" "$target_digest" 2>&1)"
    new_hb="$(jq -r '.last_heartbeat_at // empty' <<<"$post_body")"
    [[ -n "$new_hb" ]] || diagnostic="${diagnostic}${diagnostic:+; }post-update heartbeat is empty"
    old_hb_epoch="$(rw_worker_heartbeat_epoch "$old_hb" 2>/dev/null || true)"
    new_hb_epoch="$(rw_worker_heartbeat_epoch "$new_hb" 2>/dev/null || true)"
    if [[ -n "$old_hb_epoch" && -n "$new_hb_epoch" ]]; then
      (( new_hb_epoch >= old_hb_epoch )) || diagnostic="${diagnostic}${diagnostic:+; }post-update heartbeat did not advance"
    fi
  elif [[ "$overall" != "PASS" ]]; then
    diagnostic='update cascade did not succeed'
  fi
  if [[ "$overall" == "PASS" ]]; then
    if ! rw_admin_request GET "/api/v1/admin/workers/${WORKER_ID}/health?level=D"; then
      diagnostic="Level D health GET transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "200" ]]; then
      diagnostic="Level D health GET returned HTTP ${RW_LAST_HTTP_STATUS}"
    else
      health_body="$RW_LAST_BODY"
      rw_update_health_smoke_ok "$health_body" || diagnostic="$(rw_update_health_smoke_ok "$health_body" 2>&1)"
    fi
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U05 post_update_release_smoke FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U05 post_update_release_smoke PASS "ReleaseIdentity digest=${target_digest}; reconnect heartbeat advanced; fresh Level D smoke evidence present" "$elapsed"
  fi

  # U06 — update normally releases its own drain after a green smoke. If the
  # worker is still excluded (operator-owned drain/quarantine), use the
  # canonical resume operation and verify its fresh smoke gate. A healthy
  # worker is an explicit PASS for automatic resume, not a false duplicate
  # resume request that would correctly return HTTP 409.
  started="$(rw_now_s)"
  diagnostic=""
  body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
  state="$(rw_lifecycle_worker_state "$body")"
  scheduling="$(jq -r '.scheduling_state // empty' <<<"$body" 2>/dev/null || true)"
  if [[ "$overall" != "PASS" ]]; then
    diagnostic='post-update checks failed; resume was not attempted'
  elif [[ "$scheduling" == "DRAINING" || "$scheduling" == "QUARANTINED" || "$scheduling" == "RESUMING" ]]; then
    if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/resume" \
      "$(jq -nc --arg reason "${RW_UPDATE_REASON}; resume certification" '{reason:$reason}')"; then
      diagnostic="resume POST transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "202" ]]; then
      diagnostic="resume POST returned HTTP ${RW_LAST_HTTP_STATUS}: ${RW_LAST_BODY}"
    else
      local resume_operation_id
      resume_operation_id="$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
      [[ -n "$resume_operation_id" ]] || diagnostic='resume response omitted operation_id'
      if [[ -z "$diagnostic" ]] && ! rw_lifecycle_poll_operation "$resume_operation_id"; then
        diagnostic="$RW_LIFECYCLE_POLL_ERROR"
      fi
      if [[ -z "$diagnostic" ]] && ! rw_lifecycle_operation_matches "$RW_LIFECYCLE_POLL_BODY" "$resume_operation_id" resume; then
        diagnostic='resume operation identity mismatch'
      fi
      if [[ -z "$diagnostic" ]] && [[ "$(jq -r '.status // empty' <<<"$RW_LIFECYCLE_POLL_BODY")" != "SUCCEEDED" ]]; then
        diagnostic='resume operation did not reach SUCCEEDED'
      fi
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
    state="$(rw_lifecycle_worker_state "$body")"
    scheduling="$(jq -r '.scheduling_state // empty' <<<"$body" 2>/dev/null || true)"
    health_state="$(jq -r '.health_state // empty' <<<"$body" 2>/dev/null || true)"
    [[ "${state%%|*}" == "CONNECTED" ]] || diagnostic="worker state after resume is ${state}"
    [[ "$scheduling" == "AVAILABLE" ]] || diagnostic="worker scheduling state after resume is ${scheduling:-<empty>} (expected AVAILABLE)"
    [[ "$health_state" == "HEALTHY" ]] || diagnostic="worker health state after resume is ${health_state:-<empty>} (expected HEALTHY)"
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U06 resume FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U06 resume PASS 'worker returned to CONNECTED and placement-eligible state after successful update smoke' "$elapsed"
  fi

  jq -n --arg schema 'velox.remote_worker.update.v1' \
    --arg worker_id "$WORKER_ID" --arg target_digest "$target_digest" \
    --arg target_image "$target_image" --arg overall "$overall" \
    --arg previous_digest "$old_digest" --arg operation_id "${operation_id:-}" \
    --argjson checks "$(printf '%s\n' "${RW_UPDATE_RESULTS[@]}" | jq -s '.')" \
    '{schema:$schema,worker_id:$worker_id,target_image:$target_image,target_digest:$target_digest,previous_digest:(if $previous_digest=="" then null else $previous_digest end),operation_id:(if $operation_id=="" then null else $operation_id end),checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
  [[ "$overall" == "PASS" ]]
}

rw_update_config_failure() {
  local diagnostic="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n --arg worker_id "${WORKER_ID:-${VELOX_WORKER_ID:-}}" --arg diagnostic "$diagnostic" \
      '{schema:"velox.remote_worker.update.v1",worker_id:$worker_id,checks:[{id:"U00",name:"configuration",status:"FAIL",elapsed_ms:0,diagnostic:$diagnostic}],overall:"FAIL",generated_at:(now|todateiso8601)}'
  else
    printf '%s\n' '{"schema":"velox.remote_worker.update.v1","checks":[{"id":"U00","name":"configuration","status":"FAIL","elapsed_ms":0,"diagnostic":"configuration validation failed"}],"overall":"FAIL"}'
  fi
}

rw_lifecycle_config_failure() {
  local diagnostic="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n \
      --arg worker_id "${WORKER_ID:-${VELOX_WORKER_ID:-}}" \
      --arg diagnostic "$diagnostic" \
      --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      '{schema:"velox.remote_worker.lifecycle.v1",worker_id:$worker_id,checks:[{id:"W00",name:"configuration",status:"FAIL",elapsed_ms:0,diagnostic:$diagnostic}],overall:"FAIL",generated_at:$generated_at}'
  else
    printf '%s\n' '{"schema":"velox.remote_worker.lifecycle.v1","checks":[{"id":"W00","name":"configuration","status":"FAIL","elapsed_ms":0,"diagnostic":"configuration validation failed"}],"overall":"FAIL"}'
  fi
}

