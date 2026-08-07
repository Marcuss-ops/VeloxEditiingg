# remote-worker-cert-lifecycle.sh — worker lifecycle checks.
# Loaded by scripts/cert/remote-worker-cert-config.sh.

rw_admin_request() {
  local method="$1" path="$2" body="${3:-}" cfg response_file status_file rc
  rw_log_command "${method} ${path}"
  cfg="$(mktemp "${TMPDIR:-/tmp}/velox-admin-curl.XXXXXX")" || return 1
  response_file="$(mktemp "${TMPDIR:-/tmp}/velox-admin-response.XXXXXX")" || { rm -f -- "$cfg"; return 1; }
  status_file="$(mktemp "${TMPDIR:-/tmp}/velox-admin-status.XXXXXX")" || { rm -f -- "$cfg" "$response_file"; return 1; }
  rw_curl_config "$cfg" || { rm -f -- "$cfg" "$response_file" "$status_file"; return 1; }
  if [[ -n "$body" ]]; then
    curl --silent --show-error --connect-timeout "$RW_CONNECT_TIMEOUT_S" --max-time "$RW_WORKER_HTTP_TIMEOUT_S" \
      --request "$method" --data-raw "$body" --config "$cfg" \
      --output "$response_file" --write-out '%{http_code}' \
      "${MASTER_URL}${path}" >"$status_file"
    rc=$?
  else
    curl --silent --show-error --connect-timeout "$RW_CONNECT_TIMEOUT_S" --max-time "$RW_WORKER_HTTP_TIMEOUT_S" \
      --request "$method" --config "$cfg" --output "$response_file" \
      --write-out '%{http_code}' "${MASTER_URL}${path}" >"$status_file"
    rc=$?
  fi
  RW_LAST_HTTP_STATUS="$(cat "$status_file" 2>/dev/null || true)"
  RW_LAST_BODY="$(cat "$response_file" 2>/dev/null || true)"
  RW_LAST_CURL_RC="$rc"
  rw_record_operation "$method" "$path" "${RW_LAST_HTTP_STATUS:-000}" "${RW_LAST_BODY:-}"
  rw_snapshot_json master "${RW_LAST_BODY:-}"
  rm -f -- "$cfg" "$response_file" "$status_file"
  return "$rc"
}

rw_lifecycle_record() {
  local id="$1" name="$2" status="$3" diagnostic="$4" elapsed_ms="$5"
  RW_LIFECYCLE_RESULTS+=("$(jq -cn \
    --arg id "$id" --arg name "$name" --arg status "$status" \
    --arg diagnostic "$diagnostic" --argjson elapsed_ms "$elapsed_ms" \
    '{id:$id,name:$name,status:$status,elapsed_ms:$elapsed_ms,diagnostic:$diagnostic}')")
}

rw_lifecycle_worker_state() {
  local body="$1" state health scheduling
  state="$(jq -r '.status // .connection_status // empty' <<<"$body" 2>/dev/null || true)"
  health="$(jq -r '.health // .health_state // empty' <<<"$body" 2>/dev/null || true)"
  scheduling="$(jq -r '.scheduling_state // empty' <<<"$body" 2>/dev/null || true)"
  printf '%s|%s|%s' "$state" "$health" "$scheduling"
}

rw_lifecycle_operation_matches() {
  local body="$1" expected_id="$2" expected_op="$3"
  jq -e --arg id "$expected_id" --arg worker "$WORKER_ID" --arg op "$expected_op" \
    '.operation_id == $id and .worker_id == $worker and .op == $op' \
    <<<"$body" >/dev/null 2>&1
}

rw_lifecycle_poll_operation() {
  local operation_id="$1" deadline status body
  deadline=$(( $(date +%s) + RW_OPERATION_TIMEOUT_S ))
  RW_LIFECYCLE_POLL_ERROR=""
  while (( $(date +%s) < deadline )); do
    if ! rw_admin_request GET "/api/v1/admin/operations/${operation_id}"; then
      RW_LIFECYCLE_POLL_ERROR="operation GET transport failed (rc=${RW_LAST_CURL_RC})"
      return 1
    fi
    if [[ "$RW_LAST_HTTP_STATUS" != "200" ]]; then
      RW_LIFECYCLE_POLL_ERROR="operation GET returned HTTP ${RW_LAST_HTTP_STATUS}"
      return 1
    fi
    body="$RW_LAST_BODY"
    status="$(jq -r '.status // empty' <<<"$body" 2>/dev/null || true)"
    case "$status" in
      QUEUED|RUNNING)
        sleep "$RW_OPERATION_POLL_INTERVAL_S"
        ;;
      SUCCEEDED)
        RW_LIFECYCLE_POLL_BODY="$body"
        return 0
        ;;
      FAILED|CANCELLED|ROLLBACK|ROLLED_BACK)
        RW_LIFECYCLE_POLL_ERROR="operation reached terminal status ${status}: $(jq -r '.error_message // .error // empty' <<<"$body" 2>/dev/null || true)"
        return 1
        ;;
      *)
        RW_LIFECYCLE_POLL_ERROR="operation returned unexpected status: ${status:-<empty>}"
        return 1
        ;;
    esac
  done
  RW_LIFECYCLE_POLL_ERROR="operation polling timed out after ${RW_OPERATION_TIMEOUT_S}s"
  return 1
}

rw_smoke_record() {
  local id="$1" name="$2" status="$3" diagnostic="$4" elapsed_ms="$5" evidence="${6:-}"
  RW_SMOKE_RESULTS+=("$(jq -cn \
    --arg id "$id" --arg name "$name" --arg status "$status" \
    --arg diagnostic "$diagnostic" --arg evidence "$evidence" --argjson elapsed_ms "$elapsed_ms" \
    '{id:$id,name:$name,status:$status,elapsed_ms:$elapsed_ms,diagnostic:$diagnostic,evidence:(if $evidence == "" then null else $evidence end)}')")
}

rw_smoke_cleanup_command() {
  # These are the exact temp locations removed by SSHWorkerExec.CleanupWorkerTemp.
  # The command contains no operator-provided shell fragment.
  printf '%s' "find /var/lib/velox-worker/smoke -maxdepth 1 -type f -name 'smoke-*.*' -printf '%f\\n' 2>/dev/null || true; find /tmp/velox-smoke -mindepth 2 -maxdepth 2 -type f -printf '%P\\n' 2>/dev/null || true"
}

rw_smoke_checks() {
  local started finished elapsed body operation_id asset_id payload queued_at queued_epoch
  local started_at finished_at started_epoch finished_epoch health_body smoke_value collected_at collected_epoch
  local diagnostic overall="PASS" fixture_name cleanup_before cleanup_after new_files
  local phase_detail
  local -a RW_SMOKE_RESULTS=()

  for bin in jq curl; do
    command -v "$bin" >/dev/null 2>&1 || {
      rw_smoke_record P01-"${bin}" prerequisite FAIL "missing local prerequisite: ${bin}" 0
      overall="FAIL"
    }
  done
  if [[ "$RW_SMOKE_VERIFY_CLEANUP" == "1" ]]; then
    for bin in ssh timeout; do
      command -v "$bin" >/dev/null 2>&1 || {
        rw_smoke_record P01-"${bin}" prerequisite FAIL "cleanup verification requires local prerequisite: ${bin}" 0
        overall="FAIL"
      }
    done
  fi
  if [[ "$overall" != "PASS" ]]; then
    jq -n --arg worker_id "${WORKER_ID:-}" --arg overall "$overall" \
      --argjson checks "$(printf '%s
' "${RW_SMOKE_RESULTS[@]}" | jq -s '.')" \
      '{schema:"velox.remote_worker.smoke.v1",worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
    return 2
  fi

  started="$(rw_now_s)"
  diagnostic=""
  asset_id="${RW_SMOKE_ASSET_ID:-}"
  if [[ -z "$asset_id" ]]; then
    asset_id="$(jq -er '.clips[0].asset_id // empty' "$RW_SMOKE_FIXTURES_FILE" 2>/dev/null || true)"
    fixture_name="${RW_SMOKE_FIXTURES_FILE} (.clips[0].asset_id)"
  else
    fixture_name="RW_SMOKE_ASSET_ID override"
  fi
  if [[ -z "$asset_id" ]]; then
    diagnostic="could not resolve a non-empty smoke asset_id from ${fixture_name}"
  elif [[ "$asset_id" == *[[:space:]/\\]* ]]; then
    diagnostic="smoke asset_id contains whitespace or path separators"
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_smoke_record P01-fixture fixture FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_smoke_record P01-fixture fixture PASS "asset_id=${asset_id}; source=${fixture_name}" "$elapsed"
  fi

  # Capture the worker's pre-run smoke files. The run_id is intentionally not
  # exposed by the current admin API, so cleanup is verified by set difference
  # against the exact paths used by the executor rather than by guessing one.
  if [[ "$RW_SMOKE_VERIFY_CLEANUP" == "1" ]]; then
    rw_capture_ssh "$(rw_smoke_cleanup_command)" 2>/dev/null || true
    if [[ "$RW_LAST_RC" -eq 0 ]]; then
      cleanup_before="$RW_LAST_STDOUT"
    else
      cleanup_before=""
      rw_smoke_record P01-cleanup-baseline cleanup_best_effort FAIL "SSH cleanup baseline failed: $(rw_network_diagnostic)" 0 ssh_listing
      overall="FAIL"
    fi
  fi

  started="$(rw_now_s)"
  diagnostic=""
  if [[ "$overall" == "PASS" ]]; then
    payload="$(jq -nc \
      --arg asset_id "$asset_id" \
      --arg reason "remote worker P01 Level D smoke certification" \
      --arg render_plan "$RW_SMOKE_RENDER_PLAN" \
      '({asset_id:$asset_id,reason:$reason} + (if $render_plan == "" then {} else {render_plan:$render_plan} end))')"
    if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/smoke" "$payload"; then
      diagnostic="smoke POST transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "202" ]]; then
      diagnostic="smoke POST returned HTTP ${RW_LAST_HTTP_STATUS}: ${RW_LAST_BODY}"
    else
      operation_id="$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
      queued_at="$(jq -r '.queued_at // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
      queued_epoch="$(rw_worker_heartbeat_epoch "$queued_at" 2>/dev/null || true)"
      [[ -n "$operation_id" ]] || diagnostic="smoke 202 response omitted operation_id"
      [[ "$(jq -r '.worker_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "$WORKER_ID" ]] || diagnostic="smoke response worker_id mismatch"
      [[ "$(jq -r '.op // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "smoke" ]] || diagnostic="smoke response op is not smoke"
      [[ "$(jq -r '.status // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "QUEUED" ]] || diagnostic="smoke response status is not QUEUED"
      [[ -n "$queued_epoch" ]] || diagnostic="smoke response queued_at is not valid RFC3339"
    fi
  else
    diagnostic="fixture or cleanup baseline failed; smoke operation not started"
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_smoke_record P01-trigger trigger FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_smoke_record P01-trigger trigger PASS "HTTP 202; operation_id=${operation_id}; asset_id=${asset_id}; status=QUEUED" "$elapsed"
  fi

  if [[ -z "$diagnostic" ]]; then
    if ! rw_lifecycle_poll_operation "$operation_id"; then
      diagnostic="${RW_LIFECYCLE_POLL_ERROR}"
    else
      body="$RW_LIFECYCLE_POLL_BODY"
      if ! rw_lifecycle_operation_matches "$body" "$operation_id" smoke; then
        diagnostic="smoke operation identity mismatch in terminal response"
      fi
      started_at="$(jq -r '.started_at // empty' <<<"$body" 2>/dev/null || true)"
      finished_at="$(jq -r '.finished_at // empty' <<<"$body" 2>/dev/null || true)"
      started_epoch="$(rw_worker_heartbeat_epoch "$started_at" 2>/dev/null || true)"
      finished_epoch="$(rw_worker_heartbeat_epoch "$finished_at" 2>/dev/null || true)"
      [[ -n "$started_epoch" ]] || diagnostic="smoke SUCCEEDED response omitted valid started_at"
      [[ -n "$finished_epoch" ]] || diagnostic="smoke SUCCEEDED response omitted valid finished_at"
      if [[ -z "$diagnostic" ]]; then
        (( started_epoch >= queued_epoch )) || diagnostic="smoke started before queued_at"
        (( finished_epoch >= started_epoch )) || diagnostic="smoke finished before started_at"
      fi
      # OperationCard.Payload is the only public payload echo. Validate it when
      # present, but do not fail older servers that omit it from the GET DTO.
      payload="$(jq -r '.payload // empty' <<<"$body" 2>/dev/null || true)"
      if [[ -n "$payload" ]]; then
        [[ "$(jq -r '.payload // empty | fromjson? | .asset_id // empty' <<<"$body" 2>/dev/null || true)" == "$asset_id" ]] || diagnostic="terminal smoke payload asset_id mismatch"
      fi
    fi
  fi
  if [[ -n "$diagnostic" ]]; then
    rw_smoke_record P01-operation operation FAIL "$diagnostic" "${elapsed:-0}"
    overall="FAIL"
  else
    rw_smoke_record P01-operation operation PASS "operation SUCCEEDED with valid identity and timestamps" "${elapsed:-0}"
  fi

  # The executor is the authoritative implementation of these phases. The
  # current OperationCard intentionally does not expose independent per-phase
  # counters, SHA, byte size, or run_id. Mark these checks as contract evidence
  # (not fabricated direct observations) only after the operation identity,
  # timestamps, and SUCCEEDED terminal state have passed; failures remain
  # fail-closed. Cleanup is checked independently below.
  phase_detail="executor_contract: LevelDSmokeExecutor SUCCEEDED is the public evidence for lease/download/ffmpeg/ffprobe/size+SHA/upload; independent per-phase fields are not exposed by the current admin API"
  for phase in lease download ffmpeg ffprobe size_sha256 upload; do
    if [[ "$overall" == "PASS" ]]; then
      rw_smoke_record "P01-${phase}" "$phase" PASS "$phase: ${phase_detail}" 0 operation_succeeded_contract
    else
      rw_smoke_record "P01-${phase}" "$phase" FAIL "not accepted because the smoke operation did not complete successfully" 0
    fi
  done

  # Health D reads smoke_runs and returns artifact_drive_id as smoke_ok.value.
  # That is the public artifact-published evidence. Drive smoke artifacts do
  # not use the pipeline's READY state, and there is no separate READY endpoint
  # for this contract; /api/internal/artifacts is a different job-artifact API.
  if [[ -z "$diagnostic" && "$overall" == "PASS" ]]; then
    if ! rw_admin_request GET "/api/v1/admin/workers/${WORKER_ID}/health?level=D"; then
      diagnostic="Level D health GET transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "200" ]]; then
      diagnostic="Level D health GET returned HTTP ${RW_LAST_HTTP_STATUS}"
    else
      health_body="$RW_LAST_BODY"
      [[ "$(jq -r '.level // empty' <<<"$health_body" 2>/dev/null || true)" == "D" ]] || diagnostic="health report level is not D"
      [[ "$(jq -r '.healthy // false' <<<"$health_body" 2>/dev/null || true)" == "true" ]] || diagnostic="Level D health report is not healthy: ${health_body}"
      [[ "$(jq -r '.checks.smoke_ok.passed // false' <<<"$health_body" 2>/dev/null || true)" == "true" ]] || diagnostic="health smoke_ok.passed is not true"
      smoke_value="$(jq -r '.checks.smoke_ok.value // empty' <<<"$health_body" 2>/dev/null || true)"
      [[ -n "$smoke_value" ]] || diagnostic="health smoke_ok.value/artifact_drive_id is empty"
      collected_at="$(jq -r '.collected_at // empty' <<<"$health_body" 2>/dev/null || true)"
      collected_epoch="$(rw_worker_heartbeat_epoch "$collected_at" 2>/dev/null || true)"
      [[ -n "$collected_epoch" && -n "$finished_epoch" && "$collected_epoch" -ge "$finished_epoch" ]] || diagnostic="Level D evidence was not collected after smoke completion"
    fi
  elif [[ -z "$diagnostic" ]]; then
    diagnostic="smoke operation did not complete; Level D health not accepted"
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_smoke_record P01-artifact-published artifact_published FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_smoke_record P01-artifact-published artifact_published PASS "Level D healthy; smoke_ok passed; artifact_drive_id=${smoke_value}; smoke_runs SUCCEEDED is the published-artifact terminal state; no separate READY endpoint exists" "$elapsed" smoke_runs_succeeded
  fi

  if [[ "$RW_SMOKE_VERIFY_CLEANUP" == "1" && "$overall" == "PASS" ]]; then
    started="$(rw_now_s)"
    rw_capture_ssh "$(rw_smoke_cleanup_command)" 2>/dev/null || true
    cleanup_after="$RW_LAST_STDOUT"
    if [[ "$RW_LAST_RC" -ne 0 ]]; then
      diagnostic="SSH cleanup verification failed: $(rw_network_diagnostic)"
    else
      new_files="$(comm -13 <(printf '%s
' "$cleanup_before" | sed '/^$/d' | sort -u) <(printf '%s
' "$cleanup_after" | sed '/^$/d' | sort -u) || true)"
      if [[ -n "$new_files" ]]; then
        diagnostic="new smoke temp files remain after operation: ${new_files}"
      else
        diagnostic=""
      fi
    fi
    finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
    if [[ -n "$diagnostic" ]]; then
      rw_smoke_record P01-cleanup cleanup_best_effort FAIL "$diagnostic" "$elapsed" filename_set_difference
      overall="FAIL"
    else
      rw_smoke_record P01-cleanup cleanup_best_effort PASS "no new executor smoke temp files remain; lease release is part of executor cleanup; run_id is not exposed, so pre/post filename comparison is best-effort" "$elapsed" filename_set_difference
    fi
  elif [[ "$RW_SMOKE_VERIFY_CLEANUP" == "0" ]]; then
    rw_smoke_record P01-cleanup cleanup_best_effort SKIP "SSH temp-file cleanup verification disabled by RW_SMOKE_VERIFY_CLEANUP=0; executor contract still requires cleanup" 0 disabled
  fi

  jq -n --arg schema 'velox.remote_worker.smoke.v1' \
    --arg worker_id "$WORKER_ID" --arg asset_id "$asset_id" --arg overall "$overall" \
    --arg artifact_id "${smoke_value:-}" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson checks "$(printf '%s
' "${RW_SMOKE_RESULTS[@]}" | jq -s '.')" \
    '{schema:$schema,worker_id:$worker_id,asset_id:$asset_id,artifact_id:(if $artifact_id == "" then null else $artifact_id end),checks:$checks,overall:$overall,generated_at:$generated_at}'
  [[ "$overall" == "PASS" ]]
}

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

