# remote-worker-cert-installation.sh — extracted worker certification lifecycle domain.
# Loaded by scripts/cert/lib/remote-worker-cert-lifecycle.sh.
# shellcheck shell=bash

rw_smoke_record() {
  local id="$1" name="$2" status="$3" diagnostic="$4" elapsed_ms="$5" evidence="${6:-}"
  RW_SMOKE_RESULTS+=("$(jq -cn \
    --arg id "$id" --arg name "$name" --arg status "$status" \
    --arg diagnostic "$diagnostic" --arg evidence "$evidence" --argjson elapsed_ms "$elapsed_ms" \
    '{id:$id,name:$name,status:$status,elapsed_ms:$elapsed_ms,diagnostic:$diagnostic,evidence:(if $evidence == "" then null else $evidence end)}')")
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
