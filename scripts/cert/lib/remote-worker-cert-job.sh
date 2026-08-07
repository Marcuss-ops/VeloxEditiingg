# remote-worker-cert-job.sh — job and artifact checks.
# Loaded by scripts/cert/remote-worker-cert-config.sh.

rw_job_record() {
  local id="$1" name="$2" status="$3" diagnostic="$4" elapsed_ms="$5" evidence="${6:-}"
  RW_JOB_RESULTS+=("$(jq -cn \
    --arg id "$id" --arg name "$name" --arg status "$status" \
    --arg diagnostic "$diagnostic" --arg evidence "$evidence" --argjson elapsed_ms "$elapsed_ms" \
    '{id:$id,name:$name,status:$status,elapsed_ms:$elapsed_ms,diagnostic:$diagnostic,evidence:(if $evidence == "" then null else $evidence end)}')")
}

rw_job_curl_config() {
  local cfg="$1" token="$2"
  umask 077
  : >"$cfg" || return 1
  printf 'header = "Authorization: Bearer %s"\\n' "$token" >"$cfg"
  printf 'header = "Content-Type: application/json"\\n' >>"$cfg"
  chmod 600 "$cfg"
}

rw_job_request() {
  local method="$1" path="$2" body="${3:-}" token="$4"
  rw_log_command "${method} ${path}"
  local cfg response_file status_file rc
  cfg="$(mktemp "${TMPDIR:-/tmp}/velox-job-curl.XXXXXX")" || return 1
  response_file="$(mktemp "${TMPDIR:-/tmp}/velox-job-response.XXXXXX")" || { rm -f -- "$cfg"; return 1; }
  status_file="$(mktemp "${TMPDIR:-/tmp}/velox-job-status.XXXXXX")" || { rm -f -- "$cfg" "$response_file"; return 1; }
  rw_job_curl_config "$cfg" "$token" || { rm -f -- "$cfg" "$response_file" "$status_file"; return 1; }
  if [[ -n "$body" ]]; then
    curl --silent --show-error --connect-timeout "$RW_CONNECT_TIMEOUT_S" --max-time "$RW_JOB_HTTP_TIMEOUT_S" \
      --request "$method" --data-raw "$body" --config "$cfg" \
      --output "$response_file" --write-out '%{http_code}' "${MASTER_URL}${path}" >"$status_file"
    rc=$?
  else
    curl --silent --show-error --connect-timeout "$RW_CONNECT_TIMEOUT_S" --max-time "$RW_JOB_HTTP_TIMEOUT_S" \
      --request "$method" --config "$cfg" --output "$response_file" \
      --write-out '%{http_code}' "${MASTER_URL}${path}" >"$status_file"
    rc=$?
  fi
  RW_JOB_HTTP_STATUS="$(cat "$status_file" 2>/dev/null || true)"
  RW_JOB_BODY="$(cat "$response_file" 2>/dev/null || true)"
  RW_JOB_CURL_RC="$rc"
  rw_record_operation "$method" "$path" "${RW_JOB_HTTP_STATUS:-000}" "${RW_JOB_BODY:-}"
  rw_snapshot_json master "${RW_JOB_BODY:-}"
  rm -f -- "$cfg" "$response_file" "$status_file"
  return "$rc"
}

rw_job_download_to_file() {
  local url="$1" output="$2" token="$3" cfg status_file rc
  cfg="$(mktemp "${TMPDIR:-/tmp}/velox-job-download-curl.XXXXXX")" || return 1
  status_file="$(mktemp "${TMPDIR:-/tmp}/velox-job-download-status.XXXXXX")" || { rm -f -- "$cfg"; return 1; }
  rw_job_curl_config "$cfg" "$token" || { rm -f -- "$cfg" "$status_file"; return 1; }
  rw_log_command "GET artifact-download"
  curl --silent --show-error --connect-timeout "$RW_CONNECT_TIMEOUT_S" --max-time "$RW_JOB_ARTIFACT_DOWNLOAD_TIMEOUT_S" \
    --request GET --config "$cfg" --output "$output" --write-out '%{http_code}' "$url" >"$status_file"
  rc=$?
  RW_JOB_DOWNLOAD_HTTP_STATUS="$(cat "$status_file" 2>/dev/null || true)"
  RW_JOB_DOWNLOAD_CURL_RC="$rc"
  rm -f -- "$cfg" "$status_file"
  return "$rc"
}

rw_job_artifact_id_from_url() {
  local value="$1"
  value="${value%%\?*}"
  if [[ "$value" =~ /api/internal/artifacts/([A-Za-z0-9._:-]+)/download$ ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
  fi
}

rw_job_artifact_download_url() {
  local artifact_id="$1" candidate="${RW_JOB_ARTIFACT_DOWNLOAD_URL:-}"
  if [[ -n "$candidate" ]]; then
    [[ "$candidate" =~ ^/api/internal/artifacts/[A-Za-z0-9._:-]+/download([?][^[:space:]]*)?$ ]] || return 1
    printf '%s%s' "$MASTER_URL" "$candidate"
  elif [[ -n "$artifact_id" ]]; then
    [[ "$artifact_id" =~ ^[A-Za-z0-9._:-]+$ ]] || return 1
    printf '%s/api/internal/artifacts/%s/download' "$MASTER_URL" "$artifact_id"
  fi
}

rw_job_lifecycle_monotonic_ok() {
  local observed="$1" state rank last_rank=-1
  local -a observed_states=()
  mapfile -t observed_states <<<"$observed"
  for state in "${observed_states[@]}"; do
    [[ -n "$state" ]] || continue
    case "$state" in
      QUEUED) rank=0 ;;
      PENDING) rank=1 ;;
      RETRY_WAIT) rank=2 ;;
      READY) rank=3 ;;
      POLLING) rank=4 ;;
      LEASED) rank=5 ;;
      RUNNING) rank=6 ;;
      AWAITING_ARTIFACT) rank=7 ;;
      FORWARDING) rank=8 ;;
      FORWARDED) rank=9 ;;
      SUCCEEDED) rank=10 ;;
      FAILED|CANCELLED) rank=99 ;;
      *)
        printf 'unknown lifecycle state %s (states=%s)' "$state" "${observed//$'\n'/ -> }"
        return 1
        ;;
    esac
    if (( rank < last_rank )); then
      printf 'lifecycle state regressed at %s (states=%s)' "$state" "${observed//$'\n'/ -> }"
      return 1
    fi
    last_rank="$rank"
  done
}

rw_job_required_states_ok() {
  local observed="$1" required_csv="${RW_JOB_REQUIRED_STATES:-PENDING,LEASED,RUNNING,AWAITING_ARTIFACT,SUCCEEDED}"
  local state required required_index last_required=-1 found index=0
  local -a required_states=() observed_states=()
  IFS=',' read -r -a required_states <<<"$required_csv"
  mapfile -t observed_states <<<"$observed"
  for state in "${observed_states[@]}"; do
    [[ -n "$state" ]] || continue
    required_index=-1
    for index in "${!required_states[@]}"; do
      required="${required_states[index]//[[:space:]]/}"
      [[ "$state" == "$required" ]] && { required_index="$index"; break; }
    done
    if (( required_index >= 0 )); then
      if (( required_index < last_required )); then
        printf 'lifecycle state regressed at %s (states=%s)' "$state" "${observed//$'\n'/ -> }"
        return 1
      fi
      last_required="$required_index"
    fi
  done
  for required in "${required_states[@]}"; do
    required="${required//[[:space:]]/}"
    [[ -n "$required" ]] || continue
    found=0
    for state in "${observed_states[@]}"; do
      [[ "$state" == "$required" ]] && { found=1; break; }
    done
    (( found == 1 )) || {
      printf 'required lifecycle state %s was not observed in order (states=%s)' "$required" "${observed//$'\n'/ -> }"
      return 1
    }
  done
}

rw_job_fixture_payload() {
  local fixture="$1" destination="${2:-}" key="remote-worker-${WORKER_ID}-$(date +%s%N)"
  jq --arg worker "$WORKER_ID" --arg destination "$destination" --arg key "$key" \
    '.idempotency_key=$key
     | .placement_pin_worker_id=$worker
     | if (.delivery_plan | type) == "array" and (.delivery_plan | length) > 0 and $destination != "" then
         .delivery_plan[0].destination_id=$destination
       else . end' "$fixture"
}

rw_job_checks() {
  local started finished elapsed body payload job_id status status_url poll_status_url
  local deadline sequence terminal_status="" diagnostic="" overall="PASS"
  local artifact_id response_artifact_id artifact_url configured_artifact_id configured_url_id download_url artifact_size expected_sha final_sha
  local artifact_file probe_json probe_duration probe_size fixture_file expected_submit_status required_states_ok state_error verifier_report verifier_log verifier_rc
  local -a RW_JOB_RESULTS=()
  local -a statuses=()

  for bin in jq curl sha256sum python3; do
    command -v "$bin" >/dev/null 2>&1 || {
      rw_job_record P02-W00 prerequisites FAIL "missing local prerequisite: ${bin}" 0
      overall="FAIL"
    }
  done
  if [[ "$RW_JOB_VERIFY_FFPROBE" == "1" ]] && ! command -v ffprobe >/dev/null 2>&1; then
    rw_job_record P03-ffprobe prerequisites FAIL 'missing local prerequisite: ffprobe' 0
    overall="FAIL"
  fi
  if [[ -z "${M2M_TOKEN:-}" ]]; then
    rw_job_record P02-m2m_token prerequisites FAIL 'M2M_TOKEN/VELOX_M2M_TOKEN is not configured' 0
    overall="FAIL"
  fi
  if [[ "$overall" != "PASS" ]]; then
    jq -n --arg worker_id "${WORKER_ID:-}" --arg overall "$overall" \
      --argjson checks "$(printf '%s\n' "${RW_JOB_RESULTS[@]}" | jq -s '.')" \
      '{schema:"velox.remote_worker.job.v1",worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
    return 2
  fi

  started="$(rw_now_s)"
  payload=""
  fixture_file="${RW_JOB_FIXTURE_FILE:-}"
  if [[ -n "${TEST_JOB_JSON:-}" ]]; then
    if [[ -n "${RW_JOB_DESTINATION_ID:-}" ]]; then
      payload="$(rw_job_fixture_payload "$TEST_JOB_JSON" "$RW_JOB_DESTINATION_ID" 2>/dev/null || true)"
    else
      payload="$(cat "$TEST_JOB_JSON")"
    fi
    if ! jq -e . >/dev/null 2>&1 <<<"$payload"; then
      diagnostic="TEST_JOB_JSON is not valid JSON"
    fi
  elif [[ -n "$fixture_file" ]]; then
    if [[ -z "${RW_JOB_DESTINATION_ID:-}" ]]; then
      diagnostic='RW_JOB_DESTINATION_ID is required when RW_JOB_FIXTURE_FILE is set'
    else
      payload="$(rw_job_fixture_payload "$fixture_file" "$RW_JOB_DESTINATION_ID" 2>/dev/null || true)"
      [[ -n "$payload" ]] || diagnostic="RW_JOB_FIXTURE_FILE emitted invalid JSON"
    fi
  else
    if [[ -z "${RW_JOB_DESTINATION_ID:-}" ]]; then
      diagnostic='RW_JOB_DESTINATION_ID is required when no explicit job fixture is configured; implicit destinations are forbidden'
    else
      local payload_file
      payload_file="$(mktemp "${TMPDIR:-/tmp}/velox-job-payload.XXXXXX")"
      if ! python3 "${RW_CERT_CONFIG_DIR}/../../tests/worker-cert/build_real_payload.py" \
        --fixtures "$RW_JOB_FIXTURES_FILE" --worker-id "$WORKER_ID" \
        --placement-pin-worker-id "$WORKER_ID" \
        --destination "$RW_JOB_DESTINATION_ID" --scenes-count "$RW_JOB_SCENES_COUNT" \
        --duration-per-scene "$RW_JOB_DURATION_PER_SCENE" --strict --output "$payload_file" >/dev/null 2>&1; then
        diagnostic="canonical job payload builder failed"
      else
        payload="$(jq --arg key "remote-worker-${WORKER_ID}-$(date +%s%N)" '.idempotency_key=$key' "$payload_file" 2>/dev/null || true)"
        [[ -n "$payload" ]] || diagnostic="canonical job payload builder emitted invalid JSON"
      fi
      rm -f -- "$payload_file"
    fi
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_job_record P02-payload payload FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_job_record P02-payload payload PASS "canonical job payload ready; source=$(if [[ -n \"${TEST_JOB_JSON:-}\" ]]; then printf '%s' TEST_JOB_JSON; else printf '%s' build_real_payload.py; fi)" "$elapsed"
  fi

  expected_submit_status="${RW_JOB_EXPECTED_SUBMIT_STATUS:-202}"
  started="$(rw_now_s)"
  if [[ "$overall" == "PASS" ]] && ! rw_job_request POST "/api/v1/jobs" "$payload" "$M2M_TOKEN"; then
    diagnostic="POST /api/v1/jobs transport failed (rc=${RW_JOB_CURL_RC})"
  elif [[ "$overall" == "PASS" && "$RW_JOB_HTTP_STATUS" != "$expected_submit_status" ]]; then
    diagnostic="POST /api/v1/jobs returned HTTP ${RW_JOB_HTTP_STATUS}; expected ${expected_submit_status}: ${RW_JOB_BODY}"
  elif [[ "$overall" == "PASS" && "$expected_submit_status" == "422" ]]; then
    finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
    if ! jq -e '(.error // .code // .message // .details) != null' >/dev/null 2>&1 <<<"$RW_JOB_BODY"; then
      diagnostic='HTTP 422 response did not contain a validation error envelope'
    else
      rw_job_record P02-submit submit PASS 'HTTP 422; invalid fixture rejected by intake as expected' "$elapsed" intake_422
      jq -n --arg schema 'velox.remote_worker.job.v1' --arg worker_id "$WORKER_ID" \
        --arg fixture "${fixture_file:-${TEST_JOB_JSON:-generated}}" --arg overall "$overall" \
        --argjson checks "$(printf '%s\n' "${RW_JOB_RESULTS[@]}" | jq -s '.')" \
        '{schema:$schema,worker_id:$worker_id,fixture:$fixture,job_id:null,terminal_status:null,artifact_id:null,checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
      return 0
    fi
  elif [[ "$overall" == "PASS" ]]; then
    job_id="$(jq -r '.job_id // empty' <<<"$RW_JOB_BODY" 2>/dev/null || true)"
    status_url="$(jq -r '.status_url // empty' <<<"$RW_JOB_BODY" 2>/dev/null || true)"
    [[ -n "$job_id" ]] || diagnostic='202 response omitted job_id'
    [[ "$status_url" == "/api/v1/jobs/${job_id}" ]] || diagnostic="202 response status_url mismatch: ${status_url:-<empty>}"
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_job_record P02-submit submit FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_job_record P02-submit submit PASS "HTTP ${expected_submit_status}; job_id=${job_id}; status_url=${status_url}" "$elapsed"
  fi

  if [[ "$overall" == "PASS" && "$RW_JOB_VERIFY_PRE_READY" == "1" && "$RW_JOB_PRE_READY_REQUIRED" == "1" ]]; then
    download_url="$(rw_job_artifact_download_url "${RW_JOB_ARTIFACT_ID:-}")"
    if [[ -n "$download_url" ]]; then
      local pre_ready_file
      pre_ready_file="$(mktemp "${TMPDIR:-/tmp}/velox-pre-ready.XXXXXX")"
      rw_job_download_to_file "$download_url" "$pre_ready_file" "$RW_ADMIN_TOKEN" || true
      if [[ "$RW_JOB_DOWNLOAD_HTTP_STATUS" == "404" ]]; then
        rw_job_record P03-pre-ready pre_ready_rejection PASS 'artifact download returned HTTP 404 before READY' 0 pre_ready_404
      else
        rw_job_record P03-pre-ready pre_ready_rejection FAIL "expected HTTP 404 before READY, got ${RW_JOB_DOWNLOAD_HTTP_STATUS:-<no-status>}" 0
        overall="FAIL"
      fi
      rm -f -- "$pre_ready_file"
    else
      rw_job_record P03-pre-ready pre_ready_rejection FAIL 'artifact_id/download URL is not observable at 202; configure RW_JOB_ARTIFACT_ID or RW_JOB_ARTIFACT_DOWNLOAD_URL for the required pre-READY 404 probe' 0 api_observability_limit
      overall="FAIL"
    fi
  fi

  if [[ "$overall" == "PASS" ]]; then
    deadline=$(( $(date +%s) + CERT_POLL_TIMEOUT_S ))
    while (( $(date +%s) < deadline )); do
      if ! rw_job_request GET "/api/v1/jobs/${job_id}" "" "$M2M_TOKEN"; then
        diagnostic="GET /api/v1/jobs/${job_id} transport failed (rc=${RW_JOB_CURL_RC})"
        break
      fi
      if [[ "$RW_JOB_HTTP_STATUS" == "200" ]]; then
        body="$RW_JOB_BODY"
        status="$(jq -r '.status // empty' <<<"$body" 2>/dev/null || true)"
        [[ -n "$status" ]] && statuses+=("$status")
        if [[ "$(jq -r '.job_id // empty' <<<"$body" 2>/dev/null || true)" != "$job_id" ]]; then
          diagnostic="GET /api/v1/jobs/${job_id} returned a mismatched job_id"
          break
        fi
        poll_status_url="$(jq -r '.status_url // empty' <<<"$body" 2>/dev/null || true)"
        if [[ -n "$poll_status_url" && "$poll_status_url" != "/api/v1/jobs/${job_id}" ]]; then
          diagnostic="GET /api/v1/jobs/${job_id} returned a mismatched status_url"
          break
        fi
        if [[ "$(jq -r '.created // false' <<<"$body" 2>/dev/null || true)" != "true" ]]; then
          diagnostic="GET /api/v1/jobs/${job_id} returned created=false"
          break
        fi
        case "$status" in
          SUCCEEDED|FAILED|CANCELLED)
            terminal_status="$status"
            break
            ;;
          PENDING|READY|LEASED|RUNNING|AWAITING_ARTIFACT|RETRY_WAIT|POLLING|FORWARDING|FORWARDED|QUEUED)
            sleep "$RW_JOB_POLL_INTERVAL_S"
            ;;
          *)
            diagnostic="GET /api/v1/jobs/${job_id} returned unexpected status: ${status:-<empty>}"
            break
            ;;
        esac
      elif [[ "$RW_JOB_HTTP_STATUS" == "404" ]]; then
        sleep "$RW_JOB_POLL_INTERVAL_S"
      else
        diagnostic="GET /api/v1/jobs/${job_id} returned HTTP ${RW_JOB_HTTP_STATUS}: ${RW_JOB_BODY}"
        break
      fi
    done
    [[ -n "$terminal_status" ]] || [[ -n "$diagnostic" ]] || diagnostic="job polling timed out after ${CERT_POLL_TIMEOUT_S}s"
    [[ "$terminal_status" == "SUCCEEDED" ]] || [[ -n "$diagnostic" ]] || diagnostic="job reached terminal status ${terminal_status}"
  fi
  started="$(rw_now_s)"; finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  sequence="$(printf '%s\n' "${statuses[@]}")"
  if [[ -z "$diagnostic" ]]; then
    if ! state_error="$(rw_job_lifecycle_monotonic_ok "$sequence" 2>&1)"; then
      diagnostic="$state_error"
    elif ! state_error="$(rw_job_required_states_ok "$sequence" 2>&1)"; then
      diagnostic="$state_error"
    fi
  fi
  if [[ -n "$diagnostic" ]]; then
    rw_job_record P02-poll poll FAIL "${diagnostic}; states=${sequence//$'\n'/ -> }" "$elapsed"
    overall="FAIL"
  else
    rw_job_record P02-poll poll PASS "states=${sequence//$'\n'/ -> }; required=${RW_JOB_REQUIRED_STATES:-PENDING,LEASED,RUNNING,AWAITING_ARTIFACT,SUCCEEDED}; terminal=SUCCEEDED" "$elapsed"
  fi

  response_artifact_id="$(jq -r '.artifact_id // .artifact.id // empty' <<<"${body:-{}}" 2>/dev/null || true)"
  artifact_id="$response_artifact_id"
  artifact_url="$(jq -r '.artifact_url // empty' <<<"${body:-{}}" 2>/dev/null || true)"
  [[ -n "$artifact_id" ]] || artifact_id="$(rw_job_artifact_id_from_url "$artifact_url")"
  configured_artifact_id="${RW_JOB_ARTIFACT_ID:-}"
  if [[ -z "$artifact_id" && ( -n "$configured_artifact_id" || -n "${RW_JOB_ARTIFACT_DOWNLOAD_URL:-}" ) ]]; then
    diagnostic="configured artifact cannot be correlated to submitted job: polling response omitted artifact_id and canonical artifact URL"
    overall="FAIL"
  fi
  configured_url_id="$(rw_job_artifact_id_from_url "${RW_JOB_ARTIFACT_DOWNLOAD_URL:-}")"
  if [[ -n "$configured_artifact_id" && -n "$configured_url_id" && "$configured_artifact_id" != "$configured_url_id" ]]; then
    diagnostic="configured artifact ID ${configured_artifact_id} does not match configured download URL artifact ID ${configured_url_id}"
    overall="FAIL"
  elif [[ -n "$artifact_id" && -n "$configured_artifact_id" && "$configured_artifact_id" != "$artifact_id" ]]; then
    diagnostic="configured artifact ID ${configured_artifact_id} does not match submitted job artifact ID ${artifact_id}"
    overall="FAIL"
  elif [[ -n "$artifact_id" && -n "$configured_url_id" && "$configured_url_id" != "$artifact_id" ]]; then
    diagnostic="configured download URL artifact ID ${configured_url_id} does not match submitted job artifact ID ${artifact_id}"
    overall="FAIL"
  fi
  artifact_size="$(jq -r '.artifact_size_bytes // .artifact.size_bytes // 0' <<<"${body:-{}}" 2>/dev/null || printf '0')"
  expected_sha="${RW_JOB_EXPECTED_SHA256:-$(jq -r '.sha256 // .artifact.sha256 // empty' <<<"${body:-{}}" 2>/dev/null || true)}"
  [[ -n "$artifact_id" ]] || artifact_id="$(rw_job_artifact_id_from_url "$artifact_url")"
  download_url="$(rw_job_artifact_download_url "${RW_JOB_ARTIFACT_ID:-${artifact_id}}")"
  [[ -n "$download_url" ]] || {
    if [[ -n "$artifact_url" ]]; then
      if [[ "$artifact_url" == /* ]]; then
        download_url="${MASTER_URL}${artifact_url}"
      elif [[ "$artifact_url" == "${MASTER_URL}"/api/internal/artifacts/*/download* ]]; then
        download_url="$artifact_url"
      fi
    fi
  }

  if [[ "$terminal_status" == "SUCCEEDED" && "$overall" == "PASS" ]]; then
    [[ -n "$download_url" ]] || diagnostic='job status did not expose artifact_id or a usable artifact download URL; set RW_JOB_ARTIFACT_ID or RW_JOB_ARTIFACT_DOWNLOAD_URL'
    artifact_file="$(mktemp -p "$RW_JOB_DOWNLOAD_DIR" "remote-worker-${job_id}.XXXXXX.mp4")"
    if [[ -z "$diagnostic" ]] && ! rw_job_download_to_file "$download_url" "$artifact_file" "$RW_ADMIN_TOKEN"; then
      diagnostic="artifact download transport failed (rc=${RW_JOB_DOWNLOAD_CURL_RC})"
    elif [[ -z "$diagnostic" && "$RW_JOB_DOWNLOAD_HTTP_STATUS" != "200" ]]; then
      diagnostic="artifact download returned HTTP ${RW_JOB_DOWNLOAD_HTTP_STATUS}; expected 200 for READY"
    elif [[ -z "$diagnostic" && ! -s "$artifact_file" ]]; then
      diagnostic='artifact download returned HTTP 200 but an empty file'
    fi
    if [[ -z "$diagnostic" ]]; then
      final_sha="$(sha256sum "$artifact_file" | awk '{print $1}')"
      if [[ -n "$expected_sha" && "$final_sha" != "$expected_sha" ]]; then
        diagnostic="artifact SHA-256 mismatch: got=${final_sha} expected=${expected_sha}"
      elif [[ "$RW_JOB_VERIFY_SHA256" == "1" && ! "$final_sha" =~ ^[a-f0-9]{64}$ ]]; then
        diagnostic="artifact SHA-256 has invalid format: ${final_sha}"
      fi
    fi
    if [[ -z "$diagnostic" && "$RW_JOB_VERIFY_FFPROBE" == "1" ]]; then
      verifier_report="$(mktemp "${TMPDIR:-/tmp}/velox-artifact-report.XXXXXX.json")"
      verifier_log="$(mktemp "${TMPDIR:-/tmp}/velox-artifact-verifier.XXXXXX.log")"
      verifier_rc=0
      "${RW_CERT_CONFIG_DIR}/../../tests/worker-cert/verify_artifact.sh" "$artifact_file" \
        --report-json "$verifier_report" >"$verifier_log" 2>&1 || verifier_rc=$?
      if (( verifier_rc != 0 )); then
        diagnostic="canonical artifact verifier failed (rc=${verifier_rc})"
      else
        probe_duration="$(jq -r '.duration_seconds // empty' "$verifier_report" 2>/dev/null || true)"
        probe_size="$(jq -r '.bytes // empty' "$verifier_report" 2>/dev/null || true)"
        [[ -n "$probe_duration" && -n "$probe_size" ]] || diagnostic='canonical artifact verifier returned incomplete ffprobe report'
      fi
      rw_record_artifact_ffprobe "$([[ "$diagnostic" == "" ]] && printf PASS || printf FAIL)" "${artifact_file:-}" "${final_sha:-}" "${verifier_report:-}" "${diagnostic:-}"
      rm -f -- "$verifier_report" "$verifier_log"
    fi
    if [[ -n "$artifact_size" && "$artifact_size" =~ ^[0-9]+$ && "$artifact_size" -gt 0 && -z "$diagnostic" ]]; then
      [[ "$(stat -c %s "$artifact_file" 2>/dev/null || wc -c <"$artifact_file")" == "$artifact_size" ]] || diagnostic="artifact byte size mismatch: downloaded=$(stat -c %s "$artifact_file" 2>/dev/null || wc -c <"$artifact_file") expected=${artifact_size}"
    fi
    if [[ -n "$diagnostic" ]]; then
      rw_job_record P03-artifact artifact FAIL "$diagnostic" 0 artifact_download
      overall="FAIL"
    else
      rw_job_record P03-artifact artifact PASS "HTTP 200 READY; bytes=$(stat -c %s "$artifact_file" 2>/dev/null || wc -c <"$artifact_file"); sha256=${final_sha}; ffprobe_duration=${probe_duration:-not_run}" 0 artifact_download
      if [[ ! -s "${RW_ARTIFACT_DIR:-}"/artifact-ffprobe.json || "$(jq -r '.status // ""' "${RW_ARTIFACT_DIR}/artifact-ffprobe.json" 2>/dev/null || true)" == NOT_RUN ]]; then
        rw_record_artifact_ffprobe PASS "$artifact_file" "$final_sha" "${verifier_report:-}" ""
      fi
    fi
    rm -f -- "$artifact_file"
  else
    rw_job_record P03-artifact artifact FAIL "artifact verification not attempted because job did not reach SUCCEEDED" 0 artifact_download
    overall="FAIL"
  fi
  if [[ "$(jq -r '.status // "NOT_RUN"' "${RW_ARTIFACT_DIR:-}/artifact-ffprobe.json" 2>/dev/null || printf 'NOT_RUN')" == "NOT_RUN" ]]; then
    rw_record_artifact_ffprobe "$([[ "$overall" == "PASS" ]] && printf PASS || printf FAIL)" "${artifact_file:-}" "${final_sha:-}" "${verifier_report:-}" "${diagnostic:-artifact verification not completed}"
  fi

  jq -n --arg schema 'velox.remote_worker.job.v1' --arg worker_id "$WORKER_ID" \
    --arg job_id "${job_id:-}" --arg terminal_status "${terminal_status:-}" \
    --arg artifact_id "${RW_JOB_ARTIFACT_ID:-${artifact_id:-}}" \
    --arg artifact_url "${download_url:-}" --arg overall "$overall" \
    --argjson checks "$(printf '%s\n' "${RW_JOB_RESULTS[@]}" | jq -s '.')" \
    '{schema:$schema,worker_id:$worker_id,job_id:$job_id,terminal_status:(if $terminal_status=="" then null else $terminal_status end),artifact_id:(if $artifact_id=="" then null else $artifact_id end),artifact_download_url:(if $artifact_url=="" then null else $artifact_url end),checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
  [[ "$overall" == "PASS" ]]
}

rw_job_config_failure() {
  local diagnostic="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n --arg worker_id "${WORKER_ID:-${VELOX_WORKER_ID:-}}" --arg diagnostic "$diagnostic" \
      '{schema:"velox.remote_worker.job.v1",worker_id:$worker_id,checks:[{id:"P02-W00",name:"configuration",status:"FAIL",elapsed_ms:0,diagnostic:$diagnostic}],overall:"FAIL",generated_at:(now|todateiso8601)}'
  else
    printf '%s\n' '{"schema":"velox.remote_worker.job.v1","checks":[{"id":"P02-W00","name":"configuration","status":"FAIL","elapsed_ms":0,"diagnostic":"configuration validation failed"}],"overall":"FAIL"}'
  fi
}

rw_smoke_config_failure() {
  local diagnostic="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n \
      --arg worker_id "${WORKER_ID:-${VELOX_WORKER_ID:-}}" \
      --arg diagnostic "$diagnostic" \
      --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      '{schema:"velox.remote_worker.smoke.v1",worker_id:$worker_id,checks:[{id:"P01-W00",name:"configuration",status:"FAIL",elapsed_ms:0,diagnostic:$diagnostic}],overall:"FAIL",generated_at:$generated_at}'
  else
    printf '%s
' '{"schema":"velox.remote_worker.smoke.v1","checks":[{"id":"P01-W00","name":"configuration","status":"FAIL","elapsed_ms":0,"diagnostic":"configuration validation failed"}],"overall":"FAIL"}'
  fi
}

rw_worker_config_failure() {
  local diagnostic="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n \
      --arg worker_id "${WORKER_ID:-${VELOX_WORKER_ID:-}}" \
      --arg diagnostic "$diagnostic" \
      --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      '{schema:"velox.remote_worker.worker.v1",worker_id:$worker_id,checks:[{id:"W00",name:"configuration",status:"FAIL",elapsed_ms:0,diagnostic:$diagnostic}],overall:"FAIL",generated_at:$generated_at}'
  else
    printf '%s\n' '{"schema":"velox.remote_worker.worker.v1","checks":[{"id":"W00","name":"configuration","status":"FAIL","elapsed_ms":0,"diagnostic":"configuration validation failed"}],"overall":"FAIL"}'
  fi
}

