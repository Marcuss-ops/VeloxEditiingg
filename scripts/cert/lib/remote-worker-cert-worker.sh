# remote-worker-cert-worker.sh — worker registration and heartbeat checks.
# Loaded by scripts/cert/remote-worker-cert-config.sh.

rw_worker_record() {
  local id="$1" name="$2" status="$3" diagnostic="$4" elapsed_ms="$5"
  RW_WORKER_RESULTS+=("$(jq -cn \
    --arg id "$id" \
    --arg name "$name" \
    --arg status "$status" \
    --arg diagnostic "$diagnostic" \
    --argjson elapsed_ms "$elapsed_ms" \
    '{id:$id,name:$name,status:$status,elapsed_ms:$elapsed_ms,diagnostic:$diagnostic}')")
}

rw_worker_admin_get() {
  local path="$1" body
  rw_log_command "GET ${path}"
  if body="$(admin_api GET "$path" --max-time "$RW_WORKER_HTTP_TIMEOUT_S")"; then
    rw_record_operation GET "$path" 200 "$body"
    if [[ "$path" == "/api/v1/workers/${WORKER_ID}" ]]; then
      rw_snapshot_json worker "$body"
    fi
    printf '%s' "$body"
  else
    rw_record_operation GET "$path" "${RW_LAST_HTTP_STATUS:-000}" "${RW_LAST_BODY:-}"
    return 1
  fi
}

rw_worker_active_session_count() {
  local body="$1"
  jq -er '[.sessions[]? | select((.status // "") == "ACTIVE" and ((.revoked // false) == false) and ((.session_type // "control") == "control"))] | length' <<<"$body"
}

rw_worker_release_diagnostic() {
  local body="$1" missing="" field value
  for field in image_digest source_commit source_hash bundle_hash engine_sha256 software_version protocol_version capability_schema; do
    value="$(jq -r --arg f "$field" '.release_identity[$f] // empty' <<<"$body" 2>/dev/null || true)"
    [[ -n "$value" && "$value" != "null" ]] || missing="${missing}${missing:+,}${field}"
  done
  if [[ -n "$missing" ]]; then
    printf 'missing ReleaseIdentity fields: %s' "$missing"
    return 1
  fi
  if ! jq -e '
    (.release_identity.capability_schema | type == "number" and . > 0) and
    (.release_identity.image_digest | type == "string" and test("^sha256:[0-9a-f]{64}$")) and
    (.release_identity.source_commit | type == "string" and length > 0) and
    (.release_identity.source_hash | type == "string" and test("^[0-9a-f]{64}$")) and
    (.release_identity.bundle_hash | type == "string" and test("^[0-9a-f]{64}$")) and
    (.release_identity.engine_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    (.release_identity.software_version | type == "string" and length > 0) and
    (.release_identity.protocol_version | type == "string" and length > 0)
  ' <<<"$body" >/dev/null 2>&1; then
    printf 'ReleaseIdentity contains invalid field types or values'
    return 1
  fi
  return 0
}

rw_worker_snapshot_ok() {
  local body="$1" expected_id="$2" active_count
  [[ "$(jq -r '.worker_id // empty' <<<"$body")" == "$expected_id" ]] || {
    printf 'worker_id mismatch (expected %s)' "$expected_id"
    return 1
  }
  [[ "$(jq -r '.status // empty' <<<"$body")" == "CONNECTED" ]] || {
    printf 'status=%s (expected CONNECTED)' "$(jq -r '.status // empty' <<<"$body")"
    return 1
  }
  [[ "$(jq -r '.session_active // false' <<<"$body")" == "true" ]] || {
    printf 'session_active=false'
    return 1
  }
  [[ -n "$(jq -r '.last_heartbeat_at // empty' <<<"$body")" ]] || {
    printf 'last_heartbeat_at is empty'
    return 1
  }
  rw_worker_release_diagnostic "$body" || return 1
  local heartbeat_age
  heartbeat_age="$(jq -r '.heartbeat_age_seconds // -1' <<<"$body" 2>/dev/null || printf '%s' '-1')"
  [[ "$heartbeat_age" =~ ^[0-9]+$ && "$heartbeat_age" -le "${RW_HEARTBEAT_MAX_AGE_S:-30}" ]] || {
    printf 'heartbeat_age_seconds=%s (maximum %ss)' "$heartbeat_age" "${RW_HEARTBEAT_MAX_AGE_S:-30}"
    return 1
  }
  active_count="$(jq -r '[.executors[]?] | length' <<<"$body" 2>/dev/null || printf '0')"
  (( active_count > 0 )) || {
    printf 'no executors advertised'
    return 1
  }
  [[ "$(jq -r '(.max_slots // .task_slots // 0) | tonumber' <<<"$body" 2>/dev/null || printf '0')" -gt 0 ]] || {
    printf 'max/task slots is not positive'
    return 1
  }
}

rw_worker_fleet_diagnostic() {
  local body="$1" expected_id="${2:-$WORKER_ID}" duplicates target_count
  duplicates="$(jq -c '[.workers[]?.worker_id] | group_by(.) | map(select(length > 1) | .[0])' <<<"$body" 2>/dev/null || printf '[]')"
  [[ "$duplicates" == "[]" ]] || {
    printf 'duplicate WorkerID values in fleet response: %s' "$duplicates"
    return 1
  }
  target_count="$(jq -r --arg id "$expected_id" '[.workers[]? | select(.worker_id == $id)] | length' <<<"$body" 2>/dev/null || printf '0')"
  [[ "$target_count" == "1" ]] || {
    printf 'expected WorkerID %s appears %s times in fleet response' "$expected_id" "$target_count"
    return 1
  }
  return 0
}

rw_worker_time_parser_available() {
  date -u -d '1970-01-01T00:00:00Z' +%s >/dev/null 2>&1 || command -v python3 >/dev/null 2>&1
}

rw_worker_heartbeat_epoch() {
  local timestamp="$1" epoch
  # API contract is RFC3339. Prefer GNU date for offsets/fractions; use
  # Python's stdlib on BSD/macOS, then jq for canonical UTC as a last resort.
  epoch="$(date -u -d "$timestamp" +%s 2>/dev/null || true)"
  if [[ "$epoch" =~ ^[0-9]+$ ]]; then
    printf '%s' "$epoch"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$timestamp" <<'PY'
import datetime
import sys

value = sys.argv[1].replace("Z", "+00:00")
parsed = datetime.datetime.fromisoformat(value)
if parsed.tzinfo is None:
    raise SystemExit(1)
print(int(parsed.timestamp()))
PY
    return $?
  fi
  jq -er 'fromdateiso8601' <<<"$timestamp" 2>/dev/null
}

rw_worker_restart_once() {
  local restart_cmd="${RW_WORKER_RESTART_CMD:-sudo systemctl restart velox-worker.service}"
  timeout "${RW_WORKER_RESTART_TIMEOUT_S}s" ssh \
    -o BatchMode=yes \
    -o ConnectTimeout="${RW_SSH_CONNECT_TIMEOUT_S}" \
    "${WORKER_SSH_USER}@${WORKER_SSH_HOST}" "$restart_cmd"
}

rw_worker_poll_connected() {
  local deadline=$(( $(date +%s) + RW_WORKER_RECONNECT_TIMEOUT_S )) body="" detail=""
  while (( $(date +%s) < deadline )); do
    body="$(rw_worker_admin_get "/api/v1/workers/${WORKER_ID}" 2>/dev/null || true)"
    if [[ -n "$body" ]] && rw_worker_snapshot_ok "$body" "$WORKER_ID" >/dev/null 2>&1; then
      printf '%s' "$body"
      return 0
    fi
    sleep "$RW_WORKER_POLL_INTERVAL_S"
  done
  detail="$(rw_worker_snapshot_ok "$body" "$WORKER_ID" 2>&1 || true)"
  printf 'reconnect timeout after %ss: %s' "$RW_WORKER_RECONNECT_TIMEOUT_S" "${detail:-no worker response}"
  return 1
}

rw_worker_checks() {
  local started finished elapsed body fleet sessions active_count
  local status overall="PASS" diagnostic i previous_hb="" current_hb="" age
  local previous_hb_epoch="" current_hb_epoch=""
  local -a RW_WORKER_RESULTS=()
  local -a observed_ids=()
  local restart_count="${RW_WORKER_RESTARTS:-5}"

  for bin in jq ssh timeout; do
    command -v "$bin" >/dev/null 2>&1 || {
      rw_worker_record W00 prerequisites FAIL "missing local prerequisite: ${bin}" 0
      overall="FAIL"
      jq -n --arg worker_id "${WORKER_ID:-}" --arg overall "$overall" --argjson checks "$(printf '%s\n' "${RW_WORKER_RESULTS[@]}" | jq -s '.')" \
        '{schema:"velox.remote_worker.worker.v1",worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
      return 2
    }
  done
  if ! rw_worker_time_parser_available; then
    rw_worker_record W00 prerequisites FAIL 'RFC3339 timestamp parser unavailable (requires GNU date -d or python3)' 0
    overall="FAIL"
    jq -n --arg worker_id "${WORKER_ID:-}" --arg overall "$overall" --argjson checks "$(printf '%s\n' "${RW_WORKER_RESULTS[@]}" | jq -s '.')" \
      '{schema:"velox.remote_worker.worker.v1",worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
    return 2
  fi
  [[ -n "${RW_ADMIN_TOKEN:-}" ]] || {
    rw_worker_record W00 prerequisites FAIL 'admin token is not configured; set VELOX_ADMIN_TOKEN or TOKEN_FILE' 0
    overall="FAIL"
  }
  [[ "$restart_count" =~ ^[1-9][0-9]*$ ]] || {
    rw_worker_record W00 prerequisites FAIL "RW_WORKER_RESTARTS must be a positive integer" 0
    overall="FAIL"
  }
  [[ "$overall" == "PASS" ]] || {
    jq -n --arg worker_id "${WORKER_ID:-}" --arg overall "$overall" --argjson checks "$(printf '%s\n' "${RW_WORKER_RESULTS[@]}" | jq -s '.')" \
      '{schema:"velox.remote_worker.worker.v1",worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
    return 2
  }

  # W01 — initial registration/readiness and complete ReleaseIdentity.
  started="$(rw_now_s)"
  body="$(rw_worker_admin_get "/api/v1/workers/${WORKER_ID}" 2>/dev/null || true)"
  fleet="$(rw_worker_admin_get "/api/v1/workers" 2>/dev/null || true)"
  sessions="$(rw_worker_admin_get "/api/v1/workers/${WORKER_ID}/sessions?include_revoked=true" 2>/dev/null || true)"
  if [[ -z "$body" || -z "$fleet" || -z "$sessions" ]]; then
    rw_worker_record W01 registration FAIL 'worker, fleet, or sessions API request failed' 0
    overall="FAIL"
  else
    diagnostic=""
    rw_worker_snapshot_ok "$body" "$WORKER_ID" || diagnostic="$(rw_worker_snapshot_ok "$body" "$WORKER_ID" 2>&1)"
    active_count="$(rw_worker_active_session_count "$sessions" 2>/dev/null || printf '0')"
    [[ "$active_count" == "1" ]] || diagnostic="${diagnostic}${diagnostic:+; }active control sessions=${active_count} (expected 1)"
    rw_worker_fleet_diagnostic "$fleet" "$WORKER_ID" || diagnostic="${diagnostic}${diagnostic:+; }$(rw_worker_fleet_diagnostic "$fleet" "$WORKER_ID" 2>&1)"
    finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
    if [[ -z "$diagnostic" ]]; then
      rw_worker_record W01 registration PASS "registered, ReleaseIdentity complete, active_sessions=1" "$elapsed"
    else
      rw_worker_record W01 registration FAIL "$diagnostic" "$elapsed"
      overall="FAIL"
    fi
  fi

  # W02 — restart the worker repeatedly; identity and active-session uniqueness
  # must survive every reconnect. The restart command executes on the worker.
  started="$(rw_now_s)"
  diagnostic=""
  for (( i=1; i<=restart_count; i++ )); do
    if ! rw_worker_restart_once >/dev/null 2>&1; then
      diagnostic="restart ${i}/${restart_count} command failed"
      break
    fi
    body="$(rw_worker_poll_connected 2>&1)" || {
      diagnostic="${diagnostic}${diagnostic:+; }restart ${i}/${restart_count}: ${body}"
      break
    }
    current_hb="$(jq -r '.last_heartbeat_at // empty' <<<"$body")"
    observed_ids+=("$(jq -r '.worker_id' <<<"$body")")
    sessions="$(rw_worker_admin_get "/api/v1/workers/${WORKER_ID}/sessions?include_revoked=true" 2>/dev/null || true)"
    active_count="$(rw_worker_active_session_count "$sessions" 2>/dev/null || printf '0')"
    [[ "$active_count" == "1" ]] || diagnostic="${diagnostic}${diagnostic:+; }restart ${i}: active sessions=${active_count}"
    [[ "$current_hb" != "" ]] || diagnostic="${diagnostic}${diagnostic:+; }restart ${i}: heartbeat missing"
    fleet="$(rw_worker_admin_get "/api/v1/workers" 2>/dev/null || true)"
    if [[ -z "$fleet" ]]; then
      diagnostic="${diagnostic}${diagnostic:+; }restart ${i}: fleet API request failed"
    else
      local fleet_diagnostic=""
      if ! fleet_diagnostic="$(rw_worker_fleet_diagnostic "$fleet" "$WORKER_ID" 2>&1)"; then
        diagnostic="${diagnostic}${diagnostic:+; }restart ${i}: ${fleet_diagnostic}"
      fi
    fi
  done
  if (( ${#observed_ids[@]} > 0 )); then
    local distinct_ids
    distinct_ids="$(printf '%s\n' "${observed_ids[@]}" | sort -u | wc -l | tr -d ' ' )"
    [[ "$distinct_ids" == "1" ]] || diagnostic="${diagnostic}${diagnostic:+; }WorkerID changed across restarts: ${observed_ids[*]}"
    [[ "$(printf '%s\n' "${observed_ids[@]}" | grep -cxF "$WORKER_ID")" == "$(( ${#observed_ids[@]} ))" ]] || diagnostic="${diagnostic}${diagnostic:+; }unexpected WorkerID observed"
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -z "$diagnostic" && ${#observed_ids[@]} -eq "$restart_count" ]]; then
    rw_worker_record W02 identity_stable PASS "${restart_count} restarts; one WorkerID and one active session per reconnect" "$elapsed"
  else
    rw_worker_record W02 identity_stable FAIL "${diagnostic:-restart sequence incomplete}" "$elapsed"
    overall="FAIL"
  fi

  # W03 — three heartbeat reads: timestamp advances, age stays within budget,
  # connection remains CONNECTED and no duplicate active session appears.
  started="$(rw_now_s)"
  diagnostic=""
  for (( i=1; i<=RW_HEARTBEAT_SAMPLES; i++ )); do
    body="$(rw_worker_admin_get "/api/v1/workers/${WORKER_ID}" 2>/dev/null || true)"
    current_hb="$(jq -r '.last_heartbeat_at // empty' <<<"$body" 2>/dev/null || true)"
    age="$(jq -r '.heartbeat_age_seconds // -1' <<<"$body" 2>/dev/null || printf '%s' '-1')"
    [[ "$current_hb" != "" ]] || diagnostic="${diagnostic}${diagnostic:+; }sample ${i}: heartbeat missing"
    [[ "$age" =~ ^[0-9]+$ && "$age" -le "$RW_HEARTBEAT_MAX_AGE_S" ]] || diagnostic="${diagnostic}${diagnostic:+; }sample ${i}: heartbeat_age_seconds=${age}"
    [[ "$(jq -r '.status // empty' <<<"$body")" == "CONNECTED" ]] || diagnostic="${diagnostic}${diagnostic:+; }sample ${i}: status not CONNECTED"
    [[ "$(jq -r '.session_active // false' <<<"$body")" == "true" ]] || diagnostic="${diagnostic}${diagnostic:+; }sample ${i}: session_active=false"
    current_hb_epoch="$(rw_worker_heartbeat_epoch "$current_hb" 2>/dev/null || true)"
    if [[ -z "$current_hb_epoch" ]]; then
      diagnostic="${diagnostic}${diagnostic:+; }sample ${i}: last_heartbeat_at is not valid RFC3339"
    elif [[ -n "$previous_hb_epoch" ]] && (( current_hb_epoch <= previous_hb_epoch )); then
      diagnostic="${diagnostic}${diagnostic:+; }heartbeat did not advance: epoch ${previous_hb_epoch} -> ${current_hb_epoch}"
    fi
    previous_hb="$current_hb"
    previous_hb_epoch="$current_hb_epoch"
    if (( i < RW_HEARTBEAT_SAMPLES )); then sleep "$RW_HEARTBEAT_INTERVAL_S"; fi
  done
  sessions="$(rw_worker_admin_get "/api/v1/workers/${WORKER_ID}/sessions?include_revoked=true" 2>/dev/null || true)"
  active_count="$(rw_worker_active_session_count "$sessions" 2>/dev/null || printf '0')"
  [[ "$active_count" == "1" ]] || diagnostic="${diagnostic}${diagnostic:+; }heartbeat sample active sessions=${active_count}"
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -z "$diagnostic" ]]; then
    rw_worker_record W03 heartbeat PASS "${RW_HEARTBEAT_SAMPLES} samples advanced with age <= ${RW_HEARTBEAT_MAX_AGE_S}s" "$elapsed"
  else
    rw_worker_record W03 heartbeat FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  fi

  jq -n \
    --arg schema 'velox.remote_worker.worker.v1' \
    --arg worker_id "$WORKER_ID" \
    --arg overall "$overall" \
    --argjson checks "$(printf '%s\n' "${RW_WORKER_RESULTS[@]}" | jq -s '.')" \
    '{schema:$schema,worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
  [[ "$overall" == "PASS" ]]
}

