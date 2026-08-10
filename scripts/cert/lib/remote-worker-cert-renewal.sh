# remote-worker-cert-renewal.sh — extracted worker certification lifecycle domain.
# Loaded by scripts/cert/lib/remote-worker-cert-lifecycle.sh.
# shellcheck shell=bash
# shellcheck disable=SC2034

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
