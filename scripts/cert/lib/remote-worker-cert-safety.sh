# remote-worker-cert-safety.sh — extracted worker certification lifecycle domain.
# Loaded by scripts/cert/lib/remote-worker-cert-lifecycle.sh.
# shellcheck shell=bash

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
