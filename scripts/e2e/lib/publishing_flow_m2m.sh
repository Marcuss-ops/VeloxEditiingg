#!/usr/bin/env bash
# Sourced M2M lifecycle helpers for publishing_flow_smoke.sh.

resolve_token() {
  local value=""
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
    value="$VELOX_ADMIN_TOKEN"
  elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
    value=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
      | sed 's/^[^=]*=//' \
      | tr -d '"' | tr -d "'" | xargs || true)
  fi
  if [[ -z "$value" ]]; then
    echo "FATAL: VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided / unreadable" >&2
    echo "  Remediation: export VELOX_ADMIN_TOKEN=... or set TOKEN_FILE=/path/to/.env" >&2
    return 1
  fi
  if [[ "$value" == *$'\r'* || "$value" == *$'\n'* ]]; then
    echo "FATAL: VELOX_ADMIN_TOKEN contains CR or LF; refusing to use it" >&2
    return 1
  fi
  printf '%s' "$value"
}

cleanup() {
  if [[ -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" ]]; then
    # Best-effort DELETE; never let cleanup mask the original exit code.
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      "${ADMIN_KEYS_URL}/${PROVISIONED_CLIENT_ID}" >/dev/null 2>&1 || true
  fi
}

post_admin_issue() {
  local rc=0
  curl -sS -m 15 -X POST -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-raw "$ISSUE_REQ" \
    "${ADMIN_KEYS_URL}" \
    -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || rc=$?
  return $rc
}
