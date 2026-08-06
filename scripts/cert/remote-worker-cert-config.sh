#!/usr/bin/env bash
# =============================================================================
# remote-worker-cert-config.sh — shared configuration for remote-worker certs.
#
# Source from a certification script:
#   source scripts/cert/remote-worker-cert-config.sh
#   rw_load_config
#   rw_remote_worker_preflight
#   admin_api GET "/api/v1/admin/workers/${WORKER_ID}"
#
# Direct execution performs only local validation and never contacts a master.
# Admin tokens follow the repository convention: the environment wins, then a
# single VELOX_ADMIN_TOKEN=... dotenv assignment in TOKEN_FILE is read without
# sourcing the file. Secret values are never printed.
# =============================================================================

rw_die() {
  printf 'remote-worker-cert: %s\n' "$*" >&2
  return 1
}

rw_require_bin() {
  local bin="$1"
  command -v "$bin" >/dev/null 2>&1 || rw_die "required binary not found in PATH: ${bin}"
}

rw_trim_trailing_slash() {
  local value="$1"
  while [[ "$value" == */ ]]; do value="${value%/}"; done
  printf '%s' "$value"
}

rw_validate_url() {
  local url="$1"
  if [[ "$url" == *'@'* || "$url" == *'?'* || "$url" == *'#'* ]]; then
    rw_die "MASTER_URL must not contain credentials, query parameters, or fragments"
    return 1
  fi
  if [[ ! "$url" =~ ^https?://[^[:space:]/:]+(:[0-9]+)?(/[^[:space:]]*)?$ ]]; then
    rw_die "MASTER_URL must be an absolute http(s) URL without whitespace"
    return 1
  fi
}

rw_validate_port() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || rw_die "${name} must be a numeric TCP port"
  (( value >= 1 && value <= 65535 )) || rw_die "${name} must be between 1 and 65535"
}

rw_validate_worker_id() {
  local worker_id="$1"
  [[ "$worker_id" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]] || \
    rw_die "WORKER_ID must be a non-empty path-safe worker identifier"
}

rw_validate_digest() {
  local name="$1" value="$2"
  [[ -z "$value" || "$value" =~ ^sha256:[a-f0-9]{64}$ ]] || \
    rw_die "${name} must be sha256:<64 lowercase hexadecimal characters>"
}

rw_read_token_file() {
  local file="$1" value perms group_read other_read
  [[ -f "$file" && -r "$file" ]] || {
    rw_die "TOKEN_FILE is missing or unreadable: ${file}"
    return 1
  }

  if [[ "$(uname -s)" != "Darwin" ]]; then
    perms="$(stat -c '%a' "$file" 2>/dev/null || printf '000')"
    group_read=$(( (10#$perms / 10) % 10 ))
    other_read=$(( 10#$perms % 10 ))
    (( group_read < 4 && other_read < 4 )) || {
      rw_die "TOKEN_FILE must not be group/world-readable (chmod 600): ${file}"
      return 1
    }
  fi

  # Read one dotenv assignment only; never source or evaluate the file.
  value="$(grep -E '^VELOX_ADMIN_TOKEN=' "$file" | head -n 1 \
    | sed 's/^VELOX_ADMIN_TOKEN=//' || true)"
  value="${value%$'\r'}"
  # Strip only a matching outer quote pair; preserve token contents exactly.
  if [[ "${#value}" -ge 2 && "${value:0:1}" == '"' && "${value: -1}" == '"' ]]; then
    value="${value:1:${#value}-2}"
  elif [[ "${#value}" -ge 2 && "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
    value="${value:1:${#value}-2}"
  fi
  [[ -n "$value" ]] || {
    rw_die "TOKEN_FILE does not contain a non-empty VELOX_ADMIN_TOKEN entry"
    return 1
  }
  printf '%s' "$value"
}

rw_resolve_admin_token() {
  local value="${VELOX_ADMIN_TOKEN:-}"
  if [[ -z "$value" && -n "${TOKEN_FILE:-}" ]]; then
    value="$(rw_read_token_file "$TOKEN_FILE")" || return 1
  fi
  [[ -n "$value" ]] || {
    rw_die "VELOX_ADMIN_TOKEN is unset; export it or set TOKEN_FILE"
    return 1
  }
  # These characters could split a header or inject a curl-config directive.
  [[ "$value" != *$'\r'* && "$value" != *$'\n'* && "$value" != *'"'* && "$value" != *'\\'* ]] || {
    rw_die "VELOX_ADMIN_TOKEN contains an unsafe control or config character"
    return 1
  }
  RW_ADMIN_TOKEN="$value"
  export RW_ADMIN_TOKEN
}

rw_load_config() {
  MASTER_URL="${MASTER_URL:-${VELOX_MASTER_URL:-}}"
  MASTER_URL="$(rw_trim_trailing_slash "$MASTER_URL")"
  MASTER_HOST="${MASTER_HOST:-${VELOX_MASTER_HOST:-}}"
  M2M_TOKEN="${M2M_TOKEN:-${VELOX_M2M_TOKEN:-}}"
  WORKER_ID="${WORKER_ID:-${VELOX_WORKER_ID:-}}"
  WORKER_SSH_HOST="${WORKER_SSH_HOST:-${VELOX_WORKER_SSH_HOST:-}}"
  WORKER_SSH_USER="${WORKER_SSH_USER:-${VELOX_WORKER_SSH_USER:-root}}"
  MASTER_REST_PORT="${MASTER_REST_PORT:-${VELOX_MASTER_REST_PORT:-8000}}"
  MASTER_GRPC_PORT="${MASTER_GRPC_PORT:-${VELOX_MASTER_GRPC_PORT:-9000}}"
  TEST_JOB_JSON="${TEST_JOB_JSON:-${VELOX_TEST_JOB_JSON:-}}"
  CERT_POLL_TIMEOUT_S="${CERT_POLL_TIMEOUT_S:-${VELOX_CERT_POLL_TIMEOUT_S:-300}}"

  [[ -n "$MASTER_URL" ]] || rw_die "MASTER_URL or VELOX_MASTER_URL is required" || return 1
  rw_validate_url "$MASTER_URL" || return 1
  [[ -n "$MASTER_HOST" ]] || rw_die "MASTER_HOST or VELOX_MASTER_HOST is required" || return 1
  [[ "$MASTER_HOST" != *[[:space:]/@?#\\]* ]] || rw_die "MASTER_HOST contains whitespace or URL/path delimiters" || return 1
  [[ -n "$WORKER_ID" ]] || rw_die "WORKER_ID or VELOX_WORKER_ID is required" || return 1
  rw_validate_worker_id "$WORKER_ID" || return 1
  [[ -n "$WORKER_SSH_HOST" ]] || rw_die "WORKER_SSH_HOST or VELOX_WORKER_SSH_HOST is required" || return 1
  [[ "$WORKER_SSH_HOST" != *[[:space:]/\\]* ]] || rw_die "WORKER_SSH_HOST contains whitespace or a path separator" || return 1
  [[ "$WORKER_SSH_USER" =~ ^[A-Za-z_][A-Za-z0-9_.-]*$ ]] || rw_die "WORKER_SSH_USER is not a valid SSH login name" || return 1
  rw_validate_port MASTER_REST_PORT "$MASTER_REST_PORT" || return 1
  rw_validate_port MASTER_GRPC_PORT "$MASTER_GRPC_PORT" || return 1
  [[ "$CERT_POLL_TIMEOUT_S" =~ ^[1-9][0-9]*$ ]] || rw_die "CERT_POLL_TIMEOUT_S must be a positive integer" || return 1

  rw_resolve_admin_token || return 1
  if [[ -n "$M2M_TOKEN" ]]; then
    [[ "$M2M_TOKEN" != *$'\r'* && "$M2M_TOKEN" != *$'\n'* && "$M2M_TOKEN" != *'"'* && "$M2M_TOKEN" != *'\\'* ]] || {
      rw_die "VELOX_M2M_TOKEN contains an unsafe control or config character"
      return 1
    }
  fi
  rw_validate_digest GOOD_DIGEST "${GOOD_DIGEST:-}" || return 1
  rw_validate_digest PREVIOUS_DIGEST "${PREVIOUS_DIGEST:-}" || return 1
  rw_validate_digest BAD_DIGEST "${BAD_DIGEST:-}" || return 1

  if [[ -n "$TEST_JOB_JSON" ]]; then
    [[ -f "$TEST_JOB_JSON" && -r "$TEST_JOB_JSON" ]] || {
      rw_die "TEST_JOB_JSON is missing or unreadable: ${TEST_JOB_JSON}"
      return 1
    }
  fi

  export MASTER_URL MASTER_HOST M2M_TOKEN WORKER_ID WORKER_SSH_HOST WORKER_SSH_USER
  export MASTER_REST_PORT MASTER_GRPC_PORT TEST_JOB_JSON CERT_POLL_TIMEOUT_S
}

rw_curl_config() {
  local cfg="$1"
  umask 077
  : >"$cfg" || return 1
  printf 'header = "Authorization: Bearer %s"\n' "$RW_ADMIN_TOKEN" >"$cfg"
  printf 'header = "Content-Type: application/json"\n' >>"$cfg"
  chmod 600 "$cfg"
}

admin_api() {
  local method="${1:-}" path="${2:-}" cfg rc arg
  local -a curl_args=()
  shift 2 || {
    rw_die "admin_api requires METHOD and PATH"
    return 2
  }
  [[ "$method" =~ ^(GET|POST|PUT|PATCH|DELETE|HEAD)$ ]] || {
    rw_die "admin_api method is not supported: ${method}"
    return 2
  }
  [[ "$path" == /* && "$path" != *[[:space:]]* ]] || {
    rw_die "admin_api path must be absolute and contain no whitespace"
    return 2
  }
  [[ -n "${RW_ADMIN_TOKEN:-}" && -n "${MASTER_URL:-}" ]] || {
    rw_die "call rw_load_config before admin_api"
    return 2
  }
  # Only permit request-body and timeout options. In particular, callers
  # cannot replace the URL, Authorization header, curl config, proxy, or
  # tracing mode supplied by this helper.
  while (( $# > 0 )); do
    arg="$1"
    case "$arg" in
      --data|--data-raw|--data-binary|--data-urlencode|--max-time|--connect-timeout|--retry|--retry-delay)
        (( $# >= 2 )) || { rw_die "${arg} requires a value"; return 2; }
        curl_args+=("$1" "$2")
        shift 2
        ;;
      *)
        rw_die "admin_api accepts only request-body and timeout options: ${arg}"
        return 2
        ;;
    esac
  done

  cfg="$(mktemp "${TMPDIR:-/tmp}/velox-admin-curl.XXXXXX")" || return 1
  rw_curl_config "$cfg" || { rm -f "$cfg"; return 1; }
  # Capture the concrete path in the trap so cleanup still works if curl is
  # interrupted. Restore the caller's traps before returning normally.
  local cleanup_cmd="rm -f -- $(printf '%q' "$cfg")"
  trap "$cleanup_cmd" EXIT INT TERM
  if curl --fail-with-body --silent --show-error --request "$method" \
    --config "$cfg" "${MASTER_URL}${path}" "${curl_args[@]}"; then
    rc=0
  else
    rc=$?
  fi
  trap - EXIT INT TERM
  rm -f -- "$cfg"
  return "$rc"
}

rw_remote_worker_preflight() {
  local bin
  for bin in curl jq ssh; do
    rw_require_bin "$bin" || return 1
  done
  [[ -n "${MASTER_URL:-}" && -n "${MASTER_HOST:-}" && -n "${WORKER_ID:-}" && -n "${WORKER_SSH_HOST:-}" && -n "${RW_ADMIN_TOKEN:-}" ]] || {
    rw_die "configuration is not loaded; run rw_load_config first"
    return 1
  }
  printf '%s\n' 'remote-worker-cert preflight: PASS'
  printf '%s\n' "  master_url: ${MASTER_URL}"
  printf '%s\n' "  master_host: ${MASTER_HOST}"
  printf '%s\n' "  worker_id: ${WORKER_ID}"
  printf '%s\n' "  ssh_target: ${WORKER_SSH_USER}@${WORKER_SSH_HOST}"
  printf '%s\n' '  admin_token: configured (redacted)'
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  rw_load_config
  rw_remote_worker_preflight
fi
