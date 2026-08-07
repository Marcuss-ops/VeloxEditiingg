# remote-worker-cert-config.sh — configuration, validation, and admin HTTP helpers.
# Loaded by scripts/cert/remote-worker-cert-config.sh.

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

rw_validate_host() {
  local name="$1" host="$2"
  # The current SSH/grpc endpoint builders use host:port syntax. Reject
  # unbracketed IPv6 until a bracketed-host contract is introduced.
  [[ "$host" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || \
    rw_die "${name} must be a hostname or IPv4 address (IPv6 is not supported yet)"
}

rw_validate_digest() {
  local name="$1" value="$2"
  [[ -z "$value" || "$value" =~ ^sha256:[a-f0-9]{64}$ ]] || \
    rw_die "${name} must be sha256:<64 lowercase hexadecimal characters>"
}

rw_validate_restart_command() {
  local command="$1"
  # The command is intentionally operator-configurable, but it is passed as
  # one remote-shell string. Permit only simple executable/argument tokens;
  # reject shell operators, substitutions, redirects, and quoting so an env
  # typo cannot turn the restart probe into arbitrary shell execution.
  [[ "$command" =~ ^[A-Za-z0-9_./:-]+([[:space:]][A-Za-z0-9_./:-]+)*$ ]] || \
    rw_die "RW_WORKER_RESTART_CMD contains unsafe shell syntax"
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
  local require_admin=1
  [[ "${1:-}" == "--network-only" ]] && require_admin=0
  MASTER_URL="${MASTER_URL:-${VELOX_MASTER_URL:-}}"
  MASTER_URL="$(rw_trim_trailing_slash "$MASTER_URL")"
  MASTER_HOST="${MASTER_HOST:-${VELOX_MASTER_HOST:-}}"
  MASTER_EXPECTED_IP="${MASTER_EXPECTED_IP:-${VELOX_MASTER_EXPECTED_IP:-}}"
  M2M_TOKEN="${M2M_TOKEN:-${VELOX_M2M_TOKEN:-}}"
  WORKER_ID="${WORKER_ID:-${VELOX_WORKER_ID:-}}"
  WORKER_SSH_HOST="${WORKER_SSH_HOST:-${VELOX_WORKER_SSH_HOST:-}}"
  WORKER_SSH_USER="${WORKER_SSH_USER:-${VELOX_WORKER_SSH_USER:-root}}"
  MASTER_REST_PORT="${MASTER_REST_PORT:-${VELOX_MASTER_REST_PORT:-8000}}"
  MASTER_GRPC_PORT="${MASTER_GRPC_PORT:-${VELOX_MASTER_GRPC_PORT:-9000}}"
  TEST_JOB_JSON="${TEST_JOB_JSON:-${VELOX_TEST_JOB_JSON:-}}"
  RW_JOB_FIXTURE_FILE="${RW_JOB_FIXTURE_FILE:-${VELOX_JOB_FIXTURE_FILE:-}}"
  RW_JOB_EXPECTED_SUBMIT_STATUS="${RW_JOB_EXPECTED_SUBMIT_STATUS:-${VELOX_JOB_EXPECTED_SUBMIT_STATUS:-202}}"
  RW_JOB_REQUIRED_STATES="${RW_JOB_REQUIRED_STATES:-${VELOX_JOB_REQUIRED_STATES:-PENDING,LEASED,RUNNING,AWAITING_ARTIFACT,SUCCEEDED}}"
  CERT_POLL_TIMEOUT_S="${CERT_POLL_TIMEOUT_S:-${VELOX_CERT_POLL_TIMEOUT_S:-300}}"
  RW_NETWORK_TIMEOUT_S="${RW_NETWORK_TIMEOUT_S:-${VELOX_NETWORK_TIMEOUT_S:-30}}"
  RW_SSH_CONNECT_TIMEOUT_S="${RW_SSH_CONNECT_TIMEOUT_S:-${VELOX_SSH_CONNECT_TIMEOUT_S:-10}}"
  RW_CONNECT_TIMEOUT_S="${RW_CONNECT_TIMEOUT_S:-${VELOX_CONNECT_TIMEOUT_S:-5}}"
  RW_REST_REQUEST_TIMEOUT_S="${RW_REST_REQUEST_TIMEOUT_S:-${VELOX_REST_REQUEST_TIMEOUT_S:-10}}"
  RW_REST_ATTEMPTS="${RW_REST_ATTEMPTS:-${VELOX_REST_ATTEMPTS:-20}}"
  RW_REST_INTERVAL_S="${RW_REST_INTERVAL_S:-${VELOX_REST_INTERVAL_S:-1}}"
  RW_GRPC_TIMEOUT_S="${RW_GRPC_TIMEOUT_S:-${VELOX_GRPC_TIMEOUT_S:-5}}"
  RW_DNS_ATTEMPTS="${RW_DNS_ATTEMPTS:-${VELOX_DNS_ATTEMPTS:-3}}"
  RW_WORKER_HTTP_TIMEOUT_S="${RW_WORKER_HTTP_TIMEOUT_S:-${VELOX_WORKER_HTTP_TIMEOUT_S:-10}}"
  RW_WORKER_RESTART_TIMEOUT_S="${RW_WORKER_RESTART_TIMEOUT_S:-${VELOX_WORKER_RESTART_TIMEOUT_S:-30}}"
  RW_WORKER_RECONNECT_TIMEOUT_S="${RW_WORKER_RECONNECT_TIMEOUT_S:-${VELOX_WORKER_RECONNECT_TIMEOUT_S:-90}}"
  RW_WORKER_POLL_INTERVAL_S="${RW_WORKER_POLL_INTERVAL_S:-${VELOX_WORKER_POLL_INTERVAL_S:-2}}"
  RW_WORKER_RESTARTS="${RW_WORKER_RESTARTS:-${VELOX_WORKER_RESTARTS:-5}}"
  RW_HEARTBEAT_SAMPLES="${RW_HEARTBEAT_SAMPLES:-${VELOX_HEARTBEAT_SAMPLES:-3}}"
  RW_HEARTBEAT_INTERVAL_S="${RW_HEARTBEAT_INTERVAL_S:-${VELOX_HEARTBEAT_INTERVAL_S:-10}}"
  RW_HEARTBEAT_MAX_AGE_S="${RW_HEARTBEAT_MAX_AGE_S:-${VELOX_HEARTBEAT_MAX_AGE_S:-30}}"
  RW_WORKER_RESTART_CMD="${RW_WORKER_RESTART_CMD:-${VELOX_WORKER_RESTART_CMD:-sudo systemctl restart velox-worker.service}}"
  RW_OPERATION_TIMEOUT_S="${RW_OPERATION_TIMEOUT_S:-${VELOX_OPERATION_TIMEOUT_S:-600}}"
  RW_OPERATION_POLL_INTERVAL_S="${RW_OPERATION_POLL_INTERVAL_S:-${VELOX_OPERATION_POLL_INTERVAL_S:-2}}"
  RW_UPDATE_TARGET_IMAGE="${RW_UPDATE_TARGET_IMAGE:-${VELOX_UPDATE_TARGET_IMAGE:-}}"
  RW_UPDATE_TARGET_DIGEST="${RW_UPDATE_TARGET_DIGEST:-${VELOX_UPDATE_TARGET_DIGEST:-}}"
  RW_UPDATE_REASON="${RW_UPDATE_REASON:-${VELOX_UPDATE_REASON:-remote worker update certification}}"
  RW_UPDATE_LEASE_TIMEOUT_S="${RW_UPDATE_LEASE_TIMEOUT_S:-${VELOX_UPDATE_LEASE_TIMEOUT_S:-300}}"
  RW_UPDATE_LEASE_POLL_INTERVAL_S="${RW_UPDATE_LEASE_POLL_INTERVAL_S:-${VELOX_UPDATE_LEASE_POLL_INTERVAL_S:-2}}"
  RW_SMOKE_FIXTURES_FILE="${RW_SMOKE_FIXTURES_FILE:-${VELOX_SMOKE_FIXTURES_FILE:-${RW_CERT_CONFIG_DIR}/../../tests/worker-cert/fixtures/assets.json}}"
  RW_SMOKE_ASSET_ID="${RW_SMOKE_ASSET_ID:-${VELOX_SMOKE_ASSET_ID:-}}"
  RW_SMOKE_RENDER_PLAN="${RW_SMOKE_RENDER_PLAN:-${VELOX_SMOKE_RENDER_PLAN:-}}"
  RW_SMOKE_VERIFY_CLEANUP="${RW_SMOKE_VERIFY_CLEANUP:-${VELOX_SMOKE_VERIFY_CLEANUP:-1}}"
  RW_JOB_FIXTURES_FILE="${RW_JOB_FIXTURES_FILE:-${VELOX_JOB_FIXTURES_FILE:-${RW_CERT_CONFIG_DIR}/../../tests/worker-cert/fixtures/assets.json}}"
  RW_JOB_DESTINATION_ID="${RW_JOB_DESTINATION_ID:-${VELOX_JOB_DESTINATION_ID:-}}"
  RW_JOB_SCENES_COUNT="${RW_JOB_SCENES_COUNT:-${VELOX_JOB_SCENES_COUNT:-2}}"
  RW_JOB_DURATION_PER_SCENE="${RW_JOB_DURATION_PER_SCENE:-${VELOX_JOB_DURATION_PER_SCENE:-3}}"
  RW_JOB_POLL_INTERVAL_S="${RW_JOB_POLL_INTERVAL_S:-${VELOX_JOB_POLL_INTERVAL_S:-2}}"
  RW_JOB_HTTP_TIMEOUT_S="${RW_JOB_HTTP_TIMEOUT_S:-${VELOX_JOB_HTTP_TIMEOUT_S:-30}}"
  RW_JOB_ARTIFACT_ID="${RW_JOB_ARTIFACT_ID:-${VELOX_JOB_ARTIFACT_ID:-}}"
  RW_JOB_ARTIFACT_DOWNLOAD_URL="${RW_JOB_ARTIFACT_DOWNLOAD_URL:-${VELOX_JOB_ARTIFACT_DOWNLOAD_URL:-}}"
  RW_JOB_EXPECTED_SHA256="${RW_JOB_EXPECTED_SHA256:-${VELOX_JOB_EXPECTED_SHA256:-}}"
  RW_JOB_PRE_READY_REQUIRED="${RW_JOB_PRE_READY_REQUIRED:-${VELOX_JOB_PRE_READY_REQUIRED:-1}}"
  RW_JOB_DOWNLOAD_DIR="${RW_JOB_DOWNLOAD_DIR:-${VELOX_JOB_DOWNLOAD_DIR:-${TMPDIR:-/tmp}}}"
  RW_JOB_VERIFY_FFPROBE="${RW_JOB_VERIFY_FFPROBE:-${VELOX_JOB_VERIFY_FFPROBE:-1}}"
  RW_JOB_VERIFY_SHA256="${RW_JOB_VERIFY_SHA256:-${VELOX_JOB_VERIFY_SHA256:-1}}"
  RW_JOB_VERIFY_PRE_READY="${RW_JOB_VERIFY_PRE_READY:-${VELOX_JOB_VERIFY_PRE_READY:-1}}"
  RW_JOB_ARTIFACT_DOWNLOAD_TIMEOUT_S="${RW_JOB_ARTIFACT_DOWNLOAD_TIMEOUT_S:-${VELOX_JOB_ARTIFACT_DOWNLOAD_TIMEOUT_S:-60}}"
  RW_JOB_MODE="${RW_JOB_MODE:-0}"
  # F01-F03 failure-injection controls. Commands are validated as simple
  # executable/argument vectors before they are sent to a remote worker or
  # executed on the operator/master host; shell operators are rejected.
  RW_FAILURE_JOB_JSON="${RW_FAILURE_JOB_JSON:-${VELOX_FAILURE_JOB_JSON:-${TEST_JOB_JSON:-}}}"
  RW_FAILURE_JOB_TIMEOUT_S="${RW_FAILURE_JOB_TIMEOUT_S:-${VELOX_FAILURE_JOB_TIMEOUT_S:-900}}"
  RW_FAILURE_POLL_INTERVAL_S="${RW_FAILURE_POLL_INTERVAL_S:-${VELOX_FAILURE_POLL_INTERVAL_S:-2}}"
  RW_FAILURE_LEASE_TIMEOUT_S="${RW_FAILURE_LEASE_TIMEOUT_S:-${VELOX_FAILURE_LEASE_TIMEOUT_S:-120}}"
  RW_FAILURE_MASTER_READY_TIMEOUT_S="${RW_FAILURE_MASTER_READY_TIMEOUT_S:-${VELOX_FAILURE_MASTER_READY_TIMEOUT_S:-120}}"
  RW_FAILURE_LATE_RESULT_REQUIRED="${RW_FAILURE_LATE_RESULT_REQUIRED:-${VELOX_FAILURE_LATE_RESULT_REQUIRED:-1}}"
  RW_FAILURE_NETWORK_REST_PORT="${RW_FAILURE_NETWORK_REST_PORT:-${VELOX_FAILURE_NETWORK_REST_PORT:-$MASTER_REST_PORT}}"
  RW_FAILURE_NETWORK_GRPC_PORT="${RW_FAILURE_NETWORK_GRPC_PORT:-${VELOX_FAILURE_NETWORK_GRPC_PORT:-$MASTER_GRPC_PORT}}"
  RW_WORKER_CRASH_CMD="${RW_WORKER_CRASH_CMD:-${VELOX_WORKER_CRASH_CMD:-sudo systemctl stop velox-worker.service}}"
  RW_MASTER_RESTART_CMD="${RW_MASTER_RESTART_CMD:-${VELOX_MASTER_RESTART_CMD:-sudo systemctl restart velox-server.service}}"
  RW_MASTER_RESTART_TIMEOUT_S="${RW_MASTER_RESTART_TIMEOUT_S:-${VELOX_MASTER_RESTART_TIMEOUT_S:-60}}"
  RW_FAILURE_LATE_RESULT_CMD="${RW_FAILURE_LATE_RESULT_CMD:-${VELOX_FAILURE_LATE_RESULT_CMD:-}}"
  RW_FAILURE_MODE="${RW_FAILURE_MODE:-0}"

  rw_validate_restart_command "$RW_WORKER_CRASH_CMD" || return 1
  rw_validate_restart_command "$RW_MASTER_RESTART_CMD" || return 1
  if [[ -n "$RW_FAILURE_LATE_RESULT_CMD" ]]; then
    rw_validate_restart_command "$RW_FAILURE_LATE_RESULT_CMD" || return 1
  fi

  [[ "$RW_FAILURE_LATE_RESULT_REQUIRED" =~ ^[01]$ ]] || {
    rw_die "RW_FAILURE_LATE_RESULT_REQUIRED must be 0 or 1"
    return 1
  }

  if [[ "$RW_FAILURE_MODE" == "1" ]]; then
    # F01-F03 submit/poll jobs and deliberately do not run P03's artifact
    # pre-READY gate. The failure suite has its own job identity checks.
    RW_JOB_VERIFY_PRE_READY=0
    RW_JOB_PRE_READY_REQUIRED=0
  fi

  for numeric in CERT_POLL_TIMEOUT_S RW_NETWORK_TIMEOUT_S RW_SSH_CONNECT_TIMEOUT_S RW_CONNECT_TIMEOUT_S RW_REST_REQUEST_TIMEOUT_S RW_REST_ATTEMPTS RW_REST_INTERVAL_S RW_GRPC_TIMEOUT_S RW_DNS_ATTEMPTS RW_WORKER_HTTP_TIMEOUT_S RW_WORKER_RECONNECT_TIMEOUT_S RW_WORKER_POLL_INTERVAL_S RW_WORKER_RESTARTS RW_HEARTBEAT_SAMPLES RW_HEARTBEAT_INTERVAL_S RW_HEARTBEAT_MAX_AGE_S RW_OPERATION_TIMEOUT_S RW_OPERATION_POLL_INTERVAL_S RW_JOB_SCENES_COUNT RW_JOB_POLL_INTERVAL_S RW_JOB_HTTP_TIMEOUT_S RW_JOB_ARTIFACT_DOWNLOAD_TIMEOUT_S RW_FAILURE_JOB_TIMEOUT_S RW_FAILURE_POLL_INTERVAL_S RW_FAILURE_LEASE_TIMEOUT_S RW_FAILURE_MASTER_READY_TIMEOUT_S RW_FAILURE_NETWORK_REST_PORT RW_FAILURE_NETWORK_GRPC_PORT RW_MASTER_RESTART_TIMEOUT_S; do
    [[ "${!numeric}" =~ ^[1-9][0-9]*$ ]] || rw_die "${numeric} must be a positive integer" || return 1
  done

  if [[ -n "$TEST_JOB_JSON" ]]; then
    [[ -f "$TEST_JOB_JSON" && -r "$TEST_JOB_JSON" ]] || {
      rw_die "TEST_JOB_JSON is missing or unreadable: ${TEST_JOB_JSON}"
      return 1
    }
  fi
  if [[ -n "$RW_JOB_FIXTURE_FILE" ]]; then
    [[ -f "$RW_JOB_FIXTURE_FILE" && -r "$RW_JOB_FIXTURE_FILE" ]] || {
      rw_die "RW_JOB_FIXTURE_FILE is missing or unreadable: ${RW_JOB_FIXTURE_FILE}"
      return 1
    }
  fi
  [[ "$RW_JOB_EXPECTED_SUBMIT_STATUS" =~ ^(202|422)$ ]] || {
    rw_die "RW_JOB_EXPECTED_SUBMIT_STATUS must be 202 (valid fixture) or 422 (invalid fixture)"
    return 1
  }
  [[ "$RW_JOB_REQUIRED_STATES" == *,* ]] || {
    rw_die "RW_JOB_REQUIRED_STATES must contain a comma-separated lifecycle sequence"
    return 1
  }
  if [[ -n "$RW_FAILURE_JOB_JSON" ]]; then
    [[ -f "$RW_FAILURE_JOB_JSON" && -r "$RW_FAILURE_JOB_JSON" ]] || {
      rw_die "RW_FAILURE_JOB_JSON is missing or unreadable: ${RW_FAILURE_JOB_JSON}"
      return 1
    }
  fi

  export RW_FAILURE_JOB_JSON RW_FAILURE_JOB_TIMEOUT_S RW_FAILURE_POLL_INTERVAL_S RW_FAILURE_LEASE_TIMEOUT_S
  export RW_FAILURE_MASTER_READY_TIMEOUT_S RW_FAILURE_LATE_RESULT_REQUIRED RW_FAILURE_NETWORK_REST_PORT
  export RW_FAILURE_NETWORK_GRPC_PORT RW_WORKER_CRASH_CMD RW_MASTER_RESTART_CMD RW_MASTER_RESTART_TIMEOUT_S RW_FAILURE_LATE_RESULT_CMD RW_FAILURE_MODE

  [[ -n "$MASTER_URL" ]] || rw_die "MASTER_URL or VELOX_MASTER_URL is required" || return 1
  rw_validate_url "$MASTER_URL" || return 1
  [[ -n "$MASTER_HOST" ]] || rw_die "MASTER_HOST or VELOX_MASTER_HOST is required" || return 1
  rw_validate_host MASTER_HOST "$MASTER_HOST" || return 1
  if [[ -n "$MASTER_EXPECTED_IP" ]]; then
    [[ "$MASTER_EXPECTED_IP" =~ ^[0-9A-Fa-f:.]+$ ]] || rw_die "MASTER_EXPECTED_IP must be an IP address literal" || return 1
  fi
  [[ -n "$WORKER_ID" ]] || rw_die "WORKER_ID or VELOX_WORKER_ID is required" || return 1
  rw_validate_worker_id "$WORKER_ID" || return 1
  [[ -n "$WORKER_SSH_HOST" ]] || rw_die "WORKER_SSH_HOST or VELOX_WORKER_SSH_HOST is required" || return 1
  [[ "$WORKER_SSH_HOST" != *[[:space:]/\\]* ]] || rw_die "WORKER_SSH_HOST contains whitespace or a path separator" || return 1
  [[ "$WORKER_SSH_USER" =~ ^[A-Za-z_][A-Za-z0-9_.-]*$ ]] || rw_die "WORKER_SSH_USER is not a valid SSH login name" || return 1
  [[ -n "$RW_WORKER_RESTART_CMD" && "$RW_WORKER_RESTART_CMD" != *$'\r'* && "$RW_WORKER_RESTART_CMD" != *$'\n'* ]] || {
    rw_die "RW_WORKER_RESTART_CMD must be non-empty and contain no CR/LF"
    return 1
  }
  rw_validate_restart_command "$RW_WORKER_RESTART_CMD" || return 1
  rw_validate_port MASTER_REST_PORT "$MASTER_REST_PORT" || return 1
  rw_validate_port MASTER_GRPC_PORT "$MASTER_GRPC_PORT" || return 1
  for numeric in CERT_POLL_TIMEOUT_S RW_NETWORK_TIMEOUT_S RW_SSH_CONNECT_TIMEOUT_S RW_CONNECT_TIMEOUT_S RW_REST_REQUEST_TIMEOUT_S RW_REST_ATTEMPTS RW_REST_INTERVAL_S RW_GRPC_TIMEOUT_S RW_DNS_ATTEMPTS RW_WORKER_HTTP_TIMEOUT_S RW_WORKER_RESTART_TIMEOUT_S RW_WORKER_RECONNECT_TIMEOUT_S RW_WORKER_POLL_INTERVAL_S RW_WORKER_RESTARTS RW_HEARTBEAT_SAMPLES RW_HEARTBEAT_INTERVAL_S RW_HEARTBEAT_MAX_AGE_S RW_OPERATION_TIMEOUT_S RW_OPERATION_POLL_INTERVAL_S RW_JOB_SCENES_COUNT RW_JOB_POLL_INTERVAL_S RW_JOB_HTTP_TIMEOUT_S RW_JOB_ARTIFACT_DOWNLOAD_TIMEOUT_S; do
    [[ "${!numeric}" =~ ^[1-9][0-9]*$ ]] || rw_die "${numeric} must be a positive integer" || return 1
  done
  [[ "$RW_SMOKE_VERIFY_CLEANUP" =~ ^[01]$ ]] || {
    rw_die "RW_SMOKE_VERIFY_CLEANUP must be 0 or 1"
    return 1
  }
  [[ "$RW_JOB_PRE_READY_REQUIRED" =~ ^[01]$ && "$RW_JOB_VERIFY_FFPROBE" =~ ^[01]$ && "$RW_JOB_VERIFY_SHA256" =~ ^[01]$ && "$RW_JOB_VERIFY_PRE_READY" =~ ^[01]$ ]] || {
    rw_die "RW_JOB_PRE_READY_REQUIRED, RW_JOB_VERIFY_FFPROBE, RW_JOB_VERIFY_SHA256, and RW_JOB_VERIFY_PRE_READY must be 0 or 1"
    return 1
  }
  [[ "$RW_JOB_DURATION_PER_SCENE" =~ ^[0-9]+([.][0-9]+)?$ && "$RW_JOB_DURATION_PER_SCENE" != 0* ]] || {
    rw_die "RW_JOB_DURATION_PER_SCENE must be a positive number"
    return 1
  }
  [[ -r "$RW_JOB_FIXTURES_FILE" ]] || {
    rw_die "RW_JOB_FIXTURES_FILE is missing or unreadable: ${RW_JOB_FIXTURES_FILE}"
    return 1
  }
  if [[ -n "$RW_JOB_DESTINATION_ID" ]]; then
    [[ "$RW_JOB_DESTINATION_ID" =~ ^[A-Za-z0-9._:-]+$ ]] || {
      rw_die "RW_JOB_DESTINATION_ID contains unsafe characters"
      return 1
    }
  fi
  if [[ -n "$RW_JOB_ARTIFACT_ID" ]]; then
    [[ "$RW_JOB_ARTIFACT_ID" =~ ^[A-Za-z0-9._:-]+$ ]] || {
      rw_die "RW_JOB_ARTIFACT_ID contains unsafe characters"
      return 1
    }
  fi
  if [[ -n "$RW_JOB_ARTIFACT_DOWNLOAD_URL" ]]; then
    [[ "$RW_JOB_ARTIFACT_DOWNLOAD_URL" =~ ^/api/internal/artifacts/[A-Za-z0-9._:-]+/download([?][^[:space:]]*)?$ ]] || {
      rw_die "RW_JOB_ARTIFACT_DOWNLOAD_URL must be the canonical /api/internal/artifacts/<id>/download path"
      return 1
    }
  fi
  if [[ -n "$RW_JOB_EXPECTED_SHA256" ]]; then
    [[ "$RW_JOB_EXPECTED_SHA256" =~ ^[a-f0-9]{64}$ ]] || {
      rw_die "RW_JOB_EXPECTED_SHA256 must be 64 lowercase hexadecimal characters"
      return 1
    }
  fi
  [[ -d "$RW_JOB_DOWNLOAD_DIR" && -w "$RW_JOB_DOWNLOAD_DIR" ]] || {
    rw_die "RW_JOB_DOWNLOAD_DIR must be an existing writable directory: ${RW_JOB_DOWNLOAD_DIR}"
    return 1
  }
  if [[ "$RW_JOB_EXPECTED_SUBMIT_STATUS" == "202" && "$RW_JOB_VERIFY_PRE_READY" == "1" && "$RW_JOB_PRE_READY_REQUIRED" == "1" && -z "$RW_JOB_ARTIFACT_ID" && -z "$RW_JOB_ARTIFACT_DOWNLOAD_URL" ]]; then
    rw_die "P03 pre-READY verification requires RW_JOB_ARTIFACT_ID or RW_JOB_ARTIFACT_DOWNLOAD_URL because POST /api/v1/jobs does not expose an artifact ID"
    return 1
  fi
  if [[ -z "$RW_SMOKE_ASSET_ID" ]]; then
    [[ -r "$RW_SMOKE_FIXTURES_FILE" ]] || {
      rw_die "RW_SMOKE_FIXTURES_FILE is missing or unreadable: ${RW_SMOKE_FIXTURES_FILE}"
      return 1
    }
  else
    [[ "$RW_SMOKE_ASSET_ID" =~ ^[A-Za-z0-9._:-]+$ ]] || {
      rw_die "RW_SMOKE_ASSET_ID contains unsafe characters"
      return 1
    }
  fi

  if (( require_admin )); then
    rw_resolve_admin_token || return 1
  elif [[ -n "${VELOX_ADMIN_TOKEN:-}" || -n "${TOKEN_FILE:-}" ]]; then
    rw_resolve_admin_token || return 1
  fi
  if [[ -n "$M2M_TOKEN" ]]; then
    [[ "$M2M_TOKEN" != *$'\r'* && "$M2M_TOKEN" != *$'\n'* && "$M2M_TOKEN" != *'"'* && "$M2M_TOKEN" != *'\\'* ]] || {
      rw_die "VELOX_M2M_TOKEN contains an unsafe control or config character"
      return 1
    }
  fi
  rw_validate_digest GOOD_DIGEST "${GOOD_DIGEST:-}" || return 1
  rw_validate_digest PREVIOUS_DIGEST "${PREVIOUS_DIGEST:-}" || return 1
  rw_validate_digest BAD_DIGEST "${BAD_DIGEST:-}" || return 1
  if [[ -n "$RW_UPDATE_TARGET_DIGEST" ]]; then
    rw_validate_digest RW_UPDATE_TARGET_DIGEST "$RW_UPDATE_TARGET_DIGEST" || return 1
  fi
  if [[ -n "$RW_UPDATE_TARGET_IMAGE" ]]; then
    [[ "$RW_UPDATE_TARGET_IMAGE" =~ ^ghcr\.io/[a-z0-9._-]+/[a-z0-9._-]+(/[a-z0-9._-]+)*@sha256:[a-f0-9]{64}$ ]] || {
      rw_die "RW_UPDATE_TARGET_IMAGE must be a pinned ghcr.io image reference"
      return 1
    }
    image_digest="${RW_UPDATE_TARGET_IMAGE##*@}"
    if [[ -n "$RW_UPDATE_TARGET_DIGEST" && "$image_digest" != "$RW_UPDATE_TARGET_DIGEST" ]]; then
      rw_die "RW_UPDATE_TARGET_IMAGE digest does not match RW_UPDATE_TARGET_DIGEST"
      return 1
    fi
    RW_UPDATE_TARGET_DIGEST="$image_digest"
  fi
  [[ "$RW_UPDATE_LEASE_TIMEOUT_S" =~ ^[1-9][0-9]*$ ]] || {
    rw_die "RW_UPDATE_LEASE_TIMEOUT_S must be a positive integer"
    return 1
  }
  [[ "$RW_UPDATE_LEASE_POLL_INTERVAL_S" =~ ^[1-9][0-9]*$ ]] || {
    rw_die "RW_UPDATE_LEASE_POLL_INTERVAL_S must be a positive integer"
    return 1
  }

  if [[ -n "$TEST_JOB_JSON" ]]; then
    [[ -f "$TEST_JOB_JSON" && -r "$TEST_JOB_JSON" ]] || {
      rw_die "TEST_JOB_JSON is missing or unreadable: ${TEST_JOB_JSON}"
      return 1
    }
  fi

  export MASTER_URL MASTER_HOST MASTER_EXPECTED_IP M2M_TOKEN WORKER_ID WORKER_SSH_HOST WORKER_SSH_USER
  export MASTER_REST_PORT MASTER_GRPC_PORT TEST_JOB_JSON RW_JOB_FIXTURE_FILE RW_JOB_EXPECTED_SUBMIT_STATUS RW_JOB_REQUIRED_STATES CERT_POLL_TIMEOUT_S
  export RW_NETWORK_TIMEOUT_S RW_SSH_CONNECT_TIMEOUT_S RW_CONNECT_TIMEOUT_S
  export RW_REST_REQUEST_TIMEOUT_S RW_REST_ATTEMPTS RW_REST_INTERVAL_S RW_GRPC_TIMEOUT_S RW_DNS_ATTEMPTS
  export RW_WORKER_HTTP_TIMEOUT_S RW_WORKER_RESTART_TIMEOUT_S RW_WORKER_RECONNECT_TIMEOUT_S
  export RW_WORKER_POLL_INTERVAL_S RW_WORKER_RESTARTS RW_HEARTBEAT_SAMPLES RW_HEARTBEAT_INTERVAL_S RW_HEARTBEAT_MAX_AGE_S
  export RW_WORKER_RESTART_CMD RW_OPERATION_TIMEOUT_S RW_OPERATION_POLL_INTERVAL_S
  export RW_UPDATE_TARGET_IMAGE RW_UPDATE_TARGET_DIGEST RW_UPDATE_REASON
  export RW_UPDATE_LEASE_TIMEOUT_S RW_UPDATE_LEASE_POLL_INTERVAL_S
  export RW_SMOKE_FIXTURES_FILE RW_SMOKE_ASSET_ID RW_SMOKE_RENDER_PLAN RW_SMOKE_VERIFY_CLEANUP
  export RW_JOB_FIXTURE_FILE RW_JOB_FIXTURES_FILE RW_JOB_DESTINATION_ID RW_JOB_SCENES_COUNT RW_JOB_DURATION_PER_SCENE
  export RW_JOB_POLL_INTERVAL_S RW_JOB_HTTP_TIMEOUT_S RW_JOB_ARTIFACT_ID RW_JOB_ARTIFACT_DOWNLOAD_URL
  export RW_JOB_EXPECTED_SHA256 RW_JOB_PRE_READY_REQUIRED RW_JOB_DOWNLOAD_DIR RW_JOB_VERIFY_FFPROBE
  export RW_JOB_VERIFY_SHA256 RW_JOB_VERIFY_PRE_READY RW_JOB_ARTIFACT_DOWNLOAD_TIMEOUT_S RW_JOB_MODE
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
    "${curl_args[@]}" --config "$cfg" "${MASTER_URL}${path}"; then
    rc=0
  else
    rc=$?
  fi
  trap - EXIT INT TERM
  rm -f -- "$cfg"
  return "$rc"
}

