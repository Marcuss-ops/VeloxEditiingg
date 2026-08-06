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
  CERT_POLL_TIMEOUT_S="${CERT_POLL_TIMEOUT_S:-${VELOX_CERT_POLL_TIMEOUT_S:-300}}"
  RW_NETWORK_TIMEOUT_S="${RW_NETWORK_TIMEOUT_S:-${VELOX_NETWORK_TIMEOUT_S:-30}}"
  RW_SSH_CONNECT_TIMEOUT_S="${RW_SSH_CONNECT_TIMEOUT_S:-${VELOX_SSH_CONNECT_TIMEOUT_S:-10}}"
  RW_CONNECT_TIMEOUT_S="${RW_CONNECT_TIMEOUT_S:-${VELOX_CONNECT_TIMEOUT_S:-5}}"
  RW_REST_REQUEST_TIMEOUT_S="${RW_REST_REQUEST_TIMEOUT_S:-${VELOX_REST_REQUEST_TIMEOUT_S:-10}}"
  RW_REST_ATTEMPTS="${RW_REST_ATTEMPTS:-${VELOX_REST_ATTEMPTS:-20}}"
  RW_REST_INTERVAL_S="${RW_REST_INTERVAL_S:-${VELOX_REST_INTERVAL_S:-1}}"
  RW_GRPC_TIMEOUT_S="${RW_GRPC_TIMEOUT_S:-${VELOX_GRPC_TIMEOUT_S:-5}}"
  RW_DNS_ATTEMPTS="${RW_DNS_ATTEMPTS:-${VELOX_DNS_ATTEMPTS:-3}}"

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
  rw_validate_port MASTER_REST_PORT "$MASTER_REST_PORT" || return 1
  rw_validate_port MASTER_GRPC_PORT "$MASTER_GRPC_PORT" || return 1
  for numeric in CERT_POLL_TIMEOUT_S RW_NETWORK_TIMEOUT_S RW_SSH_CONNECT_TIMEOUT_S RW_CONNECT_TIMEOUT_S RW_REST_REQUEST_TIMEOUT_S RW_REST_ATTEMPTS RW_REST_INTERVAL_S RW_GRPC_TIMEOUT_S RW_DNS_ATTEMPTS; do
    [[ "${!numeric}" =~ ^[1-9][0-9]*$ ]] || rw_die "${numeric} must be a positive integer" || return 1
  done

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

  if [[ -n "$TEST_JOB_JSON" ]]; then
    [[ -f "$TEST_JOB_JSON" && -r "$TEST_JOB_JSON" ]] || {
      rw_die "TEST_JOB_JSON is missing or unreadable: ${TEST_JOB_JSON}"
      return 1
    }
  fi

  export MASTER_URL MASTER_HOST MASTER_EXPECTED_IP M2M_TOKEN WORKER_ID WORKER_SSH_HOST WORKER_SSH_USER
  export MASTER_REST_PORT MASTER_GRPC_PORT TEST_JOB_JSON CERT_POLL_TIMEOUT_S
  export RW_NETWORK_TIMEOUT_S RW_SSH_CONNECT_TIMEOUT_S RW_CONNECT_TIMEOUT_S
  export RW_REST_REQUEST_TIMEOUT_S RW_REST_ATTEMPTS RW_REST_INTERVAL_S RW_GRPC_TIMEOUT_S RW_DNS_ATTEMPTS
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

rw_now_s() {
  date +%s
}

rw_capture_ssh() {
  local remote_cmd="$1" out_file err_file rc
  out_file="$(mktemp "${TMPDIR:-/tmp}/velox-worker-ssh-out.XXXXXX")"
  err_file="$(mktemp "${TMPDIR:-/tmp}/velox-worker-ssh-err.XXXXXX")"
  if timeout "${RW_NETWORK_TIMEOUT_S}s" ssh \
    -o BatchMode=yes \
    -o ConnectTimeout="${RW_SSH_CONNECT_TIMEOUT_S}" \
    "${WORKER_SSH_USER}@${WORKER_SSH_HOST}" "$remote_cmd" \
    >"$out_file" 2>"$err_file"; then
    rc=0
  else
    rc=$?
  fi
  # Preserve line structure: R02 counts one result per line and R04 parses
  # hostname/docker version as separate records. jq performs JSON escaping
  # later when diagnostics are emitted.
  RW_LAST_STDOUT="$(head -c 4096 "$out_file")"
  RW_LAST_STDERR="$(head -c 4096 "$err_file")"
  RW_LAST_RC="$rc"
  rm -f -- "$out_file" "$err_file"
}

rw_record_network_check() {
  local id="$1" name="$2" status="$3" diagnostic="$4" elapsed_ms="$5"
  RW_NETWORK_RESULTS+=("$(jq -cn \
    --arg id "$id" \
    --arg name "$name" \
    --arg status "$status" \
    --arg diagnostic "$diagnostic" \
    --argjson elapsed_ms "$elapsed_ms" \
    '{id:$id,name:$name,status:$status,elapsed_ms:$elapsed_ms,diagnostic:$diagnostic}')")
}

rw_network_diagnostic() {
  if [[ -n "${RW_LAST_STDERR:-}" ]]; then
    printf '%s' "$RW_LAST_STDERR"
  elif [[ -n "${RW_LAST_STDOUT:-}" ]]; then
    printf '%s' "$RW_LAST_STDOUT"
  else
    printf 'command exited with rc=%s' "${RW_LAST_RC:-unknown}"
  fi
}

rw_network_prereq_failure() {
  local diagnostic="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n \
      --arg diagnostic "$diagnostic" \
      --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      '{schema:"velox.remote_worker.network.v1",worker_id:(env.WORKER_ID // ""),master_url:(env.MASTER_URL // ""),master_host:(env.MASTER_HOST // ""),checks:[],overall:"FAIL",diagnostic:$diagnostic,generated_at:$generated_at}'
  else
    printf '{"schema":"velox.remote_worker.network.v1","checks":[],"overall":"FAIL","diagnostic":"required JSON encoder unavailable"}\n'
  fi
}

rw_network_checks() {
  local started finished elapsed host_q url_q endpoint_q remote_cmd
  local status diagnostic rest_count rest_bad dns_count dns_unique dns_ip grpc_mode docker_host docker_version
  local checks_json overall="PASS" result
  local -a RW_NETWORK_RESULTS=()

  local bin numeric missing=""
  for bin in jq ssh timeout; do
    if ! command -v "$bin" >/dev/null 2>&1; then
      missing="${missing}${missing:+,}${bin}"
    fi
  done
  if [[ -n "$missing" ]]; then
    rw_network_prereq_failure "missing local prerequisites: ${missing}"
    return 2
  fi
  [[ -n "${MASTER_URL:-}" && -n "${MASTER_HOST:-}" && -n "${WORKER_ID:-}" ]] || {
    rw_die "call rw_load_config before rw_network_checks"
    return 2
  }

  # R01 — DNS resolution from the worker, not from the operator machine.
  started="$(rw_now_s)"
  printf -v host_q '%q' "$MASTER_HOST"
  remote_cmd="for i in \$(seq 1 ${RW_DNS_ATTEMPTS}); do ip=\$(getent hosts ${host_q} | awk 'NR==1 {print \$1; exit}'); [ -n \"\$ip\" ] || exit 1; printf '%s\\n' \"\$ip\"; done"
  rw_capture_ssh "$remote_cmd"
  finished="$(rw_now_s)"
  elapsed=$(( (finished - started) * 1000 ))
  dns_count="$(printf '%s\n' "$RW_LAST_STDOUT" | awk 'NF {n++} END {print n+0}')"
  dns_unique="$(printf '%s\n' "$RW_LAST_STDOUT" | awk 'NF {print $1}' | sort -u | awk 'NF {n++} END {print n+0}')"
  dns_ip="$(printf '%s\n' "$RW_LAST_STDOUT" | awk 'NF {print $1; exit}')"
  if [[ "$RW_LAST_RC" -eq 0 && "$dns_count" -eq "$RW_DNS_ATTEMPTS" && "$dns_unique" -eq 1 && ( -z "$MASTER_EXPECTED_IP" || "$dns_ip" == "$MASTER_EXPECTED_IP" ) ]]; then
    rw_record_network_check R01 dns PASS "resolved_ip=${dns_ip}; stable=${dns_count}/${RW_DNS_ATTEMPTS}" "$elapsed"
  else
    diagnostic="resolved=${RW_LAST_STDOUT}"
    [[ -n "$MASTER_EXPECTED_IP" ]] && diagnostic="${diagnostic}; expected_ip=${MASTER_EXPECTED_IP}"
    [[ -n "$RW_LAST_STDERR" ]] && diagnostic="${diagnostic}; ${RW_LAST_STDERR}"
    [[ -n "$diagnostic" ]] || diagnostic="DNS resolution failed (rc=${RW_LAST_RC})"
    rw_record_network_check R01 dns FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  fi

  # R02 — repeated REST readiness probes from the worker.
  started="$(rw_now_s)"
  printf -v url_q '%q' "${MASTER_URL}/health/ready"
  remote_cmd="for i in \$(seq 1 ${RW_REST_ATTEMPTS}); do curl --silent --show-error --connect-timeout ${RW_CONNECT_TIMEOUT_S} --max-time ${RW_REST_REQUEST_TIMEOUT_S} -o /dev/null -w '%{http_code} %{time_connect} %{time_total}\\n' ${url_q} || printf '000 0 0\\n'; if [ \"\$i\" -lt ${RW_REST_ATTEMPTS} ]; then sleep ${RW_REST_INTERVAL_S}; fi; done"
  rw_capture_ssh "$remote_cmd"
  finished="$(rw_now_s)"
  elapsed=$(( (finished - started) * 1000 ))
  rest_count="$(printf '%s\n' "$RW_LAST_STDOUT" | awk '$1 != "" {n++} END {print n+0}')"
  rest_bad="$(printf '%s\n' "$RW_LAST_STDOUT" | awk '$1 != "200" && $1 != "" {n++} END {print n+0}')"
  if [[ "$RW_LAST_RC" -eq 0 && "$rest_count" -eq "$RW_REST_ATTEMPTS" && "$rest_bad" -eq 0 ]]; then
    rw_record_network_check R02 rest PASS "${rest_count}/${RW_REST_ATTEMPTS} HTTP 200 readiness responses" "$elapsed"
  else
    diagnostic="${RW_LAST_STDOUT}"
    [[ -n "$RW_LAST_STDERR" ]] && diagnostic="${diagnostic} ${RW_LAST_STDERR}"
    [[ -n "$diagnostic" ]] || diagnostic="REST readiness probe failed (rc=${RW_LAST_RC})"
    rw_record_network_check R02 rest FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  fi

  # R03 — prefer grpcurl (application handshake); otherwise use nc as a
  # clearly-labelled TCP fallback. A fallback is WARN, never a false PASS.
  started="$(rw_now_s)"
  printf -v endpoint_q '%q' "${MASTER_HOST}:${MASTER_GRPC_PORT}"
  grpc_mode="${MASTER_GRPC_TLS_MODE:-tls}"
  remote_cmd="if command -v grpcurl >/dev/null 2>&1; then grpcurl -connect-timeout ${RW_GRPC_TIMEOUT_S}s"
  if [[ "$grpc_mode" == "plaintext" ]]; then
    remote_cmd+=" -plaintext"
  fi
  if [[ -n "${GRPCURL_CA_FILE:-}" ]]; then
    printf -v result '%q' "$GRPCURL_CA_FILE"
    remote_cmd+=" -cacert ${result}"
  fi
  if [[ -n "${GRPCURL_CERT_FILE:-}" && -n "${GRPCURL_KEY_FILE:-}" ]]; then
    printf -v result '%q' "$GRPCURL_CERT_FILE"
    remote_cmd+=" -cert ${result}"
    printf -v result '%q' "$GRPCURL_KEY_FILE"
    remote_cmd+=" -key ${result}"
  fi
  remote_cmd+=" ${endpoint_q} list >/dev/null && printf grpcurl; elif command -v nc >/dev/null 2>&1; then nc -z -w ${RW_GRPC_TIMEOUT_S} ${MASTER_HOST} ${MASTER_GRPC_PORT} && printf nc; else printf missing_grpc_probe; exit 127; fi"
  rw_capture_ssh "$remote_cmd"
  finished="$(rw_now_s)"
  elapsed=$(( (finished - started) * 1000 ))
  if [[ "$RW_LAST_RC" -eq 0 && "$RW_LAST_STDOUT" == *grpcurl* ]]; then
    rw_record_network_check R03 grpc PASS "grpcurl application probe succeeded (${grpc_mode})" "$elapsed"
  elif [[ "$RW_LAST_RC" -eq 0 && "$RW_LAST_STDOUT" == *nc* ]]; then
    rw_record_network_check R03 grpc WARN "TCP port reachable via nc; grpcurl application handshake unavailable on worker" "$elapsed"
    [[ "$overall" == "PASS" ]] && overall="WARN"
  else
    rw_record_network_check R03 grpc FAIL "$(rw_network_diagnostic)" "$elapsed"
    overall="FAIL"
  fi

  # R04 — deployment-plane SSH, sudo, and Docker server-version probe.
  started="$(rw_now_s)"
  printf -v result '%q' '{{.Server.Version}}'
  remote_cmd="hostname && sudo -n true && docker version --format ${result}"
  rw_capture_ssh "$remote_cmd"
  finished="$(rw_now_s)"
  elapsed=$(( (finished - started) * 1000 ))
  docker_host="$(printf '%s\n' "$RW_LAST_STDOUT" | sed -n '1p')"
  docker_version="$(printf '%s\n' "$RW_LAST_STDOUT" | sed -n '2p')"
  if [[ "$RW_LAST_RC" -eq 0 && -n "$docker_host" && -n "$docker_version" ]]; then
    if [[ -n "${WORKER_EXPECTED_HOSTNAME:-}" && "$docker_host" != "$WORKER_EXPECTED_HOSTNAME" ]]; then
      rw_record_network_check R04 ssh_docker FAIL "SSH hostname mismatch: got ${docker_host}, expected ${WORKER_EXPECTED_HOSTNAME}" "$elapsed"
      overall="FAIL"
    else
      rw_record_network_check R04 ssh_docker PASS "hostname=${docker_host}; docker_server=${docker_version}" "$elapsed"
    fi
  else
    rw_record_network_check R04 ssh_docker FAIL "$(rw_network_diagnostic)" "$elapsed"
    overall="FAIL"
  fi

  checks_json="$(printf '%s\n' "${RW_NETWORK_RESULTS[@]}" | jq -s '.')"
  jq -n \
    --arg schema 'velox.remote_worker.network.v1' \
    --arg worker_id "$WORKER_ID" \
    --arg master_url "$MASTER_URL" \
    --arg master_host "$MASTER_HOST" \
    --arg overall "$overall" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson checks "$checks_json" \
    '{schema:$schema,worker_id:$worker_id,master_url:$master_url,master_host:$master_host,checks:$checks,overall:$overall,generated_at:$generated_at}'

  [[ "$overall" != "FAIL" ]]
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  case "${1:-}" in
    --network|--network-json)
      shift
      [[ "$#" -eq 0 ]] || { rw_die "network mode does not accept positional arguments"; exit 2; }
      rw_load_config --network-only
      rw_network_checks
      ;;
    --help|-h)
      printf '%s\n' 'Usage: remote-worker-cert-config.sh [--network-json]' \
        'Default mode runs local preflight only.' \
        '--network-json runs R01-R04 and emits one JSON document on stdout.'
      ;;
    '')
      rw_load_config
      rw_remote_worker_preflight
      ;;
    *)
      rw_die "unknown option: $1 (use --help)"
      exit 2
      ;;
  esac
fi
