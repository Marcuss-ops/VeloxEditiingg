# remote-worker-cert-network.sh — network and preflight checks.
# Loaded by scripts/cert/remote-worker-cert-config.sh.

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
  rw_log_command "SSH ${WORKER_SSH_USER:-unknown}@${WORKER_SSH_HOST:-unknown}"
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
  remote_cmd="for i in \$(seq 1 ${RW_DNS_ATTEMPTS}); do ip=\$(getent hosts ${host_q} | awk 'NR==1 {print \$1; exit}'); [ -n \"\$ip\" ] || exit 1; printf '%s
' \"\$ip\"; done"
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

