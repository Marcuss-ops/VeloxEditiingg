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

RW_CERT_CONFIG_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

rw_die() {
  printf 'remote-worker-cert: %s\n' "$*" >&2
  return 1
}

# Evidence output is initialized only for a CLI certification run. Sourcing this
# file remains side-effect free until rw_init_artifacts is called.
rw_init_artifacts() {
  RW_RUN_ID="${RW_RUN_ID:-${VELOX_CERT_RUN_ID:-cert-$(date -u +%Y%m%dT%H%M%SZ)-$$}}"
  RW_ARTIFACT_DIR="${RW_ARTIFACT_DIR:-${VELOX_CERT_ARTIFACT_DIR:-${TMPDIR:-/tmp}/velox-cert-${RW_RUN_ID}}}"
  mkdir -p -- "$RW_ARTIFACT_DIR" || return 1
  : >"${RW_ARTIFACT_DIR}/commands.log" || return 1
  printf '%s\n' "run_id=${RW_RUN_ID} mode=${RW_CERT_MODE:-unknown}" >>"${RW_ARTIFACT_DIR}/commands.log"
  jq -n --arg run_id "$RW_RUN_ID" --arg status NOT_RUN \
    '{run_id:$run_id,status:$status,operations:[]}' >"${RW_ARTIFACT_DIR}/operations.json"
  jq -n --arg run_id "$RW_RUN_ID" --arg status NOT_RUN \
    '{run_id:$run_id,status:$status}' >"${RW_ARTIFACT_DIR}/artifact-ffprobe.json"
  for snapshot in worker-before worker-after master-before master-after; do
    jq -n --arg run_id "$RW_RUN_ID" --arg status NOT_OBSERVED \
      '{run_id:$run_id,status:$status}' >"${RW_ARTIFACT_DIR}/${snapshot}.json"
  done
  export RW_RUN_ID RW_ARTIFACT_DIR
}

rw_log_command() {
  [[ -n "${RW_ARTIFACT_DIR:-}" ]] || return 0
  # Callers pass only method/path or an already-sanitized remote command.
  # Credentials and request bodies are intentionally never logged.
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"${RW_ARTIFACT_DIR}/commands.log"
}

rw_snapshot_json() {
  local kind="$1" body="$2" normalized
  [[ -n "${RW_ARTIFACT_DIR:-}" ]] || return 0
  if jq -e . >/dev/null 2>&1 <<<"$body"; then
    normalized="$body"
  else
    normalized="$(jq -cn --arg raw "$body" '{raw:$raw}')"
  fi
  printf '%s\n' "$normalized" >"${RW_ARTIFACT_DIR}/${kind}-after.json"
  if [[ ! -s "${RW_ARTIFACT_DIR}/${kind}-before.json" ]] || jq -e '.status == "NOT_OBSERVED"' "${RW_ARTIFACT_DIR}/${kind}-before.json" >/dev/null 2>&1; then
    printf '%s\n' "$normalized" >"${RW_ARTIFACT_DIR}/${kind}-before.json"
  fi
  printf '%s\n' "$normalized" >"${RW_ARTIFACT_DIR}/${kind}.json"
}

rw_record_operation() {
  local method="$1" path="$2" http_status="$3" body="${4:-}" operation_json
  [[ -n "${RW_ARTIFACT_DIR:-}" ]] || return 0
  operation_json="$(jq -cn --arg run_id "${RW_RUN_ID:-}" --arg method "$method" --arg path "$path" \
    --arg http_status "$http_status" --arg body "$body" \
    '{run_id:$run_id,method:$method,path:$path,http_status:($http_status|tonumber? // $http_status),response:(try ($body|fromjson) catch {raw:$body})}')"
  jq --argjson operation "$operation_json" '.operations += [$operation] | .status="RECORDED"' \
    "${RW_ARTIFACT_DIR}/operations.json" >"${RW_ARTIFACT_DIR}/operations.json.tmp" \
    && mv -f -- "${RW_ARTIFACT_DIR}/operations.json.tmp" "${RW_ARTIFACT_DIR}/operations.json"
}

rw_record_artifact_ffprobe() {
  local status="$1" artifact_file="${2:-}" sha256="${3:-}" verifier_report="${4:-}" diagnostic="${5:-}"
  [[ -n "${RW_ARTIFACT_DIR:-}" ]] || return 0
  if [[ -n "$verifier_report" && -r "$verifier_report" ]] && jq -e . "$verifier_report" >/dev/null 2>&1; then
    jq --arg run_id "${RW_RUN_ID:-}" --arg status "$status" --arg file "$artifact_file" \
      --arg sha256 "$sha256" --arg diagnostic "$diagnostic" \
      '. + {run_id:$run_id,status:$status,artifact_file:$file,sha256:$sha256,diagnostic:$diagnostic}' \
      "$verifier_report" >"${RW_ARTIFACT_DIR}/artifact-ffprobe.json"
  else
    jq -n --arg run_id "${RW_RUN_ID:-}" --arg status "$status" --arg file "$artifact_file" \
      --arg sha256 "$sha256" --arg diagnostic "$diagnostic" \
      '{run_id:$run_id,status:$status,artifact_file:$file,sha256:$sha256,diagnostic:$diagnostic}' \
      >"${RW_ARTIFACT_DIR}/artifact-ffprobe.json"
  fi
}

rw_junit_escape() {
  sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g; s/'"'"'/\&apos;/g'
}

rw_write_junit() {
  local report_file="$1" junit_file="$2" mode="$3" report_json
  report_json="$(cat "$report_file" 2>/dev/null || printf '%s' '{}')"
  python3 - "$junit_file" "$mode" "$report_json" <<'PY'
import json, sys
from xml.sax.saxutils import escape, quoteattr
out, mode, raw = sys.argv[1:]
try:
    report = json.loads(raw)
except json.JSONDecodeError:
    report = {"overall": "FAIL", "checks": [{"id": "REPORT", "status": "FAIL", "diagnostic": "invalid report JSON"}]}
checks = report.get("checks") or []
failures = sum(1 for c in checks if c.get("status") == "FAIL")
if report.get("overall") == "FAIL" and not failures:
    failures = 1
with open(out, "w", encoding="utf-8") as fh:
    fh.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    fh.write('<testsuite name="velox.remote_worker.%s" tests="%d" failures="%d">\n' % (escape(mode), max(1, len(checks)), failures))
    if checks:
        for check in checks:
            name = str(check.get("id") or check.get("name") or "check")
            status = check.get("status", "FAIL")
            diagnostic = str(check.get("diagnostic") or "")
            fh.write('  <testcase name=%s>' % quoteattr(name))
            if status == "FAIL":
                fh.write('<failure message=%s>%s</failure>' % (quoteattr(diagnostic[:500]), escape(diagnostic)))
            fh.write('</testcase>\n')
    else:
        if report.get("overall") == "PASS":
            fh.write('  <testcase name="certification"/>\n')
        else:
            fh.write('  <testcase name="certification"><failure message="no checks"/></testcase>\n')
    fh.write('</testsuite>\n')
PY
}

rw_finalize_artifacts() {
  local raw_report="$1" rc="$2" mode="$3" report_file="${RW_ARTIFACT_DIR}/report.json"
  local overall
  if ! jq -e . "$raw_report" >/dev/null 2>&1; then
    jq -n --arg run_id "${RW_RUN_ID:-}" --arg mode "$mode" --arg status FAIL --arg diagnostic "runner emitted invalid JSON" \
      '{run_id:$run_id,mode:$mode,overall:$status,checks:[{id:"REPORT",name:"report",status:$status,diagnostic:$diagnostic}],result:null}' >"$report_file"
  else
    overall="$(jq -r --arg fallback "$([[ "$rc" -eq 0 ]] && printf PASS || printf FAIL)" '.overall // $fallback' "$raw_report")"
    jq --arg run_id "${RW_RUN_ID:-}" --arg mode "$mode" --argjson exit_code "$rc" \
      --arg artifact_dir "${RW_ARTIFACT_DIR:-}" --arg overall "$overall" \
      '{run_id:$run_id,mode:$mode,overall:$overall,exit_code:$exit_code,artifact_dir:$artifact_dir,checks:(.checks // []),result:.}' \
      "$raw_report" >"${report_file}.tmp" && mv -f -- "${report_file}.tmp" "$report_file"
  fi
  rw_write_junit "$report_file" "${RW_ARTIFACT_DIR}/report.junit.xml" "$mode"
  jq --arg status "$(jq -r '.overall' "$report_file")" '.status=$status' "${RW_ARTIFACT_DIR}/operations.json" >"${RW_ARTIFACT_DIR}/operations.json.tmp" \
    && mv -f -- "${RW_ARTIFACT_DIR}/operations.json.tmp" "${RW_ARTIFACT_DIR}/operations.json"
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

rw_smoke_record() {
  local id="$1" name="$2" status="$3" diagnostic="$4" elapsed_ms="$5" evidence="${6:-}"
  RW_SMOKE_RESULTS+=("$(jq -cn \
    --arg id "$id" --arg name "$name" --arg status "$status" \
    --arg diagnostic "$diagnostic" --arg evidence "$evidence" --argjson elapsed_ms "$elapsed_ms" \
    '{id:$id,name:$name,status:$status,elapsed_ms:$elapsed_ms,diagnostic:$diagnostic,evidence:(if $evidence == "" then null else $evidence end)}')")
}

rw_smoke_cleanup_command() {
  # These are the exact temp locations removed by SSHWorkerExec.CleanupWorkerTemp.
  # The command contains no operator-provided shell fragment.
  printf '%s' "find /var/lib/velox-worker/smoke -maxdepth 1 -type f -name 'smoke-*.*' -printf '%f\\n' 2>/dev/null || true; find /tmp/velox-smoke -mindepth 2 -maxdepth 2 -type f -printf '%P\\n' 2>/dev/null || true"
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

rw_lifecycle_checks() {
  local started finished elapsed body operation_id duplicate_body state health scheduling
  local diagnostic overall="PASS" health_body health_report smoke_passed smoke_value
  local resume_queued_at resume_queued_epoch resume_started_at resume_finished_at
  local resume_started_epoch resume_finished_epoch smoke_collected_at smoke_collected_epoch
  local -a RW_LIFECYCLE_RESULTS=()

  for bin in jq curl; do
    command -v "$bin" >/dev/null 2>&1 || {
      rw_lifecycle_record W00 prerequisites FAIL "missing local prerequisite: ${bin}" 0
      overall="FAIL"
    }
  done
  if [[ "$overall" != "PASS" ]]; then
    jq -n --arg worker_id "${WORKER_ID:-}" --arg overall "$overall" \
      --argjson checks "$(printf '%s\n' "${RW_LIFECYCLE_RESULTS[@]}" | jq -s '.')" \
      '{schema:"velox.remote_worker.lifecycle.v1",worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
    return 2
  fi

  # W04 — drain immediately excludes the worker from placement and a
  # second drain is rejected with HTTP 409 without another operation.
  started="$(rw_now_s)"
  diagnostic=""
  if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/drain" \
    "$(jq -nc --arg reason "remote worker W04 drain certification" '{reason:$reason}')"; then
    diagnostic="drain POST transport failed (rc=${RW_LAST_CURL_RC})"
  elif [[ "$RW_LAST_HTTP_STATUS" != "202" ]]; then
    diagnostic="drain POST returned HTTP ${RW_LAST_HTTP_STATUS}: ${RW_LAST_BODY}"
  else
    operation_id="$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
    [[ -n "$operation_id" ]] || diagnostic="drain response omitted operation_id"
    [[ "$(jq -r '.op // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "drain" ]] || diagnostic="drain response op is not drain"
    [[ "$(jq -r '.status // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "QUEUED" ]] || diagnostic="drain response status is not QUEUED"
  fi
  if [[ -z "$diagnostic" ]]; then
    body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
    state="$(rw_lifecycle_worker_state "$body")"
    [[ "${state%%|*}" == "CONNECTED" ]] || diagnostic="worker connection state after drain is ${state} (expected CONNECTED with drain exclusion)"
    [[ "$(jq -r '.drain // false' <<<"$body" 2>/dev/null || true)" == "true" ]] || diagnostic="admin worker drain flag is not true after drain"
    scheduling="${state##*|}"
    [[ "$scheduling" == "DRAINING" ]] || diagnostic="worker scheduling state after drain is ${scheduling:-<empty>} (expected DRAINING)"
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_lifecycle_poll_operation "$operation_id"; then
      diagnostic="${RW_LIFECYCLE_POLL_ERROR}"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_lifecycle_operation_matches "$RW_LIFECYCLE_POLL_BODY" "$operation_id" drain; then
      diagnostic="drain operation identity mismatch in terminal response"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/drain" \
      '{"reason":"duplicate W04 drain"}'; then
      diagnostic="duplicate drain transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "409" ]]; then
      diagnostic="duplicate drain returned HTTP ${RW_LAST_HTTP_STATUS}, expected 409"
    elif [[ "$(jq -r '.error // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" != *DRAINING* ]]; then
      diagnostic="duplicate drain 409 did not explain DRAINING: ${RW_LAST_BODY}"
    elif [[ -n "$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" ]]; then
      diagnostic="duplicate drain 409 unexpectedly returned operation_id"
    fi
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_lifecycle_record W04 drain_placement FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_lifecycle_record W04 drain_placement PASS "HTTP 202 + operation SUCCEEDED; CONNECTED with drain=true/scheduling=DRAINING; duplicate drain HTTP 409" "$elapsed"
  fi

  # W05 — resume is asynchronous and the worker may become HEALTHY only
  # after a fresh Level D smoke is green. Health D is checked explicitly.
  started="$(rw_now_s)"
  diagnostic=""
  if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/resume" \
    "$(jq -nc --arg reason "remote worker W05 resume certification" '{reason:$reason}')"; then
    diagnostic="resume POST transport failed (rc=${RW_LAST_CURL_RC})"
  elif [[ "$RW_LAST_HTTP_STATUS" != "202" ]]; then
    diagnostic="resume POST returned HTTP ${RW_LAST_HTTP_STATUS}: ${RW_LAST_BODY}"
  else
    operation_id="$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
    resume_queued_at="$(jq -r '.queued_at // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
    [[ -n "$operation_id" ]] || diagnostic="resume response omitted operation_id"
    [[ -n "$resume_queued_at" ]] || diagnostic="resume response omitted queued_at"
    resume_queued_epoch="$(rw_worker_heartbeat_epoch "$resume_queued_at" 2>/dev/null || true)"
    [[ -n "$resume_queued_epoch" ]] || diagnostic="resume queued_at is not valid RFC3339"
    [[ "$(jq -r '.op // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "resume" ]] || diagnostic="resume response op is not resume"
    [[ "$(jq -r '.status // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "QUEUED" ]] || diagnostic="resume response status is not QUEUED"
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_lifecycle_poll_operation "$operation_id"; then
      diagnostic="${RW_LIFECYCLE_POLL_ERROR}"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_lifecycle_operation_matches "$RW_LIFECYCLE_POLL_BODY" "$operation_id" resume; then
      diagnostic="resume operation identity mismatch in terminal response"
    fi
  fi
  # Require the successful resume operation's fresh Level D gate and then
  # observe a healthy Level D report after the operation completed.
  if [[ -z "$diagnostic" ]]; then
    # A successful resume is the authoritative correlation point: the real
    # ResumeExecutor runs a fresh Level D smoke synchronously inside the
    # operation and clears drain only after that smoke succeeds. Require the
    # terminal operation timestamps so a fake/partial 202 cannot be mistaken
    # for a completed smoke gate.
    resume_started_at="$(jq -r '.started_at // empty' <<<"$RW_LIFECYCLE_POLL_BODY" 2>/dev/null || true)"
    resume_finished_at="$(jq -r '.finished_at // empty' <<<"$RW_LIFECYCLE_POLL_BODY" 2>/dev/null || true)"
    resume_started_epoch="$(rw_worker_heartbeat_epoch "$resume_started_at" 2>/dev/null || true)"
    resume_finished_epoch="$(rw_worker_heartbeat_epoch "$resume_finished_at" 2>/dev/null || true)"
    [[ -n "$resume_started_epoch" ]] || diagnostic="resume operation SUCCEEDED without valid started_at"
    [[ -n "$resume_finished_epoch" ]] || diagnostic="resume operation SUCCEEDED without valid finished_at"
    if [[ -z "$diagnostic" ]]; then
      (( resume_started_epoch >= resume_queued_epoch )) || diagnostic="resume operation started before it was queued (started_at=${resume_started_at}, queued_at=${resume_queued_at})"
      (( resume_finished_epoch >= resume_started_epoch )) || diagnostic="resume operation finished before it started (finished_at=${resume_finished_at}, started_at=${resume_started_at})"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_admin_request GET "/api/v1/admin/workers/${WORKER_ID}/health?level=D"; then
      diagnostic="Level D health GET transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "200" ]]; then
      diagnostic="Level D health GET returned HTTP ${RW_LAST_HTTP_STATUS}"
    else
      health_body="$RW_LAST_BODY"
      [[ "$(jq -r '.level // empty' <<<"$health_body" 2>/dev/null || true)" == "D" ]] || diagnostic="health report level is not D"
      [[ "$(jq -r '.healthy // false' <<<"$health_body" 2>/dev/null || true)" == "true" ]] || diagnostic="Level D smoke report is not healthy: ${health_body}"
      smoke_passed="$(jq -r '.checks.smoke_ok.passed // false' <<<"$health_body" 2>/dev/null || true)"
      smoke_value="$(jq -r '.checks.smoke_ok.value // empty' <<<"$health_body" 2>/dev/null || true)"
      [[ "$smoke_passed" == "true" && -n "$smoke_value" ]] || diagnostic="Level D smoke gate lacks passed smoke_ok/artifact evidence"
      smoke_collected_at="$(jq -r '.collected_at // empty' <<<"$health_body" 2>/dev/null || true)"
      smoke_collected_epoch="$(rw_worker_heartbeat_epoch "$smoke_collected_at" 2>/dev/null || true)"
      # `collected_at` is the health-probe timestamp, not the smoke_runs
      # timestamp. Do not present it as direct smoke-run evidence. The
      # authoritative freshness proof is the successful resume operation:
      # ResumeExecutor runs a new smoke within that operation and only then
      # clears drain. The D probe is sampled after operation completion and
      # must still be healthy with a non-empty artifact value.
      [[ -n "$smoke_collected_epoch" && -n "$resume_finished_epoch" && "$smoke_collected_epoch" -ge "$resume_finished_epoch" ]] || diagnostic="Level D probe was not collected after the successful resume operation (collected_at=${smoke_collected_at:-<empty>}, finished_at=${resume_finished_at:-<empty>})"
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
    state="$(rw_lifecycle_worker_state "$body")"
    [[ "${state%%|*}" == "CONNECTED" ]] || diagnostic="worker connection state after resume is ${state}"
    health="${state#*|}"; health="${health%%|*}"
    [[ "$health" == "HEALTHY" ]] || diagnostic="worker health after resume is ${health:-<empty>} (expected HEALTHY)"
    [[ "$(jq -r '.drain // false' <<<"$body" 2>/dev/null || true)" == "false" ]] || diagnostic="worker drain flag remains true after successful resume"
    scheduling="${state##*|}"
    [[ "$scheduling" != "DRAINING" && "$scheduling" != "QUARANTINED" ]] || diagnostic="worker scheduling state remains excluded: ${scheduling}"
  fi
  if [[ -z "$diagnostic" ]]; then
    if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/resume" \
      '{"reason":"duplicate W05 resume"}'; then
      diagnostic="duplicate resume transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "409" ]]; then
      diagnostic="duplicate resume returned HTTP ${RW_LAST_HTTP_STATUS}, expected 409"
    elif [[ "$(jq -r '.error // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" != *HEALTHY* ]]; then
      diagnostic="duplicate resume 409 did not explain HEALTHY no-op: ${RW_LAST_BODY}"
    elif [[ -n "$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" ]]; then
      diagnostic="duplicate resume 409 unexpectedly returned operation_id"
    fi
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_lifecycle_record W05 resume_smoke_gate FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_lifecycle_record W05 resume_smoke_gate PASS "HTTP 202 + operation SUCCEEDED with started_at/finished_at; fresh Level D smoke gate PASS; worker CONNECTED/HEALTHY; duplicate resume HTTP 409" "$elapsed"
  fi

  jq -n --arg schema 'velox.remote_worker.lifecycle.v1' \
    --arg worker_id "$WORKER_ID" --arg overall "$overall" \
    --argjson checks "$(printf '%s\n' "${RW_LIFECYCLE_RESULTS[@]}" | jq -s '.')" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{schema:$schema,worker_id:$worker_id,checks:$checks,overall:$overall,generated_at:$generated_at}'
  [[ "$overall" == "PASS" ]]
}

rw_update_record() {
  local id="$1" name="$2" status="$3" diagnostic="$4" elapsed_ms="$5" evidence="${6:-}"
  RW_UPDATE_RESULTS+=("$(jq -cn \
    --arg id "$id" --arg name "$name" --arg status "$status" \
    --arg diagnostic "$diagnostic" --arg evidence "$evidence" --argjson elapsed_ms "$elapsed_ms" \
    '{id:$id,name:$name,status:$status,elapsed_ms:$elapsed_ms,diagnostic:$diagnostic,evidence:(if $evidence == "" then null else $evidence end)}')")
}

rw_update_active_lease_count() {
  local body="$1"
  jq -er '
    if (.active_tasks? != null) then (.active_tasks | tonumber)
    elif (.active_slots? != null) then (.active_slots | tonumber)
    elif (.active_jobs? | type) == "number" then .active_jobs
    elif (.active_jobs? | type) == "array" then (.active_jobs | length)
    else empty
    end
  ' <<<"$body" 2>/dev/null
}

rw_update_poll_idle() {
  local deadline=$(( $(date +%s) + RW_UPDATE_LEASE_TIMEOUT_S )) body="" active="" detail=""
  RW_UPDATE_IDLE_BODY=""
  RW_UPDATE_IDLE_ERROR=""
  while (( $(date +%s) < deadline )); do      # The admin card's active_jobs/active_slots values are hydrated from
      # the authoritative lease projection; the diagnostic endpoint's
      # active_tasks value is heartbeat telemetry only.
      body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
      active="$(rw_update_active_lease_count "$body" 2>/dev/null || true)"
    if [[ "$active" =~ ^[0-9]+$ && "$active" == "0" ]]; then
      RW_UPDATE_IDLE_BODY="$body"
      return 0
    fi
    sleep "$RW_UPDATE_LEASE_POLL_INTERVAL_S"
  done
  detail="$(rw_update_active_lease_count "$body" 2>&1 || true)"
  RW_UPDATE_IDLE_ERROR="active lease check timed out after ${RW_UPDATE_LEASE_TIMEOUT_S}s: active_tasks=${detail:-<missing>}${body:+; last worker snapshot received}"
  export RW_UPDATE_IDLE_ERROR
  printf '%s' "$RW_UPDATE_IDLE_ERROR"
  return 1
}

rw_update_release_matches() {
  local body="$1" expected_digest="$2" actual_digest
  actual_digest="$(jq -r '.release_identity.image_digest // empty' <<<"$body" 2>/dev/null || true)"
  [[ "$actual_digest" == "$expected_digest" ]] || {
    printf 'ReleaseIdentity.image_digest=%s (expected %s)' "${actual_digest:-<empty>}" "$expected_digest"
    return 1
  }
  rw_worker_release_diagnostic "$body" || return 1
}

rw_update_health_smoke_ok() {
  local body="$1" smoke_passed smoke_value
  [[ "$(jq -r '.level // empty' <<<"$body" 2>/dev/null || true)" == "D" ]] || {
    printf 'health report level is not D'
    return 1
  }
  [[ "$(jq -r '.healthy // false' <<<"$body" 2>/dev/null || true)" == "true" ]] || {
    printf 'Level D report is not healthy'
    return 1
  }
  smoke_passed="$(jq -r '.checks.smoke_ok.passed // false' <<<"$body" 2>/dev/null || true)"
  smoke_value="$(jq -r '.checks.smoke_ok.value // empty' <<<"$body" 2>/dev/null || true)"
  [[ "$smoke_passed" == "true" && -n "$smoke_value" ]] || {
    printf 'Level D smoke_ok evidence is missing or not passed'
    return 1
  }
}

rw_update_checks() {
  local started finished elapsed body pre_body post_body health_body operation_id
  local diagnostic overall="PASS" target_digest target_image old_digest old_hb new_hb
  local old_hb_epoch new_hb_epoch active_count state scheduling health_state
  local -a RW_UPDATE_RESULTS=()

  for bin in jq curl; do
    command -v "$bin" >/dev/null 2>&1 || {
      rw_update_record U00 prerequisites FAIL "missing local prerequisite: ${bin}" 0
      overall="FAIL"
    }
  done
  target_image="${RW_UPDATE_TARGET_IMAGE:-}"
  target_digest="${RW_UPDATE_TARGET_DIGEST:-}"
  if [[ -z "$target_image" || -z "$target_digest" ]]; then
    rw_update_record U00 configuration FAIL 'RW_UPDATE_TARGET_IMAGE and RW_UPDATE_TARGET_DIGEST are required for update certification' 0
    overall="FAIL"
  fi
  if [[ "$overall" != "PASS" ]]; then
    jq -n --arg worker_id "${WORKER_ID:-}" --arg overall "$overall" \
      --argjson checks "$(printf '%s\n' "${RW_UPDATE_RESULTS[@]}" | jq -s '.')" \
      '{schema:"velox.remote_worker.update.v1",worker_id:$worker_id,target_digest:(env.RW_UPDATE_TARGET_DIGEST // ""),checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
    return 2
  fi

  # U01 — capture the pre-update identity and heartbeat. The worker must be
  # connected before the mutating operation is published.
  started="$(rw_now_s)"
  pre_body="$(rw_worker_admin_get "/api/v1/workers/${WORKER_ID}" 2>/dev/null || true)"
  diagnostic=""
  if [[ -z "$pre_body" ]]; then
    diagnostic='pre-update worker snapshot request failed'
  else
    rw_worker_snapshot_ok "$pre_body" "$WORKER_ID" || diagnostic="$(rw_worker_snapshot_ok "$pre_body" "$WORKER_ID" 2>&1)"
  fi
  old_digest="$(jq -r '.release_identity.image_digest // empty' <<<"$pre_body" 2>/dev/null || true)"
  old_hb="$(jq -r '.last_heartbeat_at // empty' <<<"$pre_body" 2>/dev/null || true)"
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U01 pre_update_identity FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U01 pre_update_identity PASS "CONNECTED worker with complete ReleaseIdentity; previous_digest=${old_digest}" "$elapsed"
  fi

  # U02 — publish the canonical update operation. The Master/UpdateExecutor
  # owns the automatic drain, idle wait, digest verification, restart and
  # forward Level-D smoke; this harness only observes its public contract.
  started="$(rw_now_s)"
  diagnostic=""
  if [[ "$overall" == "PASS" ]]; then
    local update_payload
    # The admin API calls the immutable image reference target_digest;
    # ReleaseIdentity.image_digest is the bare sha256 suffix checked later.
    update_payload="$(jq -cn --arg target_digest "$target_image" --arg reason "${RW_UPDATE_REASON}" '{target_digest:$target_digest,reason:$reason}')"
    if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/update" "$update_payload"; then
      diagnostic="update POST transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "202" ]]; then
      diagnostic="update POST returned HTTP ${RW_LAST_HTTP_STATUS}: ${RW_LAST_BODY}"
    else
      operation_id="$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
      [[ -n "$operation_id" ]] || diagnostic='update response omitted operation_id'
      [[ "$(jq -r '.op // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "update" ]] || diagnostic='update response op is not update'
      [[ "$(jq -r '.status // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)" == "QUEUED" ]] || diagnostic='update response status is not QUEUED'
    fi
  else
    diagnostic='previous update-certification check failed'
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U02 update_queued FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U02 update_queued PASS "HTTP 202; operation_id=${operation_id}; target_digest=${target_digest}" "$elapsed" "$operation_id"
  fi

  # U03 — observe the executor's automatic drain and canonical active-task
  # signal. Do this before waiting for terminal status so a stuck lease is
  # reported as its own certification failure, not as an opaque timeout.
  started="$(rw_now_s)"
  diagnostic=""
  if [[ "$overall" == "PASS" ]]; then
    if rw_update_poll_idle; then
      active_count="$(rw_update_active_lease_count "$RW_UPDATE_IDLE_BODY" 2>/dev/null || true)"
      [[ "$active_count" == "0" ]] || diagnostic="active lease count after drain is ${active_count}, expected 0"
      scheduling="$(jq -r '.scheduling // .scheduling_state // empty' <<<"$RW_UPDATE_IDLE_BODY" 2>/dev/null || true)"
      state="$(jq -r '.status // empty' <<<"$RW_UPDATE_IDLE_BODY" 2>/dev/null || true)"
      if [[ "$scheduling" != "DRAINING" && "$state" != "DRAINING" && "$(jq -r '.drain // false' <<<"$RW_UPDATE_IDLE_BODY" 2>/dev/null || true)" != "true" ]]; then
        diagnostic="automatic drain was not observable before idle: status=${state:-<empty>}; scheduling=${scheduling:-<empty>}"
      fi
    else
      diagnostic="${RW_UPDATE_IDLE_ERROR:-active lease check failed}"
    fi
  else
    diagnostic='update operation was not queued'
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U03 drain_idle FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U03 drain_idle PASS 'automatic drain observed; active_tasks/active_slots=0 before update completion' "$elapsed"
  fi

  # U04 — wait for the UpdateExecutor's complete terminal cascade. It
  # includes restart, reconnect/readiness, target digest validation, and a
  # fresh Level-D smoke before SUCCEEDED is written.
  started="$(rw_now_s)"
  diagnostic=""
  if [[ "$overall" == "PASS" ]]; then
    if ! rw_lifecycle_poll_operation "$operation_id"; then
      diagnostic="$RW_LIFECYCLE_POLL_ERROR"
    elif ! rw_lifecycle_operation_matches "$RW_LIFECYCLE_POLL_BODY" "$operation_id" update; then
      diagnostic='update operation identity mismatch in terminal response'
    fi
    if [[ -z "$diagnostic" ]]; then
      [[ "$(jq -r '.status // empty' <<<"$RW_LIFECYCLE_POLL_BODY")" == "SUCCEEDED" ]] || diagnostic='update operation did not reach SUCCEEDED'
    fi
  else
    diagnostic='update operation was not eligible for polling'
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U04 update_cascade FAIL "$diagnostic" "$elapsed" "${operation_id:-}"
    overall="FAIL"
  else
    rw_update_record U04 update_cascade PASS 'UpdateExecutor reached SUCCEEDED after digest/restart/readiness/master/smoke cascade' "$elapsed" "$operation_id"
  fi

  # U05 — verify post-restart identity and smoke evidence from public reads.
  started="$(rw_now_s)"
  diagnostic=""
  post_body="$(rw_worker_poll_connected 2>/dev/null || true)"
  if [[ "$overall" == "PASS" && -z "$post_body" ]]; then
    diagnostic='post-update worker did not reconnect with a valid heartbeat'
  elif [[ "$overall" == "PASS" ]]; then
    rw_update_release_matches "$post_body" "$target_digest" || diagnostic="$(rw_update_release_matches "$post_body" "$target_digest" 2>&1)"
    new_hb="$(jq -r '.last_heartbeat_at // empty' <<<"$post_body")"
    [[ -n "$new_hb" ]] || diagnostic="${diagnostic}${diagnostic:+; }post-update heartbeat is empty"
    old_hb_epoch="$(rw_worker_heartbeat_epoch "$old_hb" 2>/dev/null || true)"
    new_hb_epoch="$(rw_worker_heartbeat_epoch "$new_hb" 2>/dev/null || true)"
    if [[ -n "$old_hb_epoch" && -n "$new_hb_epoch" ]]; then
      (( new_hb_epoch >= old_hb_epoch )) || diagnostic="${diagnostic}${diagnostic:+; }post-update heartbeat did not advance"
    fi
  elif [[ "$overall" != "PASS" ]]; then
    diagnostic='update cascade did not succeed'
  fi
  if [[ "$overall" == "PASS" ]]; then
    if ! rw_admin_request GET "/api/v1/admin/workers/${WORKER_ID}/health?level=D"; then
      diagnostic="Level D health GET transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "200" ]]; then
      diagnostic="Level D health GET returned HTTP ${RW_LAST_HTTP_STATUS}"
    else
      health_body="$RW_LAST_BODY"
      rw_update_health_smoke_ok "$health_body" || diagnostic="$(rw_update_health_smoke_ok "$health_body" 2>&1)"
    fi
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U05 post_update_release_smoke FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U05 post_update_release_smoke PASS "ReleaseIdentity digest=${target_digest}; reconnect heartbeat advanced; fresh Level D smoke evidence present" "$elapsed"
  fi

  # U06 — update normally releases its own drain after a green smoke. If the
  # worker is still excluded (operator-owned drain/quarantine), use the
  # canonical resume operation and verify its fresh smoke gate. A healthy
  # worker is an explicit PASS for automatic resume, not a false duplicate
  # resume request that would correctly return HTTP 409.
  started="$(rw_now_s)"
  diagnostic=""
  body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
  state="$(rw_lifecycle_worker_state "$body")"
  scheduling="$(jq -r '.scheduling_state // empty' <<<"$body" 2>/dev/null || true)"
  if [[ "$overall" != "PASS" ]]; then
    diagnostic='post-update checks failed; resume was not attempted'
  elif [[ "$scheduling" == "DRAINING" || "$scheduling" == "QUARANTINED" || "$scheduling" == "RESUMING" ]]; then
    if ! rw_admin_request POST "/api/v1/admin/workers/${WORKER_ID}/resume" \
      "$(jq -nc --arg reason "${RW_UPDATE_REASON}; resume certification" '{reason:$reason}')"; then
      diagnostic="resume POST transport failed (rc=${RW_LAST_CURL_RC})"
    elif [[ "$RW_LAST_HTTP_STATUS" != "202" ]]; then
      diagnostic="resume POST returned HTTP ${RW_LAST_HTTP_STATUS}: ${RW_LAST_BODY}"
    else
      local resume_operation_id
      resume_operation_id="$(jq -r '.operation_id // empty' <<<"$RW_LAST_BODY" 2>/dev/null || true)"
      [[ -n "$resume_operation_id" ]] || diagnostic='resume response omitted operation_id'
      if [[ -z "$diagnostic" ]] && ! rw_lifecycle_poll_operation "$resume_operation_id"; then
        diagnostic="$RW_LIFECYCLE_POLL_ERROR"
      fi
      if [[ -z "$diagnostic" ]] && ! rw_lifecycle_operation_matches "$RW_LIFECYCLE_POLL_BODY" "$resume_operation_id" resume; then
        diagnostic='resume operation identity mismatch'
      fi
      if [[ -z "$diagnostic" ]] && [[ "$(jq -r '.status // empty' <<<"$RW_LIFECYCLE_POLL_BODY")" != "SUCCEEDED" ]]; then
        diagnostic='resume operation did not reach SUCCEEDED'
      fi
    fi
  fi
  if [[ -z "$diagnostic" ]]; then
    body="$(rw_worker_admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
    state="$(rw_lifecycle_worker_state "$body")"
    scheduling="$(jq -r '.scheduling_state // empty' <<<"$body" 2>/dev/null || true)"
    health_state="$(jq -r '.health_state // empty' <<<"$body" 2>/dev/null || true)"
    [[ "${state%%|*}" == "CONNECTED" ]] || diagnostic="worker state after resume is ${state}"
    [[ "$scheduling" == "AVAILABLE" ]] || diagnostic="worker scheduling state after resume is ${scheduling:-<empty>} (expected AVAILABLE)"
    [[ "$health_state" == "HEALTHY" ]] || diagnostic="worker health state after resume is ${health_state:-<empty>} (expected HEALTHY)"
  fi
  finished="$(rw_now_s)"; elapsed=$(( (finished - started) * 1000 ))
  if [[ -n "$diagnostic" ]]; then
    rw_update_record U06 resume FAIL "$diagnostic" "$elapsed"
    overall="FAIL"
  else
    rw_update_record U06 resume PASS 'worker returned to CONNECTED and placement-eligible state after successful update smoke' "$elapsed"
  fi

  jq -n --arg schema 'velox.remote_worker.update.v1' \
    --arg worker_id "$WORKER_ID" --arg target_digest "$target_digest" \
    --arg target_image "$target_image" --arg overall "$overall" \
    --arg previous_digest "$old_digest" --arg operation_id "${operation_id:-}" \
    --argjson checks "$(printf '%s\n' "${RW_UPDATE_RESULTS[@]}" | jq -s '.')" \
    '{schema:$schema,worker_id:$worker_id,target_image:$target_image,target_digest:$target_digest,previous_digest:(if $previous_digest=="" then null else $previous_digest end),operation_id:(if $operation_id=="" then null else $operation_id end),checks:$checks,overall:$overall,generated_at:(now|todateiso8601)}'
  [[ "$overall" == "PASS" ]]
}

rw_update_config_failure() {
  local diagnostic="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n --arg worker_id "${WORKER_ID:-${VELOX_WORKER_ID:-}}" --arg diagnostic "$diagnostic" \
      '{schema:"velox.remote_worker.update.v1",worker_id:$worker_id,checks:[{id:"U00",name:"configuration",status:"FAIL",elapsed_ms:0,diagnostic:$diagnostic}],overall:"FAIL",generated_at:(now|todateiso8601)}'
  else
    printf '%s\n' '{"schema":"velox.remote_worker.update.v1","checks":[{"id":"U00","name":"configuration","status":"FAIL","elapsed_ms":0,"diagnostic":"configuration validation failed"}],"overall":"FAIL"}'
  fi
}

rw_lifecycle_config_failure() {
  local diagnostic="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n \
      --arg worker_id "${WORKER_ID:-${VELOX_WORKER_ID:-}}" \
      --arg diagnostic "$diagnostic" \
      --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      '{schema:"velox.remote_worker.lifecycle.v1",worker_id:$worker_id,checks:[{id:"W00",name:"configuration",status:"FAIL",elapsed_ms:0,diagnostic:$diagnostic}],overall:"FAIL",generated_at:$generated_at}'
  else
    printf '%s\n' '{"schema":"velox.remote_worker.lifecycle.v1","checks":[{"id":"W00","name":"configuration","status":"FAIL","elapsed_ms":0,"diagnostic":"configuration validation failed"}],"overall":"FAIL"}'
  fi
}

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

rw_run_certification() {
  local mode="$1" runner="$2" failure_renderer="$3" loader_arg="${4:-}" raw_file config_error_file config_diagnostic rc
  RW_CERT_MODE="$mode"
  export RW_CERT_MODE
  rw_init_artifacts || {
    rw_die "unable to initialize certification artifacts"
    return 2
  }
  raw_file="$(mktemp "${TMPDIR:-/tmp}/velox-cert-raw.XXXXXX")" || return 2
  config_error_file="$(mktemp "${TMPDIR:-/tmp}/velox-cert-config-error.XXXXXX")" || {
    rm -f -- "$raw_file"
    return 2
  }
  if [[ "$loader_arg" == "--network-only" ]]; then
    if rw_load_config --network-only 2>"$config_error_file"; then
      rm -f -- "$config_error_file"
      if "$runner" >"$raw_file"; then rc=0; else rc=$?; fi
    else
      config_diagnostic="$(cat "$config_error_file")"
      config_diagnostic="${config_diagnostic//$'\n'/; }"
      "$failure_renderer" "${config_diagnostic:-configuration validation failed}" >"$raw_file"
      rc=2
    fi
  elif rw_load_config 2>"$config_error_file"; then
    rm -f -- "$config_error_file"
    if "$runner" >"$raw_file"; then rc=0; else rc=$?; fi
  else
    config_diagnostic="$(cat "$config_error_file")"
    config_diagnostic="${config_diagnostic//$'\n'/; }"
    "$failure_renderer" "${config_diagnostic:-configuration validation failed}" >"$raw_file"
    rc=2
  fi
  rm -f -- "$config_error_file"
  cat "$raw_file"
  rw_finalize_artifacts "$raw_file" "$rc" "$mode"
  rm -f -- "$raw_file"
  return "$rc"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail

  case "${1:-}" in
    --network|--network-json)
      shift
      [[ "$#" -eq 0 ]] || { rw_die "network mode does not accept positional arguments"; exit 2; }
      rw_run_certification network rw_network_checks rw_network_prereq_failure --network-only
      ;;
    --worker|--worker-json)
      shift
      [[ "$#" -eq 0 ]] || { rw_die "worker mode does not accept positional arguments"; exit 2; }
      rw_run_certification worker rw_worker_checks rw_worker_config_failure
      ;;
    --lifecycle|--lifecycle-json)
      shift
      [[ "$#" -eq 0 ]] || { rw_die "lifecycle mode does not accept positional arguments"; exit 2; }
      rw_run_certification lifecycle rw_lifecycle_checks rw_lifecycle_config_failure
      ;;
    --update|--update-json)
      shift
      [[ "$#" -eq 0 ]] || { rw_die "update mode does not accept positional arguments"; exit 2; }
      rw_run_certification update rw_update_checks rw_update_config_failure
      ;;
    --smoke|--smoke-json)
      shift
      [[ "$#" -eq 0 ]] || { rw_die "smoke mode does not accept positional arguments"; exit 2; }
      rw_run_certification smoke rw_smoke_checks rw_smoke_config_failure
      ;;
    --job|--job-json)
      shift
      [[ "$#" -eq 0 ]] || { rw_die "job mode does not accept positional arguments"; exit 2; }
      rw_run_certification job rw_job_checks rw_job_config_failure
      ;;
    --help|-h)
      printf '%s\n' 'Usage: remote-worker-cert-config.sh [--network-json]' \
        'Default mode runs local preflight only.' \
        '--network-json runs R01-R04 and emits one JSON document on stdout.' \
        '--worker-json runs W01-W03 (restart, identity, heartbeat) and emits JSON.' \
        '--lifecycle-json runs W04-W05 (drain, placement, resume, Level D smoke) and emits JSON.' \
        '--update-json runs U01-U06 (automatic drain/idle, digest, ReleaseIdentity, restart, smoke, resume) and emits JSON.' \
        '--smoke-json runs P01 real Level D smoke and emits JSON.' \
        '--job-json runs P02 job E2E polling and P03 artifact verification and emits JSON.'
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
