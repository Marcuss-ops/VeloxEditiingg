#!/usr/bin/env bash
# =============================================================================
# Velox staging acceptance runner
# =============================================================================
# Starts one isolated master, two workers, a local artifact tree, and a runtime
# generated video fixture. It never stores credentials or fixture binaries in
# the repository. Images are accepted only as immutable @sha256 references.
#
# Usage:
#   cp tests/e2e/staging-acceptance/staging.env.example /tmp/velox-staging.env
#   $EDITOR /tmp/velox-staging.env
#   bash tests/e2e/staging-acceptance/run.sh --env-file /tmp/velox-staging.env
#
# The runner is intentionally orchestration-only: it does not silently invent
# image digests and it does not contact production URLs by default.
# =============================================================================

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yml"
ENV_FILE="${VELOX_STAGING_ENV_FILE:-}"
KEEP_WORKDIR="${VELOX_STAGING_KEEP_WORKDIR:-1}"
COMPOSE_BIN="docker compose"
COMPOSE_CMD=(docker compose)
PROJECT="${VELOX_STAGING_PROJECT:-velox-staging-acceptance}"
WORKDIR="${VELOX_STAGING_WORKDIR:-/tmp/${PROJECT}}"
MASTER_URL="${VELOX_STAGING_MASTER_URL:-http://127.0.0.1:18080}"
MASTER_URL="${MASTER_URL%/}"
POLL_SECONDS="${VELOX_STAGING_POLL_SECONDS:-2}"
OPERATION_TIMEOUT="${VELOX_STAGING_OPERATION_TIMEOUT:-300}"
READINESS_TIMEOUT="${VELOX_STAGING_READINESS_TIMEOUT:-180}"

usage() {
  cat <<'EOF'
Usage: run.sh --env-file PATH

Required runtime variables in PATH:
  VELOX_SERVER_IMAGE, VELOX_WORKER_IMAGE_A, VELOX_WORKER_IMAGE_B
  VELOX_STAGING_ADMIN_TOKEN, VELOX_WORKER_SECRET_A, VELOX_WORKER_SECRET_B

Optional:
  VELOX_STAGING_KEEP_WORKDIR=1 (preserve evidence; default)
  VELOX_STAGING_OPERATION_TIMEOUT=300
  VELOX_STAGING_READINESS_TIMEOUT=180
EOF
}

info() { printf '[staging-acceptance][INFO] %s\n' "$*"; }
pass() { printf '[staging-acceptance][PASS] %s\n' "$*"; }
fatal() { printf '[staging-acceptance][FAIL] %s\n' "$*" >&2; exit 1; }

((${#COMPOSE_CMD[@]} > 0)) || fatal "COMPOSE_BIN must not be empty"

while (($#)); do
  case "$1" in
    --env-file)
      (($# >= 2)) || fatal "--env-file requires a path"
      ENV_FILE="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      fatal "unknown argument: $1 (use --help)"
      ;;
  esac
done

[[ -n "$ENV_FILE" ]] || fatal "an env file is required; use --env-file outside the repository"
[[ -r "$ENV_FILE" ]] || fatal "env file is not readable: $ENV_FILE"

load_env_file() {
  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line//[[:space:]]/}" || "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]] || \
      fatal "invalid env-file line (expected KEY=VALUE): ${line%%=*}"
    key="${BASH_REMATCH[1]}"
    case "$key" in
      VELOX_*|COMPOSE_BIN) ;;
      *) fatal "unsupported env-file key: $key" ;;
    esac
    value="${BASH_REMATCH[2]}"
    value="${value#${value%%[![:space:]]*}}"
    value="${value%${value##*[![:space:]]}}"
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi
    [[ "$value" != *$'\r'* && "$value" != *$'\n'* ]] || \
      fatal "env-file value for $key contains CR/LF"
    printf -v "$key" '%s' "$value"
    export "$key"
  done <"$ENV_FILE"
}

# Parse a dotenv-like file as data, never as shell code. This prevents a
# staging env file from executing command substitutions or arbitrary commands.
load_env_file

if [[ -n "${COMPOSE_BIN:-}" && "${COMPOSE_BIN}" != "docker compose" ]]; then
  fatal "COMPOSE_BIN is fixed to the canonical 'docker compose' command"
fi
PROJECT="${VELOX_STAGING_PROJECT:-$PROJECT}"
[[ "$PROJECT" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || fatal "VELOX_STAGING_PROJECT contains invalid Compose project-name characters"
WORKDIR="${VELOX_STAGING_WORKDIR:-$WORKDIR}"
MASTER_URL="${VELOX_STAGING_MASTER_URL:-http://127.0.0.1:${VELOX_STAGING_REST_PORT:-18080}}"
MASTER_URL="${MASTER_URL%/}"
[[ "$MASTER_URL" =~ ^https?://[^/[:space:]]+$ ]] || fatal "VELOX_STAGING_MASTER_URL must be an absolute http(s) URL"
if [[ "${VELOX_STAGING_ALLOW_REMOTE:-0}" != "1" && ! "$MASTER_URL" =~ ^https?://(127\.0\.0\.1|localhost)(:[0-9]+)?$ ]]; then
  fatal "non-local VELOX_STAGING_MASTER_URL requires VELOX_STAGING_ALLOW_REMOTE=1"
fi
KEEP_WORKDIR="${VELOX_STAGING_KEEP_WORKDIR:-$KEEP_WORKDIR}"
POLL_SECONDS="${VELOX_STAGING_POLL_SECONDS:-$POLL_SECONDS}"
OPERATION_TIMEOUT="${VELOX_STAGING_OPERATION_TIMEOUT:-$OPERATION_TIMEOUT}"
READINESS_TIMEOUT="${VELOX_STAGING_READINESS_TIMEOUT:-$READINESS_TIMEOUT}"
[[ "$WORKDIR" = /* ]] || fatal "VELOX_STAGING_WORKDIR must be an absolute path for bind-mount isolation"
VELOX_WORKER_ID_A="${VELOX_WORKER_ID_A:-worker-e2e-a}"
VELOX_WORKER_ID_B="${VELOX_WORKER_ID_B:-worker-e2e-b}"
for worker_id in "$VELOX_WORKER_ID_A" "$VELOX_WORKER_ID_B"; do
  [[ "$worker_id" =~ ^[a-z][a-z0-9_-]{2,62}$ ]] || fatal "worker ID has invalid canonical shape: $worker_id"
done
export PROJECT WORKDIR VELOX_WORKER_ID_A VELOX_WORKER_ID_B
COMPOSE_ARGS=(-p "$PROJECT" -f "$COMPOSE_FILE" --env-file "$ENV_FILE")

require_bin() { command -v "$1" >/dev/null 2>&1 || fatal "required command not found: $1"; }
require_bin docker
require_bin curl
require_bin jq
require_bin ffmpeg
require_bin sha256sum

# Verify Compose is available without leaking env contents.
"${COMPOSE_CMD[@]}" version >/dev/null 2>&1 || fatal "Docker Compose is unavailable: $COMPOSE_BIN"

validate_digest() {
  local name="$1" value="${!1:-}"
  [[ -n "$value" ]] || fatal "$name is required"
  [[ "$value" =~ ^[^@[:space:]]+@sha256:[0-9a-fA-F]{64}$ ]] || fatal "$name must be an immutable @sha256:<64 hex> reference"
}

validate_secret() {
  local name="$1" value="${!1:-}"
  [[ -n "$value" ]] || fatal "$name is required"
  [[ "$value" != *$'\r'* && "$value" != *$'\n'* ]] || fatal "$name must not contain CR/LF"
  [[ "$value" != CHANGE_ME_* ]] || fatal "$name still contains a placeholder"
}

validate_digest VELOX_SERVER_IMAGE
validate_digest VELOX_WORKER_IMAGE_A
validate_digest VELOX_WORKER_IMAGE_B
validate_secret VELOX_STAGING_ADMIN_TOKEN
validate_secret VELOX_WORKER_SECRET_A
validate_secret VELOX_WORKER_SECRET_B

WORKER_ID_A="$VELOX_WORKER_ID_A"
WORKER_ID_B="$VELOX_WORKER_ID_B"
[[ "$WORKER_ID_A" != "$WORKER_ID_B" ]] || fatal "worker IDs must be distinct"

mkdir -p "$WORKDIR"/{master,worker-a,worker-b,config,fixture,logs}
# The official images run as uid/gid 10001. This is a disposable local
# acceptance tree, so grant only this isolated workdir broad access instead
# of changing the image's non-root runtime user or production paths.
chmod 0777 "$WORKDIR" "$WORKDIR/master" "$WORKDIR/worker-a" "$WORKDIR/worker-b"
mkdir -p "$WORKDIR/master/data"/{staging,storage,videos}
chmod 0777 "$WORKDIR/master/data" "$WORKDIR/master/data/staging" "$WORKDIR/master/data/storage" "$WORKDIR/master/data/videos"
storage_probe="$WORKDIR/master/data/storage/.staging-acceptance-probe"
printf 'staging-acceptance\n' >"$storage_probe"
[[ "$(cat "$storage_probe")" == "staging-acceptance" ]] || fatal "isolated artifact storage is not readable"
rm -f "$storage_probe"

cleanup() {
  local rc=$?
  set +e
  info "stopping isolated Compose project ${PROJECT}"
  "${COMPOSE_CMD[@]}" "${COMPOSE_ARGS[@]}" down --remove-orphans -v >/dev/null 2>&1
  if [[ "$KEEP_WORKDIR" != "1" ]]; then
    rm -rf -- "$WORKDIR"
  else
    info "evidence preserved at $WORKDIR"
  fi
  return "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

write_worker_config() {
  local id="$1" out="$2"
  cat >"$out" <<JSON
{
  "master_url": "http://master:8000",
  "control_grpc_url": "master:9000",
  "worker_id": "${id}",
  "worker_name": "${id}",
  "work_dir": "/var/lib/velox-worker/work",
  "state_dir": "/var/lib/velox-worker",
  "environment": "staging",
  "allow_insecure_grpc_dev": true,
  "max_active_jobs": 1,
  "health_port": 8081,
  "prometheus_port": 0,
  "protocol_version": "v3"
}
JSON
}

info "generating isolated worker configs and ${VELOX_STAGING_FIXTURE_SECONDS:-5}s video fixture"
write_worker_config "$WORKER_ID_A" "$WORKDIR/config/worker-a.json"
write_worker_config "$WORKER_ID_B" "$WORKDIR/config/worker-b.json"
ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i "color=c=0x008080:s=320x180:d=${VELOX_STAGING_FIXTURE_SECONDS:-5}" \
  -f lavfi -i "anullsrc=r=48000:cl=mono" \
  -shortest -c:v libx264 -pix_fmt yuv420p -c:a aac \
  "$WORKDIR/fixture/acceptance-fixture.mp4"
ffprobe -v error -show_entries format=duration,size -of json "$WORKDIR/fixture/acceptance-fixture.mp4" >"$WORKDIR/fixture/ffprobe.json"
[[ -s "$WORKDIR/fixture/acceptance-fixture.mp4" ]] || fatal "fixture generation failed"
sha256sum "$WORKDIR/fixture/acceptance-fixture.mp4" >"$WORKDIR/fixture/acceptance-fixture.mp4.sha256"

admin_api() {
  local method="$1" path="$2"
  shift 2
  curl --fail-with-body --silent --show-error --max-time "${VELOX_STAGING_HTTP_TIMEOUT:-15}" \
    -X "$method" \
    -H "Authorization: Bearer ${VELOX_STAGING_ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    "${MASTER_URL}${path}" "$@"
}

admin_api_capture() {
  local output_file="$1" method="$2" path="$3"
  shift 3
  curl --silent --show-error --max-time "${VELOX_STAGING_HTTP_TIMEOUT:-15}" \
    -X "$method" \
    -H "Authorization: Bearer ${VELOX_STAGING_ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    "${MASTER_URL}${path}" "$@" -o "$output_file" -w '%{http_code}'
}

wait_operation() {
  local operation_id="$1" timeout_s="${2:-$OPERATION_TIMEOUT}"
  local deadline=$((SECONDS + timeout_s)) response status
  [[ -n "$operation_id" ]] || fatal "wait_operation requires operation_id"
  while ((SECONDS < deadline)); do
    response="$(admin_api GET "/api/v1/admin/operations/${operation_id}" 2>/dev/null || true)"
    status="$(jq -r '.status // empty' <<<"$response" 2>/dev/null || true)"
    printf '[staging-acceptance][OP] %s status=%s\n' "$operation_id" "${status:-UNKNOWN}"
    case "$status" in
      SUCCEEDED)
        printf '%s\n' "$response"
        return 0
        ;;
      FAILED|CANCELLED|ROLLED_BACK|ROLLBACK)
        printf '%s\n' "$response" >&2
        return 1
        ;;
    esac
    sleep "$POLL_SECONDS"
  done
  printf '[staging-acceptance][FAIL] operation %s timed out after %ss\n' "$operation_id" "$timeout_s" >&2
  [[ -z "${response:-}" ]] || jq . <<<"$response" >&2 || true
  return 1
}

info "validating Compose configuration"
"${COMPOSE_CMD[@]}" "${COMPOSE_ARGS[@]}" config >/dev/null
info "starting isolated master and worker pair"
"${COMPOSE_CMD[@]}" "${COMPOSE_ARGS[@]}" up -d master worker-a worker-b

info "waiting for REST readiness"
deadline=$((SECONDS + READINESS_TIMEOUT))
until curl --fail --silent --show-error --max-time 5 \
  "${MASTER_URL}/health/ready" >/dev/null 2>&1; do
  ((SECONDS < deadline)) || fatal "master did not become ready within ${READINESS_TIMEOUT}s"
  sleep "$POLL_SECONDS"
done
pass "master readiness is green"

info "checking both worker health endpoints"
for port in "${VELOX_STAGING_WORKER_A_HEALTH_PORT:-18081}" "${VELOX_STAGING_WORKER_B_HEALTH_PORT:-18082}"; do
  deadline=$((SECONDS + READINESS_TIMEOUT))
  until curl --fail --silent --show-error --max-time 5 "http://127.0.0.1:${port}/health/ready" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || fatal "worker health endpoint :$port did not become ready"
    sleep "$POLL_SECONDS"
  done
done
pass "both workers are ready"

workers="$(admin_api GET /api/v1/admin/workers)"
jq -e --arg a "$WORKER_ID_A" --arg b "$WORKER_ID_B" \
  '([.. | objects | select(.worker_id? == $a or .worker_id? == $b)]
    | map(select(.session_active == true and (.status == "CONNECTED" or .status == "HEALTHY")))
    | map(.worker_id) | unique | length) == 2' \
  <<<"$workers" >/dev/null || fatal "admin worker view does not show both workers active and eligible"
pass "admin worker view contains exactly the configured pair"

# Exercise the asynchronous operation contract with a harmless drain action.
# This is deliberately not a real production mutation: it targets only the
# isolated local staging project and is useful even when no render job fixture
# has been submitted yet.
response_file="$(mktemp "$WORKDIR/drain-response.XXXXXX")"
http_status="$(admin_api_capture "$response_file" POST "/api/v1/admin/workers/${WORKER_ID_A}/drain" --data '{"reason":"staging acceptance bootstrap"}')" || fatal "admin drain request failed"
[[ "$http_status" == "202" ]] || { jq . "$response_file" >&2 || true; fatal "drain request returned HTTP $http_status, want 202"; }
response="$(cat "$response_file")"
operation_id="$(jq -er '.operation_id' <<<"$response")" || fatal "drain response omitted operation_id"
wait_operation "$operation_id" >/dev/null || fatal "drain operation did not reach SUCCEEDED"
pass "admin_api and wait_operation verified (operation_id=$operation_id)"

[[ -d "$WORKDIR/master/data/storage" && -d "$WORKDIR/master/data/staging" ]] || fatal "isolated artifact storage directories disappeared"
info "staging acceptance bootstrap completed; fixture and storage are isolated under $WORKDIR"
pass "acceptance environment is ready for workload-specific scenarios"
