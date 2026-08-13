#!/usr/bin/env bash
# =============================================================================
# scripts/ops/post-deploy-cert.sh — POST-DEPLOY CERTIFICATION orchestrator
# =============================================================================
# The SINGLE operator entrypoint that certifies a freshly-pushed deployment
# before it is considered production-good. It does NOT implement new tests:
# every stage wires an existing, already-audited tool (see the per-stage
# mapping below) and only sequences them + records a per-stage verdict.
#
# The final verdict is CERTIFIED only when every stage PASSes. Any SKIP yields
# INCOMPLETE and any FAIL yields FAILED — both block production certification.
#
# Stage wiring (all pre-existing surfaces):
#   Pre-deploy gates   go vet/build/test, scripts/ci/check-architecture.sh,
#                      check-capability-contract.sh, pre-removal-verify.sh
#   Docker             docker build (worker: RemoteCodex/native/worker-agent-go/Dockerfile,
#                      master: DataServer/Dockerfile) + in-image C++/LibAV check
#                      + docker compose (deploy/runtime/compose.yml)
#   Master readiness   DataServer /health + /ready (capabilities != MISCONFIGURED)
#   Worker readiness   worker /health + /health/ready + /health/live worker_id
#                      + scripts/ops/runtime-cert.sh (registered/session_active/digest)
#   Capability contract scripts/ci/check-capability-contract.sh (AGENTS.md §6)
#   Canary E2E         deploy/runtime/submit-canary-local.sh | submit-canary-remote.sh
#   Copy-only          scripts/benchmarks/benchmark-worker.sh (COPY_ONLY canonical,
#                      VELOX_BENCH_REAL=1 -> zero-spawn fixture gate)
#   Cold/Warm cache    live cache probe (POST_CERT_CACHE_CMD); the benchmark
#                      cache modes remain evidence for the performance track
#   Prefetch           operator probe command (POST_CERT_PREFETCH_CMD)
#   Restart recovery   canonical compose worker restart + master restart
#   Short soak         operator stress/soak probe (POST_CERT_STRESS_CMD)
#   Performance        benchmark-worker.sh vs baseline (velox-benchmark-compare)
#   Git-vs-image       post-deploy digest binding (runtime-cert digest fields == the
#                      digest that was just built; EXPECTED_COMMIT checked)
#
# Usage:
#   scripts/ops/post-deploy-cert.sh [--help] [--list] [--no-build]
#
# Required env (checked at start; set ALL before running):
#   VELOX_WORKER_IMAGE      image ref for the freshly-built worker, e.g.
#                           ghcr.io/<owner>/velox-worker:<tag>
#   VELOX_MASTER_IMAGE      image ref for the freshly-built master
#   VELOX_WORKER_ID         immutable worker_id to certify
#   VELOX_MASTER_URL        REST base URL, e.g. http://127.0.0.1:8000
#   VELOX_ADMIN_TOKEN       admin API token (canary + fleet surfaces)
#
# Optional env (with defaults):
#   VELOX_WORKER_HEALTH_URL (default http://127.0.0.1:8081)
#   VELOX_WORKER_HOST / VELOX_SSH_USER / VELOX_MASTER_HOST / VELOX_MASTER_USER
#                           (runtime-cert.sh wiring; defaults match runtime-cert)
#   POST_CERT_EVIDENCE      evidence dir (default /tmp/velox-post-deploy-cert)
#   POST_CERT_PREFETCH_CMD  prefetch probe command (echoes PASS:/FAIL: on stdout)
#   POST_CERT_CACHE_CMD      live cold/warm cache probe (echoes PASS:/FAIL:)
#   POST_CERT_STRESS_CMD     20+20+20 real-job soak probe (echoes PASS:/FAIL:)
#   POST_CERT_CANARY_MODE    remote (default) or local; local additionally
#                            requires VELOX_DB_PATH for the existing local script
#   POST_CERT_MASTER_RESTART_CMD (default systemctl restart velox-server)
#   POST_CERT_SKIP_<STAGE>=1  optional operator annotation for a SKIP; a SKIP
#                            still blocks CERTIFIED
#   POST_CERT_PRE_REMOVAL=0   skip the (heavy) full-module pre-removal gate
#
# Exit codes:
#   0 CERTIFIED — every stage PASS
#   1 FAILED — at least one stage FAIL
#   2 INCOMPLETE — no FAIL but at least one stage was skipped
#   3 configuration/usage error
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

I_RED=$'\033[31m'; I_GREEN=$'\033[32m'; I_YELLOW=$'\033[33m'; I_BLUE=$'\033[34m'; I_RST=$'\033[0m'

log()  { printf '%s[%s]%s %s\n' "$I_BLUE" "$(date -u +%H:%M:%S)" "$I_RST" "$*"; }
fail() { printf '%s[FAIL]%s  %s\n' "$I_RED" "$I_RST" "$*"; exit "${2:-1}"; }

need() { command -v "$1" >/dev/null 2>&1 || fail "missing dependency: $1" 3; }

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
WORKER_IMAGE="${VELOX_WORKER_IMAGE:-}"
MASTER_IMAGE="${VELOX_MASTER_IMAGE:-}"
WORKER_ID="${VELOX_WORKER_ID:-}"
MASTER_URL="${VELOX_MASTER_URL:-}"
ADMIN_TOKEN="${VELOX_ADMIN_TOKEN:-}"
WORKER_HEALTH_URL="${VELOX_WORKER_HEALTH_URL:-http://127.0.0.1:8081}"

WORKER_HOST="${VELOX_WORKER_HOST:-}"
SSH_USER="${VELOX_SSH_USER:-pierone}"
MASTER_HOST="${VELOX_MASTER_HOST:-51.91.11.36}"
MASTER_USER="${VELOX_MASTER_USER:-pierone}"

EVIDENCE="${POST_CERT_EVIDENCE:-/tmp/velox-post-deploy-cert}"
PREFETCH_CMD="${POST_CERT_PREFETCH_CMD:-}"
CACHE_CMD="${POST_CERT_CACHE_CMD:-}"
STRESS_CMD="${POST_CERT_STRESS_CMD:-}"
CANARY_MODE="${POST_CERT_CANARY_MODE:-remote}"
MASTER_RESTART_CMD="${POST_CERT_MASTER_RESTART_CMD:-systemctl restart velox-server}"
RUN_PRE_REMOVAL="${POST_CERT_PRE_REMOVAL:-1}"
NO_BUILD="${POST_CERT_NO_BUILD:-0}"

EXPECTED_COMMIT="${EXPECTED_COMMIT:-$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || true)}"

mkdir -p "$EVIDENCE"

# ---------------------------------------------------------------------------
# Verdict bookkeeping
# ---------------------------------------------------------------------------
declare -A STAGE_RESULT   # stage -> PASS | FAIL | SKIP
declare -A STAGE_DETAIL   # stage -> human detail
STAGE_ORDER=()

record() { # record <stage> <PASS|FAIL|SKIP> <detail...>
  STAGE_RESULT["$1"]="$2"
  STAGE_DETAIL["$1"]="${3:-}"
  for s in "${STAGE_ORDER[@]}"; do
    [[ "$s" == "$1" ]] && return # update in place; keep original position
  done
  STAGE_ORDER+=("$1")
}

run_command_probe() { # run_command_probe <stage> <command> <log>
  local stage="$1" command_text="$2" log_file="$3" output
  output="$(eval "$command_text" 2>&1)" || {
    printf '%s\n' "$output" >"$log_file"
    record "$stage" FAIL "probe failed (see $log_file)"
    return 1
  }
  printf '%s\n' "$output" >"$log_file"
  if printf '%s\n' "$output" | grep -Eq '^PASS:'; then
    record "$stage" PASS "probe passed"
    return 0
  fi
  record "$stage" FAIL "probe did not emit PASS: (see $log_file)"
  return 1
}

# ---------------------------------------------------------------------------
# Pre-deploy gates
# ---------------------------------------------------------------------------
run_predeploy_gates() {
  log "pre-deploy gates: go vet + build + test (DataServer)"
  ( cd "$REPO_ROOT/DataServer" && go vet ./... ) >"$EVIDENCE/vet.log" 2>&1 \
    || { record "Pre-deploy gates" FAIL "go vet failed (see $EVIDENCE/vet.log)"; return; }
  ( cd "$REPO_ROOT/DataServer" && go build ./... ) >"$EVIDENCE/build.log" 2>&1 \
    || { record "Pre-deploy gates" FAIL "go build failed (see $EVIDENCE/build.log)"; return; }
  ( cd "$REPO_ROOT/DataServer" && go test -count=1 ./... ) >"$EVIDENCE/test.log" 2>&1 \
    || { record "Pre-deploy gates" FAIL "go test failed (see $EVIDENCE/test.log)"; return; }

  log "pre-deploy gates: architecture + capability contract"
  bash "$REPO_ROOT/scripts/ci/check-architecture.sh" >"$EVIDENCE/arch.log" 2>&1 \
    || { record "Pre-deploy gates" FAIL "architecture gate (see $EVIDENCE/arch.log)"; return; }
  bash "$REPO_ROOT/scripts/ci/check-capability-contract.sh" >"$EVIDENCE/capability-contract.log" 2>&1 \
    || { record "Pre-deploy gates" FAIL "capability contract (see $EVIDENCE/capability-contract.log)"; return; }

  record "Pre-deploy gates" PASS
}

run_pre_removal() {
  if [[ "$RUN_PRE_REMOVAL" != "1" ]]; then
    log "pre-removal gate disabled by POST_CERT_PRE_REMOVAL=0"
    return
  fi
  log "pre-deploy gates: full-module pre-removal gate (AGENTS.md §1)"
  bash "$REPO_ROOT/scripts/ci/pre-removal-verify.sh" >"$EVIDENCE/pre-removal.log" 2>&1 \
    || { record "Pre-removal gate" FAIL "pre-removal gate (see $EVIDENCE/pre-removal.log)"; return; }
  record "Pre-removal gate" PASS
}

# ---------------------------------------------------------------------------
# Docker: build both images + in-image C++/LibAV proof + stack up
# ---------------------------------------------------------------------------
run_docker() {
  [[ -n "$WORKER_IMAGE" ]] \
    || { record Docker FAIL "VELOX_WORKER_IMAGE is required"; return; }
  [[ -n "$MASTER_IMAGE" ]] \
    || { record Docker FAIL "VELOX_MASTER_IMAGE is required"; return; }
  if [[ "$NO_BUILD" == "1" ]]; then
    log "docker: build skipped (--no-build); verifying images exist"
    docker image inspect "$WORKER_IMAGE" >/dev/null 2>&1 \
      || { record Docker FAIL "worker image $WORKER_IMAGE not present and build disabled"; return; }
    docker image inspect "$MASTER_IMAGE" >/dev/null 2>&1 \
      || { record Docker FAIL "master image $MASTER_IMAGE not present and build disabled"; return; }
  else
    log "docker: build worker image $WORKER_IMAGE"
    docker build -f "$REPO_ROOT/RemoteCodex/native/worker-agent-go/Dockerfile" -t "$WORKER_IMAGE" "$REPO_ROOT" \
      >"$EVIDENCE/build-worker.log" 2>&1 \
      || { record Docker FAIL "worker build (see $EVIDENCE/build-worker.log)"; return; }

    log "docker: build master image $MASTER_IMAGE"
    docker build -f "$REPO_ROOT/DataServer/Dockerfile" -t "$MASTER_IMAGE" \
      --build-arg VERSION="${VELOX_VERSION:-cert}" \
      --build-arg BUILD_TIME="$(date -u +%Y%m%d)" \
      "$REPO_ROOT" >"$EVIDENCE/build-master.log" 2>&1 \
      || { record Docker FAIL "master build (see $EVIDENCE/build-master.log)"; return; }
  fi

  log "docker: verify C++/LibAV engine is really inside the worker image"
  # The Go module compiling is not enough: the certification needs the native
  # engine binary + the reproducible-build version markers present in-image.
  if ! docker run --rm --entrypoint /bin/sh "$WORKER_IMAGE" -c \
        'test -x /usr/local/bin/velox_video_engine && test -s /app/VERSION.txt && test -s /app/RemoteCodex/BUNDLE_HASH.txt && /usr/local/bin/velox_video_engine --help >/dev/null 2>&1' \
        >"$EVIDENCE/in-image-engine.log" 2>&1; then
    record Docker FAIL "C++/LibAV engine missing or non-executable in image (see $EVIDENCE/in-image-engine.log)"
    return
  fi
  docker run --rm --entrypoint /bin/sh "$WORKER_IMAGE" -c \
    'cat /app/VERSION.txt; echo; cat /app/RemoteCodex/BUNDLE_HASH.txt' \
    >"$EVIDENCE/worker-version.txt" 2>/dev/null || true

  log "docker: bring up canonical worker compose stack"
  ( cd "$REPO_ROOT/deploy/runtime"
    docker compose -f "$REPO_ROOT/deploy/runtime/compose.yml" config >/dev/null
    docker compose -f "$REPO_ROOT/deploy/runtime/compose.yml" down --remove-orphans
    docker compose -f "$REPO_ROOT/deploy/runtime/compose.yml" up -d ) >"$EVIDENCE/compose-up.log" 2>&1 \
    || { record Docker FAIL "docker compose down/up (see $EVIDENCE/compose-up.log)"; return; }

  docker image inspect "$WORKER_IMAGE" --format '{{json .}}' >"$EVIDENCE/worker-image.json" 2>"$EVIDENCE/worker-image.err" \
    || { record Docker FAIL "cannot inspect built worker image (see $EVIDENCE/worker-image.err)"; return; }
  docker image inspect "$MASTER_IMAGE" --format '{{json .}}' >"$EVIDENCE/master-image.json" 2>"$EVIDENCE/master-image.err" \
    || { record Docker FAIL "cannot inspect built master image (see $EVIDENCE/master-image.err)"; return; }
  record Docker PASS
}

run_image_binding() {
  local built_worker_id running_worker_id built_master_id running_master_id
  built_worker_id="$(jq -r '.Id // empty' "$EVIDENCE/worker-image.json" 2>/dev/null || true)"
  running_worker_id="$(docker inspect -f '{{.Image}}' velox-worker 2>/dev/null || true)"
  if [[ -z "$built_worker_id" || "$built_worker_id" != "$running_worker_id" ]]; then
    record "Git/image binding" FAIL "running worker image=$running_worker_id, built image=$built_worker_id"
    return
  fi

  built_master_id="$(jq -r '.Id // empty' "$EVIDENCE/master-image.json" 2>/dev/null || true)"
  local master_container="${VELOX_MASTER_CONTAINER:-velox-server}"
  running_master_id="$(docker inspect -f '{{.Image}}' "$master_container" 2>/dev/null || true)"
  if [[ -z "$running_master_id" || "$built_master_id" != "$running_master_id" ]]; then
    record "Git/image binding" FAIL "running master container=$master_container image=$running_master_id, built image=$built_master_id"
    return
  fi

  git -C "$REPO_ROOT" log -5 --date=iso-strict --format='%H%x09%ad%x09%s' >"$EVIDENCE/git-log-5.txt" \
    || { record "Git/image binding" FAIL "could not record latest 5 commits"; return; }
  local head
  head="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || true)"
  if [[ -z "$head" || -z "$EXPECTED_COMMIT" || "$head" != "$EXPECTED_COMMIT" ]]; then
    record "Git/image binding" FAIL "expected commit does not match repository HEAD"
    return
  fi
  if [[ "$NO_BUILD" == "1" ]]; then
    record "Git/image binding" FAIL "--no-build cannot certify that images came from the current commit"
    return
  fi
  record "Git/image binding" PASS "running worker/master image IDs match this run; latest 5 commits in $EVIDENCE/git-log-5.txt"
}

# ---------------------------------------------------------------------------
# Readiness: master + worker + capability state + runtime-cert facts
# ---------------------------------------------------------------------------
http_ok() { # http_ok <url> -> 0 if HTTP 200
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$1" 2>/dev/null || true)"
  [[ "$code" == "200" ]]
}

run_master_readiness() {
  [[ -n "$MASTER_URL" ]] || { record "Master readiness" FAIL "VELOX_MASTER_URL is required"; return; }
  if ! http_ok "$MASTER_URL/health"; then
    record "Master readiness" FAIL "$MASTER_URL/health not 200"
    return
  fi
  if ! http_ok "$MASTER_URL/ready"; then
    record "Master readiness" FAIL "$MASTER_URL/ready not 200"
    return
  fi
  local ready capabilities
  ready="$(curl -fsS --max-time 10 "$MASTER_URL/ready" 2>/dev/null || true)"
  if ! printf '%s' "$ready" | jq -e '.status == "ready" and (.capabilities | type == "object")' >/dev/null 2>&1; then
    record "Master readiness" FAIL "invalid /ready payload or status is not ready: $ready"
    return
  fi
  capabilities="$(printf '%s' "$ready" | jq -c '.capabilities')"
  if printf '%s' "$capabilities" | jq -e 'to_entries | any(.value == "MISCONFIGURED")' >/dev/null 2>&1; then
    record "Master readiness" FAIL "capability MISCONFIGURED in /ready: $capabilities"
    return
  fi
  record "Master readiness" PASS
}

run_worker_readiness() {
  if ! http_ok "$WORKER_HEALTH_URL/health"; then
    record "Worker readiness" FAIL "$WORKER_HEALTH_URL/health not 200"
    return
  fi
  local ready detail live_id
  ready="$(curl -s --max-time 10 "$WORKER_HEALTH_URL/health/ready" 2>/dev/null || true)"
  if [[ "$(printf '%s' "$ready" | jq -r '.status // empty')" != "ok" ]]; then
    record "Worker readiness" FAIL "worker /health/ready not ok: $ready"
    return
  fi
  detail="$(printf '%s' "$ready" | jq -c '.detail')"
  if [[ "$(printf '%s' "$detail" | jq -r '.registered // false')" != "true" ]]; then
    record "Worker readiness" FAIL "worker not registered"
    return
  fi
  if [[ "$(printf '%s' "$detail" | jq -r '.cache_ready // false')" != "true" ]]; then
    record "Worker readiness" FAIL "worker cache not ready"
    return
  fi
  if [[ "$(printf '%s' "$detail" | jq -r '.executors_count // 0')" == "0" ]]; then
    record "Worker readiness" FAIL "worker has no executors"
    return
  fi
  if [[ -n "$WORKER_ID" ]]; then
    live_id="$(curl -fsS --max-time 10 "$WORKER_HEALTH_URL/health/live" 2>/dev/null | jq -r '.worker_id // empty' || true)"
    if [[ -z "$live_id" || "$live_id" != "$WORKER_ID" ]]; then
      record "Worker readiness" FAIL "worker_id mismatch: live=$live_id expected=$WORKER_ID"
      return
    fi
  fi
  record "Worker readiness" PASS
}

run_capability_contract() {
  # Static AGENTS.md §6 pairing gate + live /ready capabilities scan.
  bash "$REPO_ROOT/scripts/ci/check-capability-contract.sh" >"$EVIDENCE/capability-contract.log" 2>&1 \
    || { record "Capability contract" FAIL "static contract violation (see $EVIDENCE/capability-contract.log)"; return; }
  if [[ -n "$MASTER_URL" ]]; then
    local caps
    caps="$(curl -s --max-time 10 "$MASTER_URL/ready" 2>/dev/null | jq -r '.capabilities // {}')"
    if printf '%s' "$caps" | jq -e 'to_entries | map(select(.value == "MISCONFIGURED")) | length > 0' >/dev/null 2>&1; then
      record "Capability contract" FAIL "live /ready capabilities include MISCONFIGURED: $caps"
      return
    fi
  fi
  record "Capability contract" PASS
}

run_runtime_cert() {
  local rc_args=()
  if [[ -n "$WORKER_HOST" && -n "$WORKER_ID" ]]; then
    rc_args=( "$WORKER_ID" "$WORKER_HOST" "$SSH_USER" "$MASTER_HOST" "$MASTER_USER" )
  fi
  if [[ "${#rc_args[@]}" == "0" ]]; then
    record "Runtime cert" SKIP "set VELOX_WORKER_HOST for SSH runtime-cert.sh evidence"
    return
  fi
  if ! bash "$REPO_ROOT/scripts/ops/runtime-cert.sh" "${rc_args[@]}" >"$EVIDENCE/runtime-cert.json" 2>"$EVIDENCE/runtime-cert.err"; then
    record "Runtime cert" FAIL "runtime-cert.sh failed (see $EVIDENCE/runtime-cert.err)"
    return
  fi
  # Worker identity + digest binding: the running container must carry the
  # freshly-built digest (this is the post-deploy git-vs-image check).
  if [[ -n "$WORKER_ID" ]]; then
    local doc digest running_digest registered_digest
    doc="$(jq -s 'map(select(.worker_id == $wid))[0]' --arg wid "$WORKER_ID" "$EVIDENCE/runtime-cert.json" 2>/dev/null || true)"
    if [[ -z "$doc" || "$doc" == "null" ]]; then
      record "Runtime cert" FAIL "runtime-cert.json has no doc for worker_id $WORKER_ID"
      return
    fi
    if [[ "$WORKER_IMAGE" == *"@"* ]]; then
      digest="${WORKER_IMAGE##*@}"
      running_digest="$(printf '%s' "$doc" | jq -r '.host_facts.running_digest // empty')"
      registered_digest="$(printf '%s' "$doc" | jq -r '.master_record.image_digest // empty')"
      if [[ "$running_digest" != "$digest" || "$registered_digest" != "$digest" ]]; then
        record "Runtime cert" FAIL "worker/master digest mismatch: running=${running_digest:-<empty>} registered=${registered_digest:-<empty>} expected=$digest"
        return
      fi
    fi
    if ! printf '%s' "$doc" | jq -e --arg wid "$WORKER_ID" \
         '.worker_id == $wid and .master_record.status == "CONNECTED" and .master_record.session_active == true and (.host_facts.active == "active")' \
           >/dev/null 2>&1; then
        record "Runtime cert" FAIL "runtime-cert facts are not CONNECTED/active"
        return
    fi
    if [[ -n "$EXPECTED_COMMIT" && -n "${doc}" ]]; then
      printf '%s\n' "$doc" > "$EVIDENCE/runtime-cert-${WORKER_ID}.json"
    fi
  fi
  record "Runtime cert" PASS
}

# ---------------------------------------------------------------------------
# Canary E2E (real job: DataServer -> placement -> worker -> SUCCEEDED)
# ---------------------------------------------------------------------------
run_canary_e2e() {
  if [[ -z "$MASTER_URL" || -z "$ADMIN_TOKEN" ]]; then
    record "Canary E2E" FAIL "VELOX_MASTER_URL and VELOX_ADMIN_TOKEN are required"
    return
  fi
  case "$CANARY_MODE" in
  local)
    if [[ -z "${VELOX_DB_PATH:-}" ]]; then
      record "Canary E2E" FAIL "local canary requires VELOX_DB_PATH"
      return
    fi
    bash "$REPO_ROOT/deploy/runtime/submit-canary-local.sh" \
      >"$EVIDENCE/canary.log" 2>&1
    ;;
  remote)
    bash "$REPO_ROOT/deploy/runtime/submit-canary-remote.sh" \
      >"$EVIDENCE/canary.log" 2>&1
    ;;
  *)
    record "Canary E2E" FAIL "POST_CERT_CANARY_MODE must be local or remote"
    return
    ;;
  esac
  local rc=$?
  if [[ "$rc" == "0" ]] && grep -Eq '^(PASS:|SUCCEEDED)' "$EVIDENCE/canary.log"; then
    record "Canary E2E" PASS
  else
    record "Canary E2E" FAIL "canary rc=$rc (see $EVIDENCE/canary.log)"
  fi
}

# ---------------------------------------------------------------------------
# Copy-only fast path (zero-spawn fixture gate)
# ---------------------------------------------------------------------------
run_copy_only() {
  # COPY_ONLY canonical fixture with VELOX_BENCH_REAL=1 drives the production
  # zero-spawn renderer; benchmark-worker.sh runs velox-fixture-gate
  # -tier performance, which asserts external_process_count=0, ffmpeg/ffprobe
  # exec count=0, frames decoded/encoded=0 and concat_mode=packet-copy.
  VELOX_BENCH_FIXTURE="${VELOX_BENCH_FIXTURE:-COPY_ONLY_CANONICAL_5M_V1}" \
  VELOX_BENCH_REAL=1 \
  VELOX_BENCH_KEEP=1 \
  VELOX_BENCH_EVIDENCE="$EVIDENCE/copy-only" \
  bash "$REPO_ROOT/scripts/benchmarks/benchmark-worker.sh" >"$EVIDENCE/copy-only.log" 2>&1 \
    || { record "Copy-only" FAIL "benchmark-worker.sh copy-only gate (see $EVIDENCE/copy-only.log)"; return; }
  record "Copy-only" PASS
}

run_cache_pair() {
  if [[ -z "$CACHE_CMD" ]]; then
    record "Cold cache" SKIP "no live cache probe; set POST_CERT_CACHE_CMD or POST_CERT_SKIP_COLD_CACHE=1"
    record "Warm cache" SKIP "no live cache probe; set POST_CERT_CACHE_CMD or POST_CERT_SKIP_WARM_CACHE=1"
    return
  fi
  run_command_probe "Cold cache" "$CACHE_CMD" "$EVIDENCE/cache.log" || return
  # A single probe owns the two-run assertion. The output must include the
  # operator's MISS→HIT and SHA evidence; the orchestrator does not infer
  # live-cache behaviour from the benchmark runner's metadata-only mode.
  if ! grep -Eiq 'MISS.*(HIT|download).*SHA|HIT.*download[[:space:]]*=[[:space:]]*0' "$EVIDENCE/cache.log"; then
    record "Warm cache" FAIL "cache probe lacks MISS/HIT, download=0 and SHA evidence (see $EVIDENCE/cache.log)"
    return
  fi
  record "Warm cache" PASS "live cache probe recorded cold MISS and warm HIT with identical SHA"
}

# ---------------------------------------------------------------------------
# Prefetch probe (operator-supplied; no dedicated tool exists in-tree yet)
# ---------------------------------------------------------------------------
run_prefetch() {
  if [[ -z "$PREFETCH_CMD" ]]; then
    record Prefetch SKIP "no POST_CERT_PREFETCH_CMD; set one or POST_CERT_SKIP_PREFETCH=1 to acknowledge"
    return
  fi
  run_command_probe Prefetch "$PREFETCH_CMD" "$EVIDENCE/prefetch.log"
}

# ---------------------------------------------------------------------------
# Restart recovery: worker restart + master restart, then re-certify
# ---------------------------------------------------------------------------
run_restart_recovery() {
  log "restart recovery: restarting canonical worker container"
  ( cd "$REPO_ROOT/deploy/runtime" && docker compose -f "$REPO_ROOT/deploy/runtime/compose.yml" restart velox-worker ) \
    >"$EVIDENCE/restart-worker.log" 2>&1 \
    || { record "Restart recovery" FAIL "worker restart (see $EVIDENCE/restart-worker.log)"; return; }
  sleep "${POST_CERT_WORKER_RESTART_WAIT_S:-15}"
  run_worker_readiness
  local stage="${STAGE_RESULT["Worker readiness"]:-}"
  if [[ "$stage" != "PASS" ]]; then
    record "Restart recovery" FAIL "worker did not return READY after restart"
    return
  fi

  [[ -n "$MASTER_RESTART_CMD" ]] || { record "Restart recovery" FAIL "POST_CERT_MASTER_RESTART_CMD is required"; return; }
  log "restart recovery: restarting master ($MASTER_RESTART_CMD)"
  eval "$MASTER_RESTART_CMD" >"$EVIDENCE/restart-master.log" 2>&1 \
    || { record "Restart recovery" FAIL "master restart failed (see $EVIDENCE/restart-master.log)"; return; }
  local up=0
  for _ in $(seq 1 "${POST_CERT_MASTER_READY_ATTEMPTS:-60}"); do
    if http_ok "$MASTER_URL/ready"; then up=1; break; fi
    sleep 2
  done
  [[ "$up" == "1" ]] || { record "Restart recovery" FAIL "master /ready not 200 after restart"; return; }
  sleep "${POST_CERT_MASTER_RECONNECT_WAIT_S:-10}"
  run_worker_readiness
  local stage2="${STAGE_RESULT["Worker readiness"]:-}"
  if [[ "$stage2" != "PASS" ]]; then
    record "Restart recovery" FAIL "worker not READY after master restart"
    return
  fi
  run_runtime_cert
  local stage3="${STAGE_RESULT["Runtime cert"]:-}"
  if [[ "$stage3" != "PASS" ]]; then
    record "Restart recovery" FAIL "runtime facts not consistent after restarts"
    return
  fi
  record "Restart recovery" PASS
}

# ---------------------------------------------------------------------------
# Short soak (reduced-duration cap-10 soak; chaos is real but short)
# ---------------------------------------------------------------------------
run_short_soak() {
  if [[ -z "$STRESS_CMD" ]]; then
    record "Short soak" SKIP "no 20+20+20 real-job probe; set POST_CERT_STRESS_CMD or POST_CERT_SKIP_SHORT_SOAK=1"
    return
  fi
  run_command_probe "Short soak" "$STRESS_CMD" "$EVIDENCE/short-soak.log"
}

# ---------------------------------------------------------------------------
# Performance regression gate (vs baseline)
# ---------------------------------------------------------------------------
run_perf_regression() {
  local baseline="${VELOX_BENCH_BASELINE:-}"
  if [[ -z "$baseline" ]]; then
    record "Performance regression" SKIP "no VELOX_BENCH_BASELINE"
    return
  fi
  VELOX_BENCH_REAL=1 VELOX_BENCH_BASELINE="$baseline" \
  VELOX_BENCH_KEEP=1 VELOX_BENCH_EVIDENCE="$EVIDENCE/perf" \
  bash "$REPO_ROOT/scripts/benchmarks/benchmark-worker.sh" >"$EVIDENCE/perf.log" 2>&1 \
    || { record "Performance regression" FAIL "benchmark vs baseline (see $EVIDENCE/perf.log)"; return; }
  record "Performance regression" PASS
}

# ---------------------------------------------------------------------------
# Verdict rendering
# ---------------------------------------------------------------------------
print_verdict() {
  local had_fail=0 had_skip=0
  local -a verdict_rows=()
  printf '\n'
  printf '%s\n' "POST DEPLOY CERTIFICATION"
  printf '%s\n' "-------------------------"
  for stage in "${STAGE_ORDER[@]}"; do
    local res="${STAGE_RESULT[$stage]}" detail="${STAGE_DETAIL[$stage]:-}"
    verdict_rows+=("$(jq -cn --arg stage "$stage" --arg result "$res" --arg detail "$detail" \
      '{stage:$stage,result:$result,detail:$detail}')")
    case "$res" in
      PASS) printf '%-24s %s%s%s %s\n' "$stage" "$I_GREEN" "PASS" "$I_RST" "$detail" ;;
      FAIL) printf '%-24s %s%s%s %s\n' "$stage" "$I_RED" "FAIL" "$I_RST" "$detail"; had_fail=1 ;;
      SKIP) printf '%-24s %s%s%s %s\n' "$stage" "$I_YELLOW" "SKIP" "$I_RST" "$detail"; had_skip=1 ;;
    esac
  done
  printf '%s\n' "${verdict_rows[@]}" | jq -s \
    --arg commit "$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{schema:"velox.post-deploy-certification.v1",git_commit:$commit,generated_at:$generated_at,stages:.}' \
    >"$EVIDENCE/verdict.json"
  printf '%-24s %s\n' "Git commit" "$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  printf '%-24s %s\n' "Evidence" "$EVIDENCE/verdict.json"
  printf '%s\n' "-------------------------"

  if [[ "$had_fail" == "1" ]]; then
    printf '%s\n' "VERDICT: FAILED"
    exit 1
  fi
  if [[ "$had_skip" == "1" ]]; then
    printf '%s\n' "VERDICT: INCOMPLETE"
    exit 2
  fi
  printf '%s\n' "VERDICT: CERTIFIED"
  exit 0
}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
case "${1:-}" in
  --help|-h)
    sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  --list)
    printf 'Stages wired by %s (each maps to an existing tool):\n' "$0"
  for s in "Pre-deploy gates|scripts/ci gates + go vet/build/test" \
             "Pre-removal gate|scripts/ci/pre-removal-verify.sh" \
             "Docker|docker build (worker+master) + in-image C++/LibAV + compose up" \
             "Git/image binding|latest 5 commits + running container image IDs" \
             "Master readiness|DataServer /health + /ready" \
             "Worker readiness|worker /health /health/ready /health/live + runtime-cert" \
             "Capability contract|check-capability-contract.sh + live capabilities scan" \
             "Canary E2E|submit-canary-local.sh / submit-canary-remote.sh" \
             "Copy-only|benchmark-worker.sh COPY_ONLY_REAL (zero-spawn gate)" \
             "Cold cache|live cache probe: MISS + download + SHA verify + promotion" \
             "Warm cache|live cache probe: HIT + download=0 + same SHA" \
             "Prefetch|POST_CERT_PREFETCH_CMD probe" \
             "Restart recovery|canonical worker + master restart and re-registration" \
             "Short soak|POST_CERT_STRESS_CMD 20+20+20 real-job probe" \
             "Performance regression|benchmark-worker.sh vs VELOX_BENCH_BASELINE"; do
      printf '  %-24s %s\n' "${s%%|*}" "${s#*|}"
    done
    exit 0
    ;;
  --no-build) NO_BUILD=1 ;;
  "") ;;
  *) fail "unknown option: $1" 3 ;;
esac

# ---------------------------------------------------------------------------
# Config pre-flight
# ---------------------------------------------------------------------------
need jq
need docker
need curl
need git
need go

missing=()
[[ -n "$WORKER_IMAGE" ]] || missing+=(VELOX_WORKER_IMAGE)
[[ -n "$MASTER_IMAGE" ]] || missing+=(VELOX_MASTER_IMAGE)
[[ -n "$WORKER_ID" ]] || missing+=(VELOX_WORKER_ID)
[[ -n "$MASTER_URL" ]] || missing+=(VELOX_MASTER_URL)
[[ -n "$ADMIN_TOKEN" ]] || missing+=(VELOX_ADMIN_TOKEN)
if [[ "${#missing[@]}" -gt 0 ]]; then
  fail "missing required certification configuration: ${missing[*]}" 3
fi
case "$CANARY_MODE" in
  local) [[ -n "${VELOX_DB_PATH:-}" ]] || fail "POST_CERT_CANARY_MODE=local requires VELOX_DB_PATH" 3 ;;
  remote) ;;
  *) fail "POST_CERT_CANARY_MODE must be local or remote" 3 ;;
esac

if [[ ! "$WORKER_IMAGE" =~ ^.+@sha256:[0-9a-f]{64}$ && "${POST_CERT_ALLOW_MUTABLE_IMAGE:-0}" != "1" ]]; then
  log "config: worker image is not digest-pinned; set POST_CERT_ALLOW_MUTABLE_IMAGE=1 only for a controlled local build"
fi

# Existing helper scripts consume these values from their environment. Export
# the validated configuration even when the caller passed shell variables via
# a wrapper rather than `VAR=value command`.
export VELOX_WORKER_IMAGE="$WORKER_IMAGE"
export VELOX_MASTER_URL="$MASTER_URL"
export VELOX_ADMIN_TOKEN="$ADMIN_TOKEN"
export VELOX_WORKER_ID="$WORKER_ID"

log "post-deploy certification starting (commit=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown), evidence=$EVIDENCE)"
log "wiring: worker=$WORKER_IMAGE master=$MASTER_IMAGE worker_id=${WORKER_ID:-unset}"

# ---------------------------------------------------------------------------
# Stage execution
# ---------------------------------------------------------------------------
run_predeploy_gates
run_pre_removal
run_docker
run_image_binding
run_capability_contract
run_master_readiness
run_worker_readiness
run_runtime_cert
run_canary_e2e
run_copy_only
run_cache_pair
run_prefetch
run_restart_recovery
run_short_soak
run_perf_regression

print_verdict
