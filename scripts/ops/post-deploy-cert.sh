#!/usr/bin/env bash
# =============================================================================
# scripts/ops/post-deploy-cert.sh — POST-DEPLOY CERTIFICATION orchestrator
# =============================================================================
# The SINGLE operator entrypoint that certifies a freshly-pushed deployment
# before it is considered production-good. It does NOT implement new tests:
# every stage wires an existing, already-audited tool (see the per-stage
# mapping below) and only sequences them + records a per-stage verdict.
#
# The final verdict is CERTIFIED only when every stage PASSes (no FAIL, no
# unacknowledged SKIP). Any unacknowledged SKIP yields INCOMPLETE; any FAIL
# yields FAILED — both block the deployment from being certified.
#
# Stage wiring (all pre-existing surfaces):
#   Pre-deploy gates   scripts/ci/check-architecture.sh, check-capability-contract.sh,
#                      pre-removal-verify.sh (AGENTS.md §1), go vet/build/test
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
#   Cold/Warm cache    benchmark-worker.sh VELOX_BENCH_CACHE=cold|warm + artifact-SHA
#                      comparison across the two runs (this file implements the
#                      cold==warm SHA proof; benchmark-worker.sh emits the SHA)
#   Prefetch           operator probe command (POST_CERT_PREFETCH_CMD); SKIP-able
#   Restart recovery   scripts/cert/cap-7-reboot-recovery.sh (worker) + master restart
#   Short soak         scripts/cert/cap-10-soak.sh with SOAK_HOURS reduced
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
#   POST_CERT_SOAK_HOURS    short-soak duration (default 1)
#   POST_CERT_PREFETCH_CMD  prefetch probe command (echoes PASS:/FAIL: on stdout)
#   POST_CERT_MASTER_RESTART_CMD (default systemctl restart velox-server)
#   POST_CERT_SKIP_<STAGE>=1  acknowledge a SKIP for that stage (blocks nothing
#                             that the operator consciously owns; unacknowledged
#                             SKIPs still block CERTIFIED)
#   POST_CERT_PRE_REMOVAL=0   skip the (heavy) full-module pre-removal gate
#
# Exit codes:
#   0 CERTIFIED — every stage PASS
#   1 FAILED — at least one stage FAIL
#   2 INCOMPLETE — no FAIL but unacknowledged SKIPs
#   3 configuration/usage error
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

I_RED=$'\033[31m'; I_GREEN=$'\033[32m'; I_YELLOW=$'\033[33m'; I_BLUE=$'\033[34m'; I_RST=$'\033[0m'

log()  { printf '%s[%s]%s %s\n' "$I_BLUE" "$(date -u +%H:%M:%S)" "$I_RST" "$*"; }
fail() { printf '%s[FAIL]%s  %s\n' "$I_RED" "$I_RST" "$*"; exit "${2:-1}"; }

need() { command -v "$1" >/dev/null 2>&1 || fail "missing dependency: $1" 3; }
need jq
need docker
need curl

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
WORKER_IMAGE="${VELOX_WORKER_IMAGE:-}"
MASTER_IMAGE="${VELOX_MASTER_IMAGE:-velox-server:cert}"
WORKER_ID="${VELOX_WORKER_ID:-}"
MASTER_URL="${VELOX_MASTER_URL:-}"
ADMIN_TOKEN="${VELOX_ADMIN_TOKEN:-}"
WORKER_HEALTH_URL="${VELOX_WORKER_HEALTH_URL:-http://127.0.0.1:8081}"

WORKER_HOST="${VELOX_WORKER_HOST:-}"
SSH_USER="${VELOX_SSH_USER:-pierone}"
MASTER_HOST="${VELOX_MASTER_HOST:-51.91.11.36}"
MASTER_USER="${VELOX_MASTER_USER:-pierone}"

EVIDENCE="${POST_CERT_EVIDENCE:-/tmp/velox-post-deploy-cert}"
SOAK_HOURS="${POST_CERT_SOAK_HOURS:-1}"
PREFETCH_CMD="${POST_CERT_PREFETCH_CMD:-}"
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

stage_skip_ack() { # stage_skip_ack <STAGE> -> 0 if acknowledged, 1 if not
  local stage="$1"
  local envvar="POST_CERT_SKIP_${stage^^}"
  envvar="${envvar//-/_}"
  [[ "${!envvar:-0}" == "1" ]]
}

# ---------------------------------------------------------------------------
# Pre-deploy gates
# ---------------------------------------------------------------------------
run_predeploy_gates() {
  log "pre-deploy gates: go vet + build (DataServer)"
  ( cd "$REPO_ROOT/DataServer" && go vet ./... ) >"$EVIDENCE/vet.log" 2>&1 \
    || { record "Pre-deploy gates" FAIL "go vet failed (see $EVIDENCE/vet.log)"; return; }
  ( cd "$REPO_ROOT/DataServer" && go build ./... ) >"$EVIDENCE/build.log" 2>&1 \
    || { record "Pre-deploy gates" FAIL "go build failed (see $EVIDENCE/build.log)"; return; }

  log "pre-deploy gates: architecture + capability contract"
  bash "$REPO_ROOT/scripts/ci/check-architecture.sh" >"$EVIDENCE/arch.log" 2>&1 \
    || { record "Pre-deploy gates" FAIL "architecture gate (see $EVIDENCE/arch.log)"; return; }
  bash "$REPO_ROOT/scripts/ci/check-capability-contract.sh" >"$EVIDENCE/capability-contract.log" 2>&1 \
    || { record "Pre-deploy gates" FAIL "capability contract (see $EVIDENCE/capability-contract.log)"; return; }

  if [[ "$RUN_PRE_REMOVAL" == "1" ]]; then
    log "pre-deploy gates: full-module pre-removal gate (AGENTS.md §1)"
    bash "$REPO_ROOT/scripts/ci/pre-removal-verify.sh" >"$EVIDENCE/pre-removal.log" 2>&1 \
      || { record "Pre-deploy gates" FAIL "pre-removal gate (see $EVIDENCE/pre-removal.log)"; return; }
  fi
  record "Pre-deploy gates" PASS
}

# ---------------------------------------------------------------------------
# Docker: build both images + in-image C++/LibAV proof + stack up
# ---------------------------------------------------------------------------
run_docker() {
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
    docker buildx build -f "$REPO_ROOT/DataServer/Dockerfile" -t "$MASTER_IMAGE" \
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
  if [[ "$WORKER_IMAGE" != "" ]]; then
    ( export VELOX_WORKER_IMAGE="$WORKER_IMAGE"
      cd "$REPO_ROOT/deploy/runtime" && docker compose down >/dev/null 2>&1 || true
      docker compose up -d ) >"$EVIDENCE/compose-up.log" 2>&1 \
      || { record Docker FAIL "docker compose up (see $EVIDENCE/compose-up.log)"; return; }
  fi
  record Docker PASS
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
  [[ -n "$MASTER_URL" ]] || { record "Master readiness" SKIP "VELOX_MASTER_URL unset"; return; }
  if ! http_ok "$MASTER_URL/health"; then
    record "Master readiness" FAIL "$MASTER_URL/health not 200"
    return
  fi
  if ! http_ok "$MASTER_URL/ready"; then
    record "Master readiness" FAIL "$MASTER_URL/ready not 200"
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
    live_id="$(curl -s --max-time 10 "$WORKER_HEALTH_URL/health/live" 2>/dev/null | jq -r '.worker_id // empty' || true)"
    if [[ -n "$live_id" && "$live_id" != "$WORKER_ID" ]]; then
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
  elif [[ -n "$WORKER_ID" ]]; then
    rc_args=( "$WORKER_ID" )
  fi
  if [[ "${#rc_args[@]}" == "0" ]]; then
    record "Runtime cert" SKIP "no worker_id/host provided"
    return
  fi
  if ! bash "$REPO_ROOT/scripts/ops/runtime-cert.sh" "${rc_args[@]}" >"$EVIDENCE/runtime-cert.json" 2>"$EVIDENCE/runtime-cert.err"; then
    record "Runtime cert" FAIL "runtime-cert.sh failed (see $EVIDENCE/runtime-cert.err)"
    return
  fi
  # Worker identity + digest binding: the running container must carry the
  # freshly-built digest (this is the post-deploy git-vs-image check).
  if [[ -n "$WORKER_ID" ]]; then
    local doc digest
    doc="$(jq -s 'map(select(.worker_id == $wid))[0]' --arg wid "$WORKER_ID" "$EVIDENCE/runtime-cert.json" 2>/dev/null || true)"
    if [[ -z "$doc" || "$doc" == "null" ]]; then
      record "Runtime cert" FAIL "runtime-cert.json has no doc for worker_id $WORKER_ID"
      return
    fi
    if [[ "$WORKER_IMAGE" == *"@"* ]]; then
      digest="${WORKER_IMAGE##*@}"
      if ! printf '%s' "$doc" | jq -e --arg d "$digest" \
           '([.image_digest.running, .image_digest.registered, .image_digest.master_registered] | map(select(. == $d)) | length) >= 1' \
           >/dev/null 2>&1; then
        record "Runtime cert" FAIL "running/registered digest does not match freshly-built digest $digest"
        return
      fi
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
  if [[ -n "$MASTER_URL" && -n "$ADMIN_TOKEN" ]]; then
    bash "$REPO_ROOT/deploy/runtime/submit-canary-local.sh" \
      >"$EVIDENCE/canary.log" 2>&1
  elif [[ -n "$MASTER_URL" ]]; then
    bash "$REPO_ROOT/deploy/runtime/submit-canary-remote.sh" \
      >"$EVIDENCE/canary.log" 2>&1
  else
    record "Canary E2E" SKIP "VELOX_MASTER_URL unset"
    return
  fi
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

# ---------------------------------------------------------------------------
# Cold + warm cache: identical artifact SHA, miss->hit
# ---------------------------------------------------------------------------
extract_artifact_sha() { # extract_artifact_sha <evidence-dir> -> sha or empty
  # The benchmark evidence JSON carries artifact_sha256 per run (fixture_gate).
  jq -r '[.. | .artifact_sha256? // empty] | .[0]' \
    "$1"/*.json 2>/dev/null | head -1
}

run_cache_pair() {
  local cold_dir="$EVIDENCE/cold-cache" warm_dir="$EVIDENCE/warm-cache"
  VELOX_BENCH_FIXTURE="${VELOX_BENCH_FIXTURE:-COPY_ONLY_CANONICAL_5M_V1}" \
  VELOX_BENCH_REAL=1 VELOX_BENCH_CACHE=cold VELOX_BENCH_KEEP=1 \
  VELOX_BENCH_EVIDENCE="$cold_dir" \
  bash "$REPO_ROOT/scripts/benchmarks/benchmark-worker.sh" >"$EVIDENCE/cold-cache.log" 2>&1 \
    || { record "Cold cache" FAIL "cold-cache benchmark (see $EVIDENCE/cold-cache.log)"; return; }
  record "Cold cache" PASS "cache miss -> download -> verify -> promote"

  VELOX_BENCH_FIXTURE="${VELOX_BENCH_FIXTURE:-COPY_ONLY_CANONICAL_5M_V1}" \
  VELOX_BENCH_REAL=1 VELOX_BENCH_CACHE=warm VELOX_BENCH_KEEP=1 \
  VELOX_BENCH_EVIDENCE="$warm_dir" \
  bash "$REPO_ROOT/scripts/benchmarks/benchmark-worker.sh" >"$EVIDENCE/warm-cache.log" 2>&1 \
    || { record "Warm cache" FAIL "warm-cache benchmark (see $EVIDENCE/warm-cache.log)"; return; }

  local cold_sha warm_sha
  cold_sha="$(extract_artifact_sha "$cold_dir")"
  warm_sha="$(extract_artifact_sha "$warm_dir")"
  if [[ -z "$cold_sha" || -z "$warm_sha" ]]; then
    record "Warm cache" FAIL "could not extract artifact_sha256 from evidence dirs"
    return
  fi
  if [[ "$cold_sha" != "$warm_sha" ]]; then
    record "Warm cache" FAIL "cold/warm artifact SHA differ: cold=$cold_sha warm=$warm_sha"
    return
  fi
  record "Warm cache" PASS "cold==warm artifact SHA ($cold_sha)"
}

# ---------------------------------------------------------------------------
# Prefetch probe (operator-supplied; no dedicated tool exists in-tree yet)
# ---------------------------------------------------------------------------
run_prefetch() {
  if [[ -z "$PREFETCH_CMD" ]]; then
    if stage_skip_ack prefetch; then
      record Prefetch SKIP "acknowledged: no POST_CERT_PREFETCH_CMD provided"
    else
      record Prefetch SKIP "no POST_CERT_PREFETCH_CMD; set one or POST_CERT_SKIP_PREFETCH=1 to acknowledge"
    fi
    return
  fi
  local out
  out="$(eval "$PREFETCH_CMD" 2>&1)" || { record Prefetch FAIL "probe failed: $out"; return; }
  if printf '%s' "$out" | grep -Eq '^PASS:'; then
    record Prefetch PASS
  else
    record Prefetch FAIL "probe did not PASS: $out"
  fi
}

# ---------------------------------------------------------------------------
# Restart recovery: worker restart + master restart, then re-certify
# ---------------------------------------------------------------------------
run_restart_recovery() {
  local cap7="$REPO_ROOT/scripts/cert/cap-7-reboot-recovery.sh"
  if [[ -f "$cap7" && "$(id -u)" == "0" ]]; then
    bash "$cap7" --evidence-root "$EVIDENCE/restart-recovery" \
      >"$EVIDENCE/restart-worker.log" 2>&1 \
      || { record "Restart recovery" FAIL "cap-7 worker reboot recovery (see $EVIDENCE/restart-worker.log)"; return; }
  else
    log "restart recovery: restarting worker container directly"
    docker compose -f "$REPO_ROOT/deploy/runtime/compose.yml" restart velox-worker \
      >"$EVIDENCE/restart-worker.log" 2>&1 \
      || { record "Restart recovery" FAIL "worker restart (see $EVIDENCE/restart-worker.log)"; return; }
    sleep 15
    run_worker_readiness
    local stage="${STAGE_RESULT["Worker readiness"]:-}"
    if [[ "$stage" != "PASS" ]]; then
      record "Restart recovery" FAIL "worker did not return READY after restart"
      return
    fi
  fi

  if [[ -n "$MASTER_RESTART_CMD" ]]; then
    log "restart recovery: restarting master ($MASTER_RESTART_CMD)"
    eval "$MASTER_RESTART_CMD" >"$EVIDENCE/restart-master.log" 2>&1 \
      || { record "Restart recovery" FAIL "master restart failed (see $EVIDENCE/restart-master.log)"; return; }
    # Poll master readiness back up.
    local up=0
    for _ in $(seq 1 60); do
      if http_ok "$MASTER_URL/ready"; then up=1; break; fi
      sleep 2
    done
    [[ "$up" == "1" ]] || { record "Restart recovery" FAIL "master /ready not 200 after restart"; return; }
    # Worker must reconnect + re-register against the fresh master.
    sleep 10
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
  fi
  record "Restart recovery" PASS
}

# ---------------------------------------------------------------------------
# Short soak (reduced-duration cap-10 soak; chaos is real but short)
# ---------------------------------------------------------------------------
run_short_soak() {
  local soak="$REPO_ROOT/scripts/cert/cap-10-soak.sh"
  if [[ ! -f "$soak" || -z "$WORKER_IMAGE" ]]; then
    if stage_skip_ack short_soak; then
      record "Short soak" SKIP "acknowledged: soak harness unavailable"
    else
      record "Short soak" SKIP "cap-10-soak harness unavailable; POST_CERT_SKIP_SHORT_SOAK=1 to acknowledge"
    fi
    return
  fi
  SOAK_HOURS="$SOAK_HOURS" EVIDENCE_ROOT_CAP10="$EVIDENCE/soak" \
  bash "$soak" >"$EVIDENCE/soak.log" 2>&1 \
    || { record "Short soak" FAIL "cap-10-soak (see $EVIDENCE/soak.log)"; return; }
  record "Short soak" PASS
}

# ---------------------------------------------------------------------------
# Performance regression gate (vs baseline)
# ---------------------------------------------------------------------------
run_perf_regression() {
  local baseline="${VELOX_BENCH_BASELINE:-}"
  if [[ -z "$baseline" ]]; then
    if stage_skip_ack perf_regression; then
      record "Performance regression" SKIP "acknowledged: no VELOX_BENCH_BASELINE"
    else
      record "Performance regression" SKIP "no VELOX_BENCH_BASELINE; set one or POST_CERT_SKIP_PERF_REGRESSION=1 to acknowledge"
    fi
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
  local had_fail=0 had_skip=0 ack_skip=0
  printf '\n'
  printf '%s\n' "POST DEPLOY CERTIFICATION"
  printf '%s\n' "-------------------------"
  for stage in "${STAGE_ORDER[@]}"; do
    local res="${STAGE_RESULT[$stage]}" detail="${STAGE_DETAIL[$stage]:-}"
    case "$res" in
      PASS) printf '%-24s %s%s%s %s\n' "$stage" "$I_GREEN" "PASS" "$I_RST" "$detail" ;;
      FAIL) printf '%-24s %s%s%s %s\n' "$stage" "$I_RED" "FAIL" "$I_RST" "$detail"; had_fail=1 ;;
      SKIP) printf '%-24s %s%s%s %s\n' "$stage" "$I_YELLOW" "SKIP" "$I_RST" "$detail"; had_skip=1
            if stage_skip_ack "$stage"; then ack_skip=1; else :; fi ;;
    esac
  done
  printf '%-24s %s\n' "Git commit" "$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  printf '%s\n' "-------------------------"

  if [[ "$had_fail" == "1" ]]; then
    printf '%s\n' "VERDICT: FAILED"
    exit 1
  fi
  if [[ "$had_skip" == "1" && "$ack_skip" == "0" ]]; then
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
             "Docker|docker build (worker+master) + in-image C++/LibAV + compose up" \
             "Master readiness|DataServer /health + /ready" \
             "Worker readiness|worker /health /health/ready /health/live + runtime-cert" \
             "Capability contract|check-capability-contract.sh + live capabilities scan" \
             "Canary E2E|submit-canary-local.sh / submit-canary-remote.sh" \
             "Copy-only|benchmark-worker.sh COPY_ONLY_REAL (zero-spawn gate)" \
             "Cold cache|benchmark-worker.sh -cache cold" \
             "Warm cache|cold vs warm artifact SHA equality" \
             "Prefetch|POST_CERT_PREFETCH_CMD probe" \
             "Restart recovery|cap-7-reboot-recovery.sh + master restart" \
             "Short soak|cap-10-soak.sh (SOAK_HOURS=$SOAK_HOURS)" \
             "Performance regression|benchmark-worker.sh vs VELOX_BENCH_BASELINE"; do
      printf '  %-24s %s\n' "${s%%|*}" "${s#*|}"
    done
    exit 0
    ;;
  --no-build) NO_BUILD=1 ;;
esac

# ---------------------------------------------------------------------------
# Config pre-flight
# ---------------------------------------------------------------------------
if [[ -z "$WORKER_ID" ]]; then
  log "config: VELOX_WORKER_ID unset — worker identity checks will be limited"
fi
if [[ -z "$MASTER_URL" ]]; then
  log "config: VELOX_MASTER_URL unset — master readiness + canary stages will SKIP"
fi

log "post-deploy certification starting (commit=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown), evidence=$EVIDENCE)"
log "wiring: worker=$WORKER_IMAGE master=$MASTER_IMAGE worker_id=${WORKER_ID:-unset}"

# ---------------------------------------------------------------------------
# Stage execution
# ---------------------------------------------------------------------------
run_predeploy_gates
run_docker
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
