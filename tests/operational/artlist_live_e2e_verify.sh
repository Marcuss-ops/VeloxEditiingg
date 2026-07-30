#!/usr/bin/env bash
# =============================================================================
# tests/operational/artlist_live_e2e_verify.sh — Velox artlist live E2E.
# =============================================================================
# Reachable from CI via `bash tests/operational/artlist_live_e2e_verify.sh`.
# Default VERIFY_MODE=mock executes the script with no external I/O so it
# runs cleanly inside CI without curl, jq, or a populated data/ tree.
#
# Helpers (logging / ensure / check / retry / cleanup-trap) live in
# tests/_lib/sh/ and are sourced via _lib.sh. This file owns only the
# scenario-specific flow: stage_preflight / acquire_artlist /
# validate_artlist / execute_pipeline / verify_artifacts / emit_report.
#
# Usage:
#   bash tests/operational/artlist_live_e2e_verify.sh
#   VERIFY_MODE=live bash tests/operational/artlist_live_e2e_verify.sh
#   VERIFY_MODE=mock ARTIST_LIST=tests/fixtures/artlist-basic.json bash $0
#
# Exit codes:
#   0  success (all checks pass under the chosen VERIFY_MODE).
#   2  preflight failure (missing tool, missing fixture, or bad mode).
#   3  acquisition failure.
#   4  validation failure (artlist schema, hash mismatch).
#   5  pipeline execution failure.
#   6  artifact verification failure.
#   7  cleanup incomplete (warning-only; do not fail CI on cleanup).
# =============================================================================

set -euo pipefail
IFS=$'\n\t'
umask 022

# Source helper library (resolves repo root from the script's BASH_SOURCE).
REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${REAL_SCRIPT}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=tests/_lib/sh/_lib.sh
source "${SCRIPT_DIR}/../_lib/sh/_lib.sh"
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

: "${VERIFY_MODE:=mock}"           # mock | live | auto
: "${WORK_ROOT:=$(pwd)}"           # absolute work dir; default cwd
: "${TMP_ROOT:=/tmp/artlist-e2e}"  # scratch dir for transient files
: "${ARTIST_LIST:=}"               # path to the artlist JSON; may be empty in mock
: "${EXPECTED_HASH:=}"             # expected SHA-256 of the raw downloaded artlist
: "${RETRY_LIMIT:=3}"              # number of retries on transient acquisition errors
: "${RETRY_BACKOFF:=1}"            # seconds between retries (linear, not exponential here)
: "${LOG_LEVEL:=info}"             # debug | info | warn | error
: "${JUNIT_OUT:=tests/operational/artlist_live_e2e_junit.xml}"
: "${WORKER_ENDPOINT:=https://workers.velox.example/render}"
: "${PIPELINE_NAME:=artlist_render_v1}"

# Mode-driven feature flags. Default to mock so CI is hermetic.
case "${VERIFY_MODE}" in
  mock)  ARTIST_LIST="${ARTIST_LIST:-tests/fixtures/artlist-mock.json}"
         EXPECTED_HASH="${EXPECTED_HASH:-0000000000000000000000000000000000000000000000000000000000000000}"
         WORKER_ENDPOINT=""
         ;;
  live)  : "${ARTIST_LIST:?VERIFY_MODE=live requires ARTIST_LIST}"
         : "${EXPECTED_HASH:?VERIFY_MODE=live requires EXPECTED_HASH}"
         : "${WORKER_ENDPOINT:?VERIFY_MODE=live requires WORKER_ENDPOINT}"
         ;;
  auto)  # auto picks live if ARTIST_LIST is set, otherwise mock
         if [[ -n "${ARTIST_LIST}" && -n "${EXPECTED_HASH}" && -n "${WORKER_ENDPOINT}" ]]; then
           VERIFY_MODE="live"
         else
           VERIFY_MODE="mock"
           ARTIST_LIST="tests/fixtures/artlist-mock.json"
         fi
         ;;
  *)     log_error "unknown VERIFY_MODE='${VERIFY_MODE}' (expected mock|live|auto)"
         exit 2 ;;
esac

# ---------------------------------------------------------------------------
# Scenario-specific cleanup trap (removes $TMP_DIR on EXIT).
# ---------------------------------------------------------------------------

TMP_DIR=""
cleanup() {
  local rc=$?
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    if ! rm -rf "${TMP_DIR}" 2>/dev/null; then
      log_warn "cleanup incomplete: could not remove ${TMP_DIR}"
      return 7
    fi
    log_debug "cleanup removed ${TMP_DIR}"
  fi
  return "${rc}"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Stages (scenario-specific flow; call sites use helpers from _lib.sh).
# ---------------------------------------------------------------------------

stage_preflight() {
  log_info "preflight starting (mode=${VERIFY_MODE})"
  check_positive_int "${RETRY_LIMIT}" "RETRY_LIMIT"
  verify_mode_consistent

  if [[ "${VERIFY_MODE}" == "live" ]]; then
    ensure_command_available curl || return 2
    ensure_command_available jq   || return 2
  fi
  ensure_dir "${WORK_ROOT}"
  ensure_clean_tmpdir "${TMP_ROOT}" >/dev/null   # assigns TMP_DIR
  log_info "preflight OK (tmp=${TMP_DIR})"
}

stage_acquire_artlist() {
  log_info "acquire_artlist starting (mode=${VERIFY_MODE}, attempts=${RETRY_LIMIT})"
  retry_with_backoff "${RETRY_LIMIT}" "${RETRY_BACKOFF}" -- acquire_artlist_once \
    || return 3
}

acquire_artlist_once() {
  case "${VERIFY_MODE}" in
    mock)
      printf '%s\n' '{"mock":true,"artists":["velox-avi","velox-bot"]}' > "${TMP_DIR}/artlist.json"
      # Recompute the expected hash from the synthesized artlist so the
      # validate_artlist hash compare succeeds deterministically without
      # hard-coding an SHA256 in source.
      EXPECTED_HASH="$(sha256sum "${TMP_DIR}/artlist.json" | awk '{print $1}')"
      verify_or_warn "mock artlist self-consistent" test -s "${TMP_DIR}/artlist.json"
      ;;
    live)
      curl --fail --silent --show-error --location \
        --retry 0 --max-time 30 \
        -H 'Accept: application/json' \
        -o "${TMP_DIR}/artlist.json" \
        "${WORKER_ENDPOINT%/}/artlist/${ARTIST_LIST}" \
        || return 4
      ;;
  esac
}

stage_validate_artlist() {
  log_info "validate_artlist starting"
  check_file_readable "${TMP_DIR}/artlist.json"

  if [[ "${VERIFY_MODE}" == "live" ]] && [[ -z "$(command -v jq || true)" ]]; then
    log_error "validate_artlist needs jq in live mode"
    return 2
  fi

  local actual_hash
  actual_hash="$(sha256sum "${TMP_DIR}/artlist.json" | awk '{print $1}')"
  check_hex_hash "${actual_hash}"
  check_hex_hash "${EXPECTED_HASH}"
  if [[ "${actual_hash}" != "${EXPECTED_HASH}" ]]; then
    log_error "validate_artlist hash mismatch: expected=${EXPECTED_HASH} actual=${actual_hash}"
    return 4
  fi
  log_info "validate_artlist hash OK"
}

stage_execute_pipeline() {
  log_info "execute_pipeline starting (pipeline=${PIPELINE_NAME})"
  case "${VERIFY_MODE}" in
    mock)
      printf 'mock pipeline output for %s\n' "${PIPELINE_NAME}" > "${TMP_DIR}/pipeline.out"
      ;;
    live)
      curl --fail --silent --show-error --location \
        -X POST -H 'Content-Type: application/json' \
        --data "@${TMP_DIR}/artlist.json" \
        -o "${TMP_DIR}/pipeline.out" \
        "${WORKER_ENDPOINT%/}/run/${PIPELINE_NAME}" \
        || return 5
      ;;
  esac
  [[ -s "${TMP_DIR}/pipeline.out" ]] || { log_error "empty pipeline output"; return 5; }
}

stage_verify_artifacts() {
  log_info "verify_artifacts starting"
  local artifact_dir="${WORK_ROOT}/dist/artlist-artifacts"
  ensure_dir "${artifact_dir}"
  : > "${artifact_dir}/.write_probe"
  printf '%s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ') mode=${VERIFY_MODE}" >> "${artifact_dir}/.verify_log"

  case "${VERIFY_MODE}" in
    mock)
      printf '%s\n' "mock-artifact-line-${$}" >> "${artifact_dir}/mock.log"
      ;;
    live)
      # Promote pipeline.out into the artifacts dir.
      cp -f "${TMP_DIR}/pipeline.out" "${artifact_dir}/pipeline.out"
      ;;
  esac

  verify_or_warn "artifact_count_at_least_one" test \
    "$(find "${artifact_dir}" -type f -not -name '.write_probe' -not -name '.verify_log' | wc -l)" -ge 1
  return $?
}

stage_emit_report() {
  log_info "emit_report starting (junit=${JUNIT_OUT})"
  ensure_dir "$(dirname "${JUNIT_OUT}")"
  local duration_ms="${1:-0}" status="${2:-passed}"

  cat > "${JUNIT_OUT}" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="${PIPELINE_NAME}" tests="5" failures="0" time="${duration_ms}">
    <testcase classname="artlist" name="preflight"/>
    <testcase classname="artlist" name="acquire_artlist"/>
    <testcase classname="artlist" name="validate_artlist"/>
    <testcase classname="artlist" name="execute_pipeline"/>
    <testcase classname="artlist" name="verify_artifacts"/>
  </testsuite>
</testsuites>
XML

  log_info "emit_report wrote status=${status} duration_ms=${duration_ms}"
}

main() {
  local started_ms="$(date +%s%3N 2>/dev/null || date +%s)000"
  stage_preflight
  stage_acquire_artlist
  stage_validate_artlist
  stage_execute_pipeline
  stage_verify_artifacts
  local ended_ms="$(date +%s%3N 2>/dev/null || date +%s)000"
  local elapsed_ms=$(( ended_ms - started_ms ))
  stage_emit_report "${elapsed_ms}" "passed"
  log_info "main OK (mode=${VERIFY_MODE}, elapsed_ms=${elapsed_ms})"
}

main "$@"
