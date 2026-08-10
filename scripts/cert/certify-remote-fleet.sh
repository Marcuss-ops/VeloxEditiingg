#!/usr/bin/env bash
# shellcheck disable=SC2034
# Shared trap state is intentionally consumed by sourced domain components.
# =============================================================================
# certify-remote-fleet.sh — orchestrate remote-worker certification per fleet.
#
# This is an external harness: it reuses the single-worker runner and never
# writes production state directly. By default workers are certified serially
# so one operator run has deterministic ordering and bounded load.
#
# Usage:
#   ./scripts/cert/certify-remote-fleet.sh \
#     --mode quick --workers worker-01,worker-02 --serial
#
# Modes:
#   quick       --worker-json
#   full        --worker-json, --lifecycle-json, --update-json, --job-json
#   destructive tests/worker-cert/worker_offline_recovery.sh (opt-in only)
#
# Destructive mode requires all of:
#   VELOX_CERT_ENV=staging|canary|development|test|local
#   VELOX_CERT_ALLOW_DESTRUCTIVE=1
#   VELOX_CERT_DESTRUCTIVE_ACK=I_UNDERSTAND_DESTRUCTIVE_CERT
#   RW_WORKER_CRASH_CMD and RW_JOB_DESTINATION_ID
# It is rejected for production-like environments and URLs.
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
SINGLE_RUNNER="${RW_FLEET_SINGLE_RUNNER:-${SCRIPT_DIR}/remote-worker-cert-config.sh}"
DESTRUCTIVE_RUNNER="${RW_FLEET_DESTRUCTIVE_RUNNER:-${REPO_ROOT}/tests/worker-cert/worker_offline_recovery.sh}"

# Runtime state used by the EXIT trap. Evidence is preserved; only the private
# transit directory and intermediate NDJSON are removed after finalization.
# These globals are consumed by the sourced cleanup/invariant/runner modules.
# shellcheck disable=SC2034
FLEET_DIR=""
FLEET_TMP_DIR=""
FLEET_RESULTS_FILE=""
FLEET_REPORT=""
FLEET_MODE=""
FLEET_RUN_ID=""
FLEET_MAIN_STATUS=FAIL
FLEET_CLEANUP_RUNNING=0
FLEET_FINALIZING=0
FLEET_WORKERS=()
CLEANUP_NETWORK_STATUS=NOT_RUN
CLEANUP_WORKER_STATUS=NOT_RUN
CLEANUP_TEMP_STATUS=NOT_RUN
# shellcheck disable=SC2034
INVARIANT_STATUS=NOT_RUN
INVARIANT_CHECKED=0
INVARIANT_DIAGNOSTIC=""
INVARIANT_LEASES=null
INVARIANT_JOBS=null
INVARIANT_TASKS=null
INVARIANT_OPERATIONS=null

fleet_die() {
  printf 'certify-remote-fleet: %s\n' "$*" >&2
  return 2
}


# Sourced components use the entrypoint's absolute SCRIPT_DIR path; ShellCheck
# cannot resolve that runtime-computed path statically.
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/remote-fleet-safety.sh"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/remote-fleet-report.sh"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/remote-fleet-invariants.sh"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/remote-fleet-cleanup.sh"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/remote-fleet-runner.sh"

trap fleet_cleanup EXIT
fleet_usage() {
  cat <<'USAGE'
Usage:
  certify-remote-fleet.sh --mode quick|full|destructive --workers ID[,ID...] [options]

Options:
  --mode MODE          Certification mode (required).
  --workers LIST       Comma-separated worker IDs (required; also VELOX_CERT_WORKERS).
  --serial             Run workers serially (default and currently enforced).
                       Parallel execution is intentionally unsupported.
  --artifact-dir DIR   Fleet evidence directory.
  --run-id ID          Explicit fleet run ID.
  --help               Show this help.

Environment:
  VELOX_CERT_WORKERS              Default worker list when --workers is omitted.
  RW_FLEET_ARTIFACT_DIR           Default artifact directory.
  RW_FLEET_SINGLE_RUNNER          Override single-worker runner for tests/operators.
  RW_FLEET_DESTRUCTIVE_RUNNER     Override destructive runner path.
  RW_FLEET_WORKER_TIMEOUT_S       Optional per-worker timeout (default: 0/unbounded).
  RW_FLEET_INVARIANTS_MODE        required (default), skip, or command.
  RW_FLEET_ORPHAN_CHECK_CMD        Command emitting {leases,jobs,tasks,operations} counts.
  RW_FLEET_RESTORE_NETWORK_CMD     Cleanup command to remove temporary network rules.
  RW_FLEET_NETWORK_RULES_APPLIED   Set 1 when this run changed network rules.
  RW_FLEET_WORKER_START_CMD        Cleanup command to ensure a worker is started.
  RW_FLEET_ENSURE_WORKER_STARTED   Set 1 to require worker-start cleanup in non-destructive modes.
  RW_FLEET_CLEANUP_TIMEOUT_S       Timeout for each cleanup command (default: 30).
  RW_FLEET_INVARIANT_TIMEOUT_S     Timeout for orphan check (default: 30).

Destructive safety:
  VELOX_CERT_ENV                   Must explicitly be staging/canary/development/test/local.
  VELOX_CERT_ALLOW_DESTRUCTIVE=1   Explicit destructive opt-in.
  VELOX_CERT_DESTRUCTIVE_ACK=I_UNDERSTAND_DESTRUCTIVE_CERT
  RW_WORKER_CRASH_CMD              Command passed to the recovery runner; stop the systemd unit.
  RW_WORKER_RESTART_OWNER_CHECK_CMD Read-only command emitting restart-owner key=value facts.
  The command is passed to worker_offline_recovery.sh before any job/stop action.
  RW_JOB_DESTINATION_ID            Explicit destination for the recovery job.

Invariant command contract:
  RW_FLEET_ORPHAN_CHECK_CMD must print one JSON object, for example:
  {"leases":0,"jobs":0,"tasks":0,"operations":0}
USAGE
}

# Domain components are sourced after fleet_die is defined below.

main() {
  local mode="" workers_csv="${VELOX_CERT_WORKERS:-}" fleet_dir="${RW_FLEET_ARTIFACT_DIR:-}" run_id="${RW_FLEET_RUN_ID:-}" serial=1
  local fleet_report results_file worker overall

  while (( $# > 0 )); do
    case "$1" in
      --mode) [[ $# -ge 2 ]] || { fleet_die "--mode requires a value"; return 2; }; mode="$2"; shift 2 ;;
      --workers) [[ $# -ge 2 ]] || { fleet_die "--workers requires a value"; return 2; }; workers_csv="$2"; shift 2 ;;
      --serial) serial=1; shift ;;
      --parallel) fleet_die "parallel execution is not supported; use --serial"; return 2 ;;
      --artifact-dir) [[ $# -ge 2 ]] || { fleet_die "--artifact-dir requires a value"; return 2; }; fleet_dir="$2"; shift 2 ;;
      --run-id) [[ $# -ge 2 ]] || { fleet_die "--run-id requires a value"; return 2; }; run_id="$2"; shift 2 ;;
      --help|-h) fleet_usage; return 0 ;;
      *) fleet_die "unknown option: $1 (use --help)"; return 2 ;;
    esac
  done

  [[ -n "$mode" ]] || { fleet_die "--mode is required"; return 2; }
  validate_mode "$mode" || return 2
  [[ -n "$workers_csv" ]] || { fleet_die "--workers or VELOX_CERT_WORKERS is required"; return 2; }
  require_command jq || return 2
  require_command python3 || return 2
  require_command timeout || return 2
  [[ -f "$SINGLE_RUNNER" ]] || { fleet_die "single-worker runner not found: $SINGLE_RUNNER"; return 2; }
  if [[ "${RW_FLEET_INVARIANTS_MODE:-required}" != skip && -z "${RW_FLEET_ORPHAN_CHECK_CMD:-}" ]]; then
    fleet_die "RW_FLEET_ORPHAN_CHECK_CMD is required for live certification (use RW_FLEET_INVARIANTS_MODE=skip only for offline/mock runs)"
    return 2
  fi
  if [[ -z "${RW_FLEET_WORKER_START_CMD:-}" ]]; then
    fleet_die "RW_FLEET_WORKER_START_CMD is required to verify worker-start cleanup"
    return 2
  fi
  export RW_FLEET_ENSURE_WORKER_STARTED=1
  if [[ "$mode" == destructive ]]; then
    guard_destructive || return 2
  fi

  run_id="${run_id:-fleet-cert-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
  fleet_dir="${fleet_dir:-${TMPDIR:-/tmp}/velox-fleet-${run_id}}"
  mkdir -p -- "$fleet_dir" || { fleet_die "cannot create artifact directory: $fleet_dir"; return 2; }
  validate_safe_path_value RW_FLEET_ARTIFACT_DIR "$fleet_dir" || return 2
  FLEET_DIR="$fleet_dir"
  FLEET_MODE="$mode"
  FLEET_RUN_ID="$run_id"
  export RW_FLEET_RUN_ID="$run_id"
  FLEET_TMP_DIR="$(mktemp -d "${fleet_dir}/.tmp.XXXXXX")" || { fleet_die "cannot create temporary directory under evidence dir"; return 2; }
  if [[ "${RW_FLEET_NETWORK_RULES_APPLIED:-0}" == 1 && -z "${RW_FLEET_RESTORE_NETWORK_CMD:-}" ]]; then
    fleet_die "RW_FLEET_RESTORE_NETWORK_CMD is required when RW_FLEET_NETWORK_RULES_APPLIED=1"
    return 2
  fi
  results_file="${fleet_dir}/.workers.ndjson"
  FLEET_RESULTS_FILE="$results_file"
  : >"$results_file"
  IFS=',' read -r -a workers <<<"$workers_csv"
  ((${#workers[@]} > 0)) || { fleet_die "worker list is empty"; return 2; }
  declare -A seen=()
  for index in "${!workers[@]}"; do
    worker="$(trim_worker_id "${workers[index]}")"
    workers[index]="$worker"
    [[ -n "$worker" ]] || { fleet_die "worker list contains an empty ID"; return 2; }
    validate_worker_id "$worker" || return 2
    [[ -z "${seen[$worker]:-}" ]] || { fleet_die "duplicate worker ID: $worker"; return 2; }
    seen[$worker]=1
  done

  printf '%s\n' "run_id=${run_id} mode=${mode} serial=${serial}" >"${fleet_dir}/commands.log"
  printf '%s\n' "workers=${workers_csv}" >>"${fleet_dir}/commands.log"
  overall=PASS
  for worker in "${workers[@]}"; do
    if ! run_one_worker "$worker" "$fleet_dir" "$mode" "$results_file"; then
      overall=FAIL
    fi
  done

  if ! fleet_check_invariants; then
    overall=FAIL
  fi
  # Run cleanup before writing the final report so cleanup and invariant
  # statuses are included. Preserve the NDJSON accumulator until jq has
  # consumed it; the EXIT trap then remains idempotent.
  FLEET_FINALIZING=1
  fleet_cleanup || overall=FAIL
  fleet_report="${fleet_dir}/fleet-report.json"
  FLEET_REPORT="$fleet_report"
  jq -n --arg run_id "$run_id" --arg mode "$mode" --arg overall "$overall" \
    --arg artifact_dir "$fleet_dir" --arg invariant_status "$INVARIANT_STATUS" \
    --arg invariant_diagnostic "$INVARIANT_DIAGNOSTIC" \
    --arg cleanup_network "$CLEANUP_NETWORK_STATUS" --arg cleanup_worker "$CLEANUP_WORKER_STATUS" \
    --arg cleanup_temp "$CLEANUP_TEMP_STATUS" --slurpfile worker_results "$results_file" \
    --argjson orphan_leases "$INVARIANT_LEASES" --argjson orphan_jobs "$INVARIANT_JOBS" \
    --argjson orphan_tasks "$INVARIANT_TASKS" --argjson orphan_operations "$INVARIANT_OPERATIONS" \
    '{schema:"velox.remote_worker.fleet.v1",run_id:$run_id,mode:$mode,serial:true,artifact_dir:$artifact_dir,overall:$overall,workers:$worker_results,cleanup:{status:(if ($cleanup_network=="FAIL" or $cleanup_worker=="FAIL" or $cleanup_temp=="FAIL") then "FAIL" else "PASS" end),network:$cleanup_network,worker:$cleanup_worker,temporary:$cleanup_temp},invariants:{status:$invariant_status,diagnostic:(if $invariant_diagnostic=="" then null else $invariant_diagnostic end),orphan_leases:$orphan_leases,orphan_jobs:$orphan_jobs,orphan_tasks:$orphan_tasks,orphan_operations:$orphan_operations}}' >"$fleet_report"
  write_fleet_junit "$fleet_report" "${fleet_dir}/fleet-report.junit.xml" "$mode"
  cp -- "$fleet_report" "${fleet_dir}/report.json"
  FLEET_REPORT_WRITTEN=1
  printf '%s\n' "fleet_report=${fleet_report}" >&2
  cat "$fleet_report"
  # The report is durable evidence; the NDJSON accumulator is not.
  rm -f -- "$results_file"
  FLEET_RESULTS_FILE=""
  FLEET_REPORT_WRITTEN=1
  [[ "$overall" == PASS ]]
}

main "$@"
