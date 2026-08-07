#!/usr/bin/env bash
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
INVARIANT_STATUS=NOT_RUN
INVARIANT_CHECKED=0
INVARIANT_DIAGNOSTIC=""
INVARIANT_LEASES=null
INVARIANT_JOBS=null
INVARIANT_TASKS=null
INVARIANT_OPERATIONS=null

trap fleet_cleanup EXIT

fleet_die() {
  printf 'certify-remote-fleet: %s\n' "$*" >&2
  return 2
}

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

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    fleet_die "required command not found: $1"
    return 1
  }
}

validate_worker_id() {
  local worker="$1"
  [[ -n "$worker" && "$worker" != *[[:space:]]* ]] || {
    fleet_die "worker ID contains whitespace: $worker"
    return 1
  }
  [[ "$worker" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]] || {
    fleet_die "invalid worker ID: $worker"
    return 1
  }
}

trim_worker_id() {
  # Trim only CSV separator padding; internal whitespace is rejected rather
  # than silently changing the requested identity.
  printf '%s' "$1" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
}

validate_mode() {
  case "$1" in
    quick|full|destructive) return 0 ;;
    *) fleet_die "mode must be quick, full, or destructive (got: $1)"; return 1 ;;
  esac
}

validate_safe_path_value() {
  local name="$1" value="$2"
  [[ -n "$value" && "$value" != *$'\n'* && "$value" != *$'\r'* ]] || {
    fleet_die "${name} must be non-empty and contain no newlines"
    return 1
  }
}

production_like() {
  local env_name="${VELOX_CERT_ENV:-${VELOX_ENV:-${ENVIRONMENT:-}}}"
  local url="${MASTER_URL:-${VELOX_MASTER_URL:-}}"
  local production_flag="${VELOX_PRODUCTION:-${PRODUCTION:-0}}"
  env_name="${env_name,,}"
  url="${url,,}"
  [[ "$production_flag" == 1 || "$production_flag" == true ]] && return 0
  case "$env_name" in
    prod|production|live|release) return 0 ;;
  esac
  case "$url" in
    *production*|*prod.*|*prod-*|*://prod/*|*://prod:*) return 0 ;;
  esac
  return 1
}

guard_destructive() {
  local env_name="${VELOX_CERT_ENV:-${VELOX_ENV:-${ENVIRONMENT:-}}}"
  env_name="${env_name,,}"
  case "$env_name" in
    staging|canary|development|test|local) ;;
    prod|production|live|release)
      fleet_die "destructive mode is blocked in environment '${env_name}'"
      return 1
      ;;
    *)
      fleet_die "destructive mode requires explicit VELOX_CERT_ENV=staging|canary|development|test|local"
      return 1
      ;;
  esac
  if production_like; then
    fleet_die "destructive mode is blocked for production-like MASTER_URL/environment"
    return 1
  fi
  [[ "${VELOX_CERT_ALLOW_DESTRUCTIVE:-0}" == 1 ]] || {
    fleet_die "set VELOX_CERT_ALLOW_DESTRUCTIVE=1 to opt into destructive mode"
    return 1
  }
  [[ "${VELOX_CERT_DESTRUCTIVE_ACK:-}" == I_UNDERSTAND_DESTRUCTIVE_CERT ]] || {
    fleet_die "set VELOX_CERT_DESTRUCTIVE_ACK=I_UNDERSTAND_DESTRUCTIVE_CERT to confirm destructive testing"
    return 1
  }
  [[ -n "${RW_WORKER_CRASH_CMD:-}" ]] || {
    fleet_die "RW_WORKER_CRASH_CMD is required for destructive mode"
    return 1
  }
  [[ -n "${RW_WORKER_RESTART_OWNER_CHECK_CMD:-}" ]] || {
    fleet_die "RW_WORKER_RESTART_OWNER_CHECK_CMD is required for destructive mode"
    return 1
  }
  [[ -n "${RW_JOB_DESTINATION_ID:-}" ]] || {
    fleet_die "RW_JOB_DESTINATION_ID is required for destructive mode"
    return 1
  }
  [[ -n "${RW_FLEET_WORKER_START_CMD:-}" ]] || {
    fleet_die "RW_FLEET_WORKER_START_CMD is required for destructive cleanup"
    return 1
  }
  if [[ "${RW_FLEET_NETWORK_RULES_APPLIED:-0}" == 1 && -z "${RW_FLEET_RESTORE_NETWORK_CMD:-}" ]]; then
    fleet_die "RW_FLEET_RESTORE_NETWORK_CMD is required when RW_FLEET_NETWORK_RULES_APPLIED=1"
    return 1
  fi
  validate_safe_path_value RW_WORKER_CRASH_CMD "$RW_WORKER_CRASH_CMD" || return 1
  validate_safe_path_value RW_WORKER_RESTART_OWNER_CHECK_CMD "$RW_WORKER_RESTART_OWNER_CHECK_CMD" || return 1
  [[ -x "$DESTRUCTIVE_RUNNER" || -f "$DESTRUCTIVE_RUNNER" ]] || {
    fleet_die "destructive runner not found: $DESTRUCTIVE_RUNNER"
    return 1
  }
}

write_fleet_junit() {
  local report="$1" output="$2" mode="$3"
  python3 - "$report" "$output" "$mode" <<'PY'
import json
import sys
from xml.sax.saxutils import escape, quoteattr

report_path, output_path, mode = sys.argv[1:]
try:
    report = json.load(open(report_path, encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    report = {"overall": "FAIL", "workers": []}
workers = report.get("workers") or []
failures = sum(1 for worker in workers if worker.get("status") != "PASS")
with open(output_path, "w", encoding="utf-8") as out:
    out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    out.write('<testsuite name=%s tests="%d" failures="%d">\n' %
              (quoteattr("velox.remote_worker.fleet." + mode), max(1, len(workers)), failures))
    if workers:
        for worker in workers:
            worker_id = str(worker.get("worker_id", "unknown"))
            status = str(worker.get("status", "FAIL"))
            diagnostic = str(worker.get("diagnostic", ""))
            out.write('  <testcase name=%s>' % quoteattr(worker_id))
            if status != "PASS":
                out.write('<failure message=%s>%s</failure>' %
                          (quoteattr(diagnostic[:500]), escape(diagnostic)))
            out.write('</testcase>\n')
    else:
        out.write('  <testcase name="fleet"><failure message="no workers"/></testcase>\n')
    out.write('</testsuite>\n')
PY
}

redact_destructive_artifacts() {
  local step_dir="$1" command="$2"
  [[ -n "$command" && -d "$step_dir" ]] || return 0
  python3 - "$step_dir" "$command" <<'PY'
from pathlib import Path
import json
import sys

root = Path(sys.argv[1])
secret = sys.argv[2]
for name in ("stdout.log", "stderr.log"):
    path = root / name
    if path.is_file():
        path.write_text(path.read_text(errors="replace").replace(secret, "[REDACTED_DESTRUCTIVE_COMMAND]"))
report = root / "report.json"
if report.is_file():
    try:
        data = json.loads(report.read_text())
        if isinstance(data, dict) and "stop_cmd" in data:
            data["stop_cmd"] = "[REDACTED_DESTRUCTIVE_COMMAND]"
            report.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    except (OSError, json.JSONDecodeError):
        pass
PY
}

run_cleanup_command() {
  local name="$1" command="$2" output rc timeout_s="${RW_FLEET_CLEANUP_TIMEOUT_S:-30}"
  [[ "$timeout_s" =~ ^[1-9][0-9]*$ ]] || {
    printf '%s' FAILED
    return 1
  }
  [[ -n "$command" ]] || {
    printf '%s' NOT_CONFIGURED
    return 1
  }
  output="$(mktemp "${TMPDIR:-/tmp}/velox-fleet-cleanup.XXXXXX")" || {
    printf '%s' FAILED
    return 1
  }
  if timeout "$timeout_s" bash -c "$command" >"$output" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  rm -f -- "$output"
  if (( rc == 0 )); then
    printf '%s' PASS
    return 0
  fi
  printf '%s' FAILED
  return 1
}

fleet_check_invariants() {
  local mode="${RW_FLEET_INVARIANTS_MODE:-required}" command output
  INVARIANT_STATUS=NOT_RUN
  INVARIANT_CHECKED=0
  INVARIANT_DIAGNOSTIC=""
  INVARIANT_LEASES=null
  INVARIANT_JOBS=null
  INVARIANT_TASKS=null
  INVARIANT_OPERATIONS=null
  case "$mode" in
    skip)      INVARIANT_STATUS=SKIPPED
      INVARIANT_CHECKED=1
      INVARIANT_DIAGNOSTIC="explicitly skipped by RW_FLEET_INVARIANTS_MODE=skip"
      return 0

      ;;
    command|required) ;;
    *)
      INVARIANT_STATUS=FAIL
      INVARIANT_CHECKED=1
      INVARIANT_DIAGNOSTIC="RW_FLEET_INVARIANTS_MODE must be required, command, or skip"
      return 1
      ;;
  esac
  command="${RW_FLEET_ORPHAN_CHECK_CMD:-}"
  if [[ -z "$command" ]]; then    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="RW_FLEET_ORPHAN_CHECK_CMD is required unless RW_FLEET_INVARIANTS_MODE=skip"
    return 1

  fi
  [[ "$command" != *$'\r'* && "$command" != *$'\n'* ]] || {
    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="RW_FLEET_ORPHAN_CHECK_CMD contains CR/LF"
    return 1
  }
  output="$(mktemp "${TMPDIR:-/tmp}/velox-fleet-invariants.XXXXXX")" || {
    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="cannot create invariant scratch file"
    return 1
  }
  local timeout_s="${RW_FLEET_INVARIANT_TIMEOUT_S:-30}"
  if ! [[ "$timeout_s" =~ ^[1-9][0-9]*$ ]]; then
    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="RW_FLEET_INVARIANT_TIMEOUT_S must be a positive integer"
    rm -f -- "$output"
    return 1
  fi
  if timeout "$timeout_s" bash -c "$command" >"$output" 2>&1 && jq -e \
      '(.leases // 0) as $leases |
       (.jobs // 0) as $jobs |
       (.tasks // 0) as $tasks |
       (.operations // 0) as $operations |
       ($leases|type)=="number" and ($leases % 1 == 0) and ($leases >= 0) and
       ($jobs|type)=="number" and ($jobs % 1 == 0) and ($jobs >= 0) and
       ($tasks|type)=="number" and ($tasks % 1 == 0) and ($tasks >= 0) and
       ($operations|type)=="number" and ($operations % 1 == 0) and ($operations >= 0)' "$output" >/dev/null 2>&1; then
    INVARIANT_LEASES="$(jq -r '.leases // 0' "$output")"
    INVARIANT_JOBS="$(jq -r '.jobs // 0' "$output")"
    INVARIANT_TASKS="$(jq -r '.tasks // 0' "$output")"
    INVARIANT_OPERATIONS="$(jq -r '.operations // 0' "$output")"
    INVARIANT_STATUS=PASS
    INVARIANT_CHECKED=1
    if (( INVARIANT_LEASES + INVARIANT_JOBS + INVARIANT_TASKS + INVARIANT_OPERATIONS > 0 )); then
      INVARIANT_STATUS=FAIL
      INVARIANT_DIAGNOSTIC="orphaned resources detected"
    fi
  else
    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="orphan check failed or returned invalid JSON"
  fi
  rm -f -- "$output"
  [[ "$INVARIANT_STATUS" == PASS ]]
}

fleet_cleanup() {
  local rc=$? network_rc=0 worker_rc=0 temp_rc=0 cleanup_report
  (( FLEET_CLEANUP_RUNNING == 0 )) || return "$rc"
  FLEET_CLEANUP_RUNNING=1

  # Early exits still receive a final invariant observation and a durable
  # cleanup report. Normal runs write the richer fleet report in main().
  if [[ -n "${FLEET_DIR:-}" && "$INVARIANT_CHECKED" == 0 ]]; then
    fleet_check_invariants || rc=1
  fi

  if [[ "${RW_FLEET_NETWORK_RULES_APPLIED:-0}" == 1 ]]; then
    if CLEANUP_NETWORK_STATUS="$(run_cleanup_command network "${RW_FLEET_RESTORE_NETWORK_CMD:-}")"; then
      :
    else
      network_rc=1
    fi
  else
    CLEANUP_NETWORK_STATUS=NOT_APPLIED
  fi

  if [[ "${RW_FLEET_ENSURE_WORKER_STARTED:-0}" == 1 ]]; then
    if CLEANUP_WORKER_STATUS="$(run_cleanup_command worker "${RW_FLEET_WORKER_START_CMD:-}")"; then
      :
    else
      worker_rc=1
    fi
  else
    CLEANUP_WORKER_STATUS=NOT_REQUIRED
  fi

  if [[ -n "${FLEET_TMP_DIR:-}" && -d "$FLEET_TMP_DIR" ]]; then
    if rm -rf -- "$FLEET_TMP_DIR"; then
      CLEANUP_TEMP_STATUS=PASS
    else
      CLEANUP_TEMP_STATUS=FAIL
      temp_rc=1
    fi
  else
    CLEANUP_TEMP_STATUS=NOT_FOUND
  fi
  if [[ -n "${FLEET_RESULTS_FILE:-}" && "$FLEET_FINALIZING" != 1 ]]; then
    rm -f -- "$FLEET_RESULTS_FILE" || temp_rc=1
  fi
  if [[ -n "${FLEET_DIR:-}" && "${FLEET_REPORT_WRITTEN:-0}" != 1 ]]; then
    cleanup_report="${FLEET_DIR}/cleanup-report.json"
    jq -n --arg schema 'velox.remote_worker.fleet.cleanup.v1' \
      --arg run_id "${FLEET_RUN_ID:-}" --arg mode "${FLEET_MODE:-}" \
      --arg status "$([[ "$rc" -eq 0 && "$INVARIANT_STATUS" == PASS ]] && printf PASS || printf FAIL)" \
      --arg invariant_status "${INVARIANT_STATUS:-NOT_RUN}" \
      --arg invariant_diagnostic "${INVARIANT_DIAGNOSTIC:-}" \
      --arg network "${CLEANUP_NETWORK_STATUS:-NOT_RUN}" \
      --arg worker "${CLEANUP_WORKER_STATUS:-NOT_RUN}" \
      --arg temporary "${CLEANUP_TEMP_STATUS:-NOT_RUN}" \
      --argjson leases "${INVARIANT_LEASES:-null}" --argjson jobs "${INVARIANT_JOBS:-null}" \
      --argjson tasks "${INVARIANT_TASKS:-null}" --argjson operations "${INVARIANT_OPERATIONS:-null}" \
      '{schema:$schema,run_id:$run_id,mode:$mode,status:$status,cleanup:{network:$network,worker:$worker,temporary:$temporary},invariants:{status:$invariant_status,diagnostic:(if $invariant_diagnostic=="" then null else $invariant_diagnostic end),orphan_leases:$leases,orphan_jobs:$jobs,orphan_tasks:$tasks,orphan_operations:$operations}}' >"$cleanup_report" || rc=1
  fi
  (( network_rc == 0 && worker_rc == 0 && temp_rc == 0 )) || rc=1
  return "$rc"
}

run_one_worker() {
  local worker="$1" fleet_dir="$2" mode="$3" results_file="$4"
  local worker_dir="${fleet_dir}/${worker}" step cli rc status diagnostic report_file step_dir
  local -a steps=()
  case "$mode" in
    quick) steps=(worker) ;;
    full) steps=(worker lifecycle update job) ;;
    destructive) steps=(destructive) ;;
  esac

  mkdir -p -- "$worker_dir"
  : >"${worker_dir}/commands.log"
  status=PASS
  diagnostic=""
  for step in "${steps[@]}"; do
    step_dir="${worker_dir}/${step}"
    mkdir -p -- "$step_dir"
    case "$step" in
      worker) cli=--worker-json ;;
      lifecycle) cli=--lifecycle-json ;;
      update) cli=--update-json ;;
      job) cli=--job-json ;;
      destructive) cli="" ;;
    esac
    export WORKER_ID="$worker"
    export RW_CERT_MODE="$mode"
    export RW_RUN_ID="${RW_FLEET_RUN_ID}-${worker}-${step}"
    export RW_ARTIFACT_DIR="$step_dir"
    printf '[%s] worker=%s step=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$worker" "$step" >>"${worker_dir}/commands.log"

    if [[ "$step" == destructive ]]; then
      if [[ -n "${RW_FLEET_WORKER_TIMEOUT_S:-}" && "${RW_FLEET_WORKER_TIMEOUT_S}" != 0 ]]; then
        timeout "${RW_FLEET_WORKER_TIMEOUT_S}" bash "$DESTRUCTIVE_RUNNER" \
          --target-worker-id "$worker" \
          --target-worker-stop-cmd "$RW_WORKER_CRASH_CMD" \
          --target-worker-inspect-cmd "$RW_WORKER_RESTART_OWNER_CHECK_CMD" \
          --destination-id "$RW_JOB_DESTINATION_ID" \
          --report-json "${step_dir}/report.json" >"${step_dir}/stdout.log" 2>"${step_dir}/stderr.log"
        rc=$?
      else
        bash "$DESTRUCTIVE_RUNNER" \
          --target-worker-id "$worker" \
          --target-worker-stop-cmd "$RW_WORKER_CRASH_CMD" \
          --target-worker-inspect-cmd "$RW_WORKER_RESTART_OWNER_CHECK_CMD" \
          --destination-id "$RW_JOB_DESTINATION_ID" \
          --report-json "${step_dir}/report.json" >"${step_dir}/stdout.log" 2>"${step_dir}/stderr.log"
        rc=$?
      fi
      redact_destructive_artifacts "$step_dir" "$RW_WORKER_CRASH_CMD"
    else
      if [[ -n "${RW_FLEET_WORKER_TIMEOUT_S:-}" && "${RW_FLEET_WORKER_TIMEOUT_S}" != 0 ]]; then
        timeout "${RW_FLEET_WORKER_TIMEOUT_S}" bash "$SINGLE_RUNNER" "$cli" >"${step_dir}/stdout.json" 2>"${step_dir}/stderr.log"
        rc=$?
      else
        bash "$SINGLE_RUNNER" "$cli" >"${step_dir}/stdout.json" 2>"${step_dir}/stderr.log"
        rc=$?
      fi
    fi

    report_file="${step_dir}/report.json"
    if [[ ! -s "$report_file" && -s "${step_dir}/stdout.json" ]] && jq -e . "${step_dir}/stdout.json" >/dev/null 2>&1; then
      cp -- "${step_dir}/stdout.json" "$report_file"
    fi
    if [[ -s "$report_file" ]] && jq -e . "$report_file" >/dev/null 2>&1; then
      case "$step" in
        destructive)
          step_status="$(jq -r 'if .overall then .overall elif .status == "SUCCEEDED" then "PASS" elif .status == "PASS" then "PASS" else "FAIL" end' "$report_file")"
          ;;
        *) step_status="$(jq -r '.overall // "FAIL"' "$report_file")" ;;
      esac
    else
      step_status=FAIL
    fi
    if [[ "$rc" -ne 0 || "$step_status" != PASS ]]; then
      status=FAIL
      diagnostic="${diagnostic}${diagnostic:+; }${step} failed (rc=${rc}, status=${step_status})"
    fi
    # Full mode is ordered: later steps must not mutate state after a failed
    # prerequisite, but we retain the failure report and continue to the next
    # worker in the fleet.
    [[ "$status" == PASS ]] || break
  done

  jq -cn --arg worker_id "$worker" --arg status "$status" --arg diagnostic "$diagnostic" \
    --arg worker_dir "$worker_dir" --arg mode "$mode" \
    '{worker_id:$worker_id,status:$status,diagnostic:(if $diagnostic=="" then null else $diagnostic end),mode:$mode,artifact_dir:$worker_dir}' >>"$results_file"
  [[ "$status" == PASS ]]
}

main() {
  local mode="" workers_csv="${VELOX_CERT_WORKERS:-}" fleet_dir="${RW_FLEET_ARTIFACT_DIR:-}" run_id="${RW_FLEET_RUN_ID:-}" serial=1
  local fleet_report results_file worker overall diagnostic

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
