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

Destructive safety:
  VELOX_CERT_ENV                   Must explicitly be staging/canary/development/test/local.
  VELOX_CERT_ALLOW_DESTRUCTIVE=1   Explicit destructive opt-in.
  VELOX_CERT_DESTRUCTIVE_ACK=I_UNDERSTAND_DESTRUCTIVE_CERT
  RW_WORKER_CRASH_CMD              Command passed to the recovery runner.
  RW_JOB_DESTINATION_ID            Explicit destination for the recovery job.
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
  [[ -n "${RW_JOB_DESTINATION_ID:-}" ]] || {
    fleet_die "RW_JOB_DESTINATION_ID is required for destructive mode"
    return 1
  }
  validate_safe_path_value RW_WORKER_CRASH_CMD "$RW_WORKER_CRASH_CMD" || return 1
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
          --destination-id "$RW_JOB_DESTINATION_ID" \
          --report-json "${step_dir}/report.json" >"${step_dir}/stdout.log" 2>"${step_dir}/stderr.log"
        rc=$?
      else
        bash "$DESTRUCTIVE_RUNNER" \
          --target-worker-id "$worker" \
          --target-worker-stop-cmd "$RW_WORKER_CRASH_CMD" \
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
  [[ -f "$SINGLE_RUNNER" ]] || { fleet_die "single-worker runner not found: $SINGLE_RUNNER"; return 2; }
  if [[ "$mode" == destructive ]]; then
    guard_destructive || return 2
  fi

  run_id="${run_id:-fleet-cert-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
  fleet_dir="${fleet_dir:-${TMPDIR:-/tmp}/velox-fleet-${run_id}}"
  mkdir -p -- "$fleet_dir" || { fleet_die "cannot create artifact directory: $fleet_dir"; return 2; }
  validate_safe_path_value RW_FLEET_ARTIFACT_DIR "$fleet_dir" || return 2
  export RW_FLEET_RUN_ID="$run_id"

  results_file="${fleet_dir}/.workers.ndjson"
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

  fleet_report="${fleet_dir}/fleet-report.json"
  jq -n --arg run_id "$run_id" --arg mode "$mode" --arg overall "$overall" \
    --arg artifact_dir "$fleet_dir" --slurpfile worker_results "$results_file" \
    '{schema:"velox.remote_worker.fleet.v1",run_id:$run_id,mode:$mode,serial:true,artifact_dir:$artifact_dir,overall:$overall,workers:$worker_results}' >"$fleet_report"
  write_fleet_junit "$fleet_report" "${fleet_dir}/fleet-report.junit.xml" "$mode"
  cp -- "$fleet_report" "${fleet_dir}/report.json"
  printf '%s\n' "fleet_report=${fleet_report}" >&2
  cat "$fleet_report"
  rm -f -- "$results_file"
  [[ "$overall" == PASS ]]
}

main "$@"
