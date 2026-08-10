#!/usr/bin/env bash
# shellcheck disable=SC2034
# Shared globals are supplied by the certify-remote-fleet.sh entrypoint.
# Per-worker runner for certify-remote-fleet.sh.
# Sourced by the Bash entrypoint; relies on shared globals and report helpers.

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
