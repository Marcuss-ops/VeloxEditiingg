# remote-worker-cert-runner.sh — CLI dispatch and certification orchestration.
# Loaded by scripts/cert/remote-worker-cert-config.sh.

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

rw_cert_main() {
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
}
