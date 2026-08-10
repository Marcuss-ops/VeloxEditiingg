#!/usr/bin/env bash
# Run the renderer-only / delivery-only isolation benchmark and the delivery
# concurrency sweep. The harness deliberately does not invent a payload or
# mutate server configuration: the caller supplies the submit/prepare command
# and starts the server with VELOX_DELIVERY_DISABLED=1 for Test A.
#
# Required:
#   VELOX_BENCH_SUBMIT_CMD     command that prints one job id per line
#   VELOX_BENCH_FLEETCTL       optional fleetctl path (default scripts/fleetctl)
#
# For Test B, VELOX_BENCH_SUBMIT_CMD must create/release eight already-rendered
# artifacts with pending delivery rows. The same command is used because the
# artifact preparation is deployment-specific; the report still measures only
# the delivery watch interval.
# For `sweep`, set VELOX_BENCH_RESTART_CMD to the operator's server restart
# command. It is invoked once per point with VELOX_DELIVERY_CONCURRENCY set to
# 1, 2, 4 and 8; without it the script still sweeps client observation
# concurrency but cannot change a running server's provider pool.
#
# Examples:
#   VELOX_DELIVERY_DISABLED=1 \
#   VELOX_BENCH_SUBMIT_CMD='scripts/fleetctl-legacy job submit --payload /tmp/p.json --workers all' \
#     scripts/benchmarks/delivery-isolation.sh render --jobs 8
#
#   VELOX_BENCH_SUBMIT_CMD='scripts/prepare-ready-deliveries.sh' \
#     scripts/benchmarks/delivery-isolation.sh delivery --jobs 8 --concurrency 4
#
# The command's stdout must contain job IDs only (one per line). Its stderr is
# left visible for operator diagnostics.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FLEETCTL="${VELOX_BENCH_FLEETCTL:-${ROOT}/scripts/fleetctl}"
OUT_ROOT="${VELOX_BENCH_OUTPUT_DIR:-${ROOT}/.velox/benchmarks/delivery-isolation}"
TIMEOUT="${VELOX_BENCH_TIMEOUT_SECONDS:-3600}"
POLL="${VELOX_BENCH_POLL_SECONDS:-5}"

die() { printf 'delivery-isolation: %s\n' "$*" >&2; exit 2; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1"; }

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  sed -n '1,40p' "$0"
  exit 0
fi

MODE="${1:-}"
shift || true
JOBS=8
CONCURRENCY=1
while (($#)); do
  case "$1" in
    --jobs) JOBS="${2:-}"; shift 2 ;;
    --concurrency) CONCURRENCY="${2:-}"; shift 2 ;;
    --help|-h)
      sed -n '1,34p' "$0"
      exit 0
      ;;
    *) die "unknown argument: $1" ;;
  esac
done

case "$MODE" in
  render|delivery|sweep) ;;
  *) die "usage: $0 render|delivery|sweep [--jobs N] [--concurrency N]" ;;
esac
[[ "$JOBS" =~ ^[1-9][0-9]*$ ]] || die '--jobs must be a positive integer'
[[ "$CONCURRENCY" =~ ^[1-9][0-9]*$ ]] || die '--concurrency must be a positive integer'
[[ -n "${VELOX_BENCH_SUBMIT_CMD:-}" ]] || die 'VELOX_BENCH_SUBMIT_CMD is required'
[[ -x "$FLEETCTL" ]] || die "fleetctl is not executable: $FLEETCTL"
need jq

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_dir="${OUT_ROOT}/${MODE}-${timestamp}"
mkdir -p "$run_dir/jobs"

submit_jobs() {
  local ids_file="$1"
  # shellcheck disable=SC2091
  bash -lc "$VELOX_BENCH_SUBMIT_CMD" >"$ids_file"
  sed -i '/^[[:space:]]*$/d' "$ids_file"
  [[ "$(wc -l <"$ids_file")" -eq "$JOBS" ]] || die "submit command returned $(wc -l <"$ids_file") jobs, expected $JOBS"
}

watch_one() {
  local job_id="$1"
  local safe_id
  safe_id="$(printf '%s' "$job_id" | tr -c 'A-Za-z0-9._-' '_')"
  "$FLEETCTL" job watch "$job_id" --timeout "$TIMEOUT" --poll "$POLL" --json \
    >"${run_dir}/jobs/${safe_id}.json"
}

watch_jobs() {
  local ids_file="$1" active=0 job_id
  while IFS= read -r job_id; do
    watch_one "$job_id" &
    active=$((active + 1))
    if ((active >= CONCURRENCY)); then
      wait -n
      active=$((active - 1))
    fi
  done <"$ids_file"
  while ((active > 0)); do
    wait -n
    active=$((active - 1))
  done
}

extract_report() {
  local ids_file="$1"
  jq -n --arg mode "$MODE" --arg concurrency "$CONCURRENCY" --arg jobs "$JOBS" \
    --slurpfile snapshots <(for f in "$run_dir"/jobs/*.json; do jq -c . "$f"; done) '
    def first($names):
      [.. | objects | to_entries[] | .key as $k | select($names | index($k)) | .value][0] // null;
    def number($names): (first($names) // 0) | tonumber;
    def sha: first(["output_sha256", "artifact_sha256", "sha256"]);
    ($snapshots) as $rows |
    ($rows | map({
      status: (first(["status", "state"]) // "UNKNOWN"),
      output_sha256: sha,
      transcode_passes: number(["encode_passes", "transcode_passes"]),
      delivery_queue_ms: number(["delivery_queue_ms"]),
      delivery_upload_ms: number(["delivery_upload_ms"]),
      delivery_total_ms: number(["delivery_total_ms"]),
      retry_count: number(["retry_count", "delivery_retry_count"]),
      timeout_count: number(["timeout_count", "delivery_timeout_count"]),
      bytes_uploaded: number(["bytes_uploaded", "delivery_bytes_uploaded"]),
      upload_mbps: number(["upload_mbps", "delivery_upload_mbps"]),
      db_write_wait_ms: number(["db_write_wait_ms"]),
      db_transaction_ms: number(["db_transaction_ms"]),
      db_busy_count: number(["db_busy_count"]),
      db_busy_timeout_count: number(["db_busy_timeout_count"]),
      db_retry_count: number(["db_retry_count"]),
      db_write_operations: number(["db_write_operations"]),
      db_read_operations: number(["db_read_operations"])
    })) as $jobs |
    {
      mode: $mode,
      concurrency: ($concurrency|tonumber),
      requested_jobs: ($jobs|length),
      succeeded: ($jobs | map(select(.status == "SUCCEEDED" or .status == "COMPLETED")) | length),
      sha_values: ($jobs | map(.output_sha256) | map(select(. != null)) | unique),
      sha_identical: (($jobs | map(.output_sha256) | map(select(. != null)) | unique | length) <= 1),
      transcode_passes: ($jobs | map(.transcode_passes) | add // 0),
      delivery: {
        queue_ms: ($jobs | map(.delivery_queue_ms) | add // 0),
        upload_ms: ($jobs | map(.delivery_upload_ms) | add // 0),
        total_ms: ($jobs | map(.delivery_total_ms) | add // 0),
        retry_count: ($jobs | map(.retry_count) | add // 0),
        timeout_count: ($jobs | map(.timeout_count) | add // 0),
        bytes_uploaded: ($jobs | map(.bytes_uploaded) | add // 0),
        upload_mbps: ($jobs | map(.upload_mbps) | add // 0)
      },
      database: {
        write_wait_ms: ($jobs | map(.db_write_wait_ms) | add // 0),
        transaction_ms: ($jobs | map(.db_transaction_ms) | add // 0),
        busy_count: ($jobs | map(.db_busy_count) | add // 0),
        busy_timeout_count: ($jobs | map(.db_busy_timeout_count) | add // 0),
        retry_count: ($jobs | map(.db_retry_count) | add // 0),
        write_operations: ($jobs | map(.db_write_operations) | add // 0),
        read_operations: ($jobs | map(.db_read_operations) | add // 0)
      },
      jobs: $jobs
    }
  ' >"${run_dir}/report.json"
  jq . "${run_dir}/report.json"
}

run_once() {
  local ids_file="${run_dir}/job_ids.txt"
  submit_jobs "$ids_file"
  watch_jobs "$ids_file"
  extract_report "$ids_file"
  printf 'delivery-isolation: report=%s\n' "${run_dir}/report.json" >&2
}

if [[ "$MODE" == sweep ]]; then
  for CONCURRENCY in 1 2 4 8; do
    if [[ -n "${VELOX_BENCH_RESTART_CMD:-}" ]]; then
      VELOX_DELIVERY_CONCURRENCY="$CONCURRENCY" bash -lc "$VELOX_BENCH_RESTART_CMD"
    fi
    run_dir="${OUT_ROOT}/sweep-${timestamp}-c${CONCURRENCY}"
    mkdir -p "$run_dir/jobs"
    run_once
  done
else
  run_once
fi
