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
#   VELOX_BENCH_SUBMIT_CMD='scripts/fleetctl job submit --payload /tmp/p.json --workers all' \
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

# Renderer-only certification must never create a live Drive/social delivery
# attempt. The server already supports VELOX_DELIVERY_DISABLED=1; require the
# explicit operator acknowledgement here so a benchmark cannot silently
# produce BLOCKED_AUTH rows or contact an external destination.
if [[ "$MODE" == "render" && "${VELOX_DELIVERY_DISABLED:-}" != "1" ]]; then
  die 'render benchmark requires VELOX_DELIVERY_DISABLED=1 (processing-only mode)'
fi

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
    >"${run_dir}/jobs/${safe_id}.watch.json"
  # `job watch --json` is an event stream: it emits one JSON object per poll
  # and intentionally does not contain execution/delivery metrics. Use the
  # terminal inspection as the benchmark row so jq never mistakes polling
  # snapshots for independent jobs.
  "$FLEETCTL" job inspect "$job_id" --json >"${run_dir}/jobs/${safe_id}.json"
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
    def attempt: (.execution.attempts // []) | if length > 0 then .[-1] else {} end;
    def metric($a; $name): (($a.metrics // {})[$name] // 0);
    ($snapshots | map(
      . as $root |
      (attempt) as $a |
      ($root.deliveries // []) as $deliveries |
      ([ $deliveries[] | .queue_ms // 0 ] | add // 0) as $queue_ms |
      ([ $deliveries[] | .upload_ms // 0 ] | add // 0) as $upload_ms |
      ([ $deliveries[] | .total_ms // 0 ] | add // 0) as $total_ms |
      ([ $deliveries[] | .retry_count // ((.attempt_count // 0) - 1) | if . > 0 then . else 0 end ] | add // 0) as $retries |
      ([ $deliveries[] | .bytes_uploaded // 0 ] | add // 0) as $bytes |
      {
        status: ($root.job.status // "UNKNOWN"),
        output_sha256: (metric($a; "output_sha256") // null),
        transcode_passes: (metric($a; "encode_passes") // 0),
        delivery_queue_ms: $queue_ms,
        delivery_upload_ms: $upload_ms,
        delivery_total_ms: $total_ms,
        retry_count: $retries,
        timeout_count: null,
        bytes_uploaded: $bytes,
        upload_mbps: (if $upload_ms > 0 then (($bytes * 8) / $upload_ms / 1000) else 0 end),
        cache_unique_assets_requested: ($root.execution.cache.unique_assets_requested // 0),
        cache_lookups: ($root.execution.cache.lookups // 0),
        cache_hits: ($root.execution.cache.hits // 0),
        cache_misses: ($root.execution.cache.misses // 0)
      }
    )) as $jobs |
    {
      mode: $mode,
      concurrency: ($concurrency|tonumber),
      requested_jobs: ($jobs|length),
      delivery_mode: (if (env.VELOX_DELIVERY_DISABLED // "") == "1" then "DISABLED_PROCESSING_ONLY" else "ENABLED" end),
      # delivery_outcome classifies the delivery result independently of
      # processing success. When delivery is disabled (benchmark mode),
      # the outcome is explicitly DISABLED so operators never mistake it
      # for a real delivery failure (e.g. BLOCKED_AUTH from a misconfigured
      # Drive). In production ENABLED mode, the outcome is derived from
      # the actual delivery rows.
      delivery_outcome: (
        if (env.VELOX_DELIVERY_DISABLED // "") == "1" then "DISABLED"
        else
          ($deliveries | length) as $count |
          if $count == 0 then "NO_DELIVERIES"
          elif ($deliveries | map(select(.status == "SUCCEEDED" or .status == "COMPLETED")) | length) == $count then "SUCCEEDED"
          elif ($deliveries | map(select(.status == "FAILED")) | length) > 0 then "FAILED"
          elif ($deliveries | map(select(.status == "BLOCKED_AUTH")) | length) > 0 then "AUTH_REQUIRED"
          else "PARTIAL"
          end
        end
      ),
      succeeded: ($jobs | map(select(.status == "SUCCEEDED" or .status == "COMPLETED")) | length),
      sha_values: ($jobs | map(.output_sha256) | map(select(. != null)) | unique),
      sha_identical: (($jobs | map(.output_sha256) | map(select(. != null)) | unique | length) <= 1),
      transcode_passes: ($jobs | map(.transcode_passes) | add // 0),
      # delivery_outcome is the per-job classification aggregated to the
      # report level. DISABLED means the benchmark explicitly opted out
      # of delivery (VELOX_DELIVERY_DISABLED=1); ENABLED paths show the
      # actual delivery result so misconfiguration never pollutes the
      # benchmark score.
      # Aggregate delivery outcomes: worst-case wins when mixed.
      # Order: AUTH_REQUIRED > FAILED > PARTIAL > SUCCEEDED > NO_DELIVERIES > DISABLED
      delivery_outcome: (
        ($jobs | map(.delivery_outcome) | unique) as $outcomes |
        if ($outcomes | length) == 1 then $outcomes[0]
        elif ($outcomes | index("AUTH_REQUIRED")) != null then "AUTH_REQUIRED"
        elif ($outcomes | index("FAILED")) != null then "FAILED"
        elif ($outcomes | index("PARTIAL")) != null then "PARTIAL"
        elif ($outcomes | index("SUCCEEDED")) != null then "SUCCEEDED"
        elif ($outcomes | index("NO_DELIVERIES")) != null then "NO_DELIVERIES"
        else "DISABLED"
        end
      ),
      delivery: {
        queue_ms: ($jobs | map(.delivery_queue_ms) | add // 0),
        upload_ms: ($jobs | map(.delivery_upload_ms) | add // 0),
        total_ms: ($jobs | map(.delivery_total_ms) | add // 0),
        retry_count: ($jobs | map(.retry_count) | add // 0),
        timeout_count: ($jobs | map(select(.timeout_count != null) | .timeout_count) | add // null),
        bytes_uploaded: ($jobs | map(.bytes_uploaded) | add // 0),
        upload_mbps: ($jobs | map(.upload_mbps) | add // 0)
      },
      database: {note: "DB telemetry is master-global Prometheus state; scrape /metrics before and after the run for deltas."},
      cache: {
        lookups: ($jobs | map(.cache_lookups) | add // 0),
        hits: ($jobs | map(.cache_hits) | add // 0),
        misses: ($jobs | map(.cache_misses) | add // 0),
        unique_assets_requested: ($jobs | map(.cache_unique_assets_requested) | add // 0),
        invariant: (($jobs | map(.cache_lookups) | add // 0) == (($jobs | map(.cache_hits) | add // 0) + ($jobs | map(.cache_misses) | add // 0)))
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
