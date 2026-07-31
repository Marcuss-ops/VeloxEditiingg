#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/perf_matrix.sh — Per-worker comparative matrix from
# sequential_bench.sh output.
# =============================================================================
# Usage:
#   ./tests/worker-cert/perf_matrix.sh
#
#   ./tests/worker-cert/perf_matrix.sh \
#       --input workers/benchmark.json \
#       --csv-out workers/perf_matrix.csv \
#       --html-out workers/perf_matrix.html \
#       --cache-mode cold_cache \
#       REGISTER_BENCHMARK_RUNS=true VELOX_ADMIN_TOKEN=... \
#       ./tests/worker-cert/perf_matrix.sh --cache-mode warm_cache
#       ./tests/worker-cert/perf_matrix.sh --cache-mode both
#
# What the script does:
#   1. Reads benchmark.json (produced by sequential_bench.sh).
#   2. Parses per-worker summary stats from .workers[].
#   3. Parses per-run details from .runs[] grouped by target_worker.
#   4. Produces a CSV file with per-worker comparative columns:
#        worker_id, image_digest_short, jobs_succeeded, jobs_failed,
#        success_rate_pct, render_median_ms, render_min_ms, render_max_ms,
#        render_avg_ms, realtime_factor, output_duration_ms,
#        submit_latency_avg_ms
#   5. When REGISTER_BENCHMARK_RUNS=true, registers every .runs[] row at
#      POST /api/v1/performance/benchmarks/runs with benchmark_case_id
#      gervais-final-v1 and the selected cold_cache/warm_cache mode.
#      --cache-mode both registers the complete input once per mode.
#   6. Produces a self-contained HTML file with:
#        - Metadata header (master URL, date, destination, epoch).
#        - A styled comparative table with best/worst highlighting.
#        - Per-run detail section (collapsible).
#
# Exit codes:
#   0  Matrix written.
#   2  usage / missing deps.
#   3  input file not found / invalid JSON.
# =============================================================================

set -uo pipefail

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─── Cross-test helpers ────────────────────────────────────────────────────
# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"

# ─── defaults ──────────────────────────────────────────────────────────────
INPUT="${INPUT:-${SCRIPT_DIR}/workers/benchmark.json}"
CSV_OUT="${CSV_OUT:-}"
HTML_OUT="${HTML_OUT:-}"
CACHE_MODE="${CACHE_MODE:-cold_cache}"
REGISTER_BENCHMARK_RUNS="${REGISTER_BENCHMARK_RUNS:-false}"
BENCHMARK_CASE_ID="gervais-final-v1"
BENCHMARK_RUNS_PATH="/api/v1/performance/benchmarks/runs"

usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then usage; fi

while (( $# > 0 )); do
  case "$1" in
    --input)     INPUT="$2";     shift 2 ;;
    --csv-out)   CSV_OUT="$2";   shift 2 ;;
    --html-out)  HTML_OUT="$2";  shift 2 ;;
    --cache-mode) CACHE_MODE="$2"; shift 2 ;;
    -h|--help)   usage ;;
    *)           log_error "unknown flag: $1"; exit 2 ;;
  esac
done

case "$CACHE_MODE" in
  cold_cache|warm_cache|both) ;;
  *) log_error "cache mode must be cold_cache, warm_cache, or both: $CACHE_MODE"; exit 2 ;;
esac

# ─── Validate input ────────────────────────────────────────────────────────
[[ -r "$INPUT" ]] || { log_error "benchmark.json not readable: $INPUT"; exit 3; }

for bin in jq awk; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    log_error "FATAL: required binary not in PATH: $bin"; exit 2
  fi
done

log_info "perf_matrix input=$INPUT csv_out=${CSV_OUT:-<stdout>} html_out=${HTML_OUT:-<none>}"

# ─── Extract metadata ──────────────────────────────────────────────────────
MASTER_URL="${VELOX_MASTER_URL:-$(jq -r '.master_url // "unknown"' "$INPUT")}"
WRITTEN_AT=$(jq -r '.written_at // "unknown"' "$INPUT")
SCHEMA=$(jq -r '.schema // "?"' "$INPUT")
DEST_ID=$(jq -r '.destination_id // "?"' "$INPUT")
JOBS_PER_WORKER=$(jq -r '.jobs_per_worker // 0' "$INPUT")
WORKERS_TESTED=$(jq -r '.workers_tested // 0' "$INPUT")
TOTAL_RUNS=$(jq -r '.total_runs // 0' "$INPUT")
ALL_OK=$(jq -r '.all_jobs_succeeded // false' "$INPUT")
EPOCH=$(jq -r '.epoch // 0' "$INPUT")

log_info "matrix: ${WORKERS_TESTED} workers × ${JOBS_PER_WORKER} jobs, total=${TOTAL_RUNS} runs, all_ok=${ALL_OK}, cache_mode=${CACHE_MODE}"

resolve_admin_token() {
  local token=""
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
    token="$VELOX_ADMIN_TOKEN"
  elif [[ -n "${TOKEN_FILE:-}" && -r "$TOKEN_FILE" ]]; then
    token=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
      | sed 's/^[^=]*=//' | tr -d '"' | tr -d "'" | xargs || true)
  fi
  printf '%s' "$token"
}

register_benchmark_runs_for_mode() {
  local mode="$1"
  [[ "$REGISTER_BENCHMARK_RUNS" == "true" ]] || return 0
  local admin_token
  admin_token=$(resolve_admin_token)
  [[ -n "$admin_token" ]] || { log_error "REGISTER_BENCHMARK_RUNS=true requires VELOX_ADMIN_TOKEN or TOKEN_FILE"; return 2; }
  [[ "$MASTER_URL" != "unknown" && -n "$MASTER_URL" ]] || { log_error "benchmark master_url is required to register runs"; return 2; }
  command -v curl >/dev/null 2>&1 || { log_error "curl is required to register benchmark runs"; return 2; }

  local payload_file status_file payload status
  payload_file=$(mktemp)
  status_file=$(mktemp)
  trap 'rm -f "${payload_file:-}" "${status_file:-}"' RETURN

  jq -c --arg case_id "$BENCHMARK_CASE_ID" --arg cache_mode "$mode" \
    --arg git_sha "${BENCHMARK_GIT_SHA:-}" \
    --arg engine_version "${BENCHMARK_ENGINE_VERSION:-}" \
    --arg ffmpeg_version "${BENCHMARK_FFMPEG_VERSION:-}" \
    --arg config_hash "${BENCHMARK_CONFIG_HASH:-}" \
    --arg docker_image_digest "${BENCHMARK_DOCKER_IMAGE_DIGEST:-}" \
    '.runs[] as $run |
     ([.workers[] | select(.worker_id == $run.target_worker)][0]) as $worker |
     ($run.attempt_id // ($run.job_id + ":" + (($run.run_idx // 0) | tostring))) as $attempt |
     {
       run_id: ($case_id + ":" + $cache_mode + ":" + $attempt),
       benchmark_case_id: $case_id,
       job_id: ($run.job_id // ""),
       task_id: ($run.task_id // ""),
       attempt_id: $attempt,
       worker_id: ($run.resp_worker_id // $run.target_worker // ""),
       cache_mode: $cache_mode,
       git_sha: $git_sha,
       engine_version: $engine_version,
       ffmpeg_version: $ffmpeg_version,
       config_hash: $config_hash,
       docker_image_digest: ($worker.image_digest // $docker_image_digest),
       status: ($run.status // "UNKNOWN"),
       render_factor: (if (($run.render_time_ms // 0) > 0 and ($worker.output_duration_ms // 0) > 0) then (($run.render_time_ms / $worker.output_duration_ms) | tonumber) else 0 end),
       wall_ms: ($run.render_time_ms // 0),
       output_duration_ms: ($worker.output_duration_ms // 0),
       output_sha256: ($run.output_sha256 // "")
     }' "$INPUT" > "$payload_file"

  while IFS= read -r payload; do
    [[ -n "$payload" ]] || continue
    status=$(curl -sS -m 30 -o "$status_file" -w '%{http_code}' \
      -H "Authorization: Bearer $admin_token" \
      -H 'Content-Type: application/json' \
      -X POST "${MASTER_URL%/}${BENCHMARK_RUNS_PATH}" \
      --data "$payload" || printf '000')
    if [[ "$status" != "200" ]]; then
      log_error "benchmark run registration failed: HTTP $status"
      cat "$status_file" >&2 || true
      return 1
    fi
  done < "$payload_file"
  log_info "registered ${TOTAL_RUNS} benchmark runs: case=${BENCHMARK_CASE_ID} cache_mode=${mode}"
}

register_benchmark_runs() {
  case "$CACHE_MODE" in
    both)
      register_benchmark_runs_for_mode cold_cache || return
      register_benchmark_runs_for_mode warm_cache || return
      ;;
    *)
      register_benchmark_runs_for_mode "$CACHE_MODE"
      ;;
  esac
}

# ─── CSV output ────────────────────────────────────────────────────────────
# Single-pass CSV with submit_latency computed from runs.
generate_csv_combined() {
  # Header
  printf '%s\n' "worker_id,image_digest_short,jobs_succeeded,jobs_failed,success_rate_pct,render_median_ms,render_min_ms,render_max_ms,render_avg_ms,realtime_factor,output_duration_ms,submit_latency_avg_ms"

  jq -r '
    # Index runs by target_worker for latency calc.
    ( [.runs[] | {w: .target_worker, l: (.submit_latency_ms // 0)}] ) as $run_latencies |
    .workers[] |
    .worker_id as $wid |
    # Compute average submit latency for this worker
    ( [ $run_latencies[] | select(.w == $wid) | .l ] | if length > 0 then (add / length | floor) else 0 end ) as $lat_avg |
    [
      .worker_id,
      (.image_digest[:12] // "?"),
      (.jobs_succeeded // 0),
      (.jobs_failed // 0),
      (if (.jobs_succeeded + .jobs_failed) > 0
       then ((.jobs_succeeded * 100 / (.jobs_succeeded + .jobs_failed)) | floor)
       else 0 end),
      (.render_time_ms.median // 0),
      (.render_time_ms.min // 0),
      (.render_time_ms.max // 0),
      (.render_time_ms.avg // 0),
      (.realtime_factor // "0"),
      (.output_duration_ms // 0),
      $lat_avg
    ] | @csv
  ' "$INPUT"
}

# ─── HTML output ───────────────────────────────────────────────────────────
generate_html() {
  local out="$1"
  local tmp
  tmp=$(mktemp "${out}.tmp-XXXXXX.html")

  # Build table rows
  local rows
  rows=$(jq -r '
    ( [.runs[] | {w: .target_worker, l: (.submit_latency_ms // 0)}] ) as $run_latencies |
    .workers[] |
    .worker_id as $wid |
    ( [ $run_latencies[] | select(.w == $wid) | .l ] | if length > 0 then (add / length | floor) else 0 end ) as $lat_avg |
    {
      worker_id: .worker_id,
      digest: (.image_digest[:12] // "?"),
      succ: (.jobs_succeeded // 0),
      fail: (.jobs_failed // 0),
      rate: (if (.jobs_succeeded + .jobs_failed) > 0
             then ((.jobs_succeeded * 100 / (.jobs_succeeded + .jobs_failed)) | floor)
             else 0 end),
      median: (.render_time_ms.median // 0),
      min: (.render_time_ms.min // 0),
      max: (.render_time_ms.max // 0),
      avg: (.render_time_ms.avg // 0),
      rtf: (.realtime_factor // "0"),
      out_dur: (.output_duration_ms // 0),
      lat: $lat_avg
    } |
    "<tr>" +
    "<td class=\"wid\">\(.worker_id)</td>" +
    "<td class=\"digest\"><code>\(.digest)</code></td>" +
    "<td class=\"num succ\">\(.succ)</td>" +
    "<td class=\"num fail\">\(.fail)</td>" +
    "<td class=\"num rate\">\(.rate)%</td>" +
    "<td class=\"num med\">\(.median) ms</td>" +
    "<td class=\"num min\">\(.min) ms</td>" +
    "<td class=\"num max\">\(.max) ms</td>" +
    "<td class=\"num avg\">\(.avg) ms</td>" +
    "<td class=\"num rtf\">\(.rtf)x</td>" +
    "<td class=\"num dur\">\(.out_dur) ms</td>" +
    "<td class=\"num lat\">\(.lat) ms</td>" +
    "</tr>"
  ' "$INPUT")

  # Build per-run detail rows
  local detail_rows
  detail_rows=$(jq -r '
    .runs[] |
    "<tr>" +
    "<td class=\"wid\">\(.target_worker // "?")</td>" +
    "<td class=\"num\">\(.run_idx // "?")</td>" +
    "<td class=\"jid\"><code>\(.job_id // "?")</code></td>" +
    "<td class=\"status\">\(.status // "?")</td>" +
    "<td class=\"num\">\(.submit_latency_ms // 0) ms</td>" +
    "<td class=\"num\">\(.render_time_ms // 0) ms</td>" +
    "<td class=\"wid\">\(.resp_worker_id // "?")</td>" +
    "<td class=\"bool\">\(.pin_ok // "?")</td>" +
    "</tr>"
  ' "$INPUT")

  cat > "$tmp" <<HTMLEOF
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Velox Worker Performance Matrix</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #0d1117; color: #c9d1d9; padding: 2rem;
  }
  h1 { font-size: 1.5rem; margin-bottom: 0.5rem; color: #58a6ff; }
  h2 { font-size: 1.1rem; margin: 1.5rem 0 0.75rem; color: #8b949e; }
  .meta {
    display: flex; flex-wrap: wrap; gap: 1rem 2rem;
    font-size: 0.85rem; color: #8b949e; margin-bottom: 1.5rem;
  }
  .meta span { white-space: nowrap; }
  .meta strong { color: #c9d1d9; }
  table {
    width: 100%; border-collapse: collapse; font-size: 0.85rem;
    margin-bottom: 1.5rem;
  }
  th, td {
    padding: 0.45rem 0.6rem; text-align: left;
    border-bottom: 1px solid #21262d;
  }
  th {
    background: #161b22; color: #8b949e; font-weight: 600;
    position: sticky; top: 0; z-index: 1;
  }
  td.wid { font-weight: 600; color: #e6edf3; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  td.succ { color: #3fb950; }
  td.fail { color: #f85149; }
  td.status { color: #f85149; font-weight: 600; }
  td.bool { text-align: center; }
  td.jid code { font-size: 0.75rem; color: #8b949e; }
  td.digest code { font-size: 0.75rem; }
  .best { background: rgba(63,185,80,0.12); }
  .worst { background: rgba(248,81,73,0.10); }
  tr:hover { background: rgba(88,166,255,0.06); }
  details { margin-top: 1rem; }
  summary {
    cursor: pointer; color: #58a6ff; font-weight: 600;
    font-size: 0.9rem; padding: 0.5rem 0;
  }
  .footer {
    margin-top: 2rem; font-size: 0.75rem; color: #484f58;
    border-top: 1px solid #21262d; padding-top: 1rem;
  }
  .ok-badge { color: #3fb950; }
  .fail-badge { color: #f85149; }
</style>
</head>
<body>

<h1>🚀 Velox Worker Performance Matrix</h1>

<div class="meta">
  <span>Master: <strong>${MASTER_URL}</strong></span>
  <span>Destination: <strong>${DEST_ID}</strong></span>
  <span>Date: <strong>${WRITTEN_AT}</strong></span>
  <span>Schema: <strong>${SCHEMA}</strong></span>
  <span>Epoch: <strong>${EPOCH}</strong></span>
  <span>Jobs/worker: <strong>${JOBS_PER_WORKER}</strong></span>
  <span>All succeeded: <strong class="$([[ "$ALL_OK" == "true" ]] && echo 'ok-badge' || echo 'fail-badge')">${ALL_OK}</strong></span>
</div>

<h2>📊 Per-Worker Summary</h2>
<table>
<thead><tr>
  <th>Worker ID</th>
  <th>Digest</th>
  <th class="num">✅ OK</th>
  <th class="num">❌ Fail</th>
  <th class="num">Rate</th>
  <th class="num">Median</th>
  <th class="num">Min</th>
  <th class="num">Max</th>
  <th class="num">Avg</th>
  <th class="num">RT Factor</th>
  <th class="num">Out Dur</th>
  <th class="num">Sub Lat</th>
</tr></thead>
<tbody>
${rows}
</tbody>
</table>

<h2>🔍 Per-Run Details</h2>
<details>
<summary>${TOTAL_RUNS} runs across ${WORKERS_TESTED} workers</summary>
<table>
<thead><tr>
  <th>Target Worker</th>
  <th class="num">Run #</th>
  <th>Job ID</th>
  <th>Status</th>
  <th class="num">Submit Lat</th>
  <th class="num">Render</th>
  <th>Actual Worker</th>
  <th>Pin OK</th>
</tr></thead>
<tbody>
${detail_rows}
</tbody>
</table>
</details>

<div class="footer">
  Generated by <code>tests/worker-cert/perf_matrix.sh</code> —
  source: <code>${INPUT}</code>
</div>

</body>
</html>
HTMLEOF

  mv "$tmp" "$out"
  log_info "HTML written: $out"
}

# ─── Main ──────────────────────────────────────────────────────────────────

# CSV output
if [[ -n "$CSV_OUT" ]]; then
  ensure_dir "$(dirname "$CSV_OUT")"
  generate_csv_combined > "$CSV_OUT"
  log_info "CSV written: $CSV_OUT  ($(wc -l < "$CSV_OUT") lines)"
else
  generate_csv_combined
fi

# HTML output
if [[ -n "$HTML_OUT" ]]; then
  ensure_dir "$(dirname "$HTML_OUT")"
  generate_html "$HTML_OUT"
fi

register_benchmark_runs
rc=$?
if (( rc != 0 )); then
  log_error "benchmark run registration failed"
  exit "$rc"
fi

log_info "perf_matrix done"
