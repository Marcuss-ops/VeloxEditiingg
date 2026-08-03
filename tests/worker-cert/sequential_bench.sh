#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/sequential_bench.sh — Sequential per-worker benchmark.
# =============================================================================
# Usage:
#   ./tests/worker-cert/sequential_bench.sh
#
#   VELOX_MASTER_URL=https://velox.example.com \
#   VELOX_ADMIN_TOKEN=... \
#   ./tests/worker-cert/sequential_bench.sh \
#       --workers host_57_129_132_133 \
#       --workers host_57_131_20_173 \
#       --workers velox-worker-13197 \
#       --workers velox-worker-523925eb
#
# What the script does per worker:
#   1. Pre-flight health: GET .../health?level=A, B, C — all must pass.
#   2. Image digest check: GET /api/v1/admin/workers/{id} → image_digest.
#   3. (Optional) Drain non-target workers so the target has zero load.
#   4. Wait for active_jobs=0 on target worker.
#   5. Submit jobs with placement_pin_worker_id targeting this worker. Each
#      payload contains two stock clips, scene voiceover, background music and
#      one ASS subtitle track. Every media reference is velox-asset://<sha256>.
#      Poll GET /api/v1/jobs/{id} until SUCCEEDED.
#   6. Scrape TaskLeaseGranted from master log to verify placement.
#   7. Collect: submit_latency_ms, started_at, completed_at,
#      render_time_ms, artifact_size_bytes, worker_id from job response.
#   8. Resume drained workers.
#
# Runtime media inputs (required; use only Master-registered READY assets):
#   BENCH_BACKGROUND_MUSIC_ASSET_ID       64-hex SHA-256 asset ID
#   BENCH_BACKGROUND_MUSIC_DURATION_SECONDS >= 6 (video is 2 x 3 seconds)
#   BENCH_SUBTITLE_ASSET_ID               64-hex SHA-256 ASS asset ID
#
# After all workers: compute median, min, max, and realtime_factor
# (render_ms / output_duration_ms) per worker. Write benchmark.json.
#
# Exit codes:
#   0  PASS — all jobs SUCCEEDED, benchmark.json written.
#   2  usage / env.
#   3  master unreachable / M2M provisioning failed.
#   4  pre-flight failed (health A/B/C or worker not CONNECTED).
#   5  non-202 on job submit.
#   6  poll timeout without SUCCEEDED on at least one job.
#   7  terminal-fail (FAILED/CANCELLED) on at least one job.
#   8  placement mismatch (TaskLeaseGranted worker != target).
# =============================================================================

set -uo pipefail

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─── Cross-test helpers ────────────────────────────────────────────────────
# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"
# shellcheck source=tests/worker-cert/lib/pluck.sh
source "${SCRIPT_DIR}/lib/pluck.sh"

# ─── Default canonical 4-worker fleet ──────────────────────────────────────
DEFAULT_WORKERS=(
  "host_57_129_132_133"
  "host_57_131_20_173"
  "velox-worker-13197"
  "velox-worker-523925eb"
)

# ─── Args / env ────────────────────────────────────────────────────────────
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then usage; fi

WORKER_IDS=()
NO_DRAIN=0
POLL_TIMEOUT_S="${BENCH_POLL_TIMEOUT_S:-300}"
JOBS_PER_WORKER="${BENCH_JOBS_PER_WORKER:-3}"
DESTINATION_ID="${BENCH_DESTINATION_ID:-comedy_test}"
BENCH_BACKGROUND_MUSIC_ASSET_ID="${BENCH_BACKGROUND_MUSIC_ASSET_ID:-}"
BENCH_BACKGROUND_MUSIC_DURATION_SECONDS="${BENCH_BACKGROUND_MUSIC_DURATION_SECONDS:-}"
BENCH_SUBTITLE_ASSET_ID="${BENCH_SUBTITLE_ASSET_ID:-}"
BENCH_OUT_ROOT="${BENCH_OUT_ROOT:-${REPO_ROOT}/tests/worker-cert/workers}"
VELOX_MASTER_LOG_PATH="${VELOX_MASTER_LOG_PATH:-}"

while (( $# > 0 )); do
  case "$1" in
    --workers)          WORKER_IDS+=("$2"); shift 2 ;;
    --no-drain)         NO_DRAIN=1; shift ;;
    --poll-timeout-s)   POLL_TIMEOUT_S="$2"; shift 2 ;;
    --jobs-per-worker)  JOBS_PER_WORKER="$2"; shift 2 ;;
    --destination-id)   DESTINATION_ID="$2"; shift 2 ;;
    --master-log-path)  VELOX_MASTER_LOG_PATH="$2"; shift 2 ;;
    -h|--help)          usage ;;
    *)                  log_error "unknown flag: $1"; exit 2 ;;
  esac
done

if (( ${#WORKER_IDS[@]} == 0 )); then
  WORKER_IDS=("${DEFAULT_WORKERS[@]}")
fi

for v in "$POLL_TIMEOUT_S" "$JOBS_PER_WORKER"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || (( v < 1 )); then
    log_error "timeout/jobs must be a positive integer (got: $v)"; exit 2
  fi
done

for bin in curl jq awk sed grep date; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    log_error "FATAL: required binary not in PATH: $bin"; exit 2
  fi
done

[[ -n "${VELOX_MASTER_URL:-}" ]] || VELOX_MASTER_URL="http://127.0.0.1:8080"
VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"
DESTINATION_ID="${DESTINATION_ID%/}"

for required_media in BENCH_BACKGROUND_MUSIC_ASSET_ID BENCH_BACKGROUND_MUSIC_DURATION_SECONDS BENCH_SUBTITLE_ASSET_ID; do
  if [[ -z "${!required_media}" ]]; then
    log_error "${required_media} is required; refusing to invent benchmark media"
    exit 2
  fi
done
if ! [[ "$BENCH_BACKGROUND_MUSIC_ASSET_ID" =~ ^[0-9a-f]{64}$ && "$BENCH_SUBTITLE_ASSET_ID" =~ ^[0-9a-f]{64}$ ]]; then
  log_error "benchmark music and ASS asset IDs must be lowercase 64-hex SHA-256 values"
  exit 2
fi
if ! [[ "$BENCH_BACKGROUND_MUSIC_DURATION_SECONDS" =~ ^[0-9]+([.][0-9]+)?$ ]] || ! awk -v d="$BENCH_BACKGROUND_MUSIC_DURATION_SECONDS" 'BEGIN { exit !(d >= 6) }'; then
  log_error "BENCH_BACKGROUND_MUSIC_DURATION_SECONDS must be numeric and >= 6 seconds"
  exit 2
fi

log_info "sequential_bench workers=${WORKER_IDS[*]} master=$VELOX_MASTER_URL dest=$DESTINATION_ID"
log_info "media: music=$BENCH_BACKGROUND_MUSIC_ASSET_ID duration=${BENCH_BACKGROUND_MUSIC_DURATION_SECONDS}s ass=$BENCH_SUBTITLE_ASSET_ID"
log_info "tunables: poll_timeout=${POLL_TIMEOUT_S}s jobs_per_worker=$JOBS_PER_WORKER no_drain=$NO_DRAIN"

# ─── Resolve admin token ───────────────────────────────────────────────────
ADMIN_TOKEN=""
if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
  ADMIN_TOKEN="$VELOX_ADMIN_TOKEN"
elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
  ADMIN_TOKEN=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
    | sed 's/^[^=]*=//' | tr -d '"' | tr -d "'" | xargs || true)
fi
[[ -n "$ADMIN_TOKEN" ]] || { log_error "VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided"; exit 2; }

# ─── M2M provisioning ──────────────────────────────────────────────────────
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
on_exit_cleanup() {
  local rc=$?
  # Resume all workers on exit (best-effort).
  for w in "${WORKER_IDS[@]}"; do
    curl -sS -m 5 -X POST \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      "${VELOX_MASTER_URL}/api/v1/admin/workers/${w}/resume" \
      >/dev/null 2>&1 || true
  done
  if [[ -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" ]]; then
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys/${PROVISIONED_CLIENT_ID}" \
      >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap on_exit_cleanup EXIT INT TERM

smoke_mint_m2m "$ADMIN_TOKEN" "$VELOX_MASTER_URL" || { log_error "M2M provisioning failed"; exit 3; }
log_info "M2M provisioned: client_id=$PROVISIONED_CLIENT_ID"

# ─── Resolve assets ────────────────────────────────────────────────────────
ASSETS_FILE="${SCRIPT_DIR}/fixtures/assets.json"
[[ -r "$ASSETS_FILE" ]] || { log_error "fixtures not readable: $ASSETS_FILE"; exit 2; }
ASSET_VO=$(jq -er '.voiceover[0].asset_id' "$ASSETS_FILE")
ASSET_CLIP_A=$(jq -er '.clips[0].asset_id' "$ASSETS_FILE")
ASSET_CLIP_B=$(jq -er '.clips[1].asset_id' "$ASSETS_FILE")
for fixture_asset in ASSET_VO ASSET_CLIP_A ASSET_CLIP_B; do
  if ! [[ "${!fixture_asset}" =~ ^[0-9a-f]{64}$ ]]; then
    log_error "fixture ${fixture_asset} is not a content-addressed SHA-256 asset ID"
    exit 2
  fi
done
log_info "assets: vo=$ASSET_VO clip_a=$ASSET_CLIP_A clip_b=$ASSET_CLIP_B"

# ─── Helper: fetch worker JSON (admin endpoint) ────────────────────────────
fetch_worker() {
  curl -sS -m 10 \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "${VELOX_MASTER_URL}/api/v1/admin/workers/${1}" 2>/dev/null || true
}

# ─── Helper: health probe ──────────────────────────────────────────────────
probe_health_level() {
  local worker_id="$1" level="$2"
  local resp
  resp=$(curl -sS -m 10 \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "${VELOX_MASTER_URL}/api/v1/admin/workers/${worker_id}/health?level=${level}" 2>/dev/null || true)
  local healthy
  healthy=$(printf '%s' "$resp" | jq -r '.healthy // false' 2>/dev/null || echo "false")
  if [[ "$healthy" == "true" ]]; then
    return 0
  fi
  return 1
}

# ─── Helper: drain / resume ────────────────────────────────────────────────
drain_worker() {
  local wid="$1"
  curl -sS -m 10 -X POST \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "${VELOX_MASTER_URL}/api/v1/admin/workers/${wid}/drain" \
    >/dev/null 2>&1 || true
}

resume_worker() {
  local wid="$1"
  curl -sS -m 10 -X POST \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "${VELOX_MASTER_URL}/api/v1/admin/workers/${wid}/resume" \
    >/dev/null 2>&1 || true
}

wait_active_jobs_zero() {
  local wid="$1" timeout_s="${2:-120}"
  local elapsed=0
  while (( elapsed < timeout_s )); do
    local wj jobs
    wj=$(fetch_worker "$wid")
    jobs=$(printf '%s' "$wj" | jq -r '.active_jobs // .active_tasks // 0' 2>/dev/null || echo "0")
    if [[ "$jobs" == "0" || "$jobs" == "null" ]]; then
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  log_warn "active_jobs did not reach 0 on $wid within ${timeout_s}s (last=$jobs)"
  return 1
}

# ─── Helper: static payload URI validation ─────────────────────────────────
validate_benchmark_payload() {
  local payload_json="$1"
  jq -e '
    ([.voiceover_paths[]?]
      + [.scenes[]?.clip_link]
      + [.scenes[]?.clip.url]
      + [.audio_tracks[]?.source_url]
      + [.subtitle_tracks[]?.source]
      + [.scenes[]?.subtitles.url])
    | map(select(. != null)) as $media
    | (($media | length > 0)
       and (all($media[]; type == "string" and startswith("velox-asset://")))
       and ([.. | strings | select(startswith("file://") or startswith("http://") or startswith("https://"))] | length == 0))
  ' <<<"$payload_json" >/dev/null 2>&1
}

# ─── Helper: submit + poll a single job ────────────────────────────────────
submit_and_poll_job() {
  local target_worker="$1" run_idx="$2" epoch="$3"
  local idem_key="seqbench-${target_worker}-${epoch}-$(date +%s%N)-${run_idx}"

  local payload
  payload=$(cat <<JSON
{
  "idempotency_key": "${idem_key}",
  "job_type": "scene.composite.v1",
  "template_id": "benchmark.clip-stock",
  "template_version": 1,
  "video_name": "sequential_bench ${target_worker} run ${run_idx}",
  "script_text": "Benchmark test script for sequential worker evaluation.",
  "placement_pin_worker_id": "${target_worker}",
  "voiceover_paths": [
    "velox-asset://${ASSET_VO}",
    "velox-asset://${ASSET_VO}"
  ],
  "audio_tracks": [
    {
      "asset_id": "${BENCH_BACKGROUND_MUSIC_ASSET_ID}",
      "source_url": "velox-asset://${BENCH_BACKGROUND_MUSIC_ASSET_ID}",
      "role": "background_music",
      "volume": 0.12,
      "start_time_offset": 0,
      "duration_seconds": ${BENCH_BACKGROUND_MUSIC_DURATION_SECONDS}
    }
  ],
  "scenes": [
    {
      "text":"Bench scene 1 — ${target_worker}",
      "clip_link":"velox-asset://${ASSET_CLIP_A}",
      "duration_seconds":3,
      "stock_links":["velox-asset://${ASSET_CLIP_A}","velox-asset://${ASSET_CLIP_B}"],
      "stock_fallback":true,
      "voiceover":{"url":"velox-asset://${ASSET_VO}","duration_ms":3000,"language":"it"},
      "subtitles": {
        "asset_id":"${BENCH_SUBTITLE_ASSET_ID}",
        "format":"ass",
        "url":"velox-asset://${BENCH_SUBTITLE_ASSET_ID}",
        "sha256":"${BENCH_SUBTITLE_ASSET_ID}",
        "language":"it"
      }
    },
    {"text":"Bench scene 2 — ${target_worker}","clip_link":"velox-asset://${ASSET_CLIP_B}","duration_seconds":3,"stock_links":["velox-asset://${ASSET_CLIP_B}","velox-asset://${ASSET_CLIP_A}"],"stock_fallback":true,"voiceover":{"url":"velox-asset://${ASSET_VO}","duration_ms":3000,"language":"it"}}
  ],
  "delivery_plan": [
    {
      "destination_id":"${DESTINATION_ID}",
      "priority":100,
      "retry_budget":1,
      "metadata":{"test_type":"sequential_stock_voiceover_music_ass"}
    }
  ]
}
JSON
)

  # Static contract guard: this benchmark must never submit local paths or
  # ordinary HTTP URLs, and every media reference must use velox-asset://.
  if ! validate_benchmark_payload "$payload"; then
    log_error "[submit ${target_worker} #${run_idx}] payload URI validation failed"
    printf '{"job_id":"","status":"PAYLOAD_INVALID","error":"media refs must use velox-asset://"}\n'
    return 1
  fi

  # ── POST /api/v1/jobs ──
  local submit_start post_status job_id
  submit_start=$(date +%s%3N)
  local hdrs_file body_file
  hdrs_file=$(mktemp); body_file=$(mktemp)
  post_status=$(curl -sS -m 30 -X POST \
    -H "Authorization: Bearer $M2M_BEARER" \
    -H "Content-Type: application/json" \
    --data-raw "$payload" \
    -D "$hdrs_file" -o "$body_file" \
    -w '%{http_code}' \
    "${VELOX_MASTER_URL}/api/v1/jobs" 2>/dev/null || echo "000")
  local submit_end
  submit_end=$(date +%s%3N)
  local submit_latency_ms=$(( submit_end - submit_start ))

  local post_body
  post_body=$(cat "$body_file" 2>/dev/null || true)
  rm -f "$hdrs_file" "$body_file"

  if [[ "$post_status" != "202" ]]; then
    log_error "[submit ${target_worker} #${run_idx}] HTTP $post_status (body: $(printf '%s' "$post_body" | head -c 300))"
    printf '{"job_id":"","status":"SUBMIT_FAILED","error":"HTTP %s"}\n' "$post_status"
    return 1
  fi
  job_id=$(printf '%s' "$post_body" | jq -er '.job_id // empty' 2>/dev/null || true)
  if [[ -z "$job_id" ]]; then
    log_error "[submit ${target_worker} #${run_idx}] 202 but no job_id"
    echo '{"job_id":"","status":"SUBMIT_FAILED","error":"202 but no job_id"}'
    return 1
  fi
  log_info "[submit ${target_worker} #${run_idx}] job_id=$job_id latency_ms=$submit_latency_ms"

  # ── Poll until SUCCEEDED ──
  local elapsed=0 sleep_s=1 last_status="" last_body=""
  while (( elapsed < POLL_TIMEOUT_S )); do
    sleep "$sleep_s"
    elapsed=$((elapsed + sleep_s))
    sleep_s=$(( sleep_s * 2 )); (( sleep_s > 16 )) && sleep_s=16

    local resp
    resp=$(curl -sS -m 10 \
      -H "Authorization: Bearer $M2M_BEARER" \
      "${VELOX_MASTER_URL}/api/v1/jobs/${job_id}" 2>/dev/null || true)

    local sv
    sv=$(printf '%s' "$resp" | jq -er '.status // empty' 2>/dev/null || true)
    [[ -n "$sv" ]] && { last_status="$sv"; last_body="$resp"; }

    case "$sv" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED)
        log_error "[poll ${target_worker} #${run_idx}] terminal-fail $sv after ${elapsed}s"
        printf '{"job_id":"%s","status":"%s","error":"terminal %s"}\n' "$job_id" "$sv" "$sv"
        return 1 ;;
    esac
  done

  if [[ "$last_status" != "SUCCEEDED" ]]; then
    log_error "[poll ${target_worker} #${run_idx}] timeout ${POLL_TIMEOUT_S}s (last=$last_status)"
    printf '{"job_id":"%s","status":"TIMEOUT"}\n' "$job_id"
    return 1
  fi

  # ── Extract metrics from response ──
  local started_at completed_at artifact_url artifact_size_bytes resp_worker_id task_id attempt_id lease_id
  started_at=$(printf '%s' "$last_body" | jq -er '.started_at   // empty' 2>/dev/null || true)
  completed_at=$(printf '%s' "$last_body" | jq -er '.completed_at // empty' 2>/dev/null || true)
  artifact_url=$(printf '%s' "$last_body" | jq -er '.artifact_url // .output_path // empty' 2>/dev/null || true)
  artifact_size_bytes=$(printf '%s' "$last_body" | jq -r '.artifact_size_bytes // 0' 2>/dev/null || echo "0")
  resp_worker_id=$(printf '%s' "$last_body" | jq -er '.worker_id // empty' 2>/dev/null || true)
  task_id=$(printf '%s' "$last_body" | jq -er '.task_id // empty' 2>/dev/null || true)
  attempt_id=$(printf '%s' "$last_body" | jq -er '.attempt_id // empty' 2>/dev/null || true)
  lease_id=$(printf '%s' "$last_body" | jq -er '.lease_id // empty' 2>/dev/null || true)

  # ── Compute render_time_ms ──
  local render_time_ms=0
  if [[ -n "$started_at" && -n "$completed_at" ]]; then
    local s_epoch c_epoch
    s_epoch=$(smoke_parse_iso8601 "$started_at")
    c_epoch=$(smoke_parse_iso8601 "$completed_at")
    if [[ -n "$s_epoch" && -n "$c_epoch" ]]; then
      render_time_ms=$(awk -v a="$s_epoch" -v b="$c_epoch" 'BEGIN{printf "%.0f", (b-a)*1000}')
    fi
  fi

  # ── Verify placement via log scrape ──
  local lease_json leased_worker
  lease_json=$(smoke_scrape_lease "$job_id" "${VELOX_MASTER_LOG_PATH:-}")
  leased_worker=$(printf '%s' "$lease_json" | jq -er '.worker_id // empty' 2>/dev/null || true)

  local pin_ok="false"
  if [[ -n "$leased_worker" && "$leased_worker" == "$target_worker" ]]; then
    pin_ok="true"
  elif [[ -n "$resp_worker_id" && "$resp_worker_id" == "$target_worker" ]]; then
    pin_ok="true"
  fi

  log_info "[done ${target_worker} #${run_idx}] job=$job_id render=${render_time_ms}ms artifact_bytes=$artifact_size_bytes pin_ok=$pin_ok"

  # Emit JSON row for this run.
  jq -n \
    --arg job_id "$job_id" \
    --arg status "SUCCEEDED" \
    --argjson submit_latency_ms "$submit_latency_ms" \
    --argjson render_time_ms "$render_time_ms" \
    --argjson artifact_size_bytes "$artifact_size_bytes" \
    --arg started_at "$started_at" \
    --arg completed_at "$completed_at" \
    --arg artifact_url "$artifact_url" \
    --arg target_worker "$target_worker" \
    --arg resp_worker_id "$resp_worker_id" \
    --arg leased_worker "$leased_worker" \
    --arg task_id "$task_id" \
    --arg attempt_id "$attempt_id" \
    --arg lease_id "$lease_id" \
    --arg pin_ok "$pin_ok" \
    --arg run_idx "$run_idx" \
    '{
      job_id: $job_id,
      status: $status,
      run_idx: ($run_idx|tonumber),
      submit_latency_ms: $submit_latency_ms,
      render_time_ms: $render_time_ms,
      artifact_size_bytes: $artifact_size_bytes,
      started_at: $started_at,
      completed_at: $completed_at,
      artifact_url: $artifact_url,
      target_worker: $target_worker,
      resp_worker_id: $resp_worker_id,
      leased_worker: $leased_worker,
      task_id: $task_id,
      attempt_id: $attempt_id,
      lease_id: $lease_id,
      pin_ok: $pin_ok
    }'
}

# ─── Pre-flight: workers list ──────────────────────────────────────────────
# NOTE: /api/v1/workers requires admin auth (not M2M job-scoped token).
smoke_workers_list "$ADMIN_TOKEN" "$VELOX_MASTER_URL" || { log_error "could not list workers"; exit 3; }

# ─── benchmark_worker: all per-worker steps ─────────────────────────────────
# Runs pre-flight, drain, submit+poll, resume, stats for a single worker.
# Sets globals ALL_RUNS_JSON, WORKER_SUMMARIES_JSON, ALL_JOBS_SUCCEEDED.
benchmark_worker() {
  local w="$1"
  log_info "═══ BENCHMARKING $w ═══"

  # ── Step 1: Pre-flight health C (connectivity) required; A/B informational ──
  local level
  for level in A B; do
    if probe_health_level "$w" "$level"; then
      log_info "[pre-flight $w] health level $level: PASS"
    else
      log_warn "[pre-flight $w] health level $level: FAIL (infrastructure — non-blocking)"
    fi
  done
  if ! probe_health_level "$w" "C"; then
    log_error "[pre-flight $w] health level C FAILED — worker not connected, skipping"
    return 0
  fi
  log_info "[pre-flight $w] health level C: PASS"

  # ── Step 2: Image digest check ──
  local worker_json image_digest
  worker_json=$(fetch_worker "$w")
  image_digest=$(printf '%s' "$worker_json" | jq -er '.image_digest // "unknown"' 2>/dev/null || echo "unknown")
  log_info "[pre-flight $w] image_digest=$image_digest"

  # Verify CONNECTED + session_active.
  local conn_status session_active
  conn_status=$(printf '%s' "$worker_json" | jq -r '.status // "UNKNOWN"' 2>/dev/null || echo "UNKNOWN")
  session_active=$(printf '%s' "$worker_json" | jq -r '.session_active // false' 2>/dev/null || echo "false")
  if [[ "$conn_status" != "CONNECTED" || "$session_active" != "true" ]]; then
    log_error "[pre-flight $w] not CONNECTED+session_active (status=$conn_status session=$session_active)"
    return 0
  fi

  # ── Step 3: Drain other workers ──
  if (( NO_DRAIN == 0 )); then
    log_info "[drain] draining non-target workers for $w"
    local other
    for other in "${WORKER_IDS[@]}"; do
      if [[ "$other" != "$w" ]]; then
        drain_worker "$other"
        log_info "[drain] $other → DRAINING"
      fi
    done
    # Wait for drain to take effect + active_jobs=0 on target.
    sleep 3
    wait_active_jobs_zero "$w" 120 || log_warn "[drain] target $w still has active jobs"
  fi

  # ── Step 4: Submit JOBS_PER_WORKER identical jobs ──
  local worker_runs_json="[]"
  local worker_succeeded=0 worker_failed=0
  local run_idx run_json run_rc
  for (( run_idx=1; run_idx<=JOBS_PER_WORKER; run_idx++ )); do
    run_json=$(submit_and_poll_job "$w" "$run_idx" "$EPOCH")
    run_rc=$?

    if (( run_rc == 0 )); then
      worker_succeeded=$((worker_succeeded + 1))
    else
      worker_failed=$((worker_failed + 1))
      ALL_JOBS_SUCCEEDED=0
    fi

    if [[ -n "$run_json" ]]; then
      worker_runs_json=$(jq -c --argjson row "$run_json" '. + [$row]' <<<"$worker_runs_json")
    fi

    # Small pause between jobs to avoid flooding.
    sleep 1
  done

  # ── Step 5: Resume drained workers ──
  if (( NO_DRAIN == 0 )); then
    local other2
    for other2 in "${WORKER_IDS[@]}"; do
      if [[ "$other2" != "$w" ]]; then
        resume_worker "$other2"
      fi
    done
    sleep 2
  fi

  # ── Step 6: Compute summary stats ──
  local render_times=()
  local render_sum=0 render_count=0 rt
  while IFS= read -r rt; do
    if [[ "$rt" =~ ^[0-9]+$ ]] && (( rt > 0 )); then
      render_times+=("$rt")
      render_sum=$((render_sum + rt))
      render_count=$((render_count + 1))
    fi
  done < <(jq -r '.[].render_time_ms // 0' <<<"$worker_runs_json")

  local median=0 min=0 max=0 avg=0
  if (( render_count > 0 )); then
    avg=$(( render_sum / render_count ))
    local sorted mid
    sorted=$(printf '%s\n' "${render_times[@]}" | sort -n)
    min=$(printf '%s\n' "$sorted" | head -1)
    max=$(printf '%s\n' "$sorted" | tail -1)
    mid=$(( (render_count + 1) / 2 ))
    median=$(printf '%s\n' "$sorted" | sed -n "${mid}p")
  fi

  # realtime_factor = render_ms / output_duration_ms.
  # Output is 2 scenes × 3s = 6000ms.
  local output_duration_ms=6000
  local realtime_factor="0"
  if (( median > 0 )); then
    realtime_factor=$(awk -v r="$median" -v d="$output_duration_ms" 'BEGIN{printf "%.2f", r/d}')
  fi

  local summary
  summary=$(jq -n \
    --arg worker_id "$w" \
    --arg image_digest "$image_digest" \
    --argjson succeeded "$worker_succeeded" \
    --argjson failed "$worker_failed" \
    --argjson median "$median" \
    --argjson min "$min" \
    --argjson max "$max" \
    --argjson avg "$avg" \
    --argjson output_duration_ms "$output_duration_ms" \
    --arg realtime_factor "$realtime_factor" \
    '{
      worker_id: $worker_id,
      image_digest: $image_digest,
      jobs_succeeded: $succeeded,
      jobs_failed: $failed,
      render_time_ms: { median: $median, min: $min, max: $max, avg: $avg },
      output_duration_ms: $output_duration_ms,
      realtime_factor: $realtime_factor
    }')
  WORKER_SUMMARIES_JSON=$(jq -c --argjson s "$summary" '. + [$s]' <<<"$WORKER_SUMMARIES_JSON")

  # Accumulate all runs.
  ALL_RUNS_JSON=$(jq -c --argjson rows "$worker_runs_json" '. + $rows' <<<"$ALL_RUNS_JSON")

  log_info "[worker $w done] succeeded=$worker_succeeded failed=$worker_failed median=${median}ms min=${min}ms max=${max}ms realtime_factor=${realtime_factor}x"
}

# ─── Per-worker benchmark loop ─────────────────────────────────────────────
EPOCH=$(date +%s)
ALL_RUNS_JSON="[]"
WORKER_SUMMARIES_JSON="[]"
ALL_JOBS_SUCCEEDED=1

for w in "${WORKER_IDS[@]}"; do
  benchmark_worker "$w"
done

# ─── Write benchmark.json ──────────────────────────────────────────────────
OUT_DIR="${BENCH_OUT_ROOT}"
ensure_dir "$OUT_DIR"
OUT_FILE="${OUT_DIR}/benchmark.json"
TMP_OUT=$(mktemp "${OUT_DIR}/benchmark-XXXXXX.json")
NOW_ISO=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
TOTAL_RUNS=$(jq 'length' <<<"$ALL_RUNS_JSON")
WORKERS_TESTED=$(jq 'length' <<<"$WORKER_SUMMARIES_JSON")

cat > "$TMP_OUT" <<JSON
{
  "schema": "tests/worker-cert/sequential_bench@1",
  "epoch": ${EPOCH},
  "workers_tested": ${WORKERS_TESTED},
  "total_runs": ${TOTAL_RUNS},
  "jobs_per_worker": ${JOBS_PER_WORKER},
  "poll_timeout_s": ${POLL_TIMEOUT_S},
  "destination_id": "${DESTINATION_ID}",
  "media_profile": "stock_clip_voiceover_background_music_ass",
  "asset_uri_scheme": "velox-asset",
  "video_duration_ms": 12000,
  "background_music_duration_seconds": ${BENCH_BACKGROUND_MUSIC_DURATION_SECONDS},
  "subtitle_format": "ass",
  "no_drain": $([[ $NO_DRAIN -eq 1 ]] && echo true || echo false),
  "all_jobs_succeeded": $([[ $ALL_JOBS_SUCCEEDED -eq 1 ]] && echo true || echo false),
  "master_url": "${VELOX_MASTER_URL}",
  "written_at": "${NOW_ISO}",
  "workers": ${WORKER_SUMMARIES_JSON},
  "runs": ${ALL_RUNS_JSON}
}
JSON
mv "$TMP_OUT" "$OUT_FILE"
log_info "wrote $OUT_FILE"

# ─── Final summary table ───────────────────────────────────────────────────
echo ""
echo "═══ SEQUENTIAL BENCH RESULTS ═══"
printf "%-32s %8s %8s %8s %8s %8s %8s\n" \
  "worker_id" "median_ms" "min_ms" "max_ms" "avg_ms" "realtime" "succ/fail"
printf -- "-%.0s" $(seq 1 85); echo
jq -r '.workers[] | "\(.worker_id) \(.render_time_ms.median) \(.render_time_ms.min) \(.render_time_ms.max) \(.render_time_ms.avg) \(.realtime_factor) \(.jobs_succeeded)/\(.jobs_failed)"' \
  "$OUT_FILE" 2>/dev/null | while read -r wid med min max avg rtf s_f; do
  printf "%-32s %8s %8s %8s %8s %8s %8s\n" "$wid" "$med" "$min" "$max" "$avg" "$rtf" "$s_f"
done

echo ""
echo "Full report: $OUT_FILE"
echo ""

if (( ALL_JOBS_SUCCEEDED == 1 )); then
  echo "OK: all $TOTAL_RUNS runs SUCCEEDED across $WORKERS_TESTED workers"
  exit 0
else
  echo "WARN: some runs failed — see $OUT_FILE for details"
  exit 7
fi
