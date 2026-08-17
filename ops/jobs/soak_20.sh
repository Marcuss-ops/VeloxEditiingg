#!/usr/bin/env bash
# ops/jobs/soak_20.sh — 20-job soak via the canonical M2M intake.
#
# Submits 20 jobs through POST /api/v1/jobs (no placement pin — the
# scheduler distributes across the fleet), polls each to a terminal state,
# and reports the aggregate verdict plus per-job cache evidence for the
# warm-cache check (high hits / downloads only when necessary).
#
# The payload is the canonical 10-clip audio-certification fixture
# (10_clip_audio_certification.generate.json), the same one that produced
# the certified warm run job_cae61e986f67cc37 (30 hit / 0 miss / 0 download).
# Each job gets a unique idempotency_key + video_name so the 20 runs are
# independent, while the assets stay identical → the cache stays warm.
#
# Flow (mirrors ops/jobs/lib/benchmark-common.sh):
#   1. Resolve VELOX_MASTER_URL + VELOX_ADMIN_TOKEN (env > TOKEN_FILE).
#   2. Mint ONE ephemeral M2M client (scopes=[jobs.submit]).
#   3. POST /api/v1/jobs × 20 with unique idempotency keys.
#   4. Poll GET /api/v1/jobs/{job_id} until terminal for every job.
#   5. Print per-job status + cache summary; exit 0 only when 20/20 SUCCEEDED.
#   6. Best-effort DELETE of the ephemeral M2M key on every exit path.
#
# Environment:
#   VELOX_MASTER_URL            (optional) default http://127.0.0.1:8080
#   VELOX_ADMIN_TOKEN           admin bearer (env var or TOKEN_FILE dotenv)
#   VELOX_SOAK_COUNT            (optional) number of jobs, default 20
#   VELOX_SOAK_PAYLOAD          (optional) payload file, default the 10-clip
#                               audio-certification fixture
#   VELOX_SOAK_POLL_TIMEOUT_S   (optional) poll cap per job, default 600
#   VELOX_SOAK_DELIVERY_DEST    (optional) delivery_plan destination override
#                               (payload default "drive-smoke")
#
# Exit codes:
#   0  all jobs SUCCEEDED
#   1  at least one job FAILED/CANCELLED
#   2  poll timeout on one or more jobs
#   3  POST rejected at intake (non-202) or other transport failure
#   4  usage/env error
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/benchmark-common.sh
source "${SCRIPT_DIR}/lib/benchmark-common.sh"

SOAK_COUNT="${VELOX_SOAK_COUNT:-20}"
DELIVERY_DEST="${VELOX_SOAK_DELIVERY_DEST:-}"
BENCHMARK_PAYLOAD_FILE="${VELOX_SOAK_PAYLOAD:-${SCRIPT_DIR}/10_clip_audio_certification.generate.json}"
if [[ ! -r "${BENCHMARK_PAYLOAD_FILE}" ]]; then
  benchmark_fail "soak payload not readable: ${BENCHMARK_PAYLOAD_FILE}"
fi

benchmark_resolve_admin_token
benchmark_mint_m2m

# One M2M client drives all 20 submissions. The canonical payload carries
# real velox-drive:// assets that are already warm in the fleet cache; each
# job only varies idempotency_key + video_name.
RUN_STAMP="$(date +%s)"
JOB_IDS=()
declare -A JOB_IDEM=()

submit_one() {
  local i="$1" key idem video
  key="velox-soak-20-${RUN_STAMP}-${i}"
  idem="velox-soak-${RUN_STAMP}-${i}"
  video="Velox soak #${i} (${RUN_STAMP})"
  # clip.stock.v1 is the canonical renderer-mode recipe that projects
  # scenes → items (scene.composite.v1 maps to an empty renderer mode and
  # the worker's hybrid.v1 Validate rejects a scenes_json-only payload).
  local filter=".idempotency_key = \$idem | .video_name = \$video | .job_type = \"clip.stock.v1\" | del(.copy_only)"
  if [[ -n "${DELIVERY_DEST}" ]]; then
    filter="${filter} | .delivery_plan[0].destination_id = \$dest"
  fi
  local staged
  staged="$(jq --arg idem "${idem}" --arg video "${video}" --arg dest "${DELIVERY_DEST}" "${filter}" "${BENCHMARK_PAYLOAD_FILE}")"
  if [[ -z "${staged}" ]]; then
    benchmark_fail "jq substitution produced an empty soak payload for job ${i}"
  fi

  local post_status post_body job_id
  printf '→ POST /api/v1/jobs [%02d/%02d] idem=%s\n' "$i" "$SOAK_COUNT" "$idem"
  if ! post_status="$(curl -sS -m 30 -X POST \
    -H "Authorization: Bearer ${M2M_BEARER}" \
    -H "Content-Type: application/json" \
    --data-raw "${staged}" \
    -w '%{http_code}' \
    -o "$_TMP_BODY" 2>"$_TMP_TRACE" \
    "${MASTER_URL}/api/v1/jobs")"; then
    benchmark_fail "curl could not reach ${MASTER_URL}/api/v1/jobs"
  fi
  post_body="$(cat "$_TMP_BODY" 2>/dev/null || true)"
  if [[ "${post_status}" != "202" ]]; then
    printf 'REJECTED: POST /api/v1/jobs returned HTTP %s\n' "${post_status:-?}"
    printf '%s\n' "${post_body}" | head -c 2000
    printf '\n'
    return 3
  fi
  job_id="$(printf '%s' "${post_body}" | jq -er '.job_id // empty')" || benchmark_fail "202 response missing job_id"
  JOB_IDS+=("${job_id}")
  JOB_IDEM["${job_id}"]="${idem}"
  printf '  accepted job_id=%s\n' "${job_id}"
  # The M2M client is minted with rate_limit_rps=5 / burst=10; pacing each
  # POST keeps the whole soak under the limit (a burst of 20 would 429).
  sleep 0.35
}

rc=0
for i in $(seq 1 "${SOAK_COUNT}"); do
  submit_one "$i" || { rc=$?; break; }
done
if [[ "$rc" != "0" ]]; then
  printf 'FAIL: intake rejected before completing all %s jobs (rc=%s)\n' "$SOAK_COUNT" "$rc"
  exit "$rc"
fi

printf '\nSubmitted %d jobs; polling to terminal state (timeout=%ss each)...\n' "${#JOB_IDS[@]}" "$POLL_TIMEOUT_S"

succeeded=0
failed=0
timedout=0
for job_id in "${JOB_IDS[@]}"; do
  local_job_id="$job_id"
  elapsed=0; sleep_s=1; status_value=""; last_body=""
  while (( elapsed < POLL_TIMEOUT_S )); do
    sleep "$sleep_s"
    elapsed=$((elapsed + sleep_s))
    sleep_s=$(( sleep_s * 2 ))
    if (( sleep_s > 16 )); then sleep_s=16; fi
    if ! curl -sS -m 10 \
      -H "Authorization: Bearer ${M2M_BEARER}" \
      "${MASTER_URL}/api/v1/jobs/${local_job_id}" \
      -D "$_TMP_HDRS" -o "$_TMP_BODY" 2>/dev/null; then
      benchmark_fail "curl could not reach ${MASTER_URL}/api/v1/jobs/${local_job_id} during poll"
    fi
    last_body="$(cat "$_TMP_BODY" 2>/dev/null || true)"
    status_value="$(printf '%s' "${last_body}" | jq -er '.status // empty' 2>/dev/null || true)"
    case "${status_value}" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED) break ;;
    esac
  done
  case "${status_value}" in
    SUCCEEDED) succeeded=$((succeeded+1)) ;;
    FAILED|CANCELLED) failed=$((failed+1)) ;;
    *) timedout=$((timedout+1)) ;;
  esac
  printf '%s %-20s status=%s\n' "$(date -u +%H:%M:%S)" "${local_job_id}" "${status_value:-(none)}"
done

printf '\n=== SOAK AGGREGATE ===\n'
printf 'submitted=%d succeeded=%d failed=%d timed_out=%d\n' \
  "${#JOB_IDS[@]}" "$succeeded" "$failed" "$timedout"
printf 'VERDICT: %s\n' "$( [[ "$succeeded" == "${#JOB_IDS[@]}" && "$failed" == "0" && "$timedout" == "0" ]] && echo PASS || echo FAIL )"
printf 'Cache evidence: run `fleetctl job inspect <job_id> --json` per job; warm runs show hits>0, download_count=0, hit_ratio=1.\n'

if [[ "$failed" != "0" ]]; then
  exit 1
fi
if [[ "$timedout" != "0" ]]; then
  exit 2
fi
exit 0
