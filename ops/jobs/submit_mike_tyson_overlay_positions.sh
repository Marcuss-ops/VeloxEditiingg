#!/usr/bin/env bash
# Submit a non-destructive overlay-position regression using the canonical
# Mike Tyson Creator Push payload as the base. The original Tyson fixture is
# never edited: only the five overlay assets and their frame windows are
# replaced in a temporary payload.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_PAYLOAD="${ROOT_DIR}/ops/jobs/mike_tyson_intro_stock.creator-push.json"
MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8000}"
MASTER_URL="${MASTER_URL%/}"
ADMIN_TOKEN="${VELOX_ADMIN_TOKEN:-}"
DESTINATION="${VELOX_TYSON_OVERLAY_DESTINATION:-drive-production}"
STATUS_PATH="${VELOX_TYSON_OVERLAY_STATUS_PATH:-/api/v1/admin/jobs}"
POLL_TIMEOUT="${VELOX_TYSON_OVERLAY_TIMEOUT:-900}"
POLL_INTERVAL="${VELOX_TYSON_OVERLAY_INTERVAL:-10}"
SOURCE_JOB_ID="${VELOX_TYSON_OVERLAY_SOURCE_JOB_ID:-mike-tyson-overlay-position-test-$(date +%s)}"
PLACEMENT_PIN_WORKER_ID="${VELOX_TYSON_OVERLAY_WORKER_ID:-}"
DRY_RUN="${VELOX_TYSON_OVERLAY_DRY_RUN:-0}"

for bin in curl jq; do
  command -v "$bin" >/dev/null 2>&1 || {
    printf 'FATAL: required binary not found: %s\n' "$bin" >&2
    exit 2
  }
done
[[ -r "$BASE_PAYLOAD" ]] || {
  printf 'FATAL: base payload is not readable: %s\n' "$BASE_PAYLOAD" >&2
  exit 2
}
[[ "$POLL_TIMEOUT" =~ ^[0-9]+$ && "$POLL_TIMEOUT" -gt 0 ]] || {
  printf 'FATAL: VELOX_TYSON_OVERLAY_TIMEOUT must be a positive integer\n' >&2
  exit 2
}
[[ "$POLL_INTERVAL" =~ ^[0-9]+$ && "$POLL_INTERVAL" -gt 0 ]] || {
  printf 'FATAL: VELOX_TYSON_OVERLAY_INTERVAL must be a positive integer\n' >&2
  exit 2
}

TMP_PAYLOAD="$(mktemp)"
TMP_BODY="$(mktemp)"
TMP_HEADERS="$(mktemp)"
trap 'rm -f "$TMP_PAYLOAD" "$TMP_BODY" "$TMP_HEADERS"' EXIT

# Frame-native windows at the canonical 24 fps:
#   001:  1–6s     (intro clip)
#   002: 30–35s    (stock)
#   003: 65–70s    (stock)
#   004: 120–125s  (stock)
#   005: 200–205s  (stock)
jq \
  --arg source_job_id "$SOURCE_JOB_ID" \
  --arg destination "$DESTINATION" \
  --arg overlay_01 "1zMJRXJ1Qaiy4BO7NkRNuwtfxL86gvwIp" \
  --arg overlay_02 "1r3SzQpTRNbnYeNhjzfRt3R9yBAxDJr_D" \
  --arg overlay_03 "1-7-ERZHtGAaAiYMy-iUiqjB2W_duhJx_" \
  --arg overlay_04 "1yopMHbkpdfmtCY_uiCKoJMCVGrhLwFUZ" \
  --arg overlay_05 "1hQD9HOtzUZYtm_KGGSYeuxqMok4_zO36" \
  --arg placement_pin_worker_id "$PLACEMENT_PIN_WORKER_ID" \
  '.source_job_id = $source_job_id
   | .payload.job_id = $source_job_id
   | if $placement_pin_worker_id != "" then .payload._placement_pin_worker_id = $placement_pin_worker_id else . end
   | .payload.delivery_plan[0].destination_id = $destination
   | .payload.overlays = [
       {id:"position-overlay-01", asset_id:$overlay_01, drive_file_id:$overlay_01,
        url:("https://drive.google.com/file/d/" + $overlay_01 + "/view?usp=drive_link"),
        start_frame:24, end_frame:144, frame_count:120, mode:"replace", z_index:10,
        audio_mode:"preserve_final_audio"},
       {id:"position-overlay-02", asset_id:$overlay_02, drive_file_id:$overlay_02,
        url:("https://drive.google.com/file/d/" + $overlay_02 + "/view?usp=drive_link"),
        start_frame:720, end_frame:840, frame_count:120, mode:"replace", z_index:10,
        audio_mode:"preserve_final_audio"},
       {id:"position-overlay-03", asset_id:$overlay_03, drive_file_id:$overlay_03,
        url:("https://drive.google.com/file/d/" + $overlay_03 + "/view?usp=drive_link"),
        start_frame:1560, end_frame:1680, frame_count:120, mode:"replace", z_index:10,
        audio_mode:"preserve_final_audio"},
       {id:"position-overlay-04", asset_id:$overlay_04, drive_file_id:$overlay_04,
        url:("https://drive.google.com/file/d/" + $overlay_04 + "/view?usp=drive_link"),
        start_frame:2880, end_frame:3000, frame_count:120, mode:"replace", z_index:10,
        audio_mode:"preserve_final_audio"},
       {id:"position-overlay-05", asset_id:$overlay_05, drive_file_id:$overlay_05,
        url:("https://drive.google.com/file/d/" + $overlay_05 + "/view?usp=drive_link"),
        start_frame:4800, end_frame:4920, frame_count:120, mode:"replace", z_index:10,
        audio_mode:"preserve_final_audio"}
     ]' "$BASE_PAYLOAD" > "$TMP_PAYLOAD"

if [[ "$DRY_RUN" == "1" ]]; then
  jq '{source_job_id, target_executor_id,
      overlays: [.payload.overlays[] | {id, asset_id, start_frame, end_frame, frame_count, mode, audio_mode}],
      delivery_plan: .payload.delivery_plan}' "$TMP_PAYLOAD"
  exit 0
fi

[[ -n "$ADMIN_TOKEN" ]] || {
  printf 'FATAL: VELOX_ADMIN_TOKEN is required\n' >&2
  exit 2
}

printf '→ POST %s/api/v1/creator/jobs\n' "$MASTER_URL"
curl -sS --max-time 60 -X POST \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary "@${TMP_PAYLOAD}" \
  -D "$TMP_HEADERS" -o "$TMP_BODY" \
  "$MASTER_URL/api/v1/creator/jobs"

HTTP_STATUS="$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HEADERS")"
if [[ "$HTTP_STATUS" != "202" ]]; then
  printf 'FAIL: creator push returned HTTP %s\n' "${HTTP_STATUS:-?}" >&2
  sed -n '1,120p' "$TMP_BODY" >&2
  exit 1
fi

JOB_ID="$(jq -er '.job_id // empty' "$TMP_BODY")" || {
  printf 'FAIL: HTTP 202 response has no job_id\n' >&2
  sed -n '1,120p' "$TMP_BODY" >&2
  exit 1
}
printf 'OK: queued job_id=%s\n' "$JOB_ID"
printf '   overlays: 1–6s, 30–35s, 65–70s, 120–125s, 200–205s @ 24 fps\n'

deadline=$(( $(date +%s) + POLL_TIMEOUT ))
status=""
while (( $(date +%s) < deadline )); do
  response="$(curl -fsS --max-time 30 \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    "$MASTER_URL${STATUS_PATH%/}/$JOB_ID")"
  status="$(jq -r '.status // .job.status // empty' <<<"$response")"
  case "$status" in
    SUCCEEDED)
      printf 'PASS: job %s reached SUCCEEDED\n' "$JOB_ID"
      exit 0
      ;;
    FAILED|CANCELLED)
      printf 'FAIL: job %s reached %s\n' "$JOB_ID" "$status" >&2
      jq . <<<"$response" >&2 || printf '%s\n' "$response" >&2
      exit 1
      ;;
  esac
  sleep "$POLL_INTERVAL"
done

printf 'FAIL: timeout after %ss; last status=%s, job_id=%s\n' "$POLL_TIMEOUT" "${status:-<none>}" "$JOB_ID" >&2
exit 1
