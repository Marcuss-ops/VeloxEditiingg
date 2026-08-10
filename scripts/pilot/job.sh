#!/usr/bin/env bash
# Sourced by scripts/pilot.sh; definitions only.

cmd_submit() {
  banner "SUBMIT: fixtures + job"

  mkdir -p "$STAGING_DIR"

  # Generate test fixtures
  log "  → silent.aac (2s silent)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i anullsrc=r=48000:cl=mono -t 2 \
    -c:a aac -b:a 64k "${STAGING_DIR}/silent.aac" 2>/dev/null || true

  log "  → scene.png (teal 320x180)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i color=c=0x008080:s=320x180:d=0.1 -frames:v 1 \
    -vcodec png "${STAGING_DIR}/scene.png" 2>/dev/null || true

  # Verify fixtures
  ls -la "${STAGING_DIR}/silent.aac" "${STAGING_DIR}/scene.png" 2>/dev/null || \
    warn "fixture files may be missing (ffmpeg might not support libmp3lame)"

  "${REPO_ROOT}/scripts/e2e/write-local-workload-fixture.sh" "$JOB_FILE" "$STAGING_DIR" "$DESTINATION_ID"

  # Submit
  local SUBMIT_OUT
  SUBMIT_OUT="$(curl -sS -m 15 -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary @"$JOB_FILE" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/script/generate-with-images" 2>&1)" || true

  echo "$SUBMIT_OUT" | python3 -m json.tool 2>/dev/null || echo "$SUBMIT_OUT"

  local JOB_ID
  JOB_ID="$(echo "$SUBMIT_OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('job_id',''))" 2>/dev/null || true)"

  if [[ -z "$JOB_ID" ]]; then
    die "job submission failed — could not extract job_id" 1
  fi

  log "job_id=${JOB_ID}"

  # Show current jobs from DB
  banner "JOBS in DB"
  sqlite3 "${DATA_DIR}/velox.db" \
    "SELECT job_id, status, video_name, created_at FROM jobs ORDER BY created_at DESC LIMIT 5;" \
    2>/dev/null || true

  ok "job submitted (PENDING)"
}

verify_completed_job() {
  local db="$1"
  local job_id="$2"
  local video
  local video_count
  video_count="$(find "$STORAGE_DIR" -type f \( -name '*.mp4' -o -name '*.f4v' \) | wc -l)"
  [[ "$video_count" -eq 1 ]] || die "expected exactly one final video artifact, found ${video_count}" 1
  video="$(find "$STORAGE_DIR" -type f \( -name '*.mp4' -o -name '*.f4v' \) -print -quit)"
  [[ -s "$video" ]] || die "final video is empty: ${video}" 1

  local probe
  probe="$(ffprobe -v error -show_entries stream=codec_type,codec_name,width,height,r_frame_rate -show_entries format=duration,size -of json "$video" 2>&1)" \
    || die "ffprobe failed for ${video}: ${probe}" 1
  grep -q '"codec_type": "video"' <<<"$probe" \
    || die "final artifact has no video stream: ${video}" 1

  local decode_log="${PILOT_DIR}/decode-errors.log"
  ffmpeg -v error -i "$video" -f null - 2>"$decode_log" \
    || die "decode command failed for ${video}" 1
  [[ ! -s "$decode_log" ]] || { cat "$decode_log"; die "final video is not fully decodable" 1; }

  local actual_sha actual_size recorded
  actual_sha="$(sha256sum "$video" | awk '{print $1}')"
  actual_size="$(stat -c '%s' "$video")"
  recorded="$(sqlite3 "$db" "SELECT status || '|' || sha256 || '|' || size_bytes || '|' || COALESCE(verified_at,'') FROM artifacts WHERE job_id='${job_id}' ORDER BY created_at DESC LIMIT 1;" 2>/dev/null || true)"
  local recorded_status recorded_sha recorded_size recorded_verified
  IFS='|' read -r recorded_status recorded_sha recorded_size recorded_verified <<<"$recorded"
  [[ "$recorded_status" == "READY" && "$recorded_sha" == "$actual_sha" \
    && "$recorded_size" == "$actual_size" && -n "$recorded_verified" ]] \
    || die "artifact DB verification mismatch: recorded=${recorded} actual=READY|${actual_sha}|${actual_size}|verified" 1

  local task_state
  task_state="$(sqlite3 "$db" "SELECT status || '|' || COALESCE(attempt_id,'') || '|' || COALESCE(winning_attempt_id,'') FROM tasks WHERE job_id='${job_id}';" 2>/dev/null || true)"
  [[ "$task_state" == SUCCEEDED\|*\|* ]] || die "task did not succeed: ${task_state}" 1
  local task_attempt task_winner
  task_attempt="${task_state#*|}"; task_attempt="${task_attempt%%|*}"
  task_winner="${task_state##*|}"
  [[ -n "$task_winner" && "$task_winner" == "$task_attempt" ]] \
    || die "succeeded task has no matching winning attempt: ${task_state}" 1

  ok "video validated: ${video} (${actual_size} bytes, sha256=${actual_sha})"
  ok "artifact READY and winning TaskAttempt verified"
}
