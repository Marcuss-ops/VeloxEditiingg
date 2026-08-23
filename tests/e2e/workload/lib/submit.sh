# shellcheck shell=bash

phase_submit() {
  info "Phase 4: submitting job"

  local audio_path="$FIXTURE_DIR/silent.mp4"
  [[ -f "$audio_path" ]] || audio_path="$FIXTURE_DIR/silent.mp3"
  local audio_file
  audio_file="$(basename "$audio_path")"

  "${REPO_ROOT}/scripts/e2e/write-local-workload-fixture.sh" "$WORKDIR/job.json" "$FIXTURE_DIR" "$DESTINATION_ID" "$audio_file"

  local submit_out
  submit_out="$(curl -sS -m 15 -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary @"$WORKDIR/job.json" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/script/generate-with-images" 2>&1)" || true

  JOB_ID="$(echo "$submit_out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('job_id',''))" 2>/dev/null || true)"

  if [[ -z "$JOB_ID" ]]; then
    fail "job submission failed — response: $submit_out"
    exit 1
  fi
  pass "job submitted: job_id=$JOB_ID"
}
