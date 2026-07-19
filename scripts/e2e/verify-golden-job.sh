#!/usr/bin/env bash
# Canonical Golden E2E verifier.  It is deliberately independent of process
# startup so it can verify a job produced by either the single-host or the
# two-host harness.
set -euo pipefail

usage() {
  echo "usage: $0 --db DB --job-id ID --storage-dir DIR --tmpdir DIR [profile/media options]" >&2
  exit 3
}

DB=""; JOB_ID=""; STORAGE_DIR=""; TMPDIR="/tmp/velox-golden-verify"
PROFILE="${GOLDEN_PROFILE:-small}"; WIDTH=1920; HEIGHT=1080; FPS=30
DURATION=2; SAMPLE_RATE=48000; CHANNELS=2; SCENES=1
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db) DB="$2"; shift 2;;
    --job-id) JOB_ID="$2"; shift 2;;
    --storage-dir) STORAGE_DIR="$2"; shift 2;;
    --tmpdir) TMPDIR="$2"; shift 2;;
    --profile) PROFILE="$2"; shift 2;;
    --width) WIDTH="$2"; shift 2;;
    --height) HEIGHT="$2"; shift 2;;
    --fps) FPS="$2"; shift 2;;
    --duration) DURATION="$2"; shift 2;;
    --sample-rate) SAMPLE_RATE="$2"; shift 2;;
    --channels) CHANNELS="$2"; shift 2;;
    --scenes) SCENES="$2"; shift 2;;
    *) usage;;
  esac
done
[[ -n "$DB" && -n "$JOB_ID" && -n "$STORAGE_DIR" ]] || usage
mkdir -p "$TMPDIR"

scalar() { sqlite3 -noheader -batch "$DB" "$1" | tr -d '\r'; }
assert_count() {
  local label="$1" expected="$2" actual="$3"
  [[ "$actual" == "$expected" ]] || { echo "FAIL: $label: got $actual, want $expected" >&2; exit 2; }
  echo "OK: $label=$actual"
}
resolve_path() {
  local local_path="$1" storage_key="$2"
  [[ -n "$local_path" && -f "$local_path" ]] && { printf '%s\n' "$local_path"; return; }
  [[ "$storage_key" = /* && -f "$storage_key" ]] && { printf '%s\n' "$storage_key"; return; }
  [[ -n "$storage_key" && -f "$STORAGE_DIR/$storage_key" ]] && { printf '%s\n' "$STORAGE_DIR/$storage_key"; return; }
  return 1
}

case "$PROFILE" in
  small) WIDTH=1920; HEIGHT=1080; DURATION=2; SCENES=1;;
  production-shaped) WIDTH=1920; HEIGHT=1080; DURATION=30; SCENES=5;;
  *) echo "FAIL: unsupported profile $PROFILE" >&2; exit 3;;
esac

assert_count "Job SUCCEEDED" 1 "$(scalar "SELECT COUNT(*) FROM jobs WHERE job_id='$JOB_ID' AND status='SUCCEEDED';")"
assert_count "tasks for Job" 1 "$(scalar "SELECT COUNT(*) FROM tasks WHERE job_id='$JOB_ID';")"
TASK_ID="$(scalar "SELECT task_id FROM tasks WHERE job_id='$JOB_ID';")"
WINNER="$(scalar "SELECT COALESCE(winning_attempt_id,'') FROM tasks WHERE task_id='$TASK_ID';")"
[[ -n "$WINNER" ]] || { echo "FAIL: task has no winning_attempt_id" >&2; exit 2; }
assert_count "Task SUCCEEDED" 1 "$(scalar "SELECT COUNT(*) FROM tasks WHERE task_id='$TASK_ID' AND status='SUCCEEDED';")"
assert_count "winning TaskAttempt" 1 "$(scalar "SELECT COUNT(*) FROM task_attempts WHERE task_id='$TASK_ID' AND id='$WINNER' AND status='SUCCEEDED';")"
assert_count "non-terminal Tasks" 0 "$(scalar "SELECT COUNT(*) FROM tasks WHERE job_id='$JOB_ID' AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT');")"
assert_count "Artifact READY" 1 "$(scalar "SELECT COUNT(*) FROM artifacts WHERE job_id='$JOB_ID' AND status='READY';")"
assert_count "ArtifactUpload COMPLETED" 1 "$(scalar "SELECT COUNT(*) FROM artifact_uploads WHERE job_id='$JOB_ID' AND status='COMPLETED';")"
assert_count "Artifact STAGING/VERIFYING" 0 "$(scalar "SELECT COUNT(*) FROM artifacts WHERE job_id='$JOB_ID' AND status IN ('STAGING','VERIFYING');")"
assert_count "open ArtifactUploads" 0 "$(scalar "SELECT COUNT(*) FROM artifact_uploads WHERE job_id='$JOB_ID' AND status IN ('CREATED','UPLOADING','RECEIVED','FINALIZING');")"

IFS='|' read -r ARTIFACT LOCAL_PATH STORAGE_KEY SHA ART_ATTEMPT DECL_ATTEMPT UPLOAD_ID UPLOAD_ATTEMPT TA_ID UPLOAD_WORKER UPLOAD_LEASE TA_WORKER TA_LEASE < <(
  sqlite3 -noheader -separator '|' "$DB" "
    SELECT a.id, COALESCE(a.local_path,''), COALESCE(a.storage_key,''), COALESCE(a.sha256,''),
           COALESCE(CAST(a.attempt_id AS TEXT),''), COALESCE(d.attempt_id,''), au.upload_id,
           CAST(au.attempt_number AS TEXT), COALESCE(ta.id,''), au.worker_id, au.lease_id,
           COALESCE(ta.worker_id,''), COALESCE(ta.lease_id,'')
      FROM artifacts a LEFT JOIN task_output_declarations d ON d.artifact_id=a.id
      JOIN artifact_uploads au ON au.artifact_id=a.id AND au.status='COMPLETED'
      LEFT JOIN task_attempts ta ON ta.task_id='$TASK_ID' AND ta.attempt_number=au.attempt_number
     WHERE a.job_id='$JOB_ID' AND a.status='READY';"
)
[[ -n "$ARTIFACT" && -n "$UPLOAD_ID" ]] || { echo "FAIL: READY artifact identity row missing" >&2; exit 2; }
[[ -z "$DECL_ATTEMPT" || "$DECL_ATTEMPT" == "$UPLOAD_ATTEMPT" ]] || { echo "FAIL: declaration/upload attempt mismatch" >&2; exit 2; }
[[ "$TA_ID" == "$WINNER" && "$ART_ATTEMPT" == "$UPLOAD_ATTEMPT" ]] || { echo "FAIL: artifact does not identify the winning attempt" >&2; exit 2; }
[[ "$UPLOAD_WORKER" == "$TA_WORKER" && "$UPLOAD_LEASE" == "$TA_LEASE" ]] || { echo "FAIL: upload fence differs from TaskAttempt" >&2; exit 2; }
[[ "$SHA" =~ ^[0-9a-fA-F]{64}$ ]] || { echo "FAIL: invalid artifact SHA" >&2; exit 2; }
VIDEO_PATH="$(resolve_path "$LOCAL_PATH" "$STORAGE_KEY")" || { echo "FAIL: artifact path not found" >&2; exit 2; }
[[ -s "$VIDEO_PATH" ]] || { echo "FAIL: artifact is empty" >&2; exit 2; }
[[ "$(sha256sum "$VIDEO_PATH" | awk '{print $1}')" == "$SHA" ]] || { echo "FAIL: artifact SHA mismatch" >&2; exit 2; }
echo "OK: artifact identity=$ARTIFACT upload=$UPLOAD_ID attempt=$WINNER worker=$UPLOAD_WORKER path=$VIDEO_PATH"

EXPECTED_DELIVERIES="$(scalar "SELECT COUNT(*) FROM job_delivery_plans WHERE job_id='$JOB_ID' AND enabled=1;")"
assert_count "JobDelivery rows" "$EXPECTED_DELIVERIES" "$(scalar "SELECT COUNT(*) FROM job_deliveries WHERE artifact_id='$ARTIFACT';")"
assert_count "invalid JobDelivery states" 0 "$(scalar "SELECT COUNT(*) FROM job_deliveries WHERE artifact_id='$ARTIFACT' AND status NOT IN ('PENDING','SUCCEEDED');")"
python3 "$REPO_ROOT/scripts/ci/golden-e2e-verify-media.py" "$VIDEO_PATH" "$WIDTH" "$HEIGHT" "$FPS" "$DURATION" "$SAMPLE_RATE" "$CHANNELS"

if [[ "$PROFILE" == production-shaped ]]; then
  declare -a RGB=("255 0 0" "0 128 0" "0 0 255" "255 255 0" "255 0 255")
  declare -a NAMES=(red green blue yellow magenta)
  for ((i=0; i<SCENES; i++)); do
    T=$((i * DURATION / SCENES + 1)); FRAME="$TMPDIR/scene-frame-$i.raw"
    ffmpeg -hide_banner -loglevel error -ss "$T" -i "$VIDEO_PATH" -frames:v 1 -vf scale=1:1 -f rawvideo -pix_fmt rgb24 "$FRAME"
    python3 - "$FRAME" "${RGB[$i]}" "$T" <<'PY'
import sys
pixel=open(sys.argv[1], 'rb').read(3); want=tuple(map(int, sys.argv[2].split()))
if len(pixel)!=3: raise SystemExit('could not sample video frame')
got=tuple(pixel)
if sum(abs(a-b) for a,b in zip(got,want)) > 180: raise SystemExit(f'RGB={got}, want near {want} at t={sys.argv[3]}s')
PY
    echo "OK: scene $((i+1)) at ${T}s matches ${NAMES[$i]}"
  done
fi
echo "GOLDEN E2E VERIFICATION PASSED: job=$JOB_ID"
