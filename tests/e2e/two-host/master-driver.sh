#!/usr/bin/env bash
# Computer A: stage inputs, submit the job, poll SQLite and run the canonical
# verifier.  The worker is intentionally never started by this script.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
MASTER_URL="${MASTER_URL:-http://127.0.0.1:8180}"
MASTER_PORT="${MASTER_PORT:-8180}"
ADMIN_TOKEN="${VELOX_ADMIN_TOKEN:?set VELOX_ADMIN_TOKEN}"
DB="${VELOX_DB_PATH:?set VELOX_DB_PATH}"
STAGING="${VELOX_STAGING_DIR:?set VELOX_STAGING_DIR}"
STORAGE="${VELOX_STORAGE_DIR:?set VELOX_STORAGE_DIR}"
TMPDIR="${TWO_HOST_TMPDIR:-/tmp/velox-two-host-master}"
PROFILE="${GOLDEN_PROFILE:-production-shaped}"
WORKER_ID="${WORKER_ID:-worker-pc-b-01}"
VERSION="$(tr -d '[:space:]' < "$ROOT/VERSION.txt")"

die() { echo "[two-host-master][FAIL] $*" >&2; exit 1; }
ok() { echo "[two-host-master][OK] $*"; }
mkdir -p "$STAGING" "$STORAGE" "$TMPDIR"
command -v sqlite3 >/dev/null || die "sqlite3 is required"
command -v ffmpeg >/dev/null || die "ffmpeg is required"

assert_master_does_not_render() {
  # ffmpeg is allowed above for fixture generation; the master must never
  # own the worker-side engine or a render invocation after that phase.
  if ps -eo args= | grep -Eq '[v]elox_video_engine|[f]fmpeg.*--render'; then
    die "local renderer detected on master — remote execution invariant violated"
  fi
}

curl -fsS "$MASTER_URL/health" >/dev/null || die "master is not healthy at $MASTER_URL"
workers="$(curl -fsS "$MASTER_URL/api/v1/workers")"
python3 - "$workers" "$WORKER_ID" <<'PY'
import json,sys
d=json.loads(sys.argv[1]); wanted=sys.argv[2]
match=next((w for w in d.get('workers',[]) if w.get('worker_id')==wanted),None)
if not match: raise SystemExit(f'worker {wanted} is not registered')
if match.get('status',match.get('state')) not in ('CONNECTED','READY','REGISTERED','active'):
    raise SystemExit(f'worker {wanted} is not connected: {match}')
PY
ok "worker $WORKER_ID is connected"

if [[ "$PROFILE" == production-shaped ]]; then DURATION=30; SCENES=5; COLORS=(red green blue yellow magenta); else DURATION=2; SCENES=1; COLORS=(teal); fi
for ((i=0;i<SCENES;i++)); do
  ffmpeg -hide_banner -loglevel error -y -f lavfi -i "color=c=${COLORS[$i]}:s=1920x1080:d=1" -frames:v 1 "$STAGING/scene$((i+1)).png"
done
ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i 'sine=frequency=440:sample_rate=48000' \
  -f lavfi -i 'sine=frequency=660:sample_rate=48000' \
  -filter_complex '[0:a][1:a]amerge=inputs=2[a]' -map '[a]' -ac 2 -ar 48000 -t "$DURATION" -c:a pcm_s16le "$STAGING/voiceover.wav"
ok "fixtures staged only on Computer A"
assert_master_does_not_render

sqlite3 "$DB" <<'SQL'
INSERT OR IGNORE INTO delivery_destinations
(destination_id,provider,external_destination_id,name,enabled,configuration_json,created_at,updated_at)
VALUES ('two-host-e2e-destination','social_gateway','two-host-e2e-external','Two-host E2E',1,'{}',STRFTIME('%Y-%m-%dT%H:%M:%fZ','now'),STRFTIME('%Y-%m-%dT%H:%M:%fZ','now'));
SQL
DEST="$(sqlite3 -noheader "$DB" "SELECT destination_id FROM delivery_destinations WHERE destination_id='two-host-e2e-destination';")"
SCENES_JSON='['
for ((i=0;i<SCENES;i++)); do
  if ((i>0)); then SCENES_JSON+=','; fi
  SCENES_JSON+="{\"text\":\"Scene $((i+1))\",\"image\":\"file://${STAGING}/scene$((i+1)).png\"}"
done
SCENES_JSON+=']'
python3 - "$SCENES_JSON" "$TMPDIR/job.json" "$STAGING/voiceover.wav" "$PROFILE" "$DEST" <<'PY'
import json,sys
scenes=json.loads(sys.argv[1])
json.dump({'video_name':'TwoHostGoldenE2E','script_text':f'Two-host {sys.argv[4]} scene contract.',
 'scenes_json':json.dumps(scenes),'voiceover_path':sys.argv[3],'render_video':True,'save_to_db':True,
 'channel_id':'two-host-e2e','audio_language_for_srt':'en',
 'delivery_plan':[{'destination_id':sys.argv[5],'retry_budget':1,'priority':0}]},open(sys.argv[2],'w'))
PY
SUBMIT="$(curl -fsS -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' --data-binary @"$TMPDIR/job.json" "$MASTER_URL/api/v1/script/generate-with-images")"
JOB_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])' <<<"$SUBMIT")"
ok "submitted job $JOB_ID"
assert_master_does_not_render

for _ in $(seq 1 "${TWO_HOST_TIMEOUT_POLLS:-90}"); do
  STATUS="$(sqlite3 -noheader "$DB" "SELECT status FROM jobs WHERE job_id='$JOB_ID';")"
  case "$STATUS" in
    SUCCEEDED) break;;
    FAILED|TIMEOUT|REJECTED|CANCELLED) die "job $JOB_ID reached $STATUS";;
  esac
  sleep "${TWO_HOST_POLL_SECONDS:-10}"
done
[[ "$(sqlite3 -noheader "$DB" "SELECT status FROM jobs WHERE job_id='$JOB_ID';")" == SUCCEEDED ]] || die "job did not reach SUCCEEDED"
assert_master_does_not_render

"$ROOT/scripts/e2e/verify-golden-job.sh" --db "$DB" --job-id "$JOB_ID" --storage-dir "$STORAGE" --tmpdir "$TMPDIR/verify" --profile "$PROFILE"
ok "two-host Golden E2E passed; artifact is on Computer A"
