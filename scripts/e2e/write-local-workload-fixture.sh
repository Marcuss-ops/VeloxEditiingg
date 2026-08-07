#!/usr/bin/env bash
# size-benchmark: 42 - 42,2 KB
# Write the canonical local real-workload payload used by pilot and E2E.
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 <output-json> <staging-dir> <destination-id> [audio-file]" >&2
  exit 2
fi

OUT=$1
STAGING=$2
DESTINATION=$3
AUDIO_FILE=${4:-silent.aac}

mkdir -p "$(dirname "$OUT")"
cat >"$OUT" <<JSON
{
  "video_name": "VeloxE2EWorkload",
  "script_text": "Deterministic local real-workload smoke test.",
  "scenes_json": "[{\"text\":\"E2E\",\"image\":\"file://${STAGING}/scene.png\"}]",
  "voiceover_path": "${STAGING}/${AUDIO_FILE}",
  "render_video": true,
  "save_to_db": true,
  "channel_id": "e2e-workload",
  "audio_language_for_srt": "en",
  "delivery_plan": [
    {
      "destination_id": "${DESTINATION}",
      "retry_budget": 1,
      "priority": 0
    }
  ]
}
JSON

[[ -s "$OUT" ]]
