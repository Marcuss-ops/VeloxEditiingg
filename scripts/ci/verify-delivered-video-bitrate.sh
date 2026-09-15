#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 OUTPUT.mp4 [MAX_VIDEO_BITRATE_BPS]" >&2
  exit 2
fi

output=$1
max_video_bps=${2:-2250000}
if [[ ! -f "$output" ]]; then
  echo "output does not exist: $output" >&2
  exit 1
fi
if ! command -v ffprobe >/dev/null 2>&1; then
  echo "ffprobe is required" >&2
  exit 1
fi

duration=$(ffprobe -v error -select_streams v:0 -show_entries stream=duration -of default=nw=1:nk=1 "$output")
video_bps=$(ffprobe -v error -select_streams v:0 -show_entries stream=bit_rate -of default=nw=1:nk=1 "$output")
audio_bps=$(ffprobe -v error -select_streams a:0 -show_entries stream=bit_rate -of default=nw=1:nk=1 "$output" || true)
size_bytes=$(stat -c '%s' "$output")

[[ -n "$duration" && -n "$video_bps" ]] || {
  echo "ffprobe could not read video duration/bitrate for $output" >&2
  exit 1
}

awk -v got="$video_bps" -v max="$max_video_bps" 'BEGIN { if (got <= 0 || got > max) exit 1 }' || {
  echo "video bitrate ${video_bps} bps exceeds cap ${max_video_bps} bps" >&2
  exit 1
}

printf '{"path":"%s","duration_seconds":%s,"size_bytes":%s,"video_bitrate_bps":%s,"audio_bitrate_bps":%s,"max_video_bitrate_bps":%s}\n' \
  "$output" "$duration" "$size_bytes" "$video_bps" "${audio_bps:-0}" "$max_video_bps"
