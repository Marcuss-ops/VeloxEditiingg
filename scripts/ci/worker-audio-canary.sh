#!/usr/bin/env bash
# worker-audio-canary.sh — immutable worker-image audio canary.
#
# Usage:
#   scripts/ci/worker-audio-canary.sh ghcr.io/org/velox-worker@sha256:<digest>
#
# The canary deliberately runs the published image by digest, not a mutable
# tag and not a host-copied engine. It creates a 30-second source track,
# renders a 95-second video with loop=true and no duration_seconds, then
# verifies duration and that no ffmpeg process survives the render.
set -Eeuo pipefail

IMAGE="${1:-}"
EXPECTED_DURATION="${VELOX_AUDIO_CANARY_DURATION:-95}"
TOLERANCE="${VELOX_AUDIO_CANARY_TOLERANCE:-0.5}"
ENGINE="${VELOX_VIDEO_ENGINE_CPP_BIN:-/usr/local/bin/velox_video_engine}"
ENGINE_SHA_FILE="${VELOX_VIDEO_ENGINE_SHA_FILE:-/usr/local/share/velox/video-engine.sha256}"

fail() { printf 'worker-audio-canary: FAIL: %s\n' "$*" >&2; exit 1; }

[[ -n "$IMAGE" ]] || fail "usage: $0 ghcr.io/<owner>/<repo>@sha256:<64hex>"
[[ "$IMAGE" =~ @sha256:[0-9a-f]{64}$ ]] || fail "image must be pinned by a 64-hex sha256 digest"
command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v awk >/dev/null 2>&1 || fail "awk is required"

# Pulling the exact reference is intentional: the command never resolves a
# mutable tag, and Docker records the immutable image identity locally.
docker pull "$IMAGE" >/dev/null
IMAGE_ID="$(docker image inspect "$IMAGE" --format '{{.Id}}')"
[[ "$IMAGE_ID" =~ ^sha256:[0-9a-fA-F]{64}$ ]] || fail "invalid local image ID: $IMAGE_ID"
REPO_DIGESTS="$(docker image inspect "$IMAGE" --format '{{range .RepoDigests}}{{println .}}{{end}}')"
printf '%s\n' "$REPO_DIGESTS" | grep -Fqx -- "$IMAGE" \
  || fail "requested digest was not resolved exactly; available RepoDigests=${REPO_DIGESTS:-<none>}"

# The default image user is intentionally preserved: the canary must exercise
# the same non-root runtime permissions as production. The tmpfs keeps output
# writable while the tested engine remains the image's immutable binary.
CANARY_OUTPUT="$(docker run --rm --tmpfs /tmp:exec,size=1g \
  --entrypoint /bin/sh "$IMAGE" -s -- "$ENGINE" "$ENGINE_SHA_FILE" "$EXPECTED_DURATION" "$TOLERANCE" "$IMAGE_ID" <<'CONTAINER_SCRIPT'
set -eu
engine="$1"
sha_file="$2"
expected="$3"
tolerance="$4"
image_id="$5"
root=/tmp/velox-audio-canary
mkdir -p "$root"
test -x "$engine"
test -s "$sha_file"
expected_sha=$(awk 'length($1)==64 && $1 ~ /^[0-9a-fA-F]+$/ {print tolower($1); exit}' "$sha_file")
actual_sha=$(sha256sum "$engine" | awk '{print $1}')
test "$expected_sha" = "$actual_sha"

ffmpeg -y -hide_banner -loglevel error \
  -f lavfi -i 'sine=frequency=440:sample_rate=48000' \
  -t 30 -c:a aac "$root/music.m4a"
cat > "$root/plan.json" <<EOF
{"version":1,"job_id":"worker-audio-canary-95s","canvas":{"width":64,"height":64,"fps":5},"timeline":[{"source":{"type":"color","color_hex":"#112233"},"duration_seconds":$expected,"include_audio":false,"transform":{"scale_mode":"stretch","slow_zoom":false}}],"audio_tracks":[{"source_url":"$root/music.m4a","volume":1.0,"start_time_offset":0.0,"role":"background_music","loop":true}],"output_path":"$root/output.mp4"}
EOF
"$engine" --render --plan "$root/plan.json" >/tmp/engine.stdout 2>/tmp/engine.stderr
ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$root/output.mp4" > "$root/duration"
duration=$(cat "$root/duration")
awk -v actual="$duration" -v expected="$expected" -v tolerance="$tolerance" 'BEGIN { if (actual < expected-tolerance || actual > expected+tolerance) exit 1 }'
# The render is synchronous. Match the unique output path to avoid false
# positives from unrelated ffmpeg jobs on a shared host. Missing pgrep is a
# canary failure, never a silent pass.
command -v pgrep >/dev/null 2>&1
for attempt in $(seq 1 10); do
  if ! pgrep -af ffmpeg 2>/dev/null | grep -F "$root/output.mp4" >/dev/null; then
    break
  fi
  if [ "$attempt" -eq 10 ]; then
    echo "residual ffmpeg process:" >&2
    pgrep -af ffmpeg >&2 || true
    exit 1
  fi
  sleep 0.1
done
printf 'image_id=%s\nengine_sha256=%s\nduration_seconds=%s\n' "$image_id" "$actual_sha" "$duration"
CONTAINER_SCRIPT
)" || fail "container canary failed"

printf 'worker-audio-canary: PASS\n%s' "$CANARY_OUTPUT"
