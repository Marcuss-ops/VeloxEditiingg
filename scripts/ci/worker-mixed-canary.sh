#!/usr/bin/env bash
# worker-mixed-canary.sh — immutable worker-image copy-only mixed-assembly canary.
#
# Usage:
#   scripts/ci/worker-mixed-canary.sh ghcr.io/org/velox-worker@sha256:<digest>
#
# This is the production invariant gate for the copy-only mixed/concat path:
#
#   successful Velox assembly MUST satisfy
#     frames_encoded == 0
#     encode_passes == 0
#
# The mixed renderer (RenderEngine::renderMixed) is copy-only: every segment
# either resolves to PACKET_COPY or REJECT, and never re-encodes. If a future
# change accidentally reintroduces libx264 into the mixed path, this canary
# must fail. It renders:
#
#   1. an all-canonical mixed plan  → SUCCEEDED with sidecar frames==0 and
#                                      encode_passes==0 (concat_mode
#                                      mixed_packet);
#   2. a mixed plan with one non-canonical 720p scene → the JOB fails
#      deterministically (segment_execution_rejected) with zero frames
#      encoded, while the engine process exits cleanly (rc=1, not a crash).
#
# Like worker-audio-canary.sh, the canary runs the published image by digest,
# not a mutable tag and not a host-copied engine.
set -Eeuo pipefail

IMAGE="${1:-}"
ENGINE="${VELOX_VIDEO_ENGINE_CPP_BIN:-/usr/local/bin/velox_video_engine}"
ENGINE_SHA_FILE="${VELOX_VIDEO_ENGINE_SHA_FILE:-/usr/local/share/velox/video-engine.sha256}"

fail() { printf 'worker-mixed-canary: FAIL: %s\n' "$*" >&2; exit 1; }

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
  --entrypoint /bin/sh "$IMAGE" -s -- "$ENGINE" "$ENGINE_SHA_FILE" "$IMAGE_ID" <<'CONTAINER_SCRIPT'
set -eu
engine="$1"
sha_file="$2"
image_id="$3"
root=/tmp/velox-mixed-canary
mkdir -p "$root"
test -x "$engine"
test -s "$sha_file"
expected_sha=$(awk 'length($1)==64 && $1 ~ /^[0-9a-fA-F]+$/ {print tolower($1); exit}' "$sha_file")
actual_sha=$(sha256sum "$engine" | awk '{print $1}')
test "$expected_sha" = "$actual_sha"

# Canonical fixtures: H.264 High level 4.0 yuv420p 30fps. The canonical source
# is 1080p (matches the canonical output profile); the non-canonical source is
# 720p and differs ONLY in resolution, so the resolver rejects it with
# "media signature mismatch: width" rather than a profile/level mismatch.
ffmpeg -y -hide_banner -loglevel error \
  -f lavfi -i 'testsrc=size=1920x1080:rate=30:duration=1.0' \
  -an -c:v libx264 -preset medium -profile:v high -level:v 4.0 \
  -pix_fmt yuv420p -r 30 "$root/canonical.mp4"
ffmpeg -y -hide_banner -loglevel error \
  -f lavfi -i 'testsrc=size=1280x720:rate=30:duration=1.0' \
  -an -c:v libx264 -preset medium -profile:v high -level:v 4.0 \
  -pix_fmt yuv420p -r 30 "$root/noncanonical.mp4"

# ── 1. All-canonical mixed plan: must SUCCEED with zero encode work. ─────
cat > "$root/plan-ok.json" <<EOF
{"version":1,"job_id":"worker-mixed-canary-ok","width":1920,"height":1080,"fps":30,"mixed":true,"timeline":[{"scene_id":"s0","duration_seconds":0.3,"include_audio":false,"scale_mode":"cover","slow_zoom":false,"source":{"type":"video","url":"$root/canonical.mp4","cache_key":""}},{"scene_id":"s1","duration_seconds":0.3,"include_audio":false,"scale_mode":"cover","slow_zoom":false,"source":{"type":"video","url":"$root/canonical.mp4","cache_key":""}},{"scene_id":"s2","duration_seconds":0.3,"include_audio":false,"scale_mode":"cover","slow_zoom":false,"source":{"type":"video","url":"$root/canonical.mp4","cache_key":""}}],"output_path":"$root/out-ok.mp4"}
EOF
"$engine" --render --plan "$root/plan-ok.json" >"$root/ok.stdout" 2>"$root/ok.stderr"
test -f "$root/out-ok.mp4"
test -f "$root/out-ok.mp4.progress.json"
# ── Copy-only invariant: a successful mixed assembly encodes nothing. ───
# The sidecar is single-line JSON; portable grep/cut (no gawk, no jq in the
# worker image) reads the three canonical fields.
frames=$(grep -o '"frames":[0-9]*' "$root/out-ok.mp4.progress.json" | head -1 | cut -d: -f2)
passes=$(grep -o '"encode_passes":[0-9]*' "$root/out-ok.mp4.progress.json" | head -1 | cut -d: -f2)
concat_mode=$(grep -o '"concat_mode":"[^"]*"' "$root/out-ok.mp4.progress.json" | head -1 | cut -d: -f2 | tr -d '"')
test "$frames" = "0"   || { echo "invariant violated: frames_encoded=$frames (want 0)" >&2; exit 1; }
test "$passes" = "0"   || { echo "invariant violated: encode_passes=$passes (want 0)" >&2; exit 1; }
test "$concat_mode" = "mixed_packet" || { echo "unexpected concat_mode=$concat_mode (want mixed_packet)" >&2; exit 1; }

# ── 2. Mixed plan with one non-canonical scene: the JOB fails, the worker
#    process stays alive. The engine must reject deterministically with
#    segment_execution_rejected (rc=1), not crash (rc>=128) and not encode. ─
cat > "$root/plan-reject.json" <<EOF
{"version":1,"job_id":"worker-mixed-canary-reject","width":1920,"height":1080,"fps":30,"mixed":true,"timeline":[{"scene_id":"s0","duration_seconds":0.3,"include_audio":false,"scale_mode":"cover","slow_zoom":false,"source":{"type":"video","url":"$root/canonical.mp4","cache_key":""}},{"scene_id":"s1","duration_seconds":0.3,"include_audio":false,"scale_mode":"cover","slow_zoom":false,"source":{"type":"video","url":"$root/noncanonical.mp4","cache_key":""}}],"output_path":"$root/out-reject.mp4"}
EOF
set +e
"$engine" --render --plan "$root/plan-reject.json" >"$root/reject.stdout" 2>"$root/reject.stderr"
reject_rc=$?
set -e
test "$reject_rc" = "1" || { echo "reject render exited rc=$reject_rc (want 1 = deterministic job failure, not a crash)" >&2; cat "$root/reject.stdout" >&2 || true; exit 1; }
test ! -e "$root/out-reject.mp4"
grep -q "segment_execution_rejected" "$root/reject.stdout"
reject_frames=$(grep -o '"frames":[0-9]*' "$root/out-reject.mp4.progress.json" | head -1 | cut -d: -f2)
test "$reject_frames" = "0" || { echo "rejected segment was re-encoded: frames=$reject_frames" >&2; exit 1; }

printf 'image_id=%s\nengine_sha256=%s\ncopy_only_invariant=frames_encoded=0 encode_passes=0 (mixed_packet)\nreject=segment_execution_rejected rc=1 frames_encoded=0\n' "$image_id" "$actual_sha"
CONTAINER_SCRIPT
)" || fail "container canary failed"

printf 'worker-mixed-canary: PASS\n%s' "$CANARY_OUTPUT"
