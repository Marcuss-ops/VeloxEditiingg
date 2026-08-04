#!/usr/bin/env bash
# verify-worker-image.sh — verify a canonical Velox worker image.
#
# Usage:
#   verify-worker-image.sh IMAGE [EXPECTED_ENGINE_SHA256] [EXPECTED_IMAGE_DIGEST]
#
# EXPECTED_IMAGE_DIGEST may be a full image ref digest
# (ghcr.io/...@sha256:<64hex>) or a local immutable image ID
# (sha256:<64hex>). For registry images, the script verifies the resolved
# RepoDigest. For locally-built images, it verifies the immutable image ID.
# The C++ engine is always checked inside the image; the host never supplies
# a binary and docker cp is intentionally not part of this contract.
set -Eeuo pipefail

IMAGE="${1:-}"
EXPECTED_ENGINE_SHA="${2:-${VELOX_EXPECTED_ENGINE_SHA256:-}}"
EXPECTED_IMAGE_DIGEST="${3:-${VELOX_EXPECTED_IMAGE_DIGEST:-}}"
ENGINE_PATH="${VELOX_VIDEO_ENGINE_CPP_BIN:-/usr/local/bin/velox_video_engine}"
ENGINE_SHA_FILE="${VELOX_VIDEO_ENGINE_SHA_FILE:-/usr/local/share/velox/video-engine.sha256}"

fail() {
  printf 'verify-worker-image: FAIL: %s\n' "$*" >&2
  exit 1
}

[[ -n "$IMAGE" ]] || fail "usage: $0 IMAGE [EXPECTED_ENGINE_SHA256] [EXPECTED_IMAGE_DIGEST]"
command -v docker >/dev/null 2>&1 || fail "docker is required"
docker image inspect "$IMAGE" >/dev/null 2>&1 || fail "image not found locally: $IMAGE"

IMAGE_ID="$(docker image inspect "$IMAGE" --format '{{.Id}}')"
[[ "$IMAGE_ID" =~ ^sha256:[0-9a-fA-F]{64}$ ]] || fail "invalid local image ID: $IMAGE_ID"

IMAGE_REPO_DIGESTS="$(docker image inspect "$IMAGE" --format '{{range .RepoDigests}}{{println .}}{{end}}')"
RESOLVED_REPO_DIGEST=""
if [[ -n "$EXPECTED_IMAGE_DIGEST" && "$EXPECTED_IMAGE_DIGEST" == *@sha256:* ]]; then
  # Compare the complete repository@digest string. This avoids accepting a
  # digest belonging to a different tag/repository when Docker reports more
  # than one RepoDigest for the local image.
  if grep -Fqx -- "$EXPECTED_IMAGE_DIGEST" <<<"$IMAGE_REPO_DIGESTS"; then
    RESOLVED_REPO_DIGEST="$EXPECTED_IMAGE_DIGEST"
  else
    fail "image repository digest mismatch: expected=$EXPECTED_IMAGE_DIGEST available=${IMAGE_REPO_DIGESTS:-<none>}"
  fi
elif [[ -n "$IMAGE_REPO_DIGESTS" ]]; then
  RESOLVED_REPO_DIGEST="$(printf '%s\n' "$IMAGE_REPO_DIGESTS" | head -1)"
fi
if [[ -n "$EXPECTED_IMAGE_DIGEST" && "$EXPECTED_IMAGE_DIGEST" != *@sha256:* ]]; then
  [[ "$EXPECTED_IMAGE_DIGEST" == "$IMAGE_ID" ]] || fail "image ID mismatch: expected=$EXPECTED_IMAGE_DIGEST actual=$IMAGE_ID"
fi

ENGINE_SHA="$(docker run --rm --entrypoint /bin/sh "$IMAGE" -c '
  set -eu
  test -x "$1"
  test -s "$2"
  ldd "$1" 2>&1 | tee /tmp/velox-engine-ldd.txt
  ! grep -q "not found" /tmp/velox-engine-ldd.txt
  expected=$(awk "NF {print \$1; exit}" "$2")
  actual=$(sha256sum "$1" | awk "{print \$1}")
  test "$expected" = "$actual"
  "$1" --help >/dev/null
  printf "%s\n" "$actual"
' sh "$ENGINE_PATH" "$ENGINE_SHA_FILE" | tail -1)"
[[ "$ENGINE_SHA" =~ ^[0-9a-fA-F]{64}$ ]] || fail "engine verification did not return a SHA-256 digest"

if [[ -n "$EXPECTED_ENGINE_SHA" ]]; then
  [[ "$ENGINE_SHA" == "$EXPECTED_ENGINE_SHA" ]] || fail "engine SHA mismatch: expected=$EXPECTED_ENGINE_SHA actual=$ENGINE_SHA"
fi

printf 'verify-worker-image: PASS\n'
printf '  image=%s\n' "$IMAGE"
printf '  image_id=%s\n' "$IMAGE_ID"
printf '  repo_digest=%s\n' "${RESOLVED_REPO_DIGEST:-<local-only>}"
printf '  engine_path=%s\n' "$ENGINE_PATH"
printf '  engine_sha256=%s\n' "$ENGINE_SHA"
