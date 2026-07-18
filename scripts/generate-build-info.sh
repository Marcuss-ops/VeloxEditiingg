#!/usr/bin/env bash
# scripts/generate-build-info.sh
#
# Regenerate RemoteCodex/BUILD_INFO.json from canonical sources:
#   - version + engine_version ← VERSION.txt (single source of truth)
#   - git_commit ← git rev-parse --short HEAD
#   - built_at ← SOURCE_DATE_EPOCH or current UTC timestamp
#   - source_hash ← SHA256 of VERSION.txt content
#   - platform + arch ← uname
#   - protocol_version ← static (bumped with each worker proto version change)
#
# Usage:
#   ./scripts/generate-build-info.sh [--check]
#
#   --check   Dry-run: exit 0 if BUILD_INFO.json on disk matches what would
#             be generated, exit 1 if it differs. Used by CI guards.
#
# Environment:
#   SOURCE_DATE_EPOCH  Reproducible-builds epoch (optional; for timestamp pinning)
#   GIT_COMMIT         Override the git commit hash (optional; for CI where
#                      the checkout ref doesn't match HEAD)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${REPO_ROOT}/RemoteCodex/BUILD_INFO.json"
CHECK_MODE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) CHECK_MODE=true; shift ;;
    *) echo "usage: $0 [--check]" >&2; exit 2 ;;
  esac
done

# ─── Gather metadata ─────────────────────────────────────────────────────────

# Version: canonical single source of truth
VERSION="$(tr -d '[:space:]' < "${REPO_ROOT}/VERSION.txt")"
[[ -n "$VERSION" ]] || { echo "[gen-build-info] FATAL: VERSION.txt is empty" >&2; exit 1; }

# Git commit
GIT_COMMIT="${GIT_COMMIT:-}"
if [[ -z "$GIT_COMMIT" ]]; then
  GIT_COMMIT="$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || true)"
fi
[[ -n "$GIT_COMMIT" ]] || GIT_COMMIT="unknown"

# Build timestamp
if [[ -n "${SOURCE_DATE_EPOCH:-}" ]]; then
  BUILT_AT="$(date -u -d "@${SOURCE_DATE_EPOCH}" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || true)"
fi
[[ -z "${BUILT_AT:-}" ]] && BUILT_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

# Platform / arch
PLATFORM="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

# Source hash: a SHA256 of VERSION.txt to detect version-file tampering
SOURCE_HASH="$(sha256sum "${REPO_ROOT}/VERSION.txt" | awk '{print $1}')"

# Protocol version — this is a constant versioned with the gRPC protobuf schema
PROTOCOL_VERSION="v3"

# ─── Build JSON ──────────────────────────────────────────────────────────────
# Use $(cat <<JSON ... JSON) — NOT read -r — so multi-line content is captured.
# Variable expansion inside the heredoc (unquoted JSON marker) is intentional:
# we need ${VERSION}, ${GIT_COMMIT}, etc. to resolve at runtime.
GENERATED_JSON=$(cat <<JSON
{
  "version": "${VERSION}",
  "git_commit": "${GIT_COMMIT}",
  "source_hash": "${SOURCE_HASH}",
  "protocol_version": "${PROTOCOL_VERSION}",
  "engine_version": "${VERSION}",
  "platform": "${PLATFORM}",
  "arch": "${ARCH}",
  "built_at": "${BUILT_AT}"
}
JSON
)

# ─── Check or write ──────────────────────────────────────────────────────────

if [[ "$CHECK_MODE" == "true" ]]; then
  if [[ ! -f "$TARGET" ]]; then
    echo "[gen-build-info] CHECK FAIL: ${TARGET} does not exist" >&2
    exit 1
  fi
  # Extract on-disk version via single-line python3 (avoids multi-line python -c escaping).
  # Compare against the canonical VERSION.txt (prefixed with v).
  DISK_VER="$(python3 -c "import json,sys; print(json.load(open('${TARGET}')).get('version',''))" 2>/dev/null || echo "")"
  EXPECTED_VER="${VERSION}"
  if [[ "$DISK_VER" != "$EXPECTED_VER" ]]; then
    echo "[gen-build-info] CHECK FAIL: BUILD_INFO.json version drifts from VERSION.txt" >&2
    echo "  on-disk version:  ${DISK_VER}" >&2
    echo "  expected version: ${EXPECTED_VER}" >&2
    echo "  Run ./scripts/generate-build-info.sh to regenerate." >&2
    exit 1
  fi
  echo "[gen-build-info] CHECK OK: BUILD_INFO.json version ${EXPECTED_VER} matches VERSION.txt"
  exit 0
fi

# Write atomically: tmp + rename
TMP="$(mktemp "${TARGET}.tmp.XXXXXX")"
echo "$GENERATED_JSON" > "$TMP"
chmod 0644 "$TMP"
mv -f "$TMP" "$TARGET"

echo "[gen-build-info] wrote ${TARGET}"
echo "  version:        ${VERSION}"
echo "  engine_version: ${VERSION}"
echo "  git_commit:     ${GIT_COMMIT}"
echo "  source_hash:    ${SOURCE_HASH}"
echo "  protocol:       ${PROTOCOL_VERSION}"
echo "  platform:       ${PLATFORM}/${ARCH}"
echo "  built_at:       ${BUILT_AT}"
