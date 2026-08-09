#!/usr/bin/env bash
set -euo pipefail

# The legacy velox-bundler intentionally packages the worker source tree but
# omits the host-side canonical runtime files.  The production rollout uses
# those files after extracting the bundle, so make the contract explicit at
# the bundle boundary.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${1:-${ROOT_DIR}/DataServer/data/worker_downloads}"
ARCHIVE_PATH="${OUTPUT_DIR}/worker_code_linux_x86_64.zip"

[[ -f "$ARCHIVE_PATH" ]] || {
  echo "worker archive not found: $ARCHIVE_PATH" >&2
  exit 1
}

for required in \
  deploy/runtime/compose.yml \
  deploy/runtime/velox-worker.service
do
  [[ -f "${ROOT_DIR}/${required}" ]] || {
    echo "required worker runtime file missing: ${ROOT_DIR}/${required}" >&2
    exit 1
  }
done

(
  cd "$ROOT_DIR"
  zip -q "$ARCHIVE_PATH" \
    deploy/runtime/compose.yml \
    deploy/runtime/velox-worker.service
)

unzip -tq "$ARCHIVE_PATH" >/dev/null
for required in \
  deploy/runtime/compose.yml \
  deploy/runtime/velox-worker.service
do
  unzip -tq "$ARCHIVE_PATH" "$required" >/dev/null
done

echo "worker archive runtime files verified: $ARCHIVE_PATH"
