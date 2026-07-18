#!/usr/bin/env bash
# size-benchmark: 42 - 42,2 KB
# Create a content-addressed local worker bundle identity.
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <work-dir> <worker-binary> <engine-binary>" >&2
  exit 2
fi

WORK_DIR=$1
WORKER_BIN=$2
ENGINE_BIN=$3

[[ -x "$WORKER_BIN" ]] || { echo "worker binary is not executable: $WORKER_BIN" >&2; exit 1; }
[[ -x "$ENGINE_BIN" ]] || { echo "engine binary is not executable: $ENGINE_BIN" >&2; exit 1; }
mkdir -p "$WORK_DIR"

HASH="$(
  sha256sum "$WORKER_BIN" "$ENGINE_BIN" |
    sha256sum |
    awk '{print $1}'
)"
[[ "$HASH" =~ ^[a-f0-9]{64}$ ]] || { echo "invalid generated bundle hash" >&2; exit 1; }
printf '%s\n' "$HASH" >"$WORK_DIR/BUNDLE_HASH.txt"
printf '%s\n' "$HASH"
