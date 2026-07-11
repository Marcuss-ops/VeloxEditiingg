#!/usr/bin/env bash
# gen-proto.sh — regenerate velox-shared/controltransport/pb/*.pb.go from the
# canonical .proto sources in proto/.
#
# This script exists because the protobuf descriptors in
# shared/controltransport/pb had drifted from the .proto schemas, producing a
# `slice bounds out of range [:22] with capacity 21` panic at package init time
# (see PR-3.11). It is the single, reproducible way to land a regen diff.
#
# Usage (from repo root):
#   bash scripts/gen-proto.sh
#
# Toolchain requirements:
#   - protoc       >= 4.25.x   (binary on $PATH; e.g. `protoc --version`)
#   - protoc-gen-go        v1.36.x  (Go plugin on $PATH)
#   - protoc-gen-go-grpc   v1.5.x+  (Go gRPC plugin on $PATH; optional)
#
# The script fails loudly if any required tool is missing so a stale
# generated tree is never silently committed. To bootstrap missing Go
# plugins run, from the repo root:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
# `protoc` itself is installed via the OS package manager or the
# protobuf release zip at https://github.com/protocolbuffers/protobuf/releases.

set -uo pipefail

# Resolve repo root regardless of where the script is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "[gen-proto] repo root: $REPO_ROOT"

# --- Tool checks ------------------------------------------------------------
missing=()

if ! command -v protoc >/dev/null 2>&1; then
  missing+=("protoc (libprotoc >= 4.25.x — install via apt or https://github.com/protocolbuffers/protobuf/releases)")
fi

if ! command -v protoc-gen-go >/dev/null 2>&1; then
  missing+=("protoc-gen-go v1.36.x — run: go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11")
fi

if [ ${#missing[@]} -gt 0 ]; then
  echo "[gen-proto] FATAL: missing required tools:" >&2
  for tool in "${missing[@]}"; do
    echo "  - $tool" >&2
  done
  exit 1
fi

PROTOC_VERSION=$(protoc --version 2>&1 | awk '{print $2}')
echo "[gen-proto] protoc       = $PROTOC_VERSION"
echo "[gen-proto] protoc-gen-go = $(protoc-gen-go --version 2>/dev/null || echo '(no --version)')"

# --- Wipe stale regen output from prior broken invocations ------------------
# Older protoc invocations used --go_opt=paths=source_relative which wrote to
# nested mirror paths like shared/controltransport/pb/velox-shared/...  The
# `--go_opt=module=velox-shared` redirection below is the canonical fix.
rm -rf shared/controltransport/pb/velox-shared shared/velox-shared

# --- Regenerate the worker_control descriptor pair --------------------------
# --go_opt=module=velox-shared strips the `velox-shared` module prefix from
# the output path so the file lands at the canonical location
# shared/controltransport/pb/worker_control.pb.go (and ..._grpc.pb.go if the
# grpc plugin is installed).
GEN_FLAGS=(
  --proto_path=proto
  --go_out=shared
  --go_opt=module=velox-shared
  proto/velox/control/worker_control.proto
)

if command -v protoc-gen-go-grpc >/dev/null 2>&1; then
  GEN_FLAGS+=(
    --go-grpc_out=shared
    --go-grpc_opt=module=velox-shared
  )
else
  echo "[gen-proto] (skipped --go-grpc_out; protoc-gen-go-grpc not installed)"
fi

echo "[gen-proto] running: protoc ${GEN_FLAGS[*]}"
protoc "${GEN_FLAGS[@]}"
status=$?

if [ $status -ne 0 ]; then
  echo "[gen-proto] FATAL: protoc exited with status $status" >&2
  exit $status
fi

# --- Optional formatting pass ----------------------------------------------
# Normalize whitespace if gofmt is available. Pure aesthetic; safe to skip.
if command -v gofmt >/dev/null 2>&1; then
  gofmt -w shared/controltransport/pb/*.pb.go
fi

# --- Verification ------------------------------------------------------------
generated=$(ls -1 shared/controltransport/pb/*.pb.go 2>/dev/null)
if [ -z "$generated" ]; then
  echo "[gen-proto] FATAL: no .pb.go files written to shared/controltransport/pb/" >&2
  echo "Hint: confirm --go_opt=module=velox-shared matches the proto's" >&2
  echo "      `option go_package = \"velox-shared/controltransport/pb\";`." >&2
  exit 1
fi

echo "[gen-proto] regenerated:"
for f in $generated; do
  size=$(stat -c '%s' "$f")
  echo "  - $f ($size bytes)"
done

# Sanity check: surface if the canonical file is missing despite the regen
# succeeding — typically points to a go_package / module mismatch.
if [ ! -f shared/controltransport/pb/worker_control.pb.go ]; then
  echo "[gen-proto] FATAL: worker_control.pb.go did NOT land at the canonical path." >&2
  echo "Listed above are the actual landing paths. Investigate go_package" >&2
  echo "vs --go_opt=module mapping before committing." >&2
  exit 1
fi

echo "[gen-proto] OK: worker_control.pb.go landed at the canonical path."
echo "[gen-proto] next step: git diff shared/controltransport/pb/ to review changes."
