#!/usr/bin/env bash
set -euo pipefail

ENGINE_SRC="${VELOX_VIDEO_ENGINE_SRC:-}"
OUT_BIN="${VELOX_VIDEO_ENGINE_OUT:-/usr/local/bin/velox_video_engine}"
BUILD_DIR="${VELOX_VIDEO_ENGINE_BUILD_DIR:-/tmp/velox-video-engine-build}"
METADATA_DIR="${VELOX_VIDEO_ENGINE_METADATA_DIR:-/usr/local/share/velox}"
ENGINE_SHA_FILE="${VELOX_VIDEO_ENGINE_SHA_FILE:-${METADATA_DIR}/video-engine.sha256}"

echo "== Velox C++ engine build =="
echo "Source: $ENGINE_SRC"
echo "Output: $OUT_BIN"
echo "Build dir: $BUILD_DIR"

resolve_engine_source() {
  local candidate
  for candidate in \
    "${ENGINE_SRC}" \
    /app/native/video-engine-cpp \
    /app/RemoteCodex/native/video-engine-cpp \
    /opt/velox/current/RemoteCodex/native/video-engine-cpp
  do
    [ -n "$candidate" ] || continue
    if [ -d "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

if [ -z "$ENGINE_SRC" ]; then
  ENGINE_SRC="$(resolve_engine_source || true)"
fi

if [ -z "$ENGINE_SRC" ] || [ ! -d "$ENGINE_SRC" ]; then
  echo "ERROR: C++ engine source directory not found. Tried /app/native/video-engine-cpp, /app/RemoteCodex/native/video-engine-cpp and /opt/velox/current/RemoteCodex/native/video-engine-cpp" >&2
  exit 10
fi

echo "Resolved source: $ENGINE_SRC"

# Clean stale artifacts from bundle (never build inside the synced dir)
rm -rf \
    "$ENGINE_SRC/build" \
    "$ENGINE_SRC/CMakeFiles" \
    "$ENGINE_SRC/CMakeCache.txt" \
    "$ENGINE_SRC/cmake_install.cmake"

# Keep the out-of-source build directory intact when it is backed by a
# BuildKit cache mount. CMake's dependency tracking plus ccache can then
# reuse unchanged objects between image builds; deleting this directory here
# turned the cache mount into an expensive no-op.
mkdir -p "$BUILD_DIR"

if [ -f "$ENGINE_SRC/CMakeLists.txt" ]; then
  echo "Detected CMake project"

  # Optional in-process LibAV backend (MediaProbe + packet mux). The CMake
  # default is OFF; the canonical builder opts in whenever the pinned dev
  # libraries are present so the produced engine keeps the zero-spawn probe
  # and copy-only packet pipeline. VELOX_VIDEO_ENGINE_LIBAV overrides the
  # pkg-config detection (set to OFF for a conservative ffprobe-only build).
  if [ -z "${VELOX_VIDEO_ENGINE_LIBAV:-}" ]; then
    if pkg-config --exists libavformat libavcodec libavutil 2>/dev/null; then
      VELOX_VIDEO_ENGINE_LIBAV=ON
    else
      VELOX_VIDEO_ENGINE_LIBAV=OFF
    fi
  fi
  echo "LibAV in-process backend: $VELOX_VIDEO_ENGINE_LIBAV"

  cmake \
      -S "$ENGINE_SRC" \
      -B "$BUILD_DIR" \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
      -DVELOX_ENABLE_LIBAV="${VELOX_VIDEO_ENGINE_LIBAV}"

  cmake --build "$BUILD_DIR" -j"$(nproc)"
  ctest --test-dir "$BUILD_DIR" --output-on-failure

  if [ -f "$BUILD_DIR/velox_video_engine" ]; then
    install -m 0755 "$BUILD_DIR/velox_video_engine" "$OUT_BIN"
  elif [ -f "$BUILD_DIR/video_engine" ]; then
    install -m 0755 "$BUILD_DIR/video_engine" "$OUT_BIN"
  else
    echo "ERROR: CMake build completed but engine binary was not found" >&2
    find "$BUILD_DIR" -maxdepth 3 -type f -perm -111 -print
    exit 11
  fi

elif [ -f "$ENGINE_SRC/Makefile" ] || [ -f "$ENGINE_SRC/makefile" ]; then
  echo "Detected Makefile project"
  make -C "$ENGINE_SRC" CC="ccache cc" CXX="ccache c++" -j"$(nproc)"

  if [ -f "$ENGINE_SRC/velox_video_engine" ]; then
    install -m 0755 "$ENGINE_SRC/velox_video_engine" "$OUT_BIN"
  elif [ -f "$ENGINE_SRC/video_engine" ]; then
    install -m 0755 "$ENGINE_SRC/video_engine" "$OUT_BIN"
  else
    echo "ERROR: Make build completed but engine binary was not found" >&2
    find "$ENGINE_SRC" -maxdepth 3 -type f -perm -111 -print
    exit 12
  fi

else
  echo "ERROR: no CMakeLists.txt or Makefile found in $ENGINE_SRC" >&2
  exit 13
fi

echo "== Built binary =="
ls -lh "$OUT_BIN"
file "$OUT_BIN" || true

# Validate the binary against the same builder image that produced it. This
# fails the image build before the artifact can cross into the runtime stage.
# The runtime stage repeats this check through worker-entrypoint.sh.
LDD_OUTPUT="$(ldd "$OUT_BIN" 2>&1 || true)"
printf '%s\n' "$LDD_OUTPUT"
if printf '%s\n' "$LDD_OUTPUT" | grep -q 'not found'; then
  echo "ERROR: unresolved shared dependencies detected for $OUT_BIN" >&2
  exit 14
fi

mkdir -p "$(dirname "$ENGINE_SHA_FILE")"
sha256sum "$OUT_BIN" > "$ENGINE_SHA_FILE"
echo "Engine SHA-256: $(awk '{print $1}' "$ENGINE_SHA_FILE")"
echo "Engine metadata: $ENGINE_SHA_FILE"
