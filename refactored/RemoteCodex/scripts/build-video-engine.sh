#!/usr/bin/env bash
set -euo pipefail

ENGINE_SRC="${VELOX_VIDEO_ENGINE_SRC:-/app/native/video-engine-cpp}"
OUT_BIN="${VELOX_VIDEO_ENGINE_OUT:-/usr/local/bin/velox_video_engine}"
BUILD_DIR="${VELOX_VIDEO_ENGINE_BUILD_DIR:-/tmp/velox-video-engine-build}"

echo "== Velox C++ engine build =="
echo "Source: $ENGINE_SRC"
echo "Output: $OUT_BIN"
echo "Build dir: $BUILD_DIR"

if [ ! -d "$ENGINE_SRC" ]; then
  echo "ERROR: C++ engine source directory not found: $ENGINE_SRC" >&2
  exit 10
fi

# Clean stale artifacts from bundle (never build inside the synced dir)
rm -rf \
    "$ENGINE_SRC/build" \
    "$ENGINE_SRC/CMakeFiles" \
    "$ENGINE_SRC/CMakeCache.txt" \
    "$ENGINE_SRC/cmake_install.cmake"

# Ensure clean out-of-source build directory
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

if [ -f "$ENGINE_SRC/CMakeLists.txt" ]; then
  echo "Detected CMake project"
  cmake \
      -S "$ENGINE_SRC" \
      -B "$BUILD_DIR" \
      -DCMAKE_BUILD_TYPE=Release

  cmake --build "$BUILD_DIR" -j"$(nproc)"

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
  make -C "$ENGINE_SRC" clean || true
  make -C "$ENGINE_SRC" -j"$(nproc)"

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
ldd "$OUT_BIN" || true
