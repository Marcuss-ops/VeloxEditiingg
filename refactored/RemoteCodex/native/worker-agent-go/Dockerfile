# ============================================================================
# Velox Worker — Single-stage runtime with deterministic C++ engine build
# ============================================================================
# Build context: RemoteCodex/
#
# Build contract (must hold before invoking `docker build`):
#
#   1. The Go worker-agent binary MUST already exist at
#        <build-context>/native/worker-agent-go/bin/velox-worker-agent
#      (this Dockerfile does NOT compile the Go binary in-image).
#      The most reliable way to ensure this is
#        make -C native/worker-agent-go agent
#      which writes the binary at the path above. The release
#      workflow at .github/workflows/worker-image.yml already
#      executes this step before invoking docker/build-push-action,
#      so manual `docker build` invocations need to mirror it.
#
#   2. The C++ engine source tree MUST be present at
#        <build-context>/native/video-engine-cpp
#      It is built in-image (see `build-video-engine.sh`).
#
# Layer ordering (cache-friendly):
#   - OS deps first (rarely change).
#   - Build scripts next.
#   - C++ engine source (changes less often than the Go binary).
#   - Go BINARY LAST (the artifact that flips most often).
#   - Runtime user / healthcheck / entrypoint after the binary, so they
#     are not invalidated by every code change.
#
# Build:
#   cd <repo-root>
#   make -C RemoteCodex/native/worker-agent-go agent
#   docker buildx build \
#     -f RemoteCodex/native/worker-agent-go/Dockerfile \
#     -t velox-worker:latest \
#     RemoteCodex
#
# ============================================================================

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV VELOX_VIDEO_ENGINE_CPP_BIN=/usr/local/bin/velox_video_engine
ENV VELOX_VIDEO_ENGINE_BUILD_DIR=/tmp/velox-video-engine-build
ENV VELOX_HEALTH_PORT=8081

WORKDIR /app

# Build tools + runtime dependencies (ffmpeg + C++ runtime)
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    g++ \
    pkg-config \
    ca-certificates \
    file \
    ffmpeg \
    curl \
    libstdc++6 \
    libc6 \
    binutils \
    && rm -rf /var/lib/apt/lists/*

# Copy build scripts and C++ engine source
COPY scripts/build-video-engine.sh /usr/local/bin/build-video-engine.sh
COPY scripts/worker-entrypoint.sh /usr/local/bin/worker-entrypoint.sh
COPY native/video-engine-cpp /app/native/video-engine-cpp

RUN chmod +x \
    /usr/local/bin/build-video-engine.sh \
    /usr/local/bin/worker-entrypoint.sh

# Build the C++ engine into the image so recreated containers keep the binary.
RUN /usr/local/bin/build-video-engine.sh && \
    test -x /usr/local/bin/velox_video_engine

# Copy Go worker agent binary (MUST be pre-built with `make agent` before
# invoking `docker build` — the release workflow at
# .github/workflows/worker-image.yml runs `make agent` in the
# `Build worker-agent binary (pre-Docker step)` step. The Dockerfile
# itself cannot compile the Go worker without the velox-shared module,
# which lives OUTSIDE this build context (see
# RemoteCodex/native/worker-agent-go/Makefile and go.mod for the
# `replace velox-shared => ../../../../shared` syntax).
COPY native/worker-agent-go/bin/velox-worker-agent /usr/local/bin/velox-worker-agent

# Create non-root user for runtime (worker agent runs as velox)
RUN useradd -r -m -s /usr/sbin/nologin velox && \
    chown -R velox:velox /app /usr/local/bin/velox-worker-agent

# Health check: verify worker agent HTTP endpoint is responding
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
    CMD /bin/sh -c 'curl -sf "http://localhost:${VELOX_HEALTH_PORT:-8081}/health" || exit 1'

ENTRYPOINT ["/usr/local/bin/worker-entrypoint.sh"]
CMD ["-master", "http://master:8000"]
