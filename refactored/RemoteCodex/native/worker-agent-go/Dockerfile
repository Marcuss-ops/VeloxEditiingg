# ============================================================================
# Velox Worker — Single-stage runtime with deterministic C++ engine build
# ============================================================================
# Build context: RemoteCodex/
#
# The C++ engine is compiled at container startup by worker-entrypoint.sh
# using an out-of-source build in /tmp/velox-video-engine-build.
# This guarantees the binary matches the container's GLIBC.
#
# Build:
#   cd RemoteCodex/native/worker-agent-go
#   make agent        # produce bin/velox-worker-agent
#   cd ../..
#   docker build -f native/worker-agent-go/Dockerfile -t velox-worker:latest .
#
# ============================================================================

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV VELOX_VIDEO_ENGINE_CPP_BIN=/usr/local/bin/velox_video_engine
ENV VELOX_VIDEO_ENGINE_BUILD_DIR=/tmp/velox-video-engine-build

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

# Copy Go worker agent binary (must be pre-built with 'make agent')
COPY native/worker-agent-go/bin/velox-worker-agent /usr/local/bin/velox-worker-agent

# Create non-root user for runtime (worker agent runs as velox)
RUN useradd -r -m -s /usr/sbin/nologin velox && \
    chown -R velox:velox /app /usr/local/bin/velox-worker-agent

# Health check via worker agent HTTP endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

ENTRYPOINT ["/usr/local/bin/worker-entrypoint.sh"]
CMD ["-master", "http://master:8000"]
