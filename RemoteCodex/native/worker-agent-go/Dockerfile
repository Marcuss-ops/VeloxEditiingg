# ============================================================================
# Velox Worker — Multistage: cpp-builder → slim runtime.
# ============================================================================
# The runtime image is a slim Debian WITHOUT compilers / cmake / ld /
# binutils, while the C++ engine is build-time-tested against the SAME
# `debian:bookworm-slim` base, so libstdc++ + glibc symbol parity is
# enforced by construction rather than a runtime `strings` scrape.
#
# Build context: <repo-root>/RemoteCodex
#
# Build contract (MUST hold before invoking `docker build`):
#
#   1. The Go worker-agent binary MUST already exist at
#        <build-context>/native/worker-agent-go/bin/velox-worker-agent
#      The Dockerfile itself does NOT recompile the Go worker because
#      velox-shared is referenced via
#        replace velox-shared v0.0.0 => ../../../shared
#      The path resolves correctly only from the agent module's own
#      working directory, NOT from a Docker build context. The release
#      flow at .github/workflows/worker-image.yml runs
#      `make -C native/worker-agent-go agent` BEFORE docker build, and
#      `make verify` (scripts/ci/verify.sh) mirrors that step. Manual
#      `docker build` invocations need to do the same.
#
#   2. The C++ engine source tree MUST be present at
#        <build-context>/native/video-engine-cpp
#      It is compiled in stage `cpp-builder` and copied into the slim
#      runtime stage by a single `COPY --from=cpp-builder` line. The
#      runtime image CANNOT rebuild the engine: cmake / g++ / pkg-config
#      are NOT installed there on purpose.
#
# Image layer order is intentional for cache locality:
#   * OS + runtime deps (stage 2): slowest-changing layer.
#   * C++ engine binary (stage-2 COPY --from): medium-frequency.
#   * Go binary COPY (stage 2): most frequent layer, applied LAST so
#     a Go-only diff does not invalidate the C++ engine cache.
#   * useradd + USER + HEALTHCHECK + ENTRYPOINT: below all binary COPYs
#     so they are not invalidated by Go binary changes.
#
# Runtime identity: container runs as the unprivileged `velox` user
# with a deterministic UID/GID of 10001 (configurable via the
# VELOX_UID / VELOX_GID ARGs at build time). Operators do NOT need a
# blanket `chown -R velox /opt/velox` — only the writable subtrees
# must be owned by this UID:
#   * {{ velox_dir }}/cache                       (XDG_CACHE_HOME)
#   * {{ velox_dir }}/current/RemoteCodex/assets_cache (writable overlay)
#   * {{ velox_worker_runtime_dir }}              (worker_config.json)
# These are configured by
#   DataServer/data/ansible/playbooks/tasks/systemd_setup.yml.
# /app itself is mounted rw at runtime because the worker writes
# worker_state.json + cache + GC temp under workDir; the in-image
# /app at build time is empty and intentionally left root-owned.
#
# Build:
#   make -C RemoteCodex/native/worker-agent-go agent
#   docker buildx build \
#     -f RemoteCodex/native/worker-agent-go/Dockerfile \
#     -t velox-worker:latest \
#     RemoteCodex
# ============================================================================

# ─── Stage 1: C++ engine builder ────────────────────────────────────────────
FROM debian:bookworm-slim AS cpp-builder

ENV DEBIAN_FRONTEND=noninteractive

# Build deps only (cmake, g++, build-essential). NOT copied to runtime.
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        g++ \
        pkg-config \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/build-video-engine.sh /usr/local/bin/build-video-engine.sh
COPY native/video-engine-cpp /app/native/video-engine-cpp

# Compile engine into /usr/local/bin/velox_video_engine. `test -x`
# hard-fails if the build did not produce the expected binary.
RUN /usr/local/bin/build-video-engine.sh && \
    test -x /usr/local/bin/velox_video_engine


# ─── Stage 2: runtime — NO compilers, NO cmake, NO binutils ─────────────────
FROM debian:bookworm-slim

# Pinned non-root identity. Operators chown 10001:10001 on the
# writable host bind-mounts (cache, assets_cache, runtime_dir) so
# files written by the container remain owned by this UID. Operators
# MUST NOT use uid 10001 on the host itself — that user would acquire
# write access to those dirs (acceptable tradeoff for a fixed UID
# compared to scraping `docker inspect` per host).
ARG VELOX_UID=10001
ARG VELOX_GID=10001

ENV DEBIAN_FRONTEND=noninteractive
ENV VELOX_VIDEO_ENGINE_CPP_BIN=/usr/local/bin/velox_video_engine
ENV VELOX_HEALTH_PORT=8081

WORKDIR /app

# Runtime deps ONLY. Nothing build-time belongs here.
#   * ca-certificates: outbound TLS (master handshake + YouTube API).
#   * ffmpeg:          runtime dep of the video engine.
#   * curl:            HEALTHCHECK below.
#   * libstdc++6:      C++ engine is dynamically linked against libstdc++
#                      (built by g++ in stage 1). Without this, the engine
#                      fails to start with "libstdc++.so.6: cannot open
#                      shared object file". Apt pulls libgcc-s1 in
#                      transitively (libstdc++6 depends on libgcc-s1 >= 3.0).
#   * libc-bin:        entrypoint runs `ldd` / `getconf GNU_LIBC_VERSION`
#                      for engine validation; `debian:bookworm-slim` does
#                      NOT ship these utilities by default (libc-bin is
#                      explicit-excluded from the slim variant). Apt pulls
#                      libc6 in transitively.
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
        ca-certificates \
        ffmpeg \
        curl \
        libstdc++6 \
        libc-bin \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built C++ engine from stage 1 (compilers are NOT in the
# runtime image, so the engine cannot be rebuilt here).
COPY --from=cpp-builder /usr/local/bin/velox_video_engine /usr/local/bin/velox_video_engine

# Copy pre-built Go worker-agent binary (MUST already exist; see contract above).
COPY native/worker-agent-go/bin/velox-worker-agent /usr/local/bin/velox-worker-agent

# Entrypoint script (validates + execs; it does NOT rebuild the engine).
COPY scripts/worker-entrypoint.sh /usr/local/bin/worker-entrypoint.sh

# Create non-root runtime user with UID pinned at ${VELOX_UID} so
# operators can chown host bind-mounts to a known UID without
# scraping `docker inspect`. /app is intentionally NOT chowned here
# because it is overlaid at runtime by systemd `ExecStart` `-v` mounts
# (see systemd_setup.yml) — at build time /app is empty. The three
# binaries are chowned individually (no `chown -R`) because they live
# in /usr/local/bin/ and must only be readable+executable by velox
# (mode 0755).
RUN groupadd --system --gid "${VELOX_GID}" velox && \
    useradd --system \
        --uid "${VELOX_UID}" \
        --gid "${VELOX_GID}" \
        --create-home \
        --home-dir /home/velox \
        --shell /usr/sbin/nologin \
        velox && \
    chown "${VELOX_UID}:${VELOX_GID}" \
        /usr/local/bin/velox_video_engine \
        /usr/local/bin/velox-worker-agent \
        /usr/local/bin/worker-entrypoint.sh && \
    chmod 0755 \
        /usr/local/bin/velox_video_engine \
        /usr/local/bin/velox-worker-agent \
        /usr/local/bin/worker-entrypoint.sh

# HOME + XDG_CACHE_HOME point into /home/velox so third-party libs
# (curl/ffmpeg TLS cache, etc.) that look for $HOME/.cache or
# $XDG_CACHE_HOME drop files into a path the container user owns.
# (velox-worker-agent itself does not strictly depend on HOME.)
ENV HOME=/home/velox
ENV XDG_CACHE_HOME=/home/velox/.cache

# Drop privileges. From here on, no setuid-root child can be spawned.
USER velox

# Health probe — see worker-entrypoint.sh for the GLIBC parity rationale.
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
    CMD /bin/sh -c 'curl -sf "http://localhost:${VELOX_HEALTH_PORT:-8081}/health" || exit 1'

ENTRYPOINT ["/usr/local/bin/worker-entrypoint.sh"]
CMD ["-master", "http://master:8000"]
