# ============================================================================
# Velox Worker — Self-sufficient multistage build: cpp-builder → go-builder → slim runtime.
# ============================================================================
# PR-3: the worker Dockerfile is now buildable from a fresh checkout with
# nothing more than `docker build` — no pre-build `make agent` step required.
# Both the C++ engine and the Go worker-agent binary are produced during
# the image build, inside their dedicated builder stages.
#
# Build context: <repo-root>
#   The build context is now the repo root (NOT RemoteCodex/, NOT
#   native/worker-agent-go/) because worker-agent-go's go.mod declares
#     replace velox-shared v0.0.0 => ../../../shared
#   which resolves to <repo-root>/shared only when the build module sits
#   at <repo-root>/RemoteCodex/native/worker-agent-go/ inside the builder's
#   filesystem. The go.work workspace-level replace is NOT taken into
#   account by `go build` from inside a go-builder stage because the
#   builder has no ambient go.work lookup path.
#
# The go-builder stage delegates the worker-agent build to the project's
# own Makefile (`make BIN_DIR=/out agent evict`) so that:
#   1. VERSION.txt is enforced (Makefile FATALs when missing — the
#      Dockerfile would otherwise silently fall back to "dev").
#   2. Reproducible Builds BUILD_TIME chain (SOURCE_DATE_EPOCH → git
#      commit timestamp → "unknown") lives in one place (Makefile).
#   3. LDFLAGS pattern (-X main.Version=… -X main.BuildTime=…) is the
#      same as on host builds — no drift risk.
# No outer build-args are required; the Makefile is the single source of
# truth for VERSION + BUILD_TIME resolution.
#
# Rendered builts:
#   `docker build -f RemoteCodex/native/worker-agent-go/Dockerfile .`
# produces the same image as the legacy flow did (pre-Docker `make agent`
# + bin/ copy), but without the pre-step requirement.
#
# Image layer order is intentional for cache locality:
#   * OS + runtime deps (stage 3): slowest-changing layer.
#   * C++ engine binary (stage-3 COPY --from=cpp-builder): medium-frequency.
#   * Go worker binary (stage-3 COPY --from=go-builder): most frequent layer,
#     applied AFTER the C++ engine COPY so a Go-only diff does NOT invalidate
#     the C++ engine build cache.
#   * useradd + USER + HEALTHCHECK + ENTRYPOINT: below all binary COPYs so
#     they are not invalidated by binary changes.
#
# Runtime identity: container runs as the unprivileged `velox` user with
# a deterministic UID/GID of 10001 (configurable via the VELOX_UID /
# VELOX_GID ARGs at build time). Operators do NOT need a blanket
# `chown -R velox /opt/velox` — only the writable subtrees must be owned
# by this UID (cache, assets_cache, runtime_dir). These are configured by
# DataServer/data/ansible/playbooks/tasks/systemd_setup.yml.

# ─── Stage 1: C++ engine builder ─────────────────────────────────────────────
# Keep one explicit base reference for both builder and runtime. Release
# automation may override it with a fully pinned @sha256 digest; using the
# same reference in both stages prevents an ABI drift between stages.
ARG VELOX_DEBIAN_BASE=debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818
ARG SOURCE_DATE_EPOCH=0
FROM ${VELOX_DEBIAN_BASE} AS cpp-builder
ARG SOURCE_DATE_EPOCH
ENV SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}

ENV DEBIAN_FRONTEND=noninteractive

# Build deps only (cmake, g++, build-essential). NOT copied to runtime.
# Pin the Debian package set as well as the base image. The pinned
# bookworm snapshot currently resolves these exact versions; an unexpected
# repository update must fail the build instead of silently changing the
# compiler/FFmpeg ABI and engine hash.
ARG BUILD_ESSENTIAL_VERSION=12.9
ARG CMAKE_VERSION=3.25.1-1
ARG GXX_VERSION=4:12.2.0-3
ARG PKG_CONFIG_VERSION=1.8.1-1
ARG CA_CERTIFICATES_VERSION=20230311+deb12u1
ARG FFMPEG_VERSION=7:5.1.9-0+deb12u1
ARG PROCPS_VERSION=2:4.0.2-3
ARG LIBC_BIN_VERSION=2.36-9+deb12u14
ARG CCACHE_VERSION=4.8.3-1
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
        build-essential=${BUILD_ESSENTIAL_VERSION} \
        cmake=${CMAKE_VERSION} \
        g++=${GXX_VERSION} \
        ccache=${CCACHE_VERSION} \
        pkg-config=${PKG_CONFIG_VERSION} \
        ca-certificates=${CA_CERTIFICATES_VERSION} \
        ffmpeg=${FFMPEG_VERSION} \
        libavformat-dev=${FFMPEG_VERSION} \
        libavcodec-dev=${FFMPEG_VERSION} \
        libavutil-dev=${FFMPEG_VERSION} \
        libswscale-dev=${FFMPEG_VERSION} \
        procps=${PROCPS_VERSION} \
        libc-bin=${LIBC_BIN_VERSION} \
    && rm -rf /var/lib/apt/lists/*

# Build context is the repo root — paths MUST reflect that layout.
COPY RemoteCodex/scripts/build-video-engine.sh /usr/local/bin/build-video-engine.sh
COPY RemoteCodex/native/video-engine-cpp /app/native/video-engine-cpp

# Compile and test the engine into /usr/local/bin/velox_video_engine.
# The builder also writes the canonical binary hash consumed by the runtime
# verification gate; no host-built or docker-copied binary is accepted.
RUN --mount=type=cache,target=/root/.ccache \
    --mount=type=cache,target=/tmp/velox-video-engine-build,sharing=locked \
    ccache -z 2>/dev/null || true; \
    /usr/local/bin/build-video-engine.sh && \
    test -x /usr/local/bin/velox_video_engine && \
    test -s /usr/local/share/velox/video-engine.sha256


# ─── Stage 2: Go worker-builder ────────────────────────────────────────────
# PR-3: this stage replaces the pre-built bin/velox-worker-agent contract.
# It is independent of the C++ engine build, so BuildKit can execute stages
# 1 and 2 in parallel when cache permits. The velox-shared replace CLI
# directive in worker-agent-go/go.mod drives the resolution to the in-tree
# shared/ tree; no ambient go.work lookup is required.
#
# The actual `go build` is delegated to the project's Makefile so the
# LDFLAGS pattern + VERSION.txt enforcement + Reproducible Builds chain
# are uniformly governed by Makefile.source-of-truth — NOT duplicated in
# Dockerfile. See RemoteCodex/native/worker-agent-go/Makefile for the
# contract (VERSION_FILE, BUILD_TIME fallback, LDFLAGS).
FROM golang:1.25.8-bookworm@sha256:4557cf171e3cdf5053a298d5171b1a5f5734d920260c25f22c79e94760eb2084 AS go-builder

ENV DEBIAN_FRONTEND=noninteractive

# golang:1.25.8-bookworm does NOT ship `make`. PR-3 requires it because
# the go-builder delegates to the host Makefile. Keep the dep set
# minimal: only what's needed to run the Makefile's agent and evict targets.
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
        make \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Workspace layout inside the builder. /src is a synthetic mount-anchor;
# `WORKDIR` is a per-stage baseline so relative paths in `go build` (e.g.
# the `replace ../../../shared` directive) resolve correctly.
WORKDIR /src

# Layer-ordering rationale:
#   * VERSION.txt first (slowest-changing; FATAL at Makefile parse time
#     when missing; changes invalidate the agent build AND the published
#     bundle-version assertion).
#   * shared/ next (slowest-changing, large; the `replace velox-shared`
#     points here).
#   * go.mod + go.sum third (cache key for go mod download).
#   * source code last (most frequent).
# This ordering lets a Go-only diff avoid re-downloading dependencies.
COPY VERSION.txt /src/VERSION.txt
COPY shared/ /src/shared/
COPY RemoteCodex/native/worker-agent-go/go.mod \
     RemoteCodex/native/worker-agent-go/go.sum \
     /src/RemoteCodex/native/worker-agent-go/

# `go mod download` reads go.mod + go.sum only — no source files yet.
# Backing the module cache with BuildKit cache mount so subsequent builds
# reuse the same /go/pkg/mod layers (Huge speedup once the dep graph lands).
WORKDIR /src/RemoteCodex/native/worker-agent-go
RUN --mount=type=cache,id=velox-worker-go-mod,target=/go/pkg/mod,sharing=locked \
    go mod download

# Now copy the full module. The Makefile's `make agent evict` targets then
# take over from BIN_DIR=/out: `agent` bakes main.Version + main.BuildTime
# via the Makefile's LDFLAGS contract (VERSION.txt + Reproducible-Builds
# chain), while `evict` builds the cache-admin CLI without version stamping.
COPY RemoteCodex/native/worker-agent-go/ /src/RemoteCodex/native/worker-agent-go/

# The Makefile is the single source of truth for the LDFLAGS pattern,
# so the image's main.Version + main.BuildTime match the host `make agent`
# output byte-for-byte (modulo /go/pkg/mod cache contents). /out is then
# COPYed in stage 3.
RUN --mount=type=cache,id=velox-worker-go-mod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=velox-worker-go-build,target=/root/.cache/go-build,sharing=locked \
    make BIN_DIR=/out agent evict && \
    test -x /out/velox-worker-agent && \
    test -x /out/velox-cache-evict


# ─── Stage 3: runtime — NO compilers, NO cmake, NO binutils ─────────────────
FROM ${VELOX_DEBIAN_BASE}
ARG SOURCE_DATE_EPOCH
ENV SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}

# Pinned non-root identity. Operators chown 10001:10001 on the writable
# host bind-mounts (cache, assets_cache, runtime_dir) so files written
# by the container remain owned by this UID.
ARG VELOX_UID=10001
ARG VELOX_GID=10001

ENV DEBIAN_FRONTEND=noninteractive
ENV VELOX_VIDEO_ENGINE_CPP_BIN=/usr/local/bin/velox_video_engine
ENV VELOX_HEALTH_PORT=8081

WORKDIR /app

# The bootstrap gate resolves the self-render baseline relative to the
# configured worker work-dir. Keep the immutable certification fixture in
# the image so legacy remote workers do not need to bind-mount the source
# tree over /app (which would hide image contents).
RUN mkdir -p /app/RemoteCodex/tests/fixtures
COPY RemoteCodex/native/worker-agent-go/tests/fixtures/engine_selftest_baseline.sha256 \
     /app/RemoteCodex/tests/fixtures/engine_selftest_baseline.sha256

# Runtime deps ONLY. Nothing build-time belongs here. The runtime package
# set is intentionally aligned with the cpp-builder base (Debian Bookworm);
# the engine is never rebuilt or replaced after this stage.
#   * ca-certificates: outbound TLS (master handshake + Social API / Unity builds remote API).
#   * ffmpeg:          runtime dep of the video engine.
#   * curl:            HEALTHCHECK below.
#   * libstdc++6:      C++ engine is dynamically linked against libstdc++
#                      (built by g++ in stage 1).
#   * libc-bin:        entrypoint runs `ldd` / `getconf GNU_LIBC_VERSION`
#                      for engine validation.
ARG RUNTIME_CA_CERTIFICATES_VERSION=20230311+deb12u1
ARG RUNTIME_FFMPEG_VERSION=7:5.1.9-0+deb12u1
ARG RUNTIME_PROCPS_VERSION=2:4.0.2-3
ARG RUNTIME_CURL_VERSION=7.88.1-10+deb12u15
ARG RUNTIME_LIBSTDCPP_VERSION=12.2.0-14+deb12u1
ARG RUNTIME_LIBC_BIN_VERSION=2.36-9+deb12u14
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
        ca-certificates=${RUNTIME_CA_CERTIFICATES_VERSION} \
        ffmpeg=${RUNTIME_FFMPEG_VERSION} \
        procps=${RUNTIME_PROCPS_VERSION} \
        curl=${RUNTIME_CURL_VERSION} \
        libstdc++6=${RUNTIME_LIBSTDCPP_VERSION} \
        libc-bin=${RUNTIME_LIBC_BIN_VERSION} \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built C++ engine from stage 1 (compilers are NOT in the runtime
# image, so the engine cannot be rebuilt here).
COPY --from=cpp-builder /usr/local/bin/velox_video_engine /usr/local/bin/velox_video_engine
COPY --from=cpp-builder /usr/local/share/velox/video-engine.sha256 /usr/local/share/velox/video-engine.sha256

# PR-3: copy the Go worker binary from the go-builder stage — no
# pre-built bin/ from the build context is required.
COPY --from=go-builder /out/velox-worker-agent /usr/local/bin/velox-worker-agent
COPY --from=go-builder /out/velox-cache-evict /usr/local/bin/velox-cache-evict

# Derive the runtime bundle identity from the exact binaries that crossed
# the builder/runtime boundary. The source-tree BUNDLE_HASH.txt is not used
# as an assertion because it can describe a different host build; this hash
# is the value operators and the release manifest can actually verify.
RUN sha256sum /usr/local/bin/velox-worker-agent /usr/local/bin/velox_video_engine /usr/local/bin/velox-cache-evict \
      | sha256sum | awk '{print $1}' > /app/RemoteCodex/BUNDLE_HASH.txt && \
    test "$(wc -c < /app/RemoteCodex/BUNDLE_HASH.txt)" -eq 65

# Entrypoint script (validates + execs; it does NOT rebuild the engine).
COPY RemoteCodex/scripts/worker-entrypoint.sh /usr/local/bin/worker-entrypoint.sh

# OCI labels are supplied by CI/build scripts where available. These static
# labels make the provenance explicit even for local builds.
LABEL org.veloxproject.engine.path="/usr/local/bin/velox_video_engine" \
      org.veloxproject.engine.build="canonical-cpp-builder" \
      org.veloxproject.engine.runtime="debian:bookworm-slim" \
      org.veloxproject.engine.sha256-file="/usr/local/share/velox/video-engine.sha256"

# Create non-root runtime user with UID pinned at ${VELOX_UID}. The three
# binaries are chowned individually (no `chown -R`) because they live in
# /usr/local/bin/ and must only be readable+executable by velox (mode 0755).
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
        /usr/local/bin/velox-cache-evict \
        /usr/local/bin/worker-entrypoint.sh && \
    chmod 0755 \
        /usr/local/bin/velox_video_engine \
        /usr/local/bin/velox-worker-agent \
        /usr/local/bin/velox-cache-evict \
        /usr/local/bin/worker-entrypoint.sh

ENV HOME=/home/velox
ENV XDG_CACHE_HOME=/home/velox/.cache

USER velox

# RW-PROD-004 §3 A5: probe canonical /health/ready (NOT legacy /health) so
# docker health verdicts correctly track the reasons taxonomy (bootstrap,
# executors, cache, blob, disk) instead of a misleading process-only "ok".
# Timing matches deploy/runtime/compose.yml so `docker run` (no compose)
# behaves identically.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=5 \
    CMD /bin/sh -c 'curl -sf "http://localhost:${VELOX_HEALTH_PORT:-8081}/health/ready" || exit 1'

ENTRYPOINT ["/usr/local/bin/worker-entrypoint.sh"]
CMD ["-master", "http://master:8000"]
