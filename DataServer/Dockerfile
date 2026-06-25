# ============================================================================
# Velox Master (DataServer) — Multi-stage runtime build
# ============================================================================
# Build context: project root (so the velox-shared module is reachable at
# `shared/` and DataServer/go.mod's
#     replace velox-shared => ../shared
# resolves correctly).
#
# Why CGO_ENABLED=1: DataServer depends on github.com/mattn/go-sqlite3,
# which is a CGO package. The binary must therefore be dynamically linked
# against glibc; we keep the final image on `debian:bookworm-slim` (same
# glibc as the builder) instead of trying to force a fully-static link
# (go-sqlite3 + glibc + `-extldflags '-static'` is fragile and breaks
# whenever SQLite or glibc upgrade).
#
# Why LDFLAGS `-X main.Version=... -X main.BuildTime=...` works: those
# variables are declared as `var` (not `const`) in
# DataServer/cmd/server/main.go. Go's linker's `-X` flag only mutates
# writable symbols — this is the standard pattern.
#
# Build (locally):
#   docker buildx build \
#     -f DataServer/Dockerfile \
#     -t velox-server:dev \
#     --build-arg VERSION=dev \
#     --build-arg BUILD_TIME=$(date -u +%Y%m%d) \
#     .
#
# In CI (.github/workflows/master-image.yml):
#   context: .
#   file:    DataServer/Dockerfile
#   build-args:
#     VERSION:   <tag>     (or "dev" on dispatch dry-runs)
#     BUILD_TIME: <now-U>
# ============================================================================

# ─── Stage 1: builder ──────────────────────────────────────────────────────
FROM golang:1.25.8-bookworm AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Build metadata — re-declared after FROM so they scope to this stage.
ARG VERSION=dev
ARG BUILD_TIME=unknown

# CGO toolchain required by go-sqlite3.
ENV CGO_ENABLED=1

WORKDIR /app

# ── Layer-cache-friendly ordering ────────────────────────────────────────
# 1. Module descriptors FIRST. `go mod download` only re-runs when
#    go.mod / go.sum change, not on every source-code edit.
COPY shared/go.mod shared/go.sum ./shared/
COPY DataServer/go.mod DataServer/go.sum ./DataServer/

# 2. Pre-download DataServer's full dep graph (including CGO sqlite3).
WORKDIR /app/DataServer
RUN go mod download

# 3. Now copy source trees (only invalidates layer 3 if any code moves).
WORKDIR /app
COPY shared/    ./shared/
COPY DataServer/ ./DataServer/

# 4. Compile.
WORKDIR /app/DataServer
# `-trimpath` strips local filesystem paths from the binary for
# reproducibility (build cache is keyed off repro builds).
# `-s -w` strips debug symbols to shrink the image; runtime panic
# stack traces are still readable enough for this project.
RUN go build \
        -trimpath \
        -ldflags "-s -w -X main.Version=${VERSION} -X main.BuildTime=${BUILD_TIME}" \
        -o /out/velox-server \
        ./cmd/server

# ─── Stage 2: runtime ──────────────────────────────────────────────────────
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV VELOX_HTTP_PORT=8000
ARG VELOX_UID=10001
ARG VELOX_GID=10001

# Runtime deps only:
#   - ca-certificates: outbound TLS to YouTube/Drive/Google APIs.
#   - tzdata:          timestamp formatting inside SQLite/datetime layer
#                      (avoids the docker-default UTC-only behaviour).
#   - curl:            HEALTHCHECK below (otherwise the probe would
#                      always fail because `/bin/sh` cannot find curl).
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary straight into /usr/local/bin.
COPY --from=builder /out/velox-server /usr/local/bin/velox-server

# Pinned runtime identity so bind-mounted state on the host can be
# chowned deterministically by the install/deploy scripts.
RUN groupadd --system --gid "${VELOX_GID}" velox && \
    useradd --system \
        --uid "${VELOX_UID}" \
        --gid "${VELOX_GID}" \
        --create-home \
        --home-dir /home/velox \
        --shell /usr/sbin/nologin \
        velox

USER velox

# Health probe — curl the local HTTP /health (registered by
# DataServer/internal/app/health.go on the gin router, both at
# `/health` and `/api/health`). Port comes from VELOX_HTTP_PORT
# (default 8000); operators overriding the port should also override
# the healthcheck port in their compose/k8s manifest rather than
# rebuilding the image.
# start-period aligned with the worker image (120 s) — DataServer
# bootstraps SQLite migrations, gRPC bindings, and the module registry
# during warm-up, and a cold first-boot (no page cache, fresh DB)
# regularly exceeds 60 s in CI smoke runs. Tightening this back to 60 s
# causes spurious `initial_unhealthy` events that look like real
# regressions in dashboards.
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
    CMD /bin/sh -c 'curl -sf "http://127.0.0.1:${VELOX_HTTP_PORT:-8000}/health" || exit 1'

# Default command = `serve` (matches DataServer/cmd/server/main.go's
# arg-parsing contract: bare invocation == "serve"). The runtime port
# is still driven by VELOX_HTTP_PORT — EXPOSE is purely documentation.
EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/velox-server"]
CMD ["serve"]
