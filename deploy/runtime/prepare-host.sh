#!/usr/bin/env bash
# Velox Worker — host preparation script.
# ─────────────────────────────────────────────────────────────────────────────
# Idempotent setup for a worker host running the Velox worker container.
# Run as root on the target host:
#   sudo deploy/runtime/prepare-host.sh
#
# What it does:
#   1. Verifies docker + docker compose plugin are installed.
#   2. Reads /etc/velox-worker/worker.env (gives VELOX_WORKER_ID +
#      VELOX_WORKER_IMAGE etc.).
#   3. Creates the directory tree under /opt/velox-worker, /etc/velox-worker,
#      and /var/lib/velox-worker/state|work|cache|output.
#   4. Sets uid 10001 ownership on the persistent data dirs (matches the
#      non-root `velox` uid inside the container).
#   5. Installs compose.yml from this repo into /opt/velox-worker/compose.yml.
#   6. `docker pull`s the pinned image digest.
#   7. Brings the worker up with an isolated project name — `velox-worker-<id>`
#      so multiple workers on one host (e.g. staging-on-prod) don't collide.

set -euo pipefail

# ── Constants ───────────────────────────────────────────────────────────────
readonly ENV_FILE_DEFAULT="/etc/velox-worker/worker.env"
readonly COMPOSE_YML_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/compose.yml"
readonly COMPOSE_YML_DST="/opt/velox-worker/compose.yml"
readonly IMAGE_UID="10001"
readonly IMAGE_GID="10001"

# ── Helpers ────────────────────────────────────────────────────────────────
RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'; BLUE=$'\033[0;34m'; NC=$'\033[0m'
log()  { echo -e "${BLUE}[prepare]${NC} $*"; }
ok()   { echo -e "${GREEN}[  OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*" >&2; exit 1; }

# ── 0. Preconditions ────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    fail "This script must run as root (use sudo)."
fi
command -v docker >/dev/null 2>&1 \
    || fail "docker CLI not found on PATH — install docker-ce first."
docker compose version >/dev/null 2>&1 \
    || fail "docker compose plugin missing — install docker-compose-plugin."
# Pre-flight: is the caller allowed to talk to the docker daemon? Without this
# check, the failure surfaces mid-execution as an opaque
# 'permission denied while connecting to Docker daemon socket'.
docker info >/dev/null 2>&1 \
    || fail "Cannot reach the docker daemon — add the caller's user to the 'docker' group, or set DOCKER_HOST explicitly."

ENV_FILE="${ENV_FILE:-$ENV_FILE_DEFAULT}"
[[ -f "$ENV_FILE" ]] \
    || fail "env file not found: $ENV_FILE. Copy deploy/runtime/worker.env.example to $ENV_FILE and edit."

# shellcheck disable=SC1090
set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a

: "${VELOX_WORKER_ID:?VELOX_WORKER_ID is missing from $ENV_FILE}"
: "${VELOX_WORKER_IMAGE:?VELOX_WORKER_IMAGE is missing from $ENV_FILE}"

log "Worker ID  : $VELOX_WORKER_ID"
log "Image      : $VELOX_WORKER_IMAGE"

# ── 1. Directory tree ───────────────────────────────────────────────────────
log "Creating /opt, /etc, /var/lib/velox-worker directory tree"
mkdir -p \
    /opt/velox-worker \
    /etc/velox-worker/certs \
    /etc/velox-worker/secrets \
    /var/lib/velox-worker/state \
    /var/lib/velox-worker/work \
    /var/lib/velox-worker/cache \
    /var/lib/velox-worker/output

# ── 2. Permissions ──────────────────────────────────────────────────────────
log "Setting uid ${IMAGE_UID}:${IMAGE_GID} on /var/lib/velox-worker"
chown -R "${IMAGE_UID}:${IMAGE_GID}" /var/lib/velox-worker
ok "/var/lib/velox-worker owned by uid ${IMAGE_UID}:${IMAGE_GID}"

log "Tightening /etc/velox-worker to root-only traversal"
chmod 0750 /etc/velox-worker
chmod 0750 /etc/velox-worker/certs /etc/velox-worker/secrets
ok "/etc/velox-worker mode 0750 (root only)"

# ── 3. Install compose.yml ─────────────────────────────────────────────────
log "Copying compose.yml to $COMPOSE_YML_DST"
install -o root -g root -m 0644 "$COMPOSE_YML_SRC" "$COMPOSE_YML_DST"
ok "compose.yml installed"

# ── 4. Pull image ──────────────────────────────────────────────────────────
log "Pulling $VELOX_WORKER_IMAGE"
docker pull "$VELOX_WORKER_IMAGE"
ok "image pulled"

# ── 5. Bring up worker ─────────────────────────────────────────────────────
PROJECT_NAME="velox-worker-${VELOX_WORKER_ID}"
cd /opt/velox-worker

log "Bringing up worker under project '$PROJECT_NAME'"
docker compose \
    -p "$PROJECT_NAME" \
    -f "$COMPOSE_YML_DST" \
    up -d --wait

log "Final state:"
docker compose \
    -p "$PROJECT_NAME" \
    -f "$COMPOSE_YML_DST" \
    ps

ok "Worker $VELOX_WORKER_ID is up."
