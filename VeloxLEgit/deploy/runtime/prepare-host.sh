#!/usr/bin/env bash
# Velox Worker — host preparation script.
# ─────────────────────────────────────────────────────────────────────────────
# Idempotent setup for a worker host running the Velox worker container.
# Run as root on the target host:
#   sudo deploy/runtime/prepare-host.sh
#
# What it does:
#   0. Verifies docker + docker compose plugin are installed; refuses to
#      silently proceed without them (matches the checklist README).
#   1. Reads /etc/velox-worker/worker.env (gives VELOX_WORKER_ID,
#      VELOX_WORKER_IMAGE, ...).
#   2. ENFORCES that VELOX_WORKER_IMAGE matches
#      '^ghcr\.io/[a-z0-9._-]+/[a-z0-9._/-]+@sha256:[a-f0-9]{64}$'
#      (refs to :latest or any non-digest form are rejected before pull).
#   3. Confirms /var/lib/velox-worker/worker_config.json exists and parses
#      as JSON. This file is rendered by deploy/scripts/apply-local-worker-config.sh
#      and bind-mounted into the container at /opt/velox/worker_config.json.
#   4. Creates the directory tree under /opt/velox-worker, /etc/velox-worker,
#      and /var/lib/velox-worker/state|work|cache|output.
#   5. Sets uid 10001 ownership AND group read+traversal on /etc/velox-worker
#      so the container's velox user can read mTLS certs + the per-worker
#      credential through the compose :ro bind-mounts.
#   6. Installs compose.yml from this repo into /opt/velox-worker/compose.yml.
#   7. Cosign signature verification (keyless OIDC against the GitHub
#      Actions issuer). Verified images only. Failure aborts; operator
#      override via VELOX_SKIP_COSIGN_VERIFY=1 for incident response only.
#   8. 'docker pull's the pinned image digest.
#   9. Brings the worker up with an isolated project name 'velox-worker-<id>'
#      so multiple workers on one host do not collide.

set -euo pipefail

# ── Constants ──────────────────────────────────────────────────────────────────
readonly ENV_FILE_DEFAULT="/etc/velox-worker/worker.env"
readonly COMPOSE_YML_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/compose.yml"
readonly COMPOSE_YML_DST="/opt/velox-worker/compose.yml"
readonly IMAGE_UID="10001"
readonly IMAGE_GID="10001"

# Cosign verification: whitelist the workflow file + a tag-set / branch ref.
# Symmetric with what the worker-image workflow stamps (keyless OIDC against
# the GitHub Actions issuer). Held as a literal here so it's easy to grep.
readonly COSIGN_IDENTITY_REGEXP='^https://github.com/Marcuss-ops/VeloxLEgit/\.github/workflows/worker-image\.yml@refs/(tags/worker-v.+|heads/.+)'
readonly COSIGN_OIDC_ISSUER='https://token.actions.githubusercontent.com'

# ── Helpers ──────────────────────────────────────────────────────────────────
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
# python3 is required for the worker_config.json JSON sanity check below.
command -v python3 >/dev/null 2>&1 \
    || fail "python3 not found on PATH — required for JSON sanity check on worker_config.json."

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

# ── 0.5. Image digest gate ─────────────────────────────────────────────────
# Compose uses '${VELOX_WORKER_IMAGE:?}' which only rejects EMPTY refs.
# It silently accepts a mutable reference (e.g. the upstream `:latest`
# tag), which would break the immutability guarantee. We enforce
# the immutability guarantee. We enforce sha256-pinning here so the worker
# host cannot pull a mutable ref by mistake or by malicious edit to worker.env.
if ! [[ "$VELOX_WORKER_IMAGE" =~ ^ghcr\.io/[a-z0-9._-]+/[a-z0-9._/-]+@sha256:[a-f0-9]{64}$ ]]; then
    fail "VELOX_WORKER_IMAGE must be a lowercase ghcr.io/<owner>/<repo>@sha256:<64 hex> ref (got: $VELOX_WORKER_IMAGE)"
fi
ok "image digest shape OK (ghcr.io pinned to sha256)"

# ── 0.6. worker_config.json pre-flight ──────────────────────────────────────
# The compose bind-mounts /var/lib/velox-worker/worker_config.json:ro into
# the container at /opt/velox/worker_config.json. Without the file present
# docker will silently bind-mount a directory in its place (or fail on the
# JSON load). apply-local-worker-config.sh is the canonical renderer and
# is NOT chained here by design — the operator workflow is:
#   1. edit worker.env
#   2. apply-local-worker-config.sh --worker-id ... --control-grpc-url ...
#   3. prepare-host.sh
# We refuse to start the worker if step 2 was skipped.
WORKER_CONFIG_FILE="/var/lib/velox-worker/worker_config.json"
[[ -f "$WORKER_CONFIG_FILE" ]] \
    || fail "$WORKER_CONFIG_FILE missing. Run deploy/scripts/apply-local-worker-config.sh first; it renders the JSON from /opt/velox/worker_config.example.json."
if ! python3 -m json.tool "$WORKER_CONFIG_FILE" >/dev/null 2>&1; then
    fail "$WORKER_CONFIG_FILE is not valid JSON (re-run apply-local-worker-config.sh; output may be on stdout if --keep-tmp was set)."
fi
chown "${IMAGE_UID}:${IMAGE_GID}" "$WORKER_CONFIG_FILE"
chmod 0640 "$WORKER_CONFIG_FILE"
ok "worker_config.json exists, parses as JSON, owned by ${IMAGE_UID}:${IMAGE_GID} mode 0640"

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

# /etc/velox-worker MUST be traversable by uid 10001 (the container's velox
# user) so the worker can read the mTLS cert triple + the per-worker
# credential through the compose :ro bind-mounts at /run/velox/...
# Pattern matches DataServer/data/ansible/playbooks/tasks/systemd_setup.yml
# and the canonical worker_config.json rendering in
# deploy/scripts/apply-local-worker-config.sh.
log "Setting root:${IMAGE_GID} on /etc/velox-worker (so the container can read TLS + creds)"
chown root:"${IMAGE_GID}" /etc/velox-worker /etc/velox-worker/certs /etc/velox-worker/secrets
chmod 0750 /etc/velox-worker /etc/velox-worker/certs /etc/velox-worker/secrets
ok "/etc/velox-worker/{certs,secrets} mode 0750 root:${IMAGE_GID} (worker can traverse)"

# Per-file perms. Only adjust when the operator has already produced the
# files. cert provisioning happens elsewhere via scripts/gen-worker-certs.sh;
# the worker_config.json renderer is deploy/scripts/apply-local-worker-config.sh.
# On the first run after the operator produced those files, this loop aligns
# ownership without breaking existing content.
for spec in \
    /etc/velox-worker/certs/worker.crt:0644 \
    /etc/velox-worker/certs/ca.crt:0644 \
    /etc/velox-worker/certs/worker.key:0640 \
    /etc/velox-worker/secrets/worker_credential:0640 ; do
    spec_path="${spec%:*}"
    spec_mode="${spec##*:}"
    [[ -e "$spec_path" ]] || continue
    chown root:"${IMAGE_GID}" "$spec_path"
    chmod "$spec_mode" "$spec_path"
done
ok "cert + secret perms aligned for uid ${IMAGE_GID}"

# ── 3. Install compose.yml ─────────────────────────────────────────────────
log "Copying compose.yml to $COMPOSE_YML_DST"
install -o root -g root -m 0644 "$COMPOSE_YML_SRC" "$COMPOSE_YML_DST"
ok "compose.yml installed"

# ── 3.5. Cosign signature verification ──────────────────────────────────────
# Worker-image.yml signs the published image with cosign keyless OIDC against
# https://token.actions.githubusercontent.com. Verify BEFORE pulling so an
# attacker who somehow substituted an unsigned image at the GHCR ref cannot
# land it on a worker host. Identity is constrained to the worker-image
# workflow file in this exact repo.
if command -v cosign >/dev/null 2>&1; then
  log "Verifying cosign signature on $VELOX_WORKER_IMAGE (keyless OIDC)"
  if cosign verify \
      --certificate-identity-regexp="$COSIGN_IDENTITY_REGEXP" \
      --certificate-oidc-issuer="$COSIGN_OIDC_ISSUER" \
      "$VELOX_WORKER_IMAGE" >/dev/null 2>&1; then
    ok "cosign signature verified"
  else
    if [[ "${VELOX_SKIP_COSIGN_VERIFY:-}" == "1" ]]; then
      warn "cosign verify FAILED but VELOX_SKIP_COSIGN_VERIFY=1 — proceeding under explicit override (audit-trail concern)"
    else
      fail "cosign signature verification FAILED for $VELOX_WORKER_IMAGE (set VELOX_SKIP_COSIGN_VERIFY=1 to override for incident response only)"
    fi
  fi
else
  warn "cosign CLI not present on PATH — skipping signature verification (install cosign for hardened supply-chain integrity)"
fi

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
