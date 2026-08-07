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
#   3. Requires VELOX_STATE_DIR and confirms
#      $VELOX_STATE_DIR/worker_config.json exists and parses as JSON. This
#      file is rendered by deploy/scripts/apply-local-worker-config.sh and
#      bind-mounted into the container at /opt/velox/worker_config.json.
#   4. Runs migrate-legacy-worker.sh once to preserve legacy state and retire
#      per-host units/containers before the canonical runtime is installed.
#   5. Creates missing paths under /opt/velox-worker, /etc/velox-worker,
#      and VELOX_STATE_DIR/state|work|cache|output without changing existing
#      owner, mode, ACLs, or contents.
#   6. Sets uid 10001 ownership AND group read+traversal on /etc/velox-worker
#      so the container's velox user can read mTLS certs + the per-worker
#      credential through the compose :ro bind-mounts.
#   7. Installs compose.yml and the canonical systemd unit into
#      /opt/velox-worker and /etc/systemd/system.
#   8. Cosign signature verification (keyless OIDC against the GitHub
#      Actions issuer). Verified images only. Failure aborts; operator
#      override via VELOX_SKIP_COSIGN_VERIFY=1 plus a non-empty
#      VELOX_COSIGN_OVERRIDE_REASON for a documented incident response only.
#   9. 'docker pull's the pinned image digest.
#   10. Enables the single velox-worker.service owner for the fixed
#       Compose project/container.

set -euo pipefail

# ── Constants ──────────────────────────────────────────────────────────────────
readonly ENV_FILE_DEFAULT="/etc/velox-worker/worker.env"
readonly COMPOSE_YML_SRC
COMPOSE_YML_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/compose.yml"
readonly COMPOSE_YML_DST="/opt/velox-worker/compose.yml"
readonly FETCH_SRC
FETCH_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/openbao-fetch-worker-secrets.sh"
readonly FETCH_DST="/opt/velox-worker/openbao-fetch-worker-secrets.sh"
readonly MIGRATION_SRC
MIGRATION_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/migrate-legacy-worker.sh"
readonly MIGRATION_DST="/opt/velox-worker/migrate-legacy-worker.sh"
readonly SERVICE_SRC
SERVICE_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/velox-worker.service"
readonly SERVICE_DST="/etc/systemd/system/velox-worker.service"
readonly IMAGE_UID="10001"
readonly IMAGE_GID="10001"

# Cosign verification: whitelist the workflow file + a tag-set / branch ref.
# Symmetric with what the worker-image workflow stamps (keyless OIDC against
# the GitHub Actions issuer). Held as a literal here so it's easy to grep.
readonly COSIGN_IDENTITY_REGEXP='^https://github.com/Marcuss-ops/VeloxEditiingg/\.github/workflows/worker-image\.yml@refs/(tags/worker-v.+|heads/.+)'
readonly COSIGN_OIDC_ISSUER='https://token.actions.githubusercontent.com'

# ── Helpers ──────────────────────────────────────────────────────────────────
RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'; BLUE=$'\033[0;34m'; NC=$'\033[0m'
log()  { echo -e "${BLUE}[prepare]${NC} $*"; }
ok()   { echo -e "${GREEN}[  OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*" >&2; exit 1; }

# Only create missing directories with canonical defaults. Existing state and
# work directories are operator-owned data surfaces: never recursively chown
# or chmod them during convergence.
ensure_dir_preserving() {
    local path="$1" owner="$2" group="$3" mode="$4"
    if [[ -e "$path" ]]; then
        [[ -d "$path" ]] || fail "$path exists but is not a directory"
        return 0
    fi
    mkdir -p "$path"
    chown "$owner:$group" "$path"
    chmod "$mode" "$path"
}

assert_no_legacy_dropins() {
    local found=0 path entry
    shopt -s nullglob
    for path in \
        /etc/systemd/system/velox-worker-*.service.d \
        /run/systemd/system/velox-worker-*.service.d \
        /lib/systemd/system/velox-worker-*.service.d \
        /usr/lib/systemd/system/velox-worker-*.service.d; do
        [[ -d "$path" ]] || continue
        for entry in "$path"/*.conf; do
            [[ -e "$entry" || -L "$entry" ]] || continue
            found=1
            warn "legacy systemd drop-in detected: $entry"
        done
    done
    shopt -u nullglob
    (( found == 0 )) || fail "velox-worker systemd drop-ins are forbidden; remove or migrate them before convergence"
}

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

# Refuse legacy per-worker drop-ins before migration can make any host-side
# changes. The canonical velox-worker.service.d directory remains allowed;
# only velox-worker-<id>.service.d directories are legacy.
assert_no_legacy_dropins

# If the canonical env already exists, load it before migration so the
# mandatory state root is available to the one-time migrator. A host without
# an explicit state root fails closed rather than falling back to a guessed
# /var/lib path.
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

log "Migrating legacy worker runtime if present"
install -o root -g root -m 0755 "$MIGRATION_SRC" "$MIGRATION_DST"
CANONICAL_ENV="$ENV_FILE" VELOX_STATE_DIR="${VELOX_STATE_DIR:-}" "$MIGRATION_DST"

[[ -f "$ENV_FILE" ]] \
    || fail "env file not found: $ENV_FILE. Copy deploy/runtime/worker.env.example to $ENV_FILE and edit."

# shellcheck disable=SC1090
set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a

: "${VELOX_WORKER_ID:?VELOX_WORKER_ID is missing from $ENV_FILE}"
: "${VELOX_WORKER_IMAGE:?VELOX_WORKER_IMAGE is missing from $ENV_FILE}"
: "${VELOX_STATE_DIR:?VELOX_STATE_DIR is missing from $ENV_FILE}"
[[ "$VELOX_STATE_DIR" == /* ]] \
    || fail "VELOX_STATE_DIR must be an absolute path (got: $VELOX_STATE_DIR)"
STATE_DIR_REAL="$(realpath -m -- "$VELOX_STATE_DIR")"
[[ "$STATE_DIR_REAL" == "$VELOX_STATE_DIR" ]] \
    || fail "VELOX_STATE_DIR must be normalized and must not contain traversal (got: $VELOX_STATE_DIR)"
# The compose command passes VELOX_MASTER_URL to the binary's -master flag
# (REST base URL). Fail fast here rather than letting the worker silently
# dial a wrong/default target after the image is pulled and the service
# is armed.
: "${VELOX_MASTER_URL:?VELOX_MASTER_URL is missing from $ENV_FILE (REST base URL, e.g. http://master:8000)}"

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
# The compose bind-mounts $VELOX_STATE_DIR/worker_config.json:ro into
# the container at /opt/velox/worker_config.json. Without the file present
# docker will silently bind-mount a directory in its place (or fail on the
# JSON load). apply-local-worker-config.sh is the canonical renderer and
# is NOT chained here by design — the operator workflow is:
#   1. edit worker.env (including the required VELOX_STATE_DIR)
#   2. apply-local-worker-config.sh --worker-id ... --control-grpc-url ...
#   3. prepare-host.sh
# We refuse to start the worker if step 2 was skipped.
WORKER_CONFIG_FILE="$VELOX_STATE_DIR/worker_config.json"
[[ -f "$WORKER_CONFIG_FILE" ]] \
    || fail "$WORKER_CONFIG_FILE missing. Run deploy/scripts/apply-local-worker-config.sh first; it renders the JSON from /opt/velox/worker_config.example.json."
if ! python3 -m json.tool "$WORKER_CONFIG_FILE" >/dev/null 2>&1; then
    fail "$WORKER_CONFIG_FILE is not valid JSON (re-run apply-local-worker-config.sh; output may be on stdout if --keep-tmp was set)."
fi
ok "worker_config.json exists and parses as JSON (existing metadata preserved)"

# ── 1. Canonical directory tree ────────────────────────────────────────────
log "Creating /opt, /etc, and state directory tree"
mkdir -p /opt/velox-worker /etc/velox-worker/certs /etc/velox-worker/secrets
ensure_dir_preserving "$VELOX_STATE_DIR" "$IMAGE_UID" "$IMAGE_GID" 0750
for state_subdir in state work cache output; do
    ensure_dir_preserving "$VELOX_STATE_DIR/$state_subdir" "$IMAGE_UID" "$IMAGE_GID" 0750
done

# ── 2. Permissions ──────────────────────────────────────────────────────────
ok "state/work directories preserved (missing paths use uid ${IMAGE_UID}:${IMAGE_GID} mode 0750)"

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
#
# RW-PROD-001 A2 (config_validate.go): in production the private key must be
# mode 0600 (no group/other bits) or the worker refuses to start fail-closed.
# The container's velox user is uid ${IMAGE_UID}, so worker.key +
# worker_credential must be OWNED by ${IMAGE_UID}:${IMAGE_GID} (not root) or
# the container cannot read them through the :ro bind-mounts at mode 0600.
for spec in \
    /etc/velox-worker/certs/worker.crt:0644:root \
    /etc/velox-worker/certs/ca.crt:0644:root \
    /etc/velox-worker/certs/worker.key:0600:${IMAGE_UID} \
    /etc/velox-worker/secrets/worker_credential:0600:${IMAGE_UID} ; do
    spec_path="${spec%:*:*}"
    spec_mode="${spec#*:}"; spec_mode="${spec_mode%:*}"
    spec_owner="${spec##*:}"
    [[ -e "$spec_path" ]] || continue
    chown "${spec_owner}:${IMAGE_GID}" "$spec_path"
    chmod "$spec_mode" "$spec_path"
done
ok "cert + secret perms aligned for uid ${IMAGE_GID} (key/credential 0600 owner ${IMAGE_UID})"

# ── 2.5. OpenBao secret resolution (migrazione, con fallback sul file) ──────
# Se VELOX_OPENBAO_ADDR è valorizzato in worker.env, worker_credential + la
# coppia mTLS vengono risolti da OpenBao via AppRole (machine identity del
# worker) da deploy/runtime/openbao-fetch-worker-secrets.sh, invece del file
# copiato a mano. Finché la migrazione non è completata il fallback resta sui
# file esistenti: se il fetch fallisce ma i file ci sono tutti, si prosegue
# con un warning (i path canonici non cambiano). Se invece un file manca,
# il bootstrap fallisce fail-closed.
log "Installing OpenBao secret resolver (worker_credential + mTLS via AppRole)"
install -o root -g root -m 0755 "$FETCH_SRC" "$FETCH_DST"
if ! "$FETCH_DST"; then
    warn "OpenBao fetch FAILED (migrazione in corso?) — fallback sui file esistenti"
    for f in \
        /etc/velox-worker/certs/worker.crt \
        /etc/velox-worker/certs/worker.key \
        /etc/velox-worker/certs/ca.crt \
        /etc/velox-worker/secrets/worker_credential ; do
        # -s (non vuoto): un file vuoto/rovinato non è un fallback valido —
        # il worker fallirebbe la registrazione sul master (credential_hash)
        [[ -s "$f" ]] \
            || fail "OpenBao fetch fallito e $f mancante O vuoto — completa la migrazione OpenBao o ripristina i file (fallback non disponibile)"
    done
    warn "fallback attivo: il worker userà i file esistenti (migrazione OpenBao non completata)"
else
    ok "worker secrets risolti (OpenBao) o non configurati (file a mano)"
fi

# ── 3. Install canonical runtime definitions ───────────────────────────────
log "Copying compose.yml to $COMPOSE_YML_DST"
install -o root -g root -m 0644 "$COMPOSE_YML_SRC" "$COMPOSE_YML_DST"
install -o root -g root -m 0644 "$SERVICE_SRC" "$SERVICE_DST"
ok "compose.yml and velox-worker.service installed"

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
      [[ -n "${VELOX_COSIGN_OVERRIDE_REASON:-}" ]] \
        || fail "cosign verification failed and VELOX_COSIGN_OVERRIDE_REASON is required when VELOX_SKIP_COSIGN_VERIFY=1"
      warn "cosign verify FAILED but explicit override is active; reason=${VELOX_COSIGN_OVERRIDE_REASON}"
    else
      fail "cosign signature verification FAILED for $VELOX_WORKER_IMAGE (set VELOX_SKIP_COSIGN_VERIFY=1 and VELOX_COSIGN_OVERRIDE_REASON for incident response only)"
    fi
  fi
else
  if [[ "${VELOX_SKIP_COSIGN_VERIFY:-}" == "1" ]]; then
    [[ -n "${VELOX_COSIGN_OVERRIDE_REASON:-}" ]] \
      || fail "cosign CLI is missing and VELOX_COSIGN_OVERRIDE_REASON is required when VELOX_SKIP_COSIGN_VERIFY=1"
    warn "cosign CLI missing; explicit override is active; reason=${VELOX_COSIGN_OVERRIDE_REASON}"
  else
    fail "cosign CLI not present on PATH — refusing to continue (set VELOX_SKIP_COSIGN_VERIFY=1 and VELOX_COSIGN_OVERRIDE_REASON for incident response only)"
  fi
fi

# ── 4. Pull image ──────────────────────────────────────────────────────────
log "Pulling $VELOX_WORKER_IMAGE"
docker pull "$VELOX_WORKER_IMAGE"
ok "image pulled"

# ── 5. Bring up the canonical systemd-owned runtime ────────────────────────
cd /opt/velox-worker
if [[ -d /run/systemd/system ]] && systemctl daemon-reload >/dev/null 2>&1; then
    systemctl enable --now velox-worker.service
    systemctl is-active --quiet velox-worker.service \
        || fail "velox-worker.service did not become active"
else
    fail "systemd is required for the canonical worker runtime"
fi

docker compose -p velox-worker -f "$COMPOSE_YML_DST" ps
ok "Worker $VELOX_WORKER_ID is up under velox-worker.service / velox-worker container."
