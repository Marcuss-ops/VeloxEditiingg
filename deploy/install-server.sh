#!/bin/bash
# Velox Master Server - Systemd Install Script
# =============================================# Usage:
#   sudo ./deploy/install-server.sh [--user velox] [--group velox] #
# Steps:
#   1. Validate the env file (must include VELOX_SERVER_IMAGE digest)
#   2. Ensure docker is installed and reachable
#   3. Prepare runtime directories for the bind-mounted container
#   4. Pull the pinned image from GHCR
#   5. Install and start the systemd wrapper unit
# =============================================

set -euo pipefail

# The install script lives at deploy/install-server.sh and computes its own
# location so it can be invoked from anywhere (sudo ./deploy/...).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$SCRIPT_DIR"
SERVICE_NAME="velox-server"
SERVICE_SRC="$DEPLOY_DIR/velox-server.service"
SERVICE_DST="/etc/systemd/system/$SERVICE_NAME.service"
# Path FIX: was "$DEPLOY_DIR/velox-server.env" (file did not exist on a fresh
# clone — only $DEPLOY_DIR/velox-server.env.example is shipped). The .example
# suffix is required so it is never read as a real config.
ENV_TEMPLATE="$DEPLOY_DIR/velox-server.env.example"
ENV_DST="/etc/velox-server.env"
YOUTUBE_RUNTIME_CREDS_DIR="/etc/velox/secrets/youtube/credentials"
YOUTUBE_RUNTIME_CREDS_FILE="$YOUTUBE_RUNTIME_CREDS_DIR/credentials.json"
IMAGE_UID="10001"
IMAGE_GID="10001"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

log()  { echo -e "${BLUE}[INSTALL]${NC} $*"; }
ok()   { echo -e "${GREEN}[  OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

# preflight_ffprobe_invariant enforces the host-level precondition for
# the VELOX_FFPROBE_VERIFY_ON_FINALIZE tripwire (RW-PROD-008 A4). When
# the env var is set to the strict literal "true", the master requires
# the `ffprobe` binary on PATH; without it, the Go gate surfaces
# ErrFFProbeInvariantMissingBinary on every Finalize and loses the
# tripwire's signal-to-noise advantage over a count-mismatch trip.
# The check is a NO-OP when the env var is unset or commented out, so
# default installs are unaffected.
#
# Detection semantics (mirror the Go gate's `os.Getenv(...) == "true"`
# literal check EXACTLY so a production deploy never trips a count-
# mismatch false-positive on a typo'd value):
#   - Strict literal "true"  → enabled.
#   - Anything else (unset,
#     commented-out, "1",
#     "TRUE", "yes", missing
#     key, malformed line)
#                                → no-op.
# We accept systemd EnvironmentFile= canonical forms (bare `=true`,
# quoted `="true"` / `='true'`, with optional trailing whitespace +
# inline comment EOL) because systemd normalizes these to the same
# env var value the Go gate will see.
preflight_ffprobe_invariant() {
    local env_file="$1"
    # Defensive fail-loud: at the call site (Step 4a, between the
    # env-file install at Step 4 and the validator at Step 4b) a
    # missing env file is a regression, not a benign no-op. Matches
    # the installer's audit-verdict #3 fail-fast mandate.
    [[ -f "$env_file" ]] || fail "preflight_ffprobe_invariant: env file $env_file missing at Step 4a (Step 4 should have installed it from ${ENV_TEMPLATE:-<unknown>})"

    local enabled=0
    local raw_line val
    # Read the first matching line, ignore the rest (operators don't
    # duplicate env keys; tee + comment-by-`#` is the only valid
    # second occurrence and operators don't author it).
    raw_line="$(grep -E '^[[:space:]]*VELOX_FFPROBE_VERIFY_ON_FINALIZE=' "$env_file" 2>/dev/null | head -1 || true)"
    if [[ -n "$raw_line" ]]; then
        # Strip the leading key prefix.
        val="${raw_line#*=}"
        # Strip inline trailing comment after whitespace (default
        # systemd EnvironmentFile= semantic: `#` outside quotes,
        # preceded by whitespace or EOL). Doesn't perfectly handle
        # `="true # inside quotes"` (matches a malformed operator
        # line — systemd preserves the `#` so the gate would also
        # NOT enable on this input; preflight agrees on no-op).
        val="${val%% \#*}"
        val="${val%%	#*}"
        # Trim leading and trailing whitespace.
        val="${val#"${val%%[![:space:]]*}"}"
        val="${val%"${val##*[![:space:]]}"}"
        # Strip surrounding single or double quotes (systemd
        # EnvironmentFile= strips these too; preflight agrees).
        case "$val" in
            \"*\") val="${val#\"}"; val="${val%\"}" ;;
            \'*\') val="${val#\'}"; val="${val%\'}" ;;
        esac
        # Trim again after quote removal.
        val="${val#"${val%%[![:space:]]*}"}"
        val="${val%"${val##*[![:space:]]}"}"
        # Strict-literal match (NOT a substring test).
        if [[ "$val" == "true" ]]; then
            enabled=1
        fi
    fi

    if [[ "$enabled" -eq 1 ]]; then
        log "VELOX_FFPROBE_VERIFY_ON_FINALIZE=true detected. Verifying ffprobe on PATH..."
        command -v ffprobe >/dev/null 2>&1 \
            || fail "ffprobe binary missing from PATH. The VELOX_FFPROBE_VERIFY_ON_FINALIZE tripwire requires ffprobe (install via 'apt-get install -y ffmpeg' or distro equivalent). Either install ffprobe and re-run, or comment out VELOX_FFPROBE_VERIFY_ON_FINALIZE in $env_file to skip the tripwire."
        ok "ffprobe binary found at $(command -v ffprobe)"
    fi
}

# ─── Check prerequisites ────────────────────────────────────────────────────

if [[ $EUID -ne 0 ]]; then
    fail "This script must be run as root (sudo)."
fi

if [[ ! -f "$SERVICE_SRC" ]]; then
    fail "Service file not found: $SERVICE_SRC. Run this script from the project root (or sudo ./deploy/install-server.sh from the repo)."
fi

# ─── Step 1: Docker preconditions ───────────────────────────────────────────
command -v docker >/dev/null 2>&1 \
    || fail "docker CLI not found on PATH — install docker-ce first."
docker info >/dev/null 2>&1 \
    || fail "Cannot reach the docker daemon — start docker and retry."

# ─── Step 2: Ensure target directory tree exists ────────────────────────────

log "Ensuring target directory tree and permissions..."
mkdir -p /opt/velox/current
mkdir -p /opt/velox/current/.velox/data
mkdir -p /opt/velox/current/.velox/secrets/youtube/tokens
mkdir -p "$YOUTUBE_RUNTIME_CREDS_DIR"
mkdir -p /var/lib/velox/data
mkdir -p /var/lib/velox/videos
mkdir -p /etc/velox/secrets
mkdir -p /etc/velox/certs
mkdir -p /etc/velox/secrets/youtube/tokens
mkdir -p /etc/velox/secrets/youtube/credentials
chown root:root /opt/velox/current
chmod 755 /opt/velox/current
chown -R "${IMAGE_UID}:${IMAGE_GID}" /var/lib/velox
ok "Directory tree ready: /opt/velox/current"

# ─── Step 3b: Sync YouTube OAuth credentials ───────────────────────────────

YOUTUBE_SOURCE_CREDS=""
for candidate in \
    "$REPO_ROOT/DataServer/data/youtube/credentials/credentials.json" \
    "$REPO_ROOT/DataServer/.velox/secrets/youtube/credentials/credentials.json"
do
    if [[ -f "$candidate" ]]; then
        YOUTUBE_SOURCE_CREDS="$candidate"
        break
    fi
done

if [[ -n "$YOUTUBE_SOURCE_CREDS" ]]; then
    log "Syncing YouTube OAuth credentials to runtime..."
    cp "$YOUTUBE_SOURCE_CREDS" "$YOUTUBE_RUNTIME_CREDS_FILE"
    chown root:root "$YOUTUBE_RUNTIME_CREDS_FILE"
    chmod 600 "$YOUTUBE_RUNTIME_CREDS_FILE"
    ok "YouTube OAuth credentials deployed"
else
    warn "YouTube OAuth credentials not found in source tree; runtime will rely on existing files"
fi

# ─── Step 3: Deploy systemd service ─────────────────────────────────────────

log "Installing systemd service..."
cp "$SERVICE_SRC" "$SERVICE_DST"
chmod 644 "$SERVICE_DST"
ok "Service file installed: $SERVICE_DST"

# ─── Step 4: Deploy env file (do NOT overwrite existing) ────────────────────

if [[ ! -f "$ENV_DST" ]]; then
    log "Installing env file template..."
    cp "$ENV_TEMPLATE" "$ENV_DST"
    chmod 600 "$ENV_DST"
    chown root:root "$ENV_DST"
    warn "⚠️  Edit $ENV_DST with your configuration before starting the service!"
else
    warn "Env file already exists: $ENV_DST (not overwritten)"
fi

# Validate env (fail-fast, audit verdict #3)
# Closes audit verdict block #3: 'deploy installer can declare success with
# invalid config'. Single canonical validator at deploy/validate-master-env.sh
# enforces: no CHANGE_ME_* literals, VELOX_ADMIN_TOKEN non-empty,
# VELOX_ALLOWED_WORKERS non-empty + non-wildcard + unique IDs,
# MASTER_PUBLIC_URL parsable, TLS triple consistent, VELOX_DB_PATH non-empty,
# VELOX_GRPC_PORT numeric. Same script invoked by deploy/playbooks/*.yml so
# ansible + bash install paths stay in lock-step.
VALIDATOR="${SCRIPT_DIR}/validate-master-env.sh"
if [[ ! -r "$VALIDATOR" ]]; then
    fail "validator not found at $VALIDATOR — re-pull deploy/ tree. Audit verdict block #3 requires the validator BEFORE any systemd operation."
fi
# Step 4a: ffprobe tripwire preflight (RW-PROD-008 A4). NO-OP unless
# the operator uncommented VELOX_FFPROBE_VERIFY_ON_FINALIZE=true. If
# they did, the ffprobe binary MUST be on PATH or the install fails
# fast — better here than on the first Finalize RPC. Runs BEFORE the
# validator so a missing env file at this stage trips the explicit
# preflight message (validator's generic rc=2 is too opaque).
preflight_ffprobe_invariant "$ENV_DST"

log "Validating $ENV_DST..."
# Capture the validator's exit code so we can distinguish:
#   rc=2 → env file unreadable / missing / malformed at line 1
#          (operator must create/fix the file itself before retrying)
#   rc=1 → env file parsed but at least one hard-fail rule tripped
#          (operator must edit VALUES, not the file structure)
# Both block Step 7; each error path emits a distinct message so the
# operator sees immediately which axis to repair. Stderr from the
# validator is preserved (no redirect) so the per-rule line-items are
# visible above whichever fail message we emit below.
# IMPORTANT: this script has `set -e`; we MUST swallow the non-zero rc
# explicitly via `|| validator_rc=$?` so errexit doesn't pre-empt the case.
validator_rc=0
bash "$VALIDATOR" "$ENV_DST" || validator_rc=$?
case "$validator_rc" in
    2)
        fail "could not read env file $ENV_DST (validator rc=2). The file is missing, unreadable, or malformed (e.g. unmatched quote). Operator MUST create/fix $ENV_DST AND ensure line 1 parses before retrying. NOT silently claiming 'Install complete!'."
        ;;
    1)
        fail "validation of $ENV_DST failed (validator rc=1, see hard-fail errors above). Operator MUST replace every CHANGE_ME_*, set VELOX_ALLOWED_WORKERS / VELOX_ADMIN_TOKEN / MASTER_PUBLIC_URL / etc., then re-run. Refusing to silently claim 'Install complete!'."
        ;;
    0)
        # PASS — fall through to ok below (no body needed; just the terminator).
        ;;
    *)
        fail "validator returned unexpected exit code rc=$validator_rc (only rc=0/1/2 are defined). Treating as hard failure. Operator MUST re-validate $ENV_DST before retrying. NOT silently claiming 'Install complete!'."
        ;;
esac
ok "env file accepted: $ENV_DST"

# ─── Step 5: Pull pinned image ──────────────────────────────────────────────
VELOX_SERVER_IMAGE="$(grep -E '^[[:space:]]*VELOX_SERVER_IMAGE=' "$ENV_DST" | head -n1 | cut -d= -f2- || true)"
[[ -n "$VELOX_SERVER_IMAGE" ]] || fail "VELOX_SERVER_IMAGE missing from $ENV_DST after validation"
log "Pulling image $VELOX_SERVER_IMAGE"
docker pull "$VELOX_SERVER_IMAGE" \
    || fail "docker pull failed. If GHCR is private, run 'docker login ghcr.io' as root on this host, then retry."
ok "Image pulled"

# ─── Step 6: Enable and start ───────────────────────────────────────────────

# Old behaviour silently swallowed systemd failures with || warn and printed
# 'Install complete!' — a fresh clone could terminate with rc=0 even when
# the service never bound its port. Audit-verdict #3 mandates fail-fast:
# either systemctl enable/start succeed or the installer exits non-zero.
# Sandbox / chroot / docker hosts lacking /run/systemd/system only get a
# loud warning + operator-action handoff (the validator at Step 6b is the
# real gate for env correctness; the systemd block is then a documented
# no-op so operators know they're on their own for the unit operations).
if [[ ! -d /run/systemd/system ]] || ! systemctl is-system-running &>/dev/null; then
    warn "/run/systemd/system not present OR systemd daemon unreachable (sandbox / chroot / docker without --privileged / degraded systemd). Skipping daemon-reload / enable / start. Operator MUST launch $SERVICE_NAME manually once env file is in place."
else
    log "Reloading systemd..."
    systemctl daemon-reload || fail "systemctl daemon-reload failed — refusing to silently continue (audit-verdict #3 fail-fast mandate)"

    log "Enabling service (will start on boot)..."
    systemctl enable "$SERVICE_NAME" || fail "systemctl enable $SERVICE_NAME failed — refusing to silently continue"

    log "Starting service..."
    systemctl start "$SERVICE_NAME" || fail "Failed to start service — check: journalctl -u $SERVICE_NAME -f. Refusing to silently claim success."
fi

# ─── Done ───────────────────────────────────────────────────────────────────

echo ""
echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Install complete!${NC}"
echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "  Service:   $SERVICE_NAME"
echo "  Image:     $VELOX_SERVER_IMAGE"
echo "  Config:    $ENV_DST"
echo ""
echo "  Status:    systemctl status $SERVICE_NAME"
echo "  Logs:      journalctl -u $SERVICE_NAME -f"
echo "  Restart:   systemctl restart $SERVICE_NAME"
echo "  Stop:      systemctl stop $SERVICE_NAME"
echo ""
echo "  ⚠️  Remember to edit $ENV_DST if this is the first install."
echo ""
