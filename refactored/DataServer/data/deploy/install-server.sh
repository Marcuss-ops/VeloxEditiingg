#!/bin/bash
# Velox Master Server - Systemd Install Script
# =============================================
# Usage:
#   sudo ./data/deploy/install-server.sh [--user velox] [--group velox]
#
# Steps:
#   1. Build the Go binary (if not already built)
#   2. Create system user (velox)
#   3. Copy systemd service file
#   4. Copy env file template (does NOT overwrite existing)
#   5. Enable and start the service
# =============================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DATASERVER_DIR="$SCRIPT_DIR/DataServer"
DEPLOY_DIR="$DATASERVER_DIR/data/deploy"
BINARY_SRC="$DATASERVER_DIR/bin/velox-server"
BINARY_DST="/opt/velox/current/DataServer/bin/velox-server"
SERVICE_NAME="velox-server"
SERVICE_SRC="$DEPLOY_DIR/velox-server.service"
SERVICE_DST="/etc/systemd/system/$SERVICE_NAME.service"
ENV_TEMPLATE="$DEPLOY_DIR/velox-server.env"
ENV_DST="/etc/velox-server.env"
YOUTUBE_RUNTIME_CREDS_DIR="/opt/velox/current/.velox/secrets/youtube/credentials"
YOUTUBE_RUNTIME_CREDS_FILE="$YOUTUBE_RUNTIME_CREDS_DIR/credentials.json"

VELOX_USER="velox"
VELOX_GROUP="velox"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

log()  { echo -e "${BLUE}[INSTALL]${NC} $*"; }
ok()   { echo -e "${GREEN}[  OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

# ─── Check prerequisites ────────────────────────────────────────────────────

if [[ $EUID -ne 0 ]]; then
    fail "This script must be run as root (sudo)."
fi

if [[ ! -f "$SERVICE_SRC" ]]; then
    fail "Service file not found: $SERVICE_SRC. Run this script from the project root."
fi

# ─── Step 1: Build binary if missing ────────────────────────────────────────

if [[ ! -f "$BINARY_SRC" ]]; then
    log "Binary not found — building..."
    if ! command -v go &>/dev/null; then
        fail "Go is not installed. Install Go first, or build the binary manually:
    cd DataServer && go build -o bin/velox-server ./cmd/server"
    fi
    cd "$DATASERVER_DIR"
    go build -o bin/velox-server ./cmd/server
    ok "Binary built: $BINARY_SRC"
else
    ok "Binary already exists: $BINARY_SRC"
fi

# ─── Step 2: Create system user and group ──────────────────────────────────

if ! getent group "$VELOX_GROUP" &>/dev/null; then
    log "Creating system group: $VELOX_GROUP"
    groupadd -r "$VELOX_GROUP"
    ok "Group $VELOX_GROUP created"
else
    ok "Group $VELOX_GROUP already exists"
fi

if ! id -u "$VELOX_USER" &>/dev/null; then
    log "Creating system user: $VELOX_USER"
    useradd -r -s /usr/sbin/nologin -m -g "$VELOX_GROUP" "$VELOX_USER"
    ok "User $VELOX_USER created"
else
    ok "User $VELOX_USER already exists"
fi

# ─── Step 3: Ensure target directory tree exists ────────────────────────────

log "Ensuring target directory tree and permissions..."
mkdir -p /opt/velox/current
mkdir -p "$(dirname "$BINARY_DST")"
mkdir -p /opt/velox/current/.velox/data
mkdir -p /opt/velox/current/.velox/secrets/youtube/tokens
mkdir -p "$YOUTUBE_RUNTIME_CREDS_DIR"
chown "$VELOX_USER:$VELOX_GROUP" /opt/velox/current
chmod 755 /opt/velox/current
chown -R "$VELOX_USER:$VELOX_GROUP" /opt/velox/current/.velox
ok "Directory tree ready: /opt/velox/current"

# ─── Step 3b: Sync YouTube OAuth credentials ───────────────────────────────

YOUTUBE_SOURCE_CREDS=""
for candidate in \
    "$DATASERVER_DIR/data/youtube/credentials/credentials.json" \
    "$DATASERVER_DIR/.velox/secrets/youtube/credentials/credentials.json"
do
    if [[ -f "$candidate" ]]; then
        YOUTUBE_SOURCE_CREDS="$candidate"
        break
    fi
done

if [[ -n "$YOUTUBE_SOURCE_CREDS" ]]; then
    log "Syncing YouTube OAuth credentials to runtime..."
    cp "$YOUTUBE_SOURCE_CREDS" "$YOUTUBE_RUNTIME_CREDS_FILE"
    chown "$VELOX_USER:$VELOX_GROUP" "$YOUTUBE_RUNTIME_CREDS_FILE"
    chmod 600 "$YOUTUBE_RUNTIME_CREDS_FILE"
    ok "YouTube OAuth credentials deployed"
else
    warn "YouTube OAuth credentials not found in source tree; runtime will rely on existing files"
fi

# ─── Step 4: Copy binary ────────────────────────────────────────────────────

log "Copying binary to $BINARY_DST..."
cp "$BINARY_SRC" "$BINARY_DST"
chown "$VELOX_USER:$VELOX_GROUP" "$BINARY_DST"
chmod 755 "$BINARY_DST"
ok "Binary deployed"

# ─── Step 5: Deploy systemd service ─────────────────────────────────────────

log "Installing systemd service..."
cp "$SERVICE_SRC" "$SERVICE_DST"
chmod 644 "$SERVICE_DST"
ok "Service file installed: $SERVICE_DST"

# ─── Step 6: Deploy env file (do NOT overwrite existing) ────────────────────

if [[ ! -f "$ENV_DST" ]]; then
    log "Installing env file template..."
    cp "$ENV_TEMPLATE" "$ENV_DST"
    chmod 600 "$ENV_DST"
    chown "$VELOX_USER:$VELOX_GROUP" "$ENV_DST"
    warn "⚠️  Edit $ENV_DST with your configuration before starting the service!"
else
    warn "Env file already exists: $ENV_DST (not overwritten)"
fi

# ─── Step 7: Enable and start ───────────────────────────────────────────────

log "Reloading systemd..."
systemctl daemon-reload

log "Enabling service (will start on boot)..."
systemctl enable "$SERVICE_NAME"

log "Starting service..."
systemctl start "$SERVICE_NAME" || warn "Service failed to start — check: journalctl -u $SERVICE_NAME -f"

# ─── Done ───────────────────────────────────────────────────────────────────

echo ""
echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Install complete!${NC}"
echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "  Service:   $SERVICE_NAME"
echo "  Binary:    $BINARY_DST"
echo "  Config:    $ENV_DST"
echo "  User:      $VELOX_USER"
echo ""
echo "  Status:    systemctl status $SERVICE_NAME"
echo "  Logs:      journalctl -u $SERVICE_NAME -f"
echo "  Restart:   systemctl restart $SERVICE_NAME"
echo "  Stop:      systemctl stop $SERVICE_NAME"
echo ""
echo "  ⚠️  Remember to edit $ENV_DST if this is the first install."
echo ""
