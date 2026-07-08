#!/bin/bash
# Deploy script for Velox Dark Editor (Next.js on port 3001)
# Usage: ./scripts/deploy-dark-editor.sh

set -euo pipefail

DARK_EDITOR_DIR="/home/pierone/Pyt/VeloxLEgit/VeloxFrontend/web/dark_editor"
DEPLOY_DIR="/opt/velox/current/dark_editor"
DATA_SYMLINK_TARGET="/opt/velox/current/.velox/data/dark_editor"
SERVICE="velox-dark-editor"

echo "=== Building Dark Editor ==="
cd "$DARK_EDITOR_DIR"
npm run build

echo "=== Stopping $SERVICE ==="
sudo systemctl stop "$SERVICE" || true

echo "=== Deploying .next ==="
sudo rm -rf "$DEPLOY_DIR/.next"
sudo cp -r "$DARK_EDITOR_DIR/.next" "$DEPLOY_DIR/"

echo "=== Deploying lib ==="
sudo cp -r "$DARK_EDITOR_DIR/lib" "$DEPLOY_DIR/"

echo "=== Setting permissions ==="
sudo chown -R velox:velox "$DEPLOY_DIR"

echo "=== Ensuring data symlink ==="
# Next.js sandboxes routes in different bundles with different process.cwd().
# This symlink makes both upload and GET routes resolve to the same temp directory.
sudo mkdir -p "$DATA_SYMLINK_TARGET/temp" "$DATA_SYMLINK_TARGET/projects"
sudo chown -R velox:velox "$DATA_SYMLINK_TARGET"

if [ ! -L "$DEPLOY_DIR/data" ]; then
    # Backup any existing real data directory (only on first deploy)
    if [ -d "$DEPLOY_DIR/data" ]; then
        BACKUP="$DEPLOY_DIR/data.bak.$(date +%Y%m%d-%H%M)"
        echo "Backing up existing data/ to $BACKUP"
        sudo mv "$DEPLOY_DIR/data" "$BACKUP"
    fi
    echo "Creating symlink: $DEPLOY_DIR/data -> $DATA_SYMLINK_TARGET"
    sudo ln -sf "$DATA_SYMLINK_TARGET" "$DEPLOY_DIR/data"
else
    echo "Symlink already exists: $(readlink -f "$DEPLOY_DIR/data")"
fi

echo "=== Starting $SERVICE ==="
sudo systemctl start "$SERVICE"

sleep 3
echo "=== Status ==="
sudo systemctl status "$SERVICE" --no-pager | head -5

echo ""
echo "=== Health check ==="
curl -s http://localhost:8000/health
echo ""

echo "Done!"
