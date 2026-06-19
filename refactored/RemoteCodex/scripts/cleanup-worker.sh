#!/usr/bin/env bash
set -euo pipefail

MASTER_URL="${MASTER_URL:-http://127.0.0.1:8000}"
VELOX_DIR="${VELOX_DIR:-/opt/velox}"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "=== VELOX WORKER FULL CLEANUP ==="
log "MASTER_URL: ${MASTER_URL} (override with MASTER_URL=… if not on the master host)"

# 1) Stop and remove all velox systemd units
log "Removing systemd units..."
for f in /etc/systemd/system/velox-worker*.service /etc/systemd/system/velox-worker*.timer /etc/systemd/system/velox-auto-update.*; do
  [ ! -f "$f" ] && continue
  BASE=$(basename "$f")
  systemctl stop "$BASE" 2>/dev/null || true
  systemctl disable "$BASE" 2>/dev/null || true
  rm -f "$f"
  log "  Removed unit: $BASE"
done
for d in /etc/systemd/system/velox-worker*.service.d; do
  [ ! -d "$d" ] && continue
  rm -rf "$d"
  log "  Removed override dir: $(basename "$d")"
done
systemctl daemon-reload 2>/dev/null || true

# 2) Remove all velox Docker containers
log "Removing Docker containers..."
for c in $(docker ps -a --filter "name=velox" --format "{{.Names}}" 2>/dev/null); do
  docker rm -f "$c" 2>/dev/null || true
  log "  Removed container: $c"
done

# 3) Remove velox Docker images
log "Removing Docker images..."
docker images --filter "reference=velox-worker*" -q 2>/dev/null | xargs -r docker rmi 2>/dev/null || true
docker rmi velox-worker-console:latest 2>/dev/null || true
log "  Images removed"

# 4) Remove old bundle and cache files
log "Removing old files..."
rm -rf "$VELOX_DIR/current/refactored"
rm -rf "$VELOX_DIR/cache/"*
rm -f "$VELOX_DIR/worker_config.json"
rm -f "$VELOX_DIR/velox-installer"
rm -f /etc/velox-worker.env
log "  Files removed"

# 5) Verify clean state
log "=== CLEAN STATE ==="
log "Systemd units: $(ls /etc/systemd/system/velox-worker*.service 2>/dev/null | wc -l) remaining"
log "Docker containers: $(docker ps -a --filter 'name=velox' --format '{{.Names}}' 2>/dev/null | wc -l) remaining"
log "Docker images: $(docker images --filter 'reference=velox-worker*' -q 2>/dev/null | wc -l) remaining"
log "$VELOX_DIR/current: $(ls "$VELOX_DIR/current/" 2>/dev/null || echo 'empty')"
log "=== CLEANUP DONE ==="
