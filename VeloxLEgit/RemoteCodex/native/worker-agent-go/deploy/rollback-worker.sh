#!/bin/bash
# Velox Worker Agent - Rollback Script
#
# Rolls back the Go worker binary to a previous backup version.
#
# Usage: sudo ./deploy/rollback-worker.sh [options]
#
# Options:
#   --version N        Rollback to N-th most recent backup (default: 1 = latest)
#   --force            Skip confirmation prompts
#   --dry-run          Preview changes without executing
#   --help             Show this help

set -e

# Defaults
VERSION=1
FORCE=false
DRY_RUN=false
WORK_DIR="/opt/velox"
PREFIX="/usr/local"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help)
            sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# //'
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Check root
if [[ $EUID -ne 0 ]] && ! $DRY_RUN; then
    echo -e "${RED}Error: This script must be run as root${NC}"
    exit 1
fi

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}Velox Worker Agent - Rollback${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""

# Function to run commands
run_cmd() {
    if $DRY_RUN; then
        echo -e "${YELLOW}[DRY-RUN]${NC} $*"
    else
        echo -e "${GREEN}[EXEC]${NC} $*"
        "$@"
    fi
}

# Check service status
SERVICE_ACTIVE=false
if systemctl is-active --quiet velox-worker 2>/dev/null; then
    SERVICE_ACTIVE=true
fi

# List available backups
echo -e "${BLUE}Available Go binary backups:${NC}"
BACKUPS=()
if [[ -d "$WORK_DIR/backups" ]]; then
    while IFS= read -r -d '' file; do
        BACKUPS+=("$file")
    done < <(find "$WORK_DIR/backups" -name "velox-worker-agent.*" -print0 | sort -rz)
fi

if [[ ${#BACKUPS[@]} -eq 0 ]]; then
    echo -e "${RED}Error: No Go binary backups available for rollback${NC}"
    exit 1
fi

for i in "${!BACKUPS[@]}"; do
    echo "  [$((i+1))] $(basename "${BACKUPS[$i]}")"
done

# Validate version number
if [[ $VERSION -lt 1 || $VERSION -gt ${#BACKUPS[@]} ]]; then
    echo -e "${RED}Error: Backup version $VERSION not available (max: ${#BACKUPS[@]})${NC}"
    exit 1
fi

TARGET_BACKUP="${BACKUPS[$((VERSION-1))]}"
echo ""
echo -e "${YELLOW}Rollback target: $(basename "$TARGET_BACKUP")${NC}"

# Confirmation
if ! $FORCE && ! $DRY_RUN; then
    echo ""
    read -p "Proceed with rollback? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Rollback cancelled"
        exit 0
    fi
fi

echo ""
echo -e "${BLUE}=== Starting Rollback ===${NC}"

# Step 1: Stop service
echo ""
echo -e "${BLUE}Step 1: Stopping service...${NC}"
if $SERVICE_ACTIVE; then
    run_cmd systemctl stop velox-worker
fi

# Step 2: Restore binary
echo ""
echo -e "${BLUE}Step 2: Restoring binary...${NC}"
if $DRY_RUN; then
    echo -e "${YELLOW}[DRY-RUN] Would restore: $TARGET_BACKUP${NC}"
else
    run_cmd cp "$TARGET_BACKUP" "$PREFIX/bin/velox-worker-agent"
    run_cmd chmod 755 "$PREFIX/bin/velox-worker-agent"
    echo -e "${GREEN}Restored binary from: $TARGET_BACKUP${NC}"
fi

# Step 3: Reload systemd
echo ""
echo -e "${BLUE}Step 3: Reloading systemd...${NC}"
if ! $DRY_RUN; then
    run_cmd systemctl daemon-reload
fi

# Step 4: Start service
echo ""
echo -e "${BLUE}Step 4: Starting service...${NC}"
if ! $DRY_RUN; then
    run_cmd systemctl start velox-worker
    sleep 2
fi

# Step 5: Verify
echo ""
echo -e "${BLUE}Step 5: Verifying...${NC}"
if ! $DRY_RUN; then
    if systemctl is-active --quiet velox-worker; then
        echo -e "${GREEN}Service is running${NC}"

        # Show recent logs
        echo ""
        echo -e "${BLUE}Recent logs:${NC}"
        journalctl -u velox-worker -n 5 --no-pager
    else
        echo -e "${RED}Service failed to start${NC}"
        echo -e "${YELLOW}Check logs: journalctl -u velox-worker -n 50${NC}"
        exit 1
    fi
fi

# Summary
echo ""
echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Rollback Complete!${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""
echo "Restored: $(basename "$TARGET_BACKUP")"
echo ""
echo "Next steps:"
echo "  1. Check status:  sudo systemctl status velox-worker"
echo "  2. View logs:     journalctl -u velox-worker -f"
echo "  3. Verify service health and worker registration"
echo ""

if $DRY_RUN; then
    echo -e "${YELLOW}=== DRY RUN COMPLETE - No changes were made ===${NC}"
fi
