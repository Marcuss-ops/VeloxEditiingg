#!/bin/bash
# Velox Worker Agent - Install Script
# AGENT 14D - Deploy Cutover & Rollback
#
# Usage: sudo ./deploy/install-worker.sh [options]
#
# Options:
#   --master URL       Master server URL (required)
#   --worker-id ID     Worker ID (auto-generated if not provided)
#   --work-dir DIR     Working directory (default: /opt/velox)
#   --user USER        System user (default: velox)
#   --group GROUP      System group (default: velox)
#   --prefix DIR       Binary installation prefix (default: /usr/local)
#   --dry-run          Preview changes without executing
#   --help             Show this help

set -e

# Defaults
MASTER_URL=""
WORKER_ID=""
WORK_DIR="/opt/velox"
VELOX_USER="velox"
VELOX_GROUP="velox"
PREFIX="/usr/local"
DRY_RUN=false

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --master)
            MASTER_URL="$2"
            shift 2
            ;;
        --worker-id)
            WORKER_ID="$2"
            shift 2
            ;;
        --work-dir)
            WORK_DIR="$2"
            shift 2
            ;;
        --user)
            VELOX_USER="$2"
            shift 2
            ;;
        --group)
            VELOX_GROUP="$2"
            shift 2
            ;;
        --prefix)
            PREFIX="$2"
            shift 2
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

# Validate required arguments
if [[ -z "$MASTER_URL" ]]; then
    echo -e "${RED}Error: --master URL is required${NC}"
    exit 1
fi

# Generate worker ID if not provided
if [[ -z "$WORKER_ID" ]]; then
    WORKER_ID="worker-$(hostname | cut -c1-8)-$(date +%s | tail -c 4)"
fi

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}Velox Worker Agent - Installation${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""
echo -e "Master URL:    ${GREEN}$MASTER_URL${NC}"
echo -e "Worker ID:     ${GREEN}$WORKER_ID${NC}"
echo -e "Work Dir:      ${GREEN}$WORK_DIR${NC}"
echo -e "User/Group:    ${GREEN}$VELOX_USER:$VELOX_GROUP${NC}"
echo -e "Prefix:        ${GREEN}$PREFIX${NC}"
echo -e "Dry Run:       ${YELLOW}$DRY_RUN${NC}"
echo ""

if $DRY_RUN; then
    echo -e "${YELLOW}=== DRY RUN - No changes will be made ===${NC}"
fi

# Function to run commands (or echo in dry-run)
run_cmd() {
    if $DRY_RUN; then
        echo -e "${YELLOW}[DRY-RUN]${NC} $*"
    else
        echo -e "${GREEN}[EXEC]${NC} $*"
        "$@"
    fi
}

# Step 1: Build Go binaries
echo ""
echo -e "${BLUE}Step 1: Building Go binaries...${NC}"
cd "$SCRIPT_DIR"
if [[ ! -f "$SCRIPT_DIR/bin/velox-worker-agent" ]] || ! $DRY_RUN; then
    run_cmd make agent
fi

# Step 2: Create system user/group
echo ""
echo -e "${BLUE}Step 2: Creating system user/group...${NC}"
if ! id "$VELOX_USER" &>/dev/null; then
    run_cmd useradd --system --shell /usr/sbin/nologin --home-dir "$WORK_DIR" "$VELOX_USER"
else
    echo -e "${YELLOW}User $VELOX_USER already exists${NC}"
fi

# Step 3: Create directories
echo ""
echo -e "${BLUE}Step 3: Creating directories...${NC}"
run_cmd mkdir -p "$WORK_DIR"
run_cmd mkdir -p "$WORK_DIR/workspace"
run_cmd mkdir -p "$WORK_DIR/logs"
run_cmd mkdir -p "$WORK_DIR/versions"
run_cmd mkdir -p "$WORK_DIR/backups"

# Step 4: Install binaries
echo ""
echo -e "${BLUE}Step 4: Installing binaries...${NC}"

# Backup existing binary if present
if [[ -f "$PREFIX/bin/velox-worker-agent" ]]; then
    BACKUP_PATH="$WORK_DIR/backups/velox-worker-agent.$(date +%Y%m%d_%H%M%S)"
    run_cmd cp "$PREFIX/bin/velox-worker-agent" "$BACKUP_PATH"
    echo -e "${YELLOW}Backed up existing binary to $BACKUP_PATH${NC}"
fi

run_cmd install -m 755 "$SCRIPT_DIR/bin/velox-worker-agent" "$PREFIX/bin/"

# Step 5: Create worker config
echo ""
echo -e "${BLUE}Step 5: Creating worker configuration...${NC}"
CONFIG_FILE="$WORK_DIR/worker_config.json"

if $DRY_RUN; then
    echo -e "${YELLOW}[DRY-RUN] Would create config at $CONFIG_FILE${NC}"
else
    cat > "$CONFIG_FILE" << EOF
{
    "worker_id": "$WORKER_ID",
    "master_url": "$MASTER_URL",
    "work_dir": "$WORK_DIR",
    "log_level": "info",
    "api_mode": "new_api",
    "enable_command_polling": false
}
EOF
    echo -e "${GREEN}Created config: $CONFIG_FILE${NC}"
fi

# Step 6: Install systemd service
echo ""
echo -e "${BLUE}Step 6: Installing systemd service...${NC}"
SERVICE_FILE="/etc/systemd/system/velox-worker.service"

# Generate service file with proper values
if $DRY_RUN; then
    echo -e "${YELLOW}[DRY-RUN] Would create service file at $SERVICE_FILE${NC}"
else
    cat > "$SERVICE_FILE" << EOF
# Velox Worker Agent - Systemd Service
# Generated by install-worker.sh
# AGENT 14D - Deploy Cutover & Rollback

[Unit]
Description=Velox Worker Agent (Go)
Documentation=https://github.com/velox/worker-agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$VELOX_USER
Group=$VELOX_GROUP
WorkingDirectory=$WORK_DIR

# Go worker as default entrypoint
ExecStart=$PREFIX/bin/velox-worker-agent \\
    -config $WORK_DIR/worker_config.json \\
    -master $MASTER_URL \\
    -work-dir $WORK_DIR

# Environment
Environment=VELOX_WORKER_MODE=go
Environment=GIN_MODE=release

# Restart policy
Restart=always
RestartSec=5

# Resource limits
LimitNOFILE=65536
MemoryMax=4G

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=velox-worker

[Install]
WantedBy=multi-user.target
EOF
    echo -e "${GREEN}Created service file: $SERVICE_FILE${NC}"
    
    run_cmd systemctl daemon-reload
    run_cmd systemctl enable velox-worker
fi

# Step 7: Set permissions
echo ""
echo -e "${BLUE}Step 7: Setting permissions...${NC}"
run_cmd chown -R "$VELOX_USER:$VELOX_GROUP" "$WORK_DIR"

# Summary
echo ""
echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Installation Complete!${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""
echo "Next steps:"
echo "  1. Verify configuration: cat $WORK_DIR/worker_config.json"
echo "  2. Start the worker:     sudo systemctl start velox-worker"
echo "  3. Check status:         sudo systemctl status velox-worker"
echo "  4. View logs:            journalctl -u velox-worker -f"
echo ""
echo "For rollback instructions, see: $SCRIPT_DIR/deploy/ROLLBACK_RUNBOOK.md"
