#!/usr/bin/env bash
# Velox Worker Deploy — thin Ansible wrapper
#
# Delegates all deploy logic to the canonical Ansible playbook.
# No Docker build, SCP, or custom entrypoints — single source of truth.
#
# Usage:
#   ./deploy/deploy-worker.sh [--limit HOST] [--dry-run] [ansible-extra-args...]
#
# Examples:
#   ./deploy/deploy-worker.sh
#   ./deploy/deploy-worker.sh --limit pi1
#   ./deploy/deploy-worker.sh --limit pi1 -e master_url=http://MASTER:8000
#   ./deploy/deploy-worker.sh --check --diff

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

PLAYBOOK="$PROJECT_ROOT/DataServer/data/ansible/playbooks/update_workers.yml"

# Inventory auto-discovery (prefer inventory.ini over legacy JSON)
INVENTORY=""
for candidate in \
    "$PROJECT_ROOT/DataServer/data/ansible/playbooks/inventory.ini" \
    "$PROJECT_ROOT/DataServer/data/ansible/playbooks/inventory.example.ini"
do
    if [ -f "$candidate" ]; then
        INVENTORY="$candidate"
        break
    fi
done

if [ -z "$INVENTORY" ]; then
    echo "ERROR: No inventory found. Create an inventory.ini or inventory.example.ini file."
    exit 1
fi

echo "==> Deploying via: ansible-playbook -i $INVENTORY $PLAYBOOK $*"
exec ansible-playbook -i "$INVENTORY" "$PLAYBOOK" "$@"
