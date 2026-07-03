#!/usr/bin/env bash
# scripts/operator/with-production-env.sh — Source the local production env.
#
# Canonical usage (after this redesign):
#   source scripts/operator/with-production-env.sh
#   ansible-playbook -i inventory/production.ini --check --diff <playbook>
#
#   # or, for `make canonical-dry`, the make target wraps the source + playbook
#   # in a single line that is the only supported invocation path.
#
# Legacy direct-exec usage (still works, will be removed in a future release):
#   scripts/operator/with-production-env.sh <command> [args...]
#   scripts/operator/with-production-env.sh curl -sS "$VELOX_MASTER_URL/health/ready"
#
# Override the env file location:
#   VELOX_PRODUCTION_ENV=/path/to/custom.env source scripts/operator/with-production-env.sh
#
# Agents MUST source this wrapper (or run via `make canonical-dry`) instead of
# `source .velox/production.env` directly. The wrapper enforces:
#   - file exists,
#   - file permissions are 600 (refuses world/group-readable credentials),
#   - the three REQUIRED variables are set after sourcing,
#   - the secret values are never echoed back to the terminal.
# Agents MUST NOT print VELOX_ADMIN_TOKEN, PATs, vault passwords, or SSH keys.
#
# Foot-gun guarded: ansible-playbook -i <script>.sh would parse this file as
# an inventory script and choke on `exec "$@"`. Source here + pass a separate
# inventory path (-i inventory/production.ini).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${VELOX_PRODUCTION_ENV:-${ROOT}/.velox/production.env}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing production environment: $ENV_FILE" >&2
    echo "Create it:" >&2
    echo "  mkdir -p .velox" >&2
    echo "  cat > .velox/production.env <<'EOF'" >&2
    echo "  VELOX_MASTER_HOST=<inserire IP o dominio>" >&2
    echo "  VELOX_MASTER_URL=https://<inserire IP o dominio>" >&2
    echo "  VELOX_ADMIN_TOKEN=<inserire token reale>" >&2
    echo "  GHCR_SERVER_REPOSITORY=ghcr.io/marcuss-ops/velox-server" >&2
    echo "  GHCR_WORKER_REPOSITORY=ghcr.io/marcuss-ops/velox-worker" >&2
    echo "  EOF" >&2
    echo "  chmod 600 .velox/production.env" >&2
    exit 1
fi

# Validate file permissions: refuse to source world-readable credential files.
if [[ "$(uname -s)" != "Darwin" ]]; then
    PERMS=$(stat -c '%a' "$ENV_FILE" 2>/dev/null || echo "000")
    GROUP_READABLE=$(( (PERMS / 10) % 10 ))
    OTHER_READABLE=$(( PERMS % 10 ))
    if [[ $GROUP_READABLE -ge 4 || $OTHER_READABLE -ge 4 ]]; then
        echo "FATAL: $ENV_FILE has overly permissive permissions (${PERMS})." >&2
        echo "Run: chmod 600 $ENV_FILE" >&2
        exit 1
    fi
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# Validate required variables.
: "${VELOX_MASTER_URL:?missing VELOX_MASTER_URL in $ENV_FILE}"
: "${VELOX_ADMIN_TOKEN:?missing VELOX_ADMIN_TOKEN in $ENV_FILE}"
: "${GHCR_SERVER_REPOSITORY:?missing GHCR_SERVER_REPOSITORY in $ENV_FILE}"

# ── LEGACY DIRECT-EXEC PATH (deprecated; will be removed) ────────────────────
# Detect direct execution (not sourcing). When sourced via `source ...`, this
# branch is skipped and the env vars above flow into the calling shell. When
# invoked as `with-production-env.sh <cmd> [args...]`, the BASH_SOURCE/$0
# identity check distinguishes and routes to `exec "$@"`.
# New operators must use `source` (or `make canonical-dry`); direct-exec is
# kept only for back-compat with cron-era scripts.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    # Sourced mode: vars exported, return control to caller.
    return 0
fi

# Direct-exec mode (legacy): print usage if no args, else exec the child.
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <command> [args...]" >&2
    echo "       (prefer: source $0 && ansible-playbook -i inventory/production.ini <args>)" >&2
    exit 1
fi

exec "$@"
