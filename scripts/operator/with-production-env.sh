#!/usr/bin/env bash
# with-production-env.sh — Source the local production environment and invoke a command.
#
# Usage:
#   scripts/operator/with-production-env.sh bash ops/jobs/submit_jackie_chan_doc_voiceover_clips.sh
#   scripts/operator/with-production-env.sh curl -sS "$VELOX_MASTER_URL/health/ready"
#
# Override the env file location:
#   VELOX_PRODUCTION_ENV=/path/to/custom.env scripts/operator/with-production-env.sh ...
#
# Agents MUST use this wrapper or explicitly source .velox/production.env.
# Agents MUST NOT print VELOX_ADMIN_TOKEN, PATs, vault passwords, or SSH keys.

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

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <command> [args...]" >&2
    exit 1
fi

exec "$@"
