#!/usr/bin/env bash
# scripts/operator/deploy-production.sh
#
# Canonical production path:
#   OpenBao → temporary Ansible extra-vars → local Ansible convergence.
#
# The default is --check. Mutating the master requires the explicit --apply
# flag. No secret is read from or written to GitHub/Ansible Vault.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OPENBAO_ADDR="${OPENBAO_ADDR:-https://127.0.0.1:18200}"
OPENBAO_CA_FILE="${OPENBAO_CA_FILE:-$ROOT/.velox/openbao/tls/server.crt}"
OPENBAO_ROLE_ID_FILE="${OPENBAO_ROLE_ID_FILE:-$ROOT/.velox/openbao/approle/master/role-id}"
OPENBAO_SECRET_ID_FILE="${OPENBAO_SECRET_ID_FILE:-$ROOT/.velox/openbao/approle/master/secret-id}"
SSH_KEY="${VELOX_MASTER_SSH_KEY:-$HOME/.ssh/id_ed25519}"
SSH_USER="${VELOX_MASTER_SSH_USER:-pierone}"
INVENTORY=""
VARS_FILE=""
MODE="check"

fail() { printf '[deploy-production] FATAL: %s\n' "$*" >&2; exit 1; }
log() { printf '[deploy-production] %s\n' "$*"; }

usage() {
    sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check) MODE=check; shift ;;
        --apply) MODE=apply; shift ;;
        -h|--help) usage 0 ;;
        *) fail "unknown option: $1" ;;
    esac
done

command -v ansible-playbook >/dev/null 2>&1 || fail "ansible-playbook non trovato"
[[ -s "$OPENBAO_CA_FILE" ]] || fail "OpenBao CA mancante: $OPENBAO_CA_FILE"
[[ -s "$OPENBAO_ROLE_ID_FILE" ]] || fail "Master AppRole role-id mancante"
[[ -s "$OPENBAO_SECRET_ID_FILE" ]] || fail "Master AppRole secret-id mancante"
[[ -r "$SSH_KEY" ]] || fail "chiave SSH master mancante: $SSH_KEY"

: "${VELOX_MASTER_HOST:?impostare VELOX_MASTER_HOST}"
: "${VELOX_SERVER_IMAGE:?impostare VELOX_SERVER_IMAGE con digest sha256}"
: "${VELOX_VERSION:?impostare VELOX_VERSION}"
: "${VELOX_MASTER_PUBLIC_URL:?impostare VELOX_MASTER_PUBLIC_URL}"
: "${VELOX_GRPC_CONTROL_ENDPOINT:?impostare VELOX_GRPC_CONTROL_ENDPOINT}"
: "${VELOX_ALLOWED_WORKERS:?impostare VELOX_ALLOWED_WORKERS CSV}"
[[ "$VELOX_SERVER_IMAGE" == *'@sha256:'* ]] || fail "VELOX_SERVER_IMAGE deve essere un riferimento pinned @sha256"

INVENTORY="$(mktemp)"
VARS_FILE="$(mktemp)"
trap 'rm -f "$INVENTORY" "$VARS_FILE"' EXIT
chmod 600 "$INVENTORY" "$VARS_FILE"

cat >"$INVENTORY" <<EOF
[velox_master]
velox-master ansible_host=${VELOX_MASTER_HOST} ansible_user=${SSH_USER} ansible_ssh_private_key_file=${SSH_KEY}

[velox_master:vars]
ansible_python_interpreter=/usr/bin/python3
EOF

OPENBAO_VARS_FILE="$VARS_FILE" \
OPENBAO_ROLE_ID_FILE="$OPENBAO_ROLE_ID_FILE" \
OPENBAO_SECRET_ID_FILE="$OPENBAO_SECRET_ID_FILE" \
OPENBAO_CA_FILE="$OPENBAO_CA_FILE" \
OPENBAO_ADDR="$OPENBAO_ADDR" \
    bash "$ROOT/deploy/openbao/scripts/resolve-master-tokens.sh" --require-all

ANSIBLE_ARGS=(
    -i "$INVENTORY"
    -e "@${VARS_FILE}"
    -e "velox_server_image=${VELOX_SERVER_IMAGE}"
    -e "velox_version=${VELOX_VERSION}"
    -e "velox_master_public_url=${VELOX_MASTER_PUBLIC_URL}"
    -e "velox_grpc_control_endpoint=${VELOX_GRPC_CONTROL_ENDPOINT}"
    -e "velox_allowed_workers=${VELOX_ALLOWED_WORKERS}"
)

if [[ "$MODE" == check ]]; then
    # Do not use --diff: the rendered master env contains OpenBao values and
    # Ansible would print them in the task diff. Check mode remains read-only.
    ANSIBLE_ARGS+=(--check)
    log "dry-run locale OpenBao → Ansible verso $VELOX_MASTER_HOST"
else
    log "apply locale OpenBao → Ansible verso $VELOX_MASTER_HOST"
fi

ANSIBLE_ARGS+=("$ROOT/deploy/playbooks/deploy-master-config.yml")
ANSIBLE_CONFIG="$ROOT/deploy/ansible.cfg" ansible-playbook "${ANSIBLE_ARGS[@]}"
log "deploy production completato (mode=$MODE)"
