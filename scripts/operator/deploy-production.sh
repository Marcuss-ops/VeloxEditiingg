#!/usr/bin/env bash
# scripts/operator/deploy-production.sh
# ─────────────────────────────────────────────────────────────────────────────
# Canonical production entrypoint (no Ansible — the legacy rollout was
# retired):
#
#   Master AppRole → OpenBao → short-lived token → KV v2 → runtime env
#   (deploy/openbao/scripts/resolve-master-env.sh) → local validator →
#   SSH convergence on the master host (/etc/velox-server.env + restart).
#
# The default is --check: it resolves the env locally, runs the canonical
# validator (deploy/validate-master-env.sh) and probes SSH — read-only,
# nothing is mutated. Mutating the master requires the explicit --apply flag.
#
# Safety: --apply refuses (fail-closed) if the CURRENT /etc/velox-server.env
# has keys the resolver would silently drop (es. VELOX_ALLOWED_WORKER_IPS,
# chiavi legacy) — the operator must either extend the resolver manifest or
# set VELOX_ALLOW_DROP_EXTRA_KEYS=1 to force. This prevents wiping
# operator-only settings on convergence.
#
# No secret is persisted on the operator machine: the rendered env lives in
# a transient mktemp file (0600) removed on exit. No GitHub/Ansible Vault is
# ever read or written.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESOLVER="$ROOT/deploy/openbao/scripts/resolve-master-env.sh"
VALIDATOR="$ROOT/deploy/validate-master-env.sh"
VALIDATOR_LIB="$ROOT/deploy/scripts/lib-validations.sh"

OPENBAO_ADDR="${OPENBAO_ADDR:-https://127.0.0.1:18200}"
OPENBAO_CA_FILE="${OPENBAO_CA_FILE:-$ROOT/.velox/openbao/tls/server.crt}"
OPENBAO_ROLE_ID_FILE="${OPENBAO_ROLE_ID_FILE:-$ROOT/.velox/openbao/approle/master/role-id}"
OPENBAO_SECRET_ID_FILE="${OPENBAO_SECRET_ID_FILE:-$ROOT/.velox/openbao/approle/master/secret-id}"
SSH_KEY="${VELOX_MASTER_SSH_KEY:-$HOME/.ssh/id_ed25519}"
SSH_USER="${VELOX_MASTER_SSH_USER:-pierone}"
MODE="check"

fail() { printf '[deploy-production] FATAL: %s\n' "$*" >&2; exit 1; }
log()  { printf '[deploy-production] %s\n' "$*"; }

usage() {
    sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
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

[[ -x "$RESOLVER" ]]   || fail "resolver non trovato: $RESOLVER"
[[ -r "$VALIDATOR" ]]  || fail "validator non trovato: $VALIDATOR"
[[ -r "$VALIDATOR_LIB" ]] || fail "validator lib mancante: $VALIDATOR_LIB"
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
: "${VELOX_SOCIAL_API_URL:?impostare VELOX_SOCIAL_API_URL}"
: "${VELOX_SOCIAL_CALLBACK_BASE_URL:?impostare VELOX_SOCIAL_CALLBACK_BASE_URL}"
[[ "$VELOX_SERVER_IMAGE" == *'@sha256:'* ]] || fail "VELOX_SERVER_IMAGE deve essere un riferimento pinned @sha256"

# Input opzionali del resolver (TLS gate) — pass-through, mai obbligatori qui:
# il resolver applica lo stesso contratto di validate-master-env.sh.
export VELOX_GRPC_TLS_CERT_FILE="${VELOX_GRPC_TLS_CERT_FILE:-}"
export VELOX_GRPC_TLS_KEY_FILE="${VELOX_GRPC_TLS_KEY_FILE:-}"
export VELOX_GRPC_TLS_CA_FILE="${VELOX_GRPC_TLS_CA_FILE:-}"
export VELOX_GRPC_ALLOW_INSECURE_DEV="${VELOX_GRPC_ALLOW_INSECURE_DEV:-}"

ENV_TMP="$(mktemp)"
trap 'rm -f "$ENV_TMP"' EXIT
chmod 600 "$ENV_TMP"

log "resolve OpenBao → master env (mode=$MODE)"
OPENBAO_ADDR="$OPENBAO_ADDR" \
OPENBAO_CA_FILE="$OPENBAO_CA_FILE" \
OPENBAO_ROLE_ID_FILE="$OPENBAO_ROLE_ID_FILE" \
OPENBAO_SECRET_ID_FILE="$OPENBAO_SECRET_ID_FILE" \
OPENBAO_ENV_FILE="$ENV_TMP" \
VELOX_MASTER_PUBLIC_URL="$VELOX_MASTER_PUBLIC_URL" \
VELOX_GRPC_CONTROL_ENDPOINT="$VELOX_GRPC_CONTROL_ENDPOINT" \
VELOX_VERSION="$VELOX_VERSION" \
VELOX_SERVER_IMAGE="$VELOX_SERVER_IMAGE" \
VELOX_ALLOWED_WORKERS="$VELOX_ALLOWED_WORKERS" \
VELOX_SOCIAL_API_URL="$VELOX_SOCIAL_API_URL" \
VELOX_SOCIAL_CALLBACK_BASE_URL="$VELOX_SOCIAL_CALLBACK_BASE_URL" \
    bash "$RESOLVER" --require-all

log "validate env (locale, blocking)"
# Il validatore è tracciato non-eseguibile (0644): lo si invoca via `bash`,
# come fa deploy/install-server.sh. Sul master (dopo l'install con mode 0755)
# viene eseguito direttamente.
bash "$VALIDATOR" "$ENV_TMP"

SSH_ARGS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)
TARGET="$SSH_USER@$VELOX_MASTER_HOST"

if [[ "$MODE" == check ]]; then
    log "check: env risolto e validato localmente; nessuna mutazione remota"
    if ssh "${SSH_ARGS[@]}" "$TARGET" 'true' >/dev/null 2>&1; then
        log "check: SSH verso $TARGET OK (probe read-only)"
    else
        fail "check: SSH verso $TARGET non raggiungibile"
    fi
    log "deploy production dry-run completato (mode=check)"
    exit 0
fi

log "apply: convergenza SSH verso $TARGET"
REMOTE_DIR="$(ssh "${SSH_ARGS[@]}" "$TARGET" 'mktemp -d /tmp/velox-deploy.XXXXXX')"
[[ -n "$REMOTE_DIR" ]] || fail "impossibile creare dir temporanea sul master"
trap 'ssh "${SSH_ARGS[@]}" "$TARGET" "rm -rf \"$REMOTE_DIR\"" >/dev/null 2>&1 || true; rm -f "$ENV_TMP"' EXIT

scp -q -i "$SSH_KEY" "$ENV_TMP" "$TARGET:$REMOTE_DIR/env"
scp -q -i "$SSH_KEY" "$VALIDATOR" "$VALIDATOR_LIB" "$TARGET:$REMOTE_DIR/"

ALLOW_DROP="${VELOX_ALLOW_DROP_EXTRA_KEYS:-0}"
ssh "${SSH_ARGS[@]}" "$TARGET" "set -e
ALLOW_DROP='$ALLOW_DROP'
# Fail-closed: mai rimuovere silenziosamente chiavi che il resolver non
# materializza (es. VELOX_ALLOWED_WORKER_IPS, chiavi legacy).
if [[ -r /etc/velox-server.env ]]; then
  grep -oE '^[A-Za-z_][A-Za-z0-9_]*=' /etc/velox-server.env | tr -d '=' | sort -u > '$REMOTE_DIR/old-keys'
  grep -oE '^[A-Za-z_][A-Za-z0-9_]*=' '$REMOTE_DIR/env' | tr -d '=' | sort -u > '$REMOTE_DIR/new-keys'
  comm -23 '$REMOTE_DIR/old-keys' '$REMOTE_DIR/new-keys' > '$REMOTE_DIR/extra-keys'
  if [[ -s '$REMOTE_DIR/extra-keys' ]]; then
    echo '[deploy-production][remote] chiavi presenti SOLO nel vecchio env (il resolver le rimuoverebbe):'
    cat '$REMOTE_DIR/extra-keys'
    if [[ \"\$ALLOW_DROP\" != \"1\" ]]; then
      echo '[deploy-production][remote] FATAL: --apply interrotto prima di rimuoverle — estendi il manifest del resolver oppure setta VELOX_ALLOW_DROP_EXTRA_KEYS=1 per forzare' >&2
      exit 1
    fi
  fi
fi
sudo install -o root -g velox -m 0640 '$REMOTE_DIR/env' /etc/velox-server.env
sudo mkdir -p /opt/velox/current/deploy/scripts
sudo install -o root -m 0755 '$REMOTE_DIR/validate-master-env.sh' /opt/velox/current/deploy/validate-master-env.sh
sudo install -o root -m 0755 '$REMOTE_DIR/lib-validations.sh' /opt/velox/current/deploy/scripts/lib-validations.sh
sudo /opt/velox/current/deploy/validate-master-env.sh /etc/velox-server.env
sudo systemctl restart velox-server
OK=0
for _ in \$(seq 1 30); do
  if systemctl is-active --quiet velox-server; then OK=1; break; fi
  sleep 2
done
[[ \"\$OK\" == \"1\" ]] || { echo '[deploy-production][remote] FATAL: velox-server non attivo dopo il restart' >&2; exit 1; }
rm -rf '$REMOTE_DIR'
echo '[deploy-production][remote] velox-server attivo e validato'"

log "deploy production completato (mode=apply)"
