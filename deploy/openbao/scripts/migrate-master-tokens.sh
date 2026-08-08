#!/usr/bin/env bash
# deploy/openbao/scripts/migrate-master-tokens.sh
# ─────────────────────────────────────────────────────────────────────────────
# Migrazione ONE-WAY dei token del MASTER da /etc/velox-server.env (o dal file
# indicato con --env-file) nel KV OpenBao:
#
#   velox/production/master/admin-token            (VELOX_ADMIN_TOKEN)
#   velox/production/master/instaedit-control-jwt-secret (INSTAEDIT_CONTROL_JWT_SECRET)
#   velox/production/master/social-api-token       (SOCIAL_API_TOKEN)
#   velox/production/master/social-webhook-secret  (SOCIAL_WEBHOOK_SECRET, opz.)
#   velox/production/master/commit-hmac-key        (VELOX_COMMIT_HMAC_KEY, opz.)
#   velox/production/services/registry/*           (--registry-username/--registry-token o env)
#
# Usa provision-kv.sh --force (nuova versione KV). Precedenza: env
# OPENBAO_VALUE_* > --env-file > /etc/velox-server.env. MAI stampa valori.
# Fail-closed: un required (admin, instaedit, social) assente → exit 1.
#
# Usage:
#   ./scripts/migrate-master-tokens.sh                        # da /etc/velox-server.env
#   ./scripts/migrate-master-tokens.sh --env-file ./server.env --dry-run
#   ./scripts/migrate-master-tokens.sh --registry-username u --registry-token t
#
# Dopo la migrazione: verifica con ./scripts/verify-kv.sh e materializza l'env
# del master con ./scripts/resolve-master-env.sh (vedi scripts/operator/deploy-production.sh).

set -euo pipefail

OPENBAO_DIR="${OPENBAO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ENV_FILE="/etc/velox-server.env"
REGISTRY_USERNAME=""
REGISTRY_TOKEN=""
DRY_RUN=0

usage() {
    sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --env-file)         ENV_FILE="${2:-}"; shift 2 ;;
        --registry-username) REGISTRY_USERNAME="${2:-}"; shift 2 ;;
        --registry-token)   REGISTRY_TOKEN="${2:-}"; shift 2 ;;
        --dry-run)          DRY_RUN=1; shift ;;
        -h|--help)          usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done

log() { printf '[migrate-master-tokens] %s\n' "$*"; }
fail() { printf '[migrate-master-tokens] FATAL: %s\n' "$*" >&2; exit 1; }

# Estrae NAME=valore dal file env (righe attive, quote rimosse) — MAI su stdout.
env_val() {
    local file="$1" key="$2"
    awk -v k="$key" '
        /^[[:space:]]*#/ { next }
        /^[[:space:]]*$/ { next }
        $0 ~ "^[[:space:]]*" k "=" {
            sub("^[[:space:]]*" k "=", "")
            sub("^\"", ""); sub("\"$", "")
            sub("^\x27", ""); sub("\x27$", "")
            print
            exit
        }
    ' "$file"
}

ARGS=(--force)
[[ "$DRY_RUN" == "1" ]] && ARGS=(--dry-run)

# ── 1. Token master (required) ──────────────────────────────────────────────
declare -A SRC
SRC[ADMIN_TOKEN]=""
SRC[INSTAEDIT_JWT]=""
SRC[SOCIAL_API_TOKEN]=""

if [[ -r "$ENV_FILE" ]]; then
    SRC[ADMIN_TOKEN]="$(env_val "$ENV_FILE" VELOX_ADMIN_TOKEN)"
    SRC[INSTAEDIT_JWT]="$(env_val "$ENV_FILE" INSTAEDIT_CONTROL_JWT_SECRET)"
    SRC[SOCIAL_API_TOKEN]="$(env_val "$ENV_FILE" SOCIAL_API_TOKEN)"
fi
# env espliciti vincono
[[ -n "${OPENBAO_VALUE_ADMIN_TOKEN:-}" ]]   && SRC[ADMIN_TOKEN]="$OPENBAO_VALUE_ADMIN_TOKEN"
[[ -n "${OPENBAO_VALUE_INSTAEDIT_JWT:-}" ]] && SRC[INSTAEDIT_JWT]="$OPENBAO_VALUE_INSTAEDIT_JWT"
[[ -n "${OPENBAO_VALUE_SOCIAL_API_TOKEN:-}" ]] && SRC[SOCIAL_API_TOKEN]="$OPENBAO_VALUE_SOCIAL_API_TOKEN"

if [[ -z "${SRC[ADMIN_TOKEN]}" || -z "${SRC[INSTAEDIT_JWT]}" || -z "${SRC[SOCIAL_API_TOKEN]}" ]]; then
    fail "token required mancanti (VELOX_ADMIN_TOKEN / INSTAEDIT_CONTROL_JWT_SECRET / SOCIAL_API_TOKEN) — nessuna migrazione eseguita"
fi

# ── 2. Opzionali ────────────────────────────────────────────────────────────
WEBHOOK="$(env_val "$ENV_FILE" SOCIAL_WEBHOOK_SECRET 2>/dev/null || true)"
[[ -n "${OPENBAO_VALUE_SOCIAL_WEBHOOK_SECRET:-}" ]] && WEBHOOK="$OPENBAO_VALUE_SOCIAL_WEBHOOK_SECRET"
HMAC="$(env_val "$ENV_FILE" VELOX_COMMIT_HMAC_KEY 2>/dev/null || true)"
[[ -n "${OPENBAO_VALUE_COMMIT_HMAC_KEY:-}" ]] && HMAC="$OPENBAO_VALUE_COMMIT_HMAC_KEY"
[[ -n "$REGISTRY_USERNAME" ]] || REGISTRY_USERNAME="${OPENBAO_VALUE_REGISTRY_USERNAME:-}"
[[ -n "$REGISTRY_TOKEN" ]] || REGISTRY_TOKEN="${OPENBAO_VALUE_REGISTRY_TOKEN:-}"

# ── 3. Provisioning (mai stampare i valori) ─────────────────────────────────
ENV_CMD=(env)
for pair in \
    "OPENBAO_VALUE_ADMIN_TOKEN=${SRC[ADMIN_TOKEN]}" \
    "OPENBAO_VALUE_INSTAEDIT_JWT=${SRC[INSTAEDIT_JWT]}" \
    "OPENBAO_VALUE_SOCIAL_API_TOKEN=${SRC[SOCIAL_API_TOKEN]}"; do
    ENV_CMD+=("$pair")
done
[[ -n "$WEBHOOK" ]] && ENV_CMD+=("OPENBAO_VALUE_SOCIAL_WEBHOOK_SECRET=$WEBHOOK")
[[ -n "$HMAC" ]] && ENV_CMD+=("OPENBAO_VALUE_COMMIT_HMAC_KEY=$HMAC")
[[ -n "$REGISTRY_USERNAME" ]] && ENV_CMD+=("OPENBAO_VALUE_REGISTRY_USERNAME=$REGISTRY_USERNAME")
[[ -n "$REGISTRY_TOKEN" ]] && ENV_CMD+=("OPENBAO_VALUE_REGISTRY_TOKEN=$REGISTRY_TOKEN")

log "sorgente: $ENV_FILE (o env) — provisioning master + registry nel KV (--force, nessun valore stampato)"
"${ENV_CMD[@]}" "$OPENBAO_DIR/scripts/provision-kv.sh" "${ARGS[@]}"
log "done — verifica con ./scripts/verify-kv.sh e materializza l'env master via ./scripts/resolve-master-env.sh"
