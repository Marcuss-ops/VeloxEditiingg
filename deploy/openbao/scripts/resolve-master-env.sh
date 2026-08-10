#!/usr/bin/env bash
# deploy/openbao/scripts/resolve-master-env.sh
# ─────────────────────────────────────────────────────────────────────────────
# Canonical Master runtime-env resolver (replaces resolve-master-tokens.sh).
#
#   Master AppRole → OpenBao → short-lived token → KV v2 → runtime env
#   materialization → /etc/velox-server.env (installed by
#   scripts/operator/deploy-production.sh)
#
# It renders the COMPLETE master env file:
#   - secrets:       OpenBao KV v2 (velox/production/master/*)
#   - non-secret:    operator env inputs (VELOX_* below) with canonical
#                    defaults that mirror the retired group_vars/all.yml.
#
# NO Ansible, NO vault_velox_* extra-vars, NO 0600 YAML: the output is a plain
# KEY=VALUE env file (0600) ready for systemd EnvironmentFile= consumption.
# The old `vault_velox_*` naming is gone — every secret materializes directly
# under its canonical runtime key (VELOX_ADMIN_TOKEN, INSTAEDIT_CONTROL_JWT_SECRET,
# SOCIAL_API_TOKEN, SOCIAL_WEBHOOK_SECRET, VELOX_COMMIT_HMAC_KEY).
#
# CONTRATTO DI EXIT:
#   0 = env file materializzato e scritto (0600);
#   1 = OpenBao non configurato, login/lettura fallita o required mancante.
#
# Config (env):
#   OPENBAO_ADDR                    es. https://127.0.0.1:8200 (obbligatorio)
#   OPENBAO_CA_FILE                 cert CA OpenBao (obbligatorio con https)
#   OPENBAO_ROLE_ID / OPENBAO_SECRET_ID            (valori diretti)
#   OPENBAO_ROLE_ID_FILE / OPENBAO_SECRET_ID_FILE  (0600; default
#                                   $ROOT/.velox/openbao/approle/master/)
#   OPENBAO_ENV_FILE                path dell'env file da scrivere (obbligatorio)
#
# Non-secret config (env inputs, defaults canonici):
#   VELOX_MASTER_PORT=8000  VELOX_GRPC_PORT=9000
#   VELOX_RUNTIME_DIR=/var/lib/velox  VELOX_DATA_DIR=/var/lib/velox/data
#   VELOX_DB_PATH=/var/lib/velox/data/velox.db  VELOX_VIDEOS_DIR=/var/lib/velox/videos
#   VELOX_SECRETS_DIR=/etc/velox/secrets  VELOX_MAX_JOB_ATTEMPTS=3
#   VELOX_WORKER_HEARTBEAT_TIMEOUT=120  VELOX_COMPATIBILITY_MODE=strict
#   VELOX_SOCIAL_API_TIMEOUT_MS=30000
#   VELOX_SOCIAL_ARTIFACT_PUBLIC_URL=
#   VELOX_DRIVE_CLIENT_ID / VELOX_DRIVE_CLIENT_SECRET / VELOX_DRIVE_REDIRECT_URI
#   VELOX_SMOKE_DRIVE_FOLDER_ID
#   VELOX_ALLOWED_WORKER_IPS (optional exact IP/CIDR allowlist for worker REST calls)
#   VELOX_GRPC_TLS_CERT_FILE / VELOX_GRPC_TLS_KEY_FILE / VELOX_GRPC_TLS_CA_FILE
#   VELOX_GRPC_ALLOW_INSECURE_DEV (true = opt-in esplicito per il dev)
#   (obbligatori, senza default: VELOX_MASTER_PUBLIC_URL,
#    VELOX_GRPC_CONTROL_ENDPOINT, VELOX_VERSION, VELOX_SERVER_IMAGE,
#    VELOX_ALLOWED_WORKERS, VELOX_SOCIAL_API_URL, VELOX_SOCIAL_CALLBACK_BASE_URL)
#   TLS gate (stesso contratto di deploy/validate-master-env.sh): la tripletta
#   VELOX_GRPC_TLS_{CERT,KEY,CA}_FILE oppure VELOX_GRPC_ALLOW_INSECURE_DEV=true
#   è obbligatoria, altrimenti il resolver esce 1 senza scrivere nulla.
#
# Flag: --require-all  esci 1 se uno dei required (admin, instaedit, social)
#                       è assente nel KV.
# Mai stampa valori: logga solo i nomi + "ok". L'env file è 0600.

set -euo pipefail

REQUIRE_ALL=0
for arg in "$@"; do
    case "$arg" in
        --require-all) REQUIRE_ALL=1 ;;
        -h|--help) sed -n '2,52p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

log() { printf '[resolve-master-env] %s\n' "$*"; }
fail() { printf '[resolve-master-env] FATAL: %s\n' "$*" >&2; exit 1; }

# ── 0. OpenBao è obbligatorio ───────────────────────────────────────────────
if [[ -z "${OPENBAO_ADDR:-}" ]]; then
    fail "OPENBAO_ADDR non impostato — il resolver richiede OpenBao (nessun fallback Vault)"
fi

command -v curl >/dev/null 2>&1 || fail "curl not found on PATH"
command -v jq   >/dev/null 2>&1 || fail "jq not found on PATH"

ADDR="${OPENBAO_ADDR%/}"
[[ -n "${OPENBAO_ENV_FILE:-}" ]] || fail "OPENBAO_ENV_FILE non impostato (path dell'env file da scrivere)"
ENV_FILE="$OPENBAO_ENV_FILE"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
ROLE_ID=""
SECRET_ID=""
if [[ -n "${OPENBAO_ROLE_ID:-}" && -n "${OPENBAO_SECRET_ID:-}" ]]; then
    ROLE_ID="$OPENBAO_ROLE_ID"; SECRET_ID="$OPENBAO_SECRET_ID"
elif [[ -n "${OPENBAO_ROLE_ID_FILE:-}" && -n "${OPENBAO_SECRET_ID_FILE:-}" ]]; then
    ROLE_ID="$(cat "$OPENBAO_ROLE_ID_FILE")"; SECRET_ID="$(cat "$OPENBAO_SECRET_ID_FILE")"
else
    MASTER_APPROLE="$ROOT/.velox/openbao/approle/master"
    [[ -s "$MASTER_APPROLE/role-id" && -s "$MASTER_APPROLE/secret-id" ]] ||
        fail "materiale AppRole master mancante (OPENBAO_ROLE_ID_FILE+OPENBAO_SECRET_ID_FILE oppure $MASTER_APPROLE/)"
    ROLE_ID="$(cat "$MASTER_APPROLE/role-id")"; SECRET_ID="$(cat "$MASTER_APPROLE/secret-id")"
fi

if [[ "$ADDR" == https://* ]]; then
    [[ -n "${OPENBAO_CA_FILE:-}" && -s "$OPENBAO_CA_FILE" ]] ||
        fail "OPENBAO_CA_FILE mancante o vuoto — TLS verification obbligatoria"
    CURL_TLS=(--cacert "$OPENBAO_CA_FILE")
elif [[ "$ADDR" == http://* && "${OPENBAO_ALLOW_INSECURE_HTTP_TEST:-0}" == "1" ]]; then
    # Only the repository's HTTP mock tests may opt into plaintext transport.
    CURL_TLS=()
else
    fail "OPENBAO_ADDR must use https:// (HTTP is test-only and requires OPENBAO_ALLOW_INSECURE_HTTP_TEST=1)"
fi

# ── 1. Login AppRole → short-lived token (mai stampato) ─────────────────────
TOKEN="$(jq -n --arg r "$ROLE_ID" --arg s "$SECRET_ID" '{role_id: $r, secret_id: $s}' |
    curl -fsS "${CURL_TLS[@]}" -X POST \
        -H 'Content-Type: application/json' \
        --data-binary @- \
        "$ADDR/v1/auth/approle/login" 2>/dev/null |
    jq -r '.auth.client_token // empty' 2>/dev/null || true)"
[[ -n "$TOKEN" ]] || fail "login AppRole master fallito verso $ADDR"

# ── 2. Lettura KV v2 → valore (404 = non provisionato) ──────────────────────
KV_HTTP_CODE=""  # ultimo codice HTTP di kv_get (per messaggi di errore)
kv_get() {
    # $1 = path sotto velox/data/... → stdout il valore; 0 = ok, 3 = 404, 1 = errore
    local path="$1" out code val
    out="$(mktemp)"
    code="$(curl -sS "${CURL_TLS[@]}" -H "X-Vault-Token: $TOKEN" \
        -o "$out" -w '%{http_code}' "$ADDR/v1/velox/data/$path" 2>/dev/null || echo 000)"
    KV_HTTP_CODE="$code"
    if [[ "$code" == "404" ]]; then rm -f "$out"; return 3; fi
    if [[ "$code" != "200" ]]; then rm -f "$out"; return 1; fi
    val="$(jq -r '.data.data.value // empty' "$out" 2>/dev/null || true)"
    rm -f "$out"
    [[ -n "$val" ]] || return 1
    printf '%s' "$val"
    return 0
}

# kv_once: cattura valore+rc in una sola chiamata (globals VAL / KV_RC)
kv_once() {
    if VAL="$(kv_get "$1")"; then KV_RC=0; else KV_RC=$?; fi
}

# ── 3. Manifest segreti: path KV | chiave runtime | required ────────────────
# Le chiavi runtime sono i nomi canonici consumati da velox-server. Rispetto
# al vecchio resolver è AGGIUNTO VELOX_COMMIT_HMAC_KEY (leaf già provisionato
# in provision-kv.sh; consumato da DataServer/cmd/server/bootstrap_assets.go).
# registry username/token non hanno consumatore runtime → non materializzati.
MANIFEST=(
    "production/master/admin-token|VELOX_ADMIN_TOKEN|1"
    "production/master/instaedit-control-jwt-secret|INSTAEDIT_CONTROL_JWT_SECRET|1"
    "production/master/social-api-token|SOCIAL_API_TOKEN|1"
    "production/master/social-webhook-secret|SOCIAL_WEBHOOK_SECRET|0"
    "production/master/commit-hmac-key|VELOX_COMMIT_HMAC_KEY|0"
)

# ── 4. Non-secret config (env inputs + default canonici) ────────────────────
VELOX_MASTER_PORT="${VELOX_MASTER_PORT:-8000}"
VELOX_GRPC_PORT="${VELOX_GRPC_PORT:-9000}"
VELOX_RUNTIME_DIR="${VELOX_RUNTIME_DIR:-/var/lib/velox}"
VELOX_DATA_DIR="${VELOX_DATA_DIR:-/var/lib/velox/data}"
VELOX_DB_PATH="${VELOX_DB_PATH:-/var/lib/velox/data/velox.db}"
VELOX_VIDEOS_DIR="${VELOX_VIDEOS_DIR:-/var/lib/velox/videos}"
VELOX_SECRETS_DIR="${VELOX_SECRETS_DIR:-/etc/velox/secrets}"
VELOX_MAX_JOB_ATTEMPTS="${VELOX_MAX_JOB_ATTEMPTS:-3}"
VELOX_WORKER_HEARTBEAT_TIMEOUT="${VELOX_WORKER_HEARTBEAT_TIMEOUT:-120}"
VELOX_COMPATIBILITY_MODE="${VELOX_COMPATIBILITY_MODE:-strict}"
VELOX_SOCIAL_API_TIMEOUT_MS="${VELOX_SOCIAL_API_TIMEOUT_MS:-30000}"
VELOX_SOCIAL_API_URL="${VELOX_SOCIAL_API_URL:-}"
VELOX_SOCIAL_CALLBACK_BASE_URL="${VELOX_SOCIAL_CALLBACK_BASE_URL:-}"
VELOX_SOCIAL_ARTIFACT_PUBLIC_URL="${VELOX_SOCIAL_ARTIFACT_PUBLIC_URL:-}"
VELOX_DRIVE_CLIENT_ID="${VELOX_DRIVE_CLIENT_ID:-}"
VELOX_DRIVE_CLIENT_SECRET="${VELOX_DRIVE_CLIENT_SECRET:-}"
VELOX_DRIVE_REDIRECT_URI="${VELOX_DRIVE_REDIRECT_URI:-}"
VELOX_SMOKE_DRIVE_FOLDER_ID="${VELOX_SMOKE_DRIVE_FOLDER_ID:-}"
VELOX_ALLOWED_WORKER_IPS="${VELOX_ALLOWED_WORKER_IPS:-}"

for req in VELOX_MASTER_PUBLIC_URL VELOX_GRPC_CONTROL_ENDPOINT VELOX_VERSION \
    VELOX_SERVER_IMAGE VELOX_ALLOWED_WORKERS VELOX_SOCIAL_API_URL \
    VELOX_SOCIAL_CALLBACK_BASE_URL; do
    [[ -n "${!req:-}" ]] || fail "config obbligatoria mancante: $req (impostala via env)"
done

# ── 4b. TLS gate — stesso contratto di deploy/validate-master-env.sh ─────────
# Produzione richiede la tripletta TLS O l'opt-in esplicito INSECURE_DEV=true.
# Il resolver rifiuta di materializzare un env che il validatore rigetterebbe.
TLS_CERT="${VELOX_GRPC_TLS_CERT_FILE:-}"
TLS_KEY="${VELOX_GRPC_TLS_KEY_FILE:-}"
TLS_CA="${VELOX_GRPC_TLS_CA_FILE:-}"
INSECURE_DEV="${VELOX_GRPC_ALLOW_INSECURE_DEV:-}"
if [[ -n "$TLS_CERT" || -n "$TLS_KEY" || -n "$TLS_CA" ]]; then
    [[ -n "$TLS_CERT" && -n "$TLS_KEY" && -n "$TLS_CA" ]] || \
        fail "TLS triple incompleta: servono tutte e tre VELOX_GRPC_TLS_{CERT,KEY,CA}_FILE"
    [[ "${INSECURE_DEV,,}" != "true" ]] || \
        fail "VELOX_GRPC_ALLOW_INSECURE_DEV=true non ammesso insieme alla tripletta TLS"
elif [[ "${INSECURE_DEV,,}" != "true" ]]; then
    fail "nessuna TLS configurata E VELOX_GRPC_ALLOW_INSECURE_DEV!=true — produzione richiede la tripletta TLS o l'opt-in esplicito (stesso gate di deploy/validate-master-env.sh)"
fi

# ── 5. Risoluzione segreti + render env ─────────────────────────────────────
declare -A SECRETS=()
missing_required=0
for entry in "${MANIFEST[@]}"; do
    path="${entry%%|*}"; rest="${entry#*|}"; key="${rest%%|*}"; required="${rest##*|}"
    kv_once "$path"
    if [[ "$KV_RC" == "3" ]]; then
        log "skip $key (non provisionato in OpenBao: $path)"
        [[ "$required" == "1" && "$REQUIRE_ALL" == "1" ]] && missing_required=1
        continue
    fi
    if [[ "$KV_RC" != "0" ]]; then
        fail "lettura fallita: $path (HTTP ${KV_HTTP_CODE:-unknown} — policy non provisionata su OpenBao? server giù?)"
    fi
    # Guardia EnvironmentFile (systemd): valori con whitespace o iniziali #/;
    # corromperebbero il parsing — meglio fallire subito con messaggio chiaro.
    if printf '%s' "$VAL" | grep -qE '[[:space:]]|^[#;]'; then
        fail "valore di $path contiene caratteri non sicuri per un env file systemd (whitespace o iniziale #/;) — ruota il segreto in OpenBao"
    fi
    SECRETS["$key"]="$VAL"
    log "risolto $key <- $path"
done

if [[ "$missing_required" == "1" ]]; then
    fail "required secret mancanti nel KV (--require-all) — materializzazione bloccata"
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
chmod 600 "$tmp"

render() { printf '%s=%s\n' "$1" "${2:-}"; }

{
    printf '# /etc/velox-server.env — materializzato da deploy/openbao/scripts/resolve-master-env.sh\n'
    printf '# Master AppRole → OpenBao → short-lived token → KV v2 → runtime env.\n'
    printf '# NON modificare a mano: la prossima materializzazione sovrascrive.\n\n'
    printf '# ── Server ────────────────────────────────────────────────────────\n'
    render VELOX_MASTER_PORT "$VELOX_MASTER_PORT"
    render GIN_MODE "release"
    render VELOX_CONTROL_PLANE_REST_PUBLIC_URL "$VELOX_MASTER_PUBLIC_URL"
    render VELOX_CONTROL_PLANE_REST_INTERNAL_URL "http://127.0.0.1:$VELOX_MASTER_PORT"
    render VELOX_CONTROL_PLANE_GRPC_URL "$VELOX_GRPC_CONTROL_ENDPOINT"
    render VELOX_CODE_VERSION "$VELOX_VERSION"
    render VELOX_SERVER_IMAGE "$VELOX_SERVER_IMAGE"
    printf '\n# ── gRPC Control Channel ────────────────────────────────────────\n'
    render VELOX_GRPC_PORT "$VELOX_GRPC_PORT"
    render VELOX_GRPC_PUSH_MODE "true"
    if [[ -n "$TLS_CERT" ]]; then
        render VELOX_GRPC_TLS_CERT_FILE "$TLS_CERT"
        render VELOX_GRPC_TLS_KEY_FILE "$TLS_KEY"
        render VELOX_GRPC_TLS_CA_FILE "$TLS_CA"
    fi
    if [[ "${INSECURE_DEV,,}" == "true" ]]; then
        render VELOX_GRPC_ALLOW_INSECURE_DEV "true"
    fi
    printf '\n# ── Storage paths ───────────────────────────────────────────────\n'
    render VELOX_RUNTIME_DIR "$VELOX_RUNTIME_DIR"
    render VELOX_DATA_DIR "$VELOX_DATA_DIR"
    render VELOX_DB_PATH "$VELOX_DB_PATH"
    render VELOX_VIDEOS_DIR "$VELOX_VIDEOS_DIR"
    render VELOX_SECRETS_DIR "$VELOX_SECRETS_DIR"
    printf '\n# ── Security ───────────────────────────────────────────────────\n'
    render VELOX_ADMIN_TOKEN "${SECRETS[VELOX_ADMIN_TOKEN]:-}"
    printf '\n# ── Worker Policy ──────────────────────────────────────────────\n'
    render VELOX_ALLOWED_WORKERS "$VELOX_ALLOWED_WORKERS"
    render VELOX_ALLOWED_WORKER_IPS "$VELOX_ALLOWED_WORKER_IPS"
    render VELOX_MAX_JOB_ATTEMPTS "$VELOX_MAX_JOB_ATTEMPTS"
    render VELOX_WORKER_HEARTBEAT_TIMEOUT "$VELOX_WORKER_HEARTBEAT_TIMEOUT"
    printf '\n# ── Compatibility migration policy ─────────────────────────────\n'
    render VELOX_COMPATIBILITY_MODE "$VELOX_COMPATIBILITY_MODE"
    printf '\n# ── InstaEdit Control JWT (InstaEdit→Velox identity) ────────────\n'
    render INSTAEDIT_CONTROL_JWT_SECRET "${SECRETS[INSTAEDIT_CONTROL_JWT_SECRET]:-}"
    printf '\n# ── External Social API (delivery destination) ─────────────────\n'
    render SOCIAL_API_URL "$VELOX_SOCIAL_API_URL"
    render SOCIAL_API_TOKEN "${SECRETS[SOCIAL_API_TOKEN]:-}"
    render SOCIAL_API_TIMEOUT_MS "$VELOX_SOCIAL_API_TIMEOUT_MS"
    render SOCIAL_CALLBACK_BASE_URL "$VELOX_SOCIAL_CALLBACK_BASE_URL"
    render SOCIAL_ARTIFACT_PUBLIC_URL "$VELOX_SOCIAL_ARTIFACT_PUBLIC_URL"
    render SOCIAL_WEBHOOK_SECRET "${SECRETS[SOCIAL_WEBHOOK_SECRET]:-}"

    printf '\n# ── Google Drive / Level-D smoke ────────────────────────────────\n'
    render VELOX_DRIVE_CLIENT_ID "$VELOX_DRIVE_CLIENT_ID"
    render VELOX_DRIVE_CLIENT_SECRET "$VELOX_DRIVE_CLIENT_SECRET"
    render VELOX_DRIVE_REDIRECT_URI "$VELOX_DRIVE_REDIRECT_URI"
    render VELOX_SMOKE_DRIVE_FOLDER_ID "$VELOX_SMOKE_DRIVE_FOLDER_ID"
    printf '\n# ── Completion coordinator HMAC (optional) ─────────────────────\n'
    render VELOX_COMMIT_HMAC_KEY "${SECRETS[VELOX_COMMIT_HMAC_KEY]:-}"
} > "$tmp"

# Fail-closed: il naming legacy vault_velox_* NON deve mai comparire nell'output.
if grep -q 'vault_velox_' "$tmp"; then
    fail "legacy vault_velox_* naming leaked into the materialized env — refusing to write"
fi

mkdir -p "$(dirname "$ENV_FILE")"
mv "$tmp" "$ENV_FILE"
chmod 600 "$ENV_FILE"
log "env file scritto: $ENV_FILE (0600) — installalo come /etc/velox-server.env e riavvia velox-server"
log "done"
