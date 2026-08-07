#!/usr/bin/env bash
# deploy/openbao/scripts/resolve-master-tokens.sh
# ─────────────────────────────────────────────────────────────────────────────
# Risolve i token del MASTER da OpenBao (KV v2) via AppRole e li scrive come
# **Ansible extra-vars** in un file YAML 0600, con i nomi vault_velox_*
# compatibili con deploy/templates/velox-server.env.j2:
#
#   velox/production/master/admin-token               → vault_velox_admin_token
#   velox/production/master/instaedit-control-jwt-secret → vault_velox_instaedit_control_jwt_secret
#   velox/production/master/social-api-token          → vault_velox_social_api_token
#   velox/production/master/social-webhook-secret     → vault_velox_social_webhook_secret (opz.)
#   velox/production/services/registry/username       → vault_velox_registry_username (opz.)
#   velox/production/services/registry/token          → vault_velox_registry_token (opz.)
#
# Il file viene passato ad ansible-playbook con `-e @<file>`: gli extra-vars
# VINCONO su group_vars → il deploy rende i token da OpenBao. Senza il file,
# ansible usa vault.yml come oggi (fallback legacy).
#
# CONTRATTO DI EXIT (per la CI deploy.yml):
#   0 = NON configurato (OPENBAO_ADDR vuoto → nessun vars file, flusso vault
#       legacy invariato) OPPURE risolto e vars file scritto (0600);
#   1 = configurato ma login/lettura fallito O un required mancante con
#       --require-all → la CI fallisce invece di deployare con token mancanti.
#
# Config (env):
#   OPENBAO_ADDR             es. https://127.0.0.1:8200
#   OPENBAO_ROLE_ID / OPENBAO_SECRET_ID           (valori, es. secrets CI)
#   OPENBAO_ROLE_ID_FILE / OPENBAO_SECRET_ID_FILE (file 0600, es. host)
#   OPENBAO_CA_FILE          opzionale: cert CA di OpenBao (--cacert)
#   OPENBAO_VARS_FILE        path del vars file da scrivere (obbligatorio)
#   OPENBAO_SKIP_VERIFY      default true (cert self-signed del listener)
#
# Flag: --require-all  esci 1 se uno dei required (admin, instaedit, social)
#                       è assente nel KV.
# Mai stampa valori: logga solo i nomi + "ok". Il vars file è 0600.

set -euo pipefail

REQUIRE_ALL=0
for arg in "$@"; do
    case "$arg" in
        --require-all) REQUIRE_ALL=1 ;;
        -h|--help) sed -n '2,38p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

log() { printf '[resolve-master-tokens] %s\n' "$*"; }
fail() { printf '[resolve-master-tokens] FATAL: %s\n' "$*" >&2; exit 1; }

# ── 0. Non configurato → flusso vault legacy (nessun vars file) ─────────────
if [[ -z "${OPENBAO_ADDR:-}" ]]; then
    log "OPENBAO_ADDR non impostato — i token master restano da ansible-vault (fallback legacy)"
    exit 0
fi

command -v curl >/dev/null 2>&1 || fail "curl not found on PATH"
command -v jq   >/dev/null 2>&1 || fail "jq not found on PATH"

ADDR="${OPENBAO_ADDR%/}"
[[ -n "${OPENBAO_VARS_FILE:-}" ]] || fail "OPENBAO_VARS_FILE non impostato (path del vars file da scrivere)"
VARS_FILE="$OPENBAO_VARS_FILE"

ROLE_ID=""
SECRET_ID=""
if [[ -n "${OPENBAO_ROLE_ID:-}" && -n "${OPENBAO_SECRET_ID:-}" ]]; then
    ROLE_ID="$OPENBAO_ROLE_ID"; SECRET_ID="$OPENBAO_SECRET_ID"
elif [[ -n "${OPENBAO_ROLE_ID_FILE:-}" && -n "${OPENBAO_SECRET_ID_FILE:-}" ]]; then
    ROLE_ID="$(cat "$OPENBAO_ROLE_ID_FILE")"; SECRET_ID="$(cat "$OPENBAO_SECRET_ID_FILE")"
else
    fail "materiale AppRole mancante (OPENBAO_ROLE_ID+OPENBAO_SECRET_ID oppure *_FILE)"
fi

CURL_TLS=(-k)
if [[ -n "${OPENBAO_CA_FILE:-}" && -f "$OPENBAO_CA_FILE" ]]; then
    CURL_TLS=(--cacert "$OPENBAO_CA_FILE")
else
    log "ATTENZIONE: TLS senza verifica (-k) — imposta OPENBAO_CA_FILE per pinnare la CA di OpenBao"
fi

# ── 1. Login AppRole → token (mai stampato) ─────────────────────────────────
TOKEN="$(jq -n --arg r "$ROLE_ID" --arg s "$SECRET_ID" '{role_id: $r, secret_id: $s}' |
    curl -fsS "${CURL_TLS[@]}" -X POST \
        -H 'Content-Type: application/json' \
        --data-binary @- \
        "$ADDR/v1/auth/approle/login" 2>/dev/null |
    jq -r '.auth.client_token // empty' 2>/dev/null || true)"
[[ -n "$TOKEN" ]] || fail "login AppRole fallito verso $ADDR"

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

# ── 3. Manifest: path KV | nome extra-var | required ────────────────────────
MANIFEST=(
    "production/master/admin-token|vault_velox_admin_token|1"
    "production/master/instaedit-control-jwt-secret|vault_velox_instaedit_control_jwt_secret|1"
    "production/master/social-api-token|vault_velox_social_api_token|1"
    "production/master/social-webhook-secret|vault_velox_social_webhook_secret|0"
    "production/services/registry/username|vault_velox_registry_username|0"
    "production/services/registry/token|vault_velox_registry_token|0"
)

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
chmod 600 "$tmp"

missing_required=0
for entry in "${MANIFEST[@]}"; do
    path="${entry%%|*}"; rest="${entry#*|}"; name="${rest%%|*}"; required="${rest##*|}"
    kv_once "$path"
    if [[ "$KV_RC" == "3" ]]; then
        log "skip $name (non provisionato in OpenBao: $path)"
        [[ "$required" == "1" && "$REQUIRE_ALL" == "1" ]] && missing_required=1
        continue
    fi
    if [[ "$KV_RC" != "0" ]]; then
        fail "lettura fallita: $path (HTTP ${KV_HTTP_CODE:-unknown} — policy non provisionata su OpenBao? server giù?)"
    fi
    # YAML-safe quoting via JSON (sottoinsieme YAML): valori con ", \, $, :, ecc.
    # NB: uso l'idioma `jq -Rs .` — `$v | tojson` SENZA -r verrebbe ri-serializzato
    # da jq 1.8 ("\"val\"" invece di "val"). -Rs . emette la stringa JSON diretta.
    printf '%s: %s\n' "$name" "$(printf '%s' "$VAL" | jq -Rs .)" >> "$tmp"
    log "risolto $name <- $path"
done

if [[ "$missing_required" == "1" ]]; then
    fail "required token mancanti nel KV (--require-all) — migrazione incompleta, deploy bloccato"
fi

if [[ ! -s "$tmp" ]]; then
    fail "nessun secret risolto — vars file vuoto (KV vuoto?)"
fi

mkdir -p "$(dirname "$VARS_FILE")"
mv "$tmp" "$VARS_FILE"
chmod 600 "$VARS_FILE"
log "vars file scritto: $VARS_FILE (0600) — passalo ad ansible-playbook con -e @$VARS_FILE"
log "done"
