#!/usr/bin/env bash
# deploy/runtime/openbao-fetch-worker-secrets.sh
# ─────────────────────────────────────────────────────────────────────────────
# Risolve worker_credential + coppia mTLS (worker.crt / worker.key / ca.crt)
# di QUESTO worker da OpenBao (KV v2) usando l'AppRole machine identity del
# worker, scrivendoli nei path canonici del bootstrap:
#
#   /etc/velox-worker/secrets/worker_credential   (0600, uid 10001)
#   /etc/velox-worker/certs/worker.crt            (0644, root:10001)
#   /etc/velox-worker/certs/worker.key            (0600, uid 10001)
#   /etc/velox-worker/certs/ca.crt                (0644, root:10001)
#
# CONTRATTO DI EXIT (usato da prepare-host.sh per il fallback):
#   0 = NON configurato (VELOX_OPENBAO_ADDR vuoto → si resta sul file copiato
#       a mano, migrazione non attiva) OPPURE fetch riuscito e file scritti;
#   1 = configurato ma login/fetch fallito → il chiamante decide: ripiega sui
#       file esistenti (se presenti) o fallisce il bootstrap.
#
# Config (via env — prepare-host.sh source-a /etc/velox-worker/worker.env):
#   VELOX_OPENBAO_ADDR            es. https://127.0.0.1:8200 (loopback o tunnel)
#   VELOX_OPENBAO_ROLE_ID_FILE    default /etc/velox-worker/secrets/approle/role-id
#   VELOX_OPENBAO_SECRET_ID_FILE  default /etc/velox-worker/secrets/approle/secret-id
#   VELOX_OPENBAO_CA_FILE         obbligatorio quando OpenBao è configurato:
#                                 cert CA di OpenBao → curl --cacert
#   VELOX_WORKER_ID               identità → KV production/workers/<id>/...
#
# Output dirs override (solo test): VELOX_WORKER_SECRETS_DIR, VELOX_WORKER_CERTS_DIR
#
# Flags:
#   --check   verifica che i file locali siano IDENTICI a OpenBao (sha256),
#             senza scrivere. Exit 0 = coerenti, 1 = mismatch / errore / segreto
#             non ancora migrato. Mai stampa valori: solo sha256 abbreviati.

set -euo pipefail

SECRETS_DIR="${VELOX_WORKER_SECRETS_DIR:-/etc/velox-worker/secrets}"
CERTS_DIR="${VELOX_WORKER_CERTS_DIR:-/etc/velox-worker/certs}"
ROLE_ID_FILE="${VELOX_OPENBAO_ROLE_ID_FILE:-$SECRETS_DIR/approle/role-id}"
SECRET_ID_FILE="${VELOX_OPENBAO_SECRET_ID_FILE:-$SECRETS_DIR/approle/secret-id}"
IMAGE_UID="${IMAGE_UID:-10001}"
IMAGE_GID="${IMAGE_GID:-10001}"

CHECK=0
for arg in "$@"; do
    case "$arg" in
        --check) CHECK=1 ;;
        -h|--help) sed -n '2,44p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

log() { printf '[openbao-fetch] %s\n' "$*"; }
fail() { printf '[openbao-fetch] FATAL: %s\n' "$*" >&2; exit 1; }

# ── 0. Non configurato → fallback sul file (migrazione non attiva) ──────────
# NB: questo check sta PRIMA dei prerequisite tool: un host senza curl/jq e
# senza VELOX_OPENBAO_ADDR deve comunque uscire 0 (contratto di prepare-host).
if [[ -z "${VELOX_OPENBAO_ADDR:-}" ]]; then
    if [[ "$CHECK" == "1" ]]; then
        fail "VELOX_OPENBAO_ADDR non impostato — impossibile verificare la coerenza con OpenBao (migrazione non attiva su questo host)"
    fi
    log "VELOX_OPENBAO_ADDR non impostato — si usa il file copiato a mano (fallback)"
    exit 0
fi

command -v curl >/dev/null 2>&1 || fail "curl not found on PATH"
command -v jq   >/dev/null 2>&1 || fail "jq not found on PATH"

ADDR="${VELOX_OPENBAO_ADDR%/}"
WORKER_ID="${VELOX_WORKER_ID:-}"
[[ -n "$WORKER_ID" ]] || fail "VELOX_WORKER_ID non impostato (necessario per il path KV)"
[[ -f "$ROLE_ID_FILE" && -s "$ROLE_ID_FILE" ]] || fail "role-id mancante: $ROLE_ID_FILE"
[[ -f "$SECRET_ID_FILE" && -s "$SECRET_ID_FILE" ]] || fail "secret-id mancante: $SECRET_ID_FILE"

if [[ "$ADDR" == https://* ]]; then
    [[ -n "${VELOX_OPENBAO_CA_FILE:-}" && -s "$VELOX_OPENBAO_CA_FILE" ]] ||
        fail "VELOX_OPENBAO_CA_FILE mancante o vuoto — TLS verification obbligatoria"
    CURL_TLS=(--cacert "$VELOX_OPENBAO_CA_FILE")
elif [[ "$ADDR" == http://* && "${VELOX_OPENBAO_ALLOW_INSECURE_HTTP_TEST:-0}" == "1" ]]; then
    # Only the repository's HTTP mock tests may opt into plaintext transport.
    CURL_TLS=()
else
    fail "VELOX_OPENBAO_ADDR must use https:// (HTTP is test-only and requires VELOX_OPENBAO_ALLOW_INSECURE_HTTP_TEST=1)"
fi

KV_ROOT="velox/data/production/workers/$WORKER_ID"
# kvpath:file:mode:owner — permessi canonici (RW-PROD-001 A2)
CERT_SPECS=(
    "cert/cert:worker.crt:0644:root"
    "cert/key:worker.key:0600:${IMAGE_UID}"
    "cert/ca:ca.crt:0644:root"
)

# ── 1. Login AppRole → token (mai stampato) ─────────────────────────────────
login_token() {
    jq -n --arg r "$(cat "$ROLE_ID_FILE")" --arg s "$(cat "$SECRET_ID_FILE")" \
        '{role_id: $r, secret_id: $s}' |
        curl -fsS "${CURL_TLS[@]}" -X POST \
            -H 'Content-Type: application/json' \
            --data-binary @- \
            "$ADDR/v1/auth/approle/login" 2>/dev/null |
        jq -r '.auth.client_token // empty' 2>/dev/null
}

# ── 2. Lettura di un secret KV v2 → stdout il valore ────────────────────────
# Ritorna: 0 = valore letto (stampato su stdout), 1 = errore, 3 = 404 (non
# provisionato). Da chiamare solo in contesto `if` (mai con set -e attivo).
kv_read() {
    local path="$1" out code val
    out="$(mktemp)"
    code="$(curl -sS "${CURL_TLS[@]}" -H "X-Vault-Token: $TOKEN" \
        -o "$out" -w '%{http_code}' \
        "$ADDR/v1/$KV_ROOT/$path" 2>/dev/null || echo 000)"
    if [[ "$code" == "404" ]]; then rm -f "$out"; return 3; fi
    if [[ "$code" != "200" ]]; then rm -f "$out"; return 1; fi
    val="$(jq -r '.data.data.value // empty' "$out" 2>/dev/null || true)"
    rm -f "$out"
    [[ -n "$val" ]] || return 1
    printf '%s' "$val"
    return 0
}

# kv_get_once: cattura valore+rc in una sola chiamata (evita doppio fetch)
#   $1 = path → imposta VAL (globale) e KV_RC
kv_get_once() {
    if VAL="$(kv_read "$1")"; then KV_RC=0; else KV_RC=$?; fi
}

write_atomic() {
    # $1 = file, $2 = contenuto, $3 = mode, $4 = owner(uid):group
    local file="$1" content="$2" mode="$3" owner="$4" tmp
    tmp="$(mktemp "${file}.XXXXXX")"
    printf '%s' "$content" > "$tmp"
    chmod "$mode" "$tmp"
    if [[ $EUID -eq 0 ]]; then
        chown "${owner}:${IMAGE_GID}" "$tmp"
    fi
    mv "$tmp" "$file"
    if [[ $EUID -eq 0 ]]; then
        chown "${owner}:${IMAGE_GID}" "$file" 2>/dev/null || true
    fi
}

sha() { printf '%s' "$1" | sha256sum | awk '{print $1}'; }

# ── 3. Login + credential ────────────────────────────────────────────────────
log "login AppRole verso $ADDR ..."
if ! TOKEN="$(login_token)"; then
    fail "login AppRole fallito verso $ADDR"
fi
[[ -n "$TOKEN" ]] || fail "login AppRole restituito senza token"

kv_get_once "credential"
[[ "$KV_RC" == "0" ]] || fail "secret workers/$WORKER_ID/credential non leggibile (migrazione KV incompleta?)"
CRED="$VAL"

# ── 4. --check: confronto sha256 locale vs OpenBao, nessuna scrittura ───────
if [[ "$CHECK" == "1" ]]; then
    rc=0
    local_sha="$(sha256sum "$SECRETS_DIR/worker_credential" 2>/dev/null | awk '{print $1}' || true)"
    remote_sha="$(sha "$CRED")"
    if [[ -z "$local_sha" ]]; then
        log "FAIL credential: file locale mancante (migrazione non completata)"
        rc=1
    elif [[ "$local_sha" != "$remote_sha" ]]; then
        log "FAIL credential: locale != OpenBao (${local_sha:0:12} vs ${remote_sha:0:12})"
        rc=1
    else
        log "OK   credential: coerente (sha256 ${remote_sha:0:12}...)"
    fi
    for spec in "${CERT_SPECS[@]}"; do
        kvpath="${spec%%:*}"; rest="${spec#*:}"; file="${rest%%:*}"; rest2="${rest#*:}"; mode="${rest2%%:*}"; owner="${spec##*:}"
        local_f="$CERTS_DIR/$file"
        kv_get_once "$kvpath"
        if [[ "$KV_RC" == "3" ]]; then
            if [[ -f "$local_f" ]]; then
                log "WARN $file: presente localmente ma NON in OpenBao (migrazione cert incompleta)"
            else
                log "OK   $file: assente sia localmente sia in OpenBao"
            fi
            continue
        fi
        if [[ "$KV_RC" != "0" ]]; then
            log "FAIL $file: lettura OpenBao fallita (verifica non riuscita)"
            rc=1
            continue
        fi
        remote_cert_sha="$(sha "$VAL")"
        local_cert_sha="$(sha256sum "$local_f" 2>/dev/null | awk '{print $1}' || true)"
        if [[ -z "$local_cert_sha" ]]; then
            log "FAIL $file: file locale mancante (migrazione non completata)"
            rc=1
        elif [[ "$local_cert_sha" != "$remote_cert_sha" ]]; then
            log "FAIL $file: locale != OpenBao (${local_cert_sha:0:12} vs ${remote_cert_sha:0:12})"
            rc=1
        else
            log "OK   $file: coerente (sha256 ${remote_cert_sha:0:12}...)"
        fi
    done
    if [[ "$rc" != "0" ]]; then
        fail "verify-openbao: mismatch rilevati (migrazione incompleta?)"
    fi
    log "verify-openbao: OK — file locali coerenti con OpenBao"
    exit 0
fi

# ── 5. Scrittura atomica con permessi canonici ──────────────────────────────
mkdir -p "$SECRETS_DIR" "$CERTS_DIR"
if [[ $EUID -eq 0 ]]; then
    chown "root:${IMAGE_GID}" "$SECRETS_DIR" "$CERTS_DIR" 2>/dev/null || true
    chmod 0750 "$SECRETS_DIR" "$CERTS_DIR" 2>/dev/null || true
fi

write_atomic "$SECRETS_DIR/worker_credential" "$CRED" 0600 "$IMAGE_UID"
log "scritto worker_credential (0600 uid $IMAGE_UID) sha256 $(sha "$CRED" | cut -c1-12)..."

for spec in "${CERT_SPECS[@]}"; do
    kvpath="${spec%%:*}"; rest="${spec#*:}"; file="${rest%%:*}"; rest2="${rest#*:}"; mode="${rest2%%:*}"; owner="${spec##*:}"
    kv_get_once "$kvpath"
    if [[ "$KV_RC" == "3" ]]; then
        log "skip $file: non provisionato in OpenBao (migrazione cert parziale)"
        continue
    fi
    if [[ "$KV_RC" != "0" ]]; then
        fail "lettura $kvpath fallita"
    fi
    write_atomic "$CERTS_DIR/$file" "$VAL" "$mode" "$owner"
    log "scritto $file ($mode)"
done

log "done — worker secrets risolti da OpenBao (AppRole $WORKER_ID)"
