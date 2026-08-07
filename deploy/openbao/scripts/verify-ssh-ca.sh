#!/usr/bin/env bash
# deploy/openbao/scripts/verify-ssh-ca.sh
# ─────────────────────────────────────────────────────────────────────────────
# End-to-end verification of the OpenBao SSH CA:
#
#   1. il secrets engine `ssh` è attivo;
#   2. la CA è configurata (public_key presente — la private key resta in OpenBao);
#   3. il role `velox-operator` esiste con key_type=ca, allow_user_certificates,
#      allowed_users = velox-admin,velox-deploy (CSV) e ttl breve atteso.
#      NB: i principals ammessi nei cert derivano da allowed_users (il role
#      non persiste un campo valid_principals separato);
#   4. firma di PROVA reale: genera una chiave ed25519 temporanea, la firma,
#      valida il cert con `ssh-keygen -L` (principals + intervallo di validità);
#   5. check NEGATIVO fail-closed: firma con un principal non consentito
#      (es. root) deve essere RIFIUTATA (HTTP 400/403) — se la firma riesce,
#      FAIL.
#
# FAIL-CLOSED: un errore di verifica (curl/jq falliti) fa FALLIRE i check,
# mai passare. Non stampa mai chiavi private (quella di prova è distrutta).
# Esce 1 al primo check fallito.
#
# Usage:
#   ./scripts/verify-ssh-ca.sh [--role <name>]

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
TOKEN_FILE="$STATE_DIR/root-token"

ROLE="${OPENBAO_SSH_ROLE:-velox-operator}"
FAILED=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --role) ROLE="${2:-}"; shift 2 ;;
        -h|--help) echo "usage: $0 [--role <name>]"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

command -v curl >/dev/null 2>&1 || { echo "FATAL: 'curl' not found on PATH" >&2; exit 1; }
command -v jq   >/dev/null 2>&1 || { echo "FATAL: 'jq' not found on PATH" >&2; exit 1; }

if [[ -z "${BAO_TOKEN:-}" ]]; then
    [[ -f "$TOKEN_FILE" ]] || {
        echo "FATAL: no BAO_TOKEN and $TOKEN_FILE missing — run bootstrap-init.sh first" >&2
        exit 1
    }
    BAO_TOKEN="$(cat "$TOKEN_FILE")"
    export BAO_TOKEN
fi

TLS_CERT_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
[[ -s "$TLS_CERT_FILE" ]] || {
    echo "FATAL: OpenBao TLS CA certificate missing: $TLS_CERT_FILE" >&2
    exit 1
}
curl_tls=(--cacert "$TLS_CERT_FILE")
export BAO_CACERT="$TLS_CERT_FILE"

check() {
    # $1 = descrizione, $2 = esito (0 ok / 1 ko)
    if [[ "$2" == "0" ]]; then
        echo "  PASS  $1"
    else
        echo "  FAIL  $1"
        FAILED=1
    fi
}

api() {
    local method="$1" path="$2" body="${3:-}"
    if [[ -n "$body" ]]; then
        curl -fsS "${curl_tls[@]}" -X "$method" \
            -H "X-Vault-Token: $BAO_TOKEN" -H 'Content-Type: application/json' \
            --data-binary "$body" "$ADDR/v1/$path"
    else
        curl -fsS "${curl_tls[@]}" -X "$method" \
            -H "X-Vault-Token: $BAO_TOKEN" "$ADDR/v1/$path"
    fi
}

api_code() {
    local method="$1" path="$2" body="${3:-}" code
    if [[ -n "$body" ]]; then
        code="$(curl -sS "${curl_tls[@]}" -X "$method" \
            -H "X-Vault-Token: $BAO_TOKEN" -H 'Content-Type: application/json' \
            --data-binary "$body" -o /dev/null -w '%{http_code}' \
            "$ADDR/v1/$path" 2>/dev/null || echo 000)"
    else
        code="$(curl -sS "${curl_tls[@]}" -X "$method" \
            -H "X-Vault-Token: $BAO_TOKEN" -o /dev/null -w '%{http_code}' \
            "$ADDR/v1/$path" 2>/dev/null || echo 000)"
    fi
    printf '%s' "$code"
}

# ── 1. engine attivo ─────────────────────────────────────────────────────────
mounts="$(api GET sys/mounts 2>/dev/null || true)"
if echo "$mounts" | jq -e 'has("ssh/")' >/dev/null 2>&1; then
    check "ssh secrets engine enabled" 0
else
    check "ssh secrets engine enabled (mount mancante — provision-ssh-ca.sh?)" 1
fi

# ── 2. CA configurata ────────────────────────────────────────────────────────
ca_pub="$(api GET ssh/config/ca 2>/dev/null | jq -r '.data.public_key // empty' 2>/dev/null || true)"
if [[ -n "$ca_pub" ]]; then
    check "CA configured (public_key present, private key in OpenBao)" 0
else
    check "CA configured (public_key assente — esegui provision-ssh-ca.sh)" 1
fi

# ── 3. role corretto ─────────────────────────────────────────────────────────
role_code="$(api_code GET "ssh/roles/$ROLE")"
if [[ "$role_code" == "200" ]]; then
    check "role $ROLE exists" 0
    role_json="$(api GET "ssh/roles/$ROLE" | jq -c '.data')"
    kt="$(echo "$role_json" | jq -r '.key_type // empty')"
    if [[ "$kt" == "ca" ]]; then
        check "role key_type=ca" 0
    else
        check "role key_type=ca (got: ${kt:-none})" 1
    fi
    au="$(echo "$role_json" | jq -r '.allowed_users // ""')"
    auc="$(echo "$role_json" | jq -r '.allow_user_certificates // false')"
    if [[ ",$au," == *",velox-admin,"* && ",$au," == *",velox-deploy,"* ]]; then
        check "allowed_users includes velox-admin + velox-deploy (principals consentiti)" 0
    else
        check "allowed_users includes velox-admin + velox-deploy (got: $au)" 1
    fi
    if [[ "$auc" == "true" ]]; then
        check "allow_user_certificates=true" 0
    else
        check "allow_user_certificates=true (got: $auc)" 1
    fi
    ttl="$(echo "$role_json" | jq -r '.ttl // ""')"
    if [[ -n "$ttl" && "$ttl" != "0s" ]]; then
        check "role ttl=${ttl} (TTL breve atteso)" 0
    else
        check "role ttl non-zero (got: ${ttl:-none})" 1
    fi
else
    check "role $ROLE exists (HTTP $role_code — esegui provision-ssh-ca.sh)" 1
fi

# ── 4-5. firma di prova + negativo ───────────────────────────────────────────
# Pubkey FISSA di test (ed25519, nessun secret) per il check NEGATIVO: la firma
# positiva e la validazione del cert richiedono ssh-keygen, ma il check di
# sicurezza "principal non consentito -> rifiutato" gira SEMPRE (il server è
# autoritativo) — mai saltato in ambienti senza ssh-keygen.
TEST_PUBKEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAICZQD0BStIVDWKe5zh5lTGnDJ+kbTK9F776JDAuEAX15 velox-ssh-ca-test"
if [[ "$role_code" == "200" && -n "$ca_pub" ]]; then
    if command -v ssh-keygen >/dev/null 2>&1; then
        tmpdir="$(mktemp -d)"; trap 'rm -rf "$tmpdir"' EXIT
        ssh-keygen -q -t ed25519 -N '' -f "$tmpdir/probe" >/dev/null 2>&1
        probe_pub="$(cat "$tmpdir/probe.pub")"

    # 4. firma positiva (principal velox-deploy, ttl breve)
    body="$(jq -n --arg k "$probe_pub" '{public_key: $k, cert_type: "user", valid_principals: "velox-deploy", ttl: "5m"}')"
    signed="$(curl -fsS "${curl_tls[@]}" -X POST \
        -H "X-Vault-Token: $BAO_TOKEN" -H 'Content-Type: application/json' \
        --data-binary "$body" "$ADDR/v1/ssh/sign/$ROLE" 2>/dev/null \
        | jq -r '.data.signed_key // empty' 2>/dev/null || true)"
    if [[ -n "$signed" ]]; then
        printf '%s\n' "$signed" > "$tmpdir/probe-cert.pub"
        check "probe sign (velox-deploy, ttl 5m) -> cert emesso" 0
        cert_info="$(ssh-keygen -L -f "$tmpdir/probe-cert.pub" 2>/dev/null || true)"
        if echo "$cert_info" | grep -A3 'Principals:' | grep -q 'velox-deploy'; then
            check "cert principal = velox-deploy" 0
        else
            check "cert principal = velox-deploy" 1
        fi
        if echo "$cert_info" | grep -qE 'Valid: from .* to .*'; then
            check "cert validity window present (TTL)" 0
        else
            check "cert validity window present (TTL)" 1
        fi
        else
            check "probe sign (velox-deploy, ttl 5m) -> cert emesso" 1
        fi
    else
        echo "  SKIP  probe firma positiva (ssh-keygen assente) — negativo eseguito con chiave fissa"
    fi

    # 5. negativo fail-closed: principal non consentito → 400/403 atteso (SEMPRE)
    body_bad="$(jq -n --arg k "$TEST_PUBKEY" '{public_key: $k, cert_type: "user", valid_principals: "root", ttl: "5m"}')"
    bad_code="$(api_code POST "ssh/sign/$ROLE" "$body_bad")"
    if [[ "$bad_code" == "400" || "$bad_code" == "403" ]]; then
        check "negative: principal 'root' REJECTED (HTTP $bad_code)" 0
    else
        check "negative: principal 'root' REJECTED (got HTTP $bad_code — role non vincolante?)" 1
    fi
else
    echo "  SKIP  firma di prova + negativo (CA/role non configurati)"
fi

echo
if [[ "$FAILED" == "1" ]]; then
    echo "verify-ssh-ca: FAIL (uno o più check non passati)" >&2
    exit 1
fi
echo "verify-ssh-ca: OK"
