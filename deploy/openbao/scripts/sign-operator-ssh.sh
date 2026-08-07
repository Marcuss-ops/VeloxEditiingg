#!/usr/bin/env bash
# deploy/openbao/scripts/sign-operator-ssh.sh
# ─────────────────────────────────────────────────────────────────────────────
# Firma la public key di un operatore con la CA SSH di OpenBao (secrets engine
# `ssh`, role `velox-operator`, key_type=ca) e restituisce il certificato
# SSH firmato (cert_type=user) con TTL BREVE (default 30m) e principals
# limitati a velox-admin/velox-deploy.
#
# Il cert va installato come ~/.ssh/<key>-cert.pub (es. ~/.ssh/velox-cert.pub):
# OpenSSH lo usa automaticamente se il nome è <chiave>-cert.pub. Il cert
# scade da solo — niente revoche manuali su 5 VPS, niente authorized_keys
# per gli operatori (solo la TrustedUserCAKeys sui nodi, vedi
# deploy/playbooks/bootstrap-ssh.yml).
#
# Usage:
#   ./scripts/sign-operator-ssh.sh --pubkey-file ~/.ssh/velox.pub
#   ./scripts/sign-operator-ssh.sh --pubkey-file ~/.ssh/velox.pub \
#       --principals velox-admin --ttl 2h --out ~/.ssh/velox-cert.pub
#
# Flags:
#   --pubkey-file  path della public key (obbligatorio; o stdin se "-")
#   --principals   principals del cert (default velox-deploy; csv)
#   --ttl          validità del cert (default 30m, max consentito dal role)
#   --role         role SSH (default velox-operator)
#   --out          file di output (default stdout; se file -> 0600)
#
# Auth: BAO_TOKEN (es. token AppRole `ssh-operator`, policy ssh-operator.hcl)
# o root token dallo state dir. MAI logga la chiave privata (non la vede) e
# il cert firmato va a stdout/file — non nei log.
#
# Nota sicurezza: il role limita i principals a velox-admin/velox-deploy —
# principals non consentiti vengono rifiutati dal server (403).

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
TOKEN_FILE="$STATE_DIR/root-token"

ROLE="${OPENBAO_SSH_ROLE:-velox-operator}"
PUBKEY_FILE=""
PRINCIPALS="velox-deploy"
TTL="30m"
OUT=""
ALLOWED_PRINCIPALS="velox-admin velox-deploy"   # pre-guardia (il server è autoritativo)

usage() {
    sed -n '2,36p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pubkey-file) PUBKEY_FILE="${2:-}"; shift 2 ;;
        --principals)  PRINCIPALS="${2:-}"; shift 2 ;;
        --ttl)         TTL="${2:-}"; shift 2 ;;
        --role)        ROLE="${2:-}"; shift 2 ;;
        --out)         OUT="${2:-}"; shift 2 ;;
        -h|--help)     usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done
[[ -n "$PUBKEY_FILE" ]] || { echo "FATAL: --pubkey-file required" >&2; usage 1; }

command -v curl >/dev/null 2>&1 || { echo "FATAL: 'curl' not found on PATH" >&2; exit 1; }
command -v jq   >/dev/null 2>&1 || { echo "FATAL: 'jq' not found on PATH" >&2; exit 1; }

# ── Chiave pubblica (file o stdin) ───────────────────────────────────────────
if [[ "$PUBKEY_FILE" == "-" ]]; then
    pubkey="$(cat || true)"
else
    [[ -f "$PUBKEY_FILE" ]] || { echo "FATAL: pubkey file not found: $PUBKEY_FILE" >&2; exit 1; }
    pubkey="$(head -1 "$PUBKEY_FILE" || true)"
fi
# pre-guardia: deve sembrare una chiave pubblica (il server fa la verifica vera)
case " $pubkey " in
    *' ssh-rsa '*|*' ssh-ed25519 '*|*' ecdsa-sha2-'*|*' sk-ssh-ed25519 '*|*' sk-ecdsa-sha2-'*)
        ;;
    *)
        echo "FATAL: input non sembra una public key SSH (riga: ${pubkey:0:40}...)" >&2
        exit 1
        ;;
esac

# ── Pre-guardia principals (fail-fast; il server è autoritativo) ─────────────
IFS=',' read -r -a wanted <<< "$PRINCIPALS"
for p in "${wanted[@]}"; do
    [[ " $ALLOWED_PRINCIPALS " == *" $p "* ]] || {
        echo "FATAL: principal '$p' non consentito (attesi: $ALLOWED_PRINCIPALS)" >&2
        exit 1
    }
done

# ── Auth ─────────────────────────────────────────────────────────────────────
if [[ -z "${BAO_TOKEN:-}" ]]; then
    [[ -f "$TOKEN_FILE" ]] || {
        echo "FATAL: no BAO_TOKEN and $TOKEN_FILE missing — usa un token AppRole ssh-operator o bootstrap-init.sh" >&2
        exit 1
    }
    BAO_TOKEN="$(cat "$TOKEN_FILE")"
    export BAO_TOKEN
fi

curl_tls=(-k)
if [[ -f "$STATE_DIR/tls/server.crt" ]]; then
    curl_tls=(--cacert "$STATE_DIR/tls/server.crt")
fi

# ── Firma ────────────────────────────────────────────────────────────────────
body="$(jq -n --arg k "$pubkey" --arg p "$PRINCIPALS" --arg t "$TTL" \
    '{public_key: $k, cert_type: "user", valid_principals: $p, ttl: $t}')"
echo "[sign] signing $PUBKEY_FILE with role $ROLE (principals=$PRINCIPALS, ttl=$TTL) ..." >&2

tmp_out="$(mktemp)"
trap 'rm -f "$tmp_out"' EXIT
code="$(curl -sS "${curl_tls[@]}" -X POST \
    -H "X-Vault-Token: $BAO_TOKEN" -H 'Content-Type: application/json' \
    --data-binary "$body" -o "$tmp_out" -w '%{http_code}' \
    "$ADDR/v1/ssh/sign/$ROLE" 2>/dev/null || echo 000)"
if [[ "$code" != "200" ]]; then
    err="$(jq -r '.errors[0] // ""' "$tmp_out" 2>/dev/null || true)"
    echo "FATAL: firma fallita (HTTP $code${err:+ — $err}) — verifica role/TTL/principals" >&2
    exit 1
fi
signed="$(jq -r '.data.signed_key // empty' "$tmp_out" 2>/dev/null || true)"
rm -f "$tmp_out"
[[ -n "$signed" ]] || { echo "FATAL: risposta di firma senza signed_key (HTTP $code)" >&2; exit 1; }

# ── Output ───────────────────────────────────────────────────────────────────
if [[ -n "$OUT" ]]; then
    printf '%s\n' "$signed" > "$OUT"
    chmod 0600 "$OUT"
    echo "[sign] cert scritto: $OUT (0600) — installa come <key>-cert.pub su ~/.ssh" >&2
else
    printf '%s\n' "$signed"
fi

# Validazione opzionale con ssh-keygen (se disponibile)
if command -v ssh-keygen >/dev/null 2>&1; then
    tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT
    printf '%s\n' "$signed" > "$tmp"
    echo "[sign] validazione ssh-keygen -L:" >&2
    ssh-keygen -L -f "$tmp" >&2 || echo "[sign] WARN: ssh-keygen -L non è riuscito a validare il cert" >&2
fi
