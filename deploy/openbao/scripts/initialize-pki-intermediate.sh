#!/usr/bin/env bash
# Initialize the production PKI intermediate without exporting its private key.
# The ceremony is deliberately split into two explicit steps:
#   --generate-csr FILE  -> OpenBao creates the key internally and returns CSR
#   --set-signed FILE    -> import only the offline-root-signed certificate chain
#   --check              -> verify that an issuer is configured

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-$OPENBAO_DIR/../../.velox/openbao}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
MOUNT="${OPENBAO_PKI_MOUNT:-pki}"
COMMON_NAME="${OPENBAO_PKI_INTERMEDIATE_COMMON_NAME:-Velox Production Intermediate CA}"
CSR_OUT=""
SIGNED_CHAIN=""
MODE=""

usage() {
    sed -n '2,8p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    printf '\nUsage: %s --generate-csr FILE | --set-signed FILE | --check\n' "${BASH_SOURCE[0]}"
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --generate-csr) [[ -z "$MODE" && -n "${2:-}" ]] || usage 1; MODE=generate; CSR_OUT="$2"; shift 2 ;;
        --set-signed) [[ -z "$MODE" && -n "${2:-}" ]] || usage 1; MODE=set-signed; SIGNED_CHAIN="$2"; shift 2 ;;
        --check) [[ -z "$MODE" ]] || usage 1; MODE=check; shift ;;
        -h|--help) usage 0 ;;
        *) echo "FATAL: unknown option: $1" >&2; usage 1 ;;
    esac
done
[[ -n "$MODE" ]] || usage 1
[[ "$MOUNT" == "pki" ]] || { echo "FATAL: OPENBAO_PKI_MOUNT must remain pki" >&2; exit 2; }

command -v bao >/dev/null 2>&1 || { echo "FATAL: bao CLI not found" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "FATAL: jq not found" >&2; exit 1; }
export BAO_ADDR="$ADDR"
TLS_CERT_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
[[ -s "$TLS_CERT_FILE" ]] || { echo "FATAL: OpenBao TLS CA missing: $TLS_CERT_FILE" >&2; exit 1; }
export BAO_CACERT="$TLS_CERT_FILE"
if [[ -z "${BAO_TOKEN:-}" ]]; then
    [[ -s "$STATE_DIR/root-token" ]] || { echo "FATAL: root token missing: $STATE_DIR/root-token" >&2; exit 1; }
    BAO_TOKEN="$(cat "$STATE_DIR/root-token")"
    export BAO_TOKEN
fi

if ! bao secrets list -format=json 2>/dev/null | jq -e --arg m "$MOUNT/" 'has($m)' >/dev/null 2>&1; then
    [[ "$MODE" != check ]] || { echo "FATAL: PKI mount $MOUNT/ is not enabled" >&2; exit 1; }
    echo "[pki] enabling PKI engine at $MOUNT/"
    bao secrets enable -path="$MOUNT" pki >/dev/null
fi

issuer_ready() { bao read "$MOUNT/issuer/default" >/dev/null 2>&1 || bao read "$MOUNT/config/ca" >/dev/null 2>&1; }

case "$MODE" in
    check)
        issuer_ready || { echo "FATAL: ROOT_CA_MATERIAL_UNAVAILABLE: no configured PKI issuer" >&2; exit 1; }
        echo "[pki] issuer ready: $MOUNT/"
        ;;
    generate)
        [[ ! -e "$CSR_OUT" ]] || { echo "FATAL: CSR output already exists: $CSR_OUT (refusing to rotate pending internal key)" >&2; exit 1; }
        mkdir -p "$(dirname "$CSR_OUT")"
        umask 077
        if issuer_ready; then
            echo "FATAL: PKI issuer already configured; refusing to create a second intermediate" >&2
            exit 1
        fi
        response="$(bao write -format=json "$MOUNT/intermediate/generate/internal" \
            common_name="$COMMON_NAME" format=pem key_type=rsa)" || {
            echo "FATAL: OpenBao could not generate the internal intermediate CSR" >&2
            exit 1
        }
        jq -e '.data.csr | type == "string" and startswith("-----BEGIN CERTIFICATE REQUEST-----")' \
            <<<"$response" >/dev/null || {
            echo "FATAL: OpenBao response did not contain a valid intermediate CSR" >&2
            exit 1
        }
        if jq -e '.data | has("private_key") or has("private_key_type")' <<<"$response" >/dev/null; then
            echo "FATAL: OpenBao returned private-key material; refusing to write response" >&2
            exit 1
        fi
        jq -r '.data.csr' <<<"$response" >"$CSR_OUT"
        chmod 0600 "$CSR_OUT"
        echo "[pki] CSR written to $CSR_OUT"
        echo "[pki] sign this CSR with the approved offline Root CA, then run --set-signed FILE"
        ;;
    set-signed)
        [[ -s "$SIGNED_CHAIN" ]] || { echo "FATAL: signed intermediate chain missing: $SIGNED_CHAIN" >&2; exit 1; }
        grep -q -- '-----BEGIN CERTIFICATE-----' "$SIGNED_CHAIN" || {
            echo "FATAL: signed intermediate input is not PEM certificate material" >&2; exit 1;
        }
        if grep -q -- '-----BEGIN .*PRIVATE KEY-----' "$SIGNED_CHAIN"; then
            echo "FATAL: signed intermediate input contains private-key material; only certificate chain is accepted" >&2
            exit 1
        fi
        command -v openssl >/dev/null 2>&1 || { echo "FATAL: openssl not found" >&2; exit 1; }
        openssl x509 -in "$SIGNED_CHAIN" -noout -text 2>/dev/null | grep -q 'CA:TRUE' || {
            echo "FATAL: signed intermediate leaf is not marked CA:TRUE" >&2; exit 1; }
        if issuer_ready; then
            echo "FATAL: PKI issuer already configured; refusing to replace it" >&2
            exit 1
        fi
        bao write "$MOUNT/intermediate/set-signed" "certificate=@$SIGNED_CHAIN" >/dev/null
        issuer_ready || { echo "FATAL: set-signed completed without a readable issuer" >&2; exit 1; }
        echo "[pki] offline-signed intermediate installed; private key remained inside OpenBao"
        ;;
esac
