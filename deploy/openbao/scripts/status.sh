#!/usr/bin/env bash
# deploy/openbao/scripts/status.sh
# ─────────────────────────────────────────────────────────────────────────────
# Shows the OpenBao seal/HA status and cross-checks the /v1/sys/health
# endpoint (200 = active/unsealed, 429 = standby, 501 = uninitialized,
# 503 = sealed). Useful right after bootstrap and for daily checks.

set -euo pipefail

ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"

command -v bao >/dev/null 2>&1 || {
    echo "[status] FATAL: 'bao' CLI not found on PATH." >&2
    exit 1
}

export BAO_ADDR="$ADDR"
OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
TLS_CERT_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
[[ -s "$TLS_CERT_FILE" ]] || {
    echo "[status] FATAL: OpenBao TLS CA certificate missing: $TLS_CERT_FILE" >&2
    exit 1
}
export BAO_CACERT="$TLS_CERT_FILE"
bao status

if command -v curl >/dev/null 2>&1; then
    code="$(curl -s --cacert "$TLS_CERT_FILE" -o /dev/null -w '%{http_code}' "$ADDR/v1/sys/health" 2>/dev/null || echo 000)"
    case "$code" in
        200) meaning="ACTIVE + unsealed" ;;
        429) meaning="standby (unsealed)" ;;
        501) meaning="not initialized" ;;
        503) meaning="sealed" ;;
        *)   meaning="unreachable/unknown" ;;
    esac
    echo
    echo "[status] GET $ADDR/v1/sys/health -> HTTP $code ($meaning)"
fi
