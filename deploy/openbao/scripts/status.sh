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
bao status -tls-skip-verify

if command -v curl >/dev/null 2>&1; then
    code="$(curl -s -o /dev/null -w '%{http_code}' -k "$ADDR/v1/sys/health" 2>/dev/null || echo 000)"
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
