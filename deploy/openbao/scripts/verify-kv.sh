#!/usr/bin/env bash
# deploy/openbao/scripts/verify-kv.sh
# ─────────────────────────────────────────────────────────────────────────────
# Prints the OpenBao KV tree for the velox/ mount with the current version of
# each leaf secret. NEVER prints secret values — only paths and version
# numbers (handy for idempotency checks and audits).
#
# Usage:
#   ./scripts/verify-kv.sh                 # walk the velox/ tree
#   ./scripts/verify-kv.sh --mount <name>  # default: velox
#
# Auth: root token from the state dir (override with BAO_TOKEN), same as the
# provisioning script.

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
MOUNT="velox"
TOKEN_FILE="$STATE_DIR/root-token"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mount) MOUNT="${2:-}"; shift 2 ;;
        -h|--help) echo "usage: $0 [--mount <name>]"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

command -v bao >/dev/null 2>&1 || { echo "FATAL: 'bao' CLI not found on PATH" >&2; exit 1; }
command -v jq  >/dev/null 2>&1 || { echo "FATAL: 'jq' not found on PATH" >&2; exit 1; }

export BAO_ADDR="$ADDR"
TLS_CERT_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
[[ -s "$TLS_CERT_FILE" ]] || {
    echo "FATAL: OpenBao TLS CA certificate missing: $TLS_CERT_FILE" >&2
    exit 1
}
export BAO_CACERT="$TLS_CERT_FILE"
if [[ -z "${BAO_TOKEN:-}" ]]; then
    [[ -f "$TOKEN_FILE" ]] || {
        echo "FATAL: no BAO_TOKEN and $TOKEN_FILE missing — run bootstrap-init.sh first" >&2
        exit 1
    }
    BAO_TOKEN="$(cat "$TOKEN_FILE")"
    export BAO_TOKEN
fi

if ! bao secrets list -format=json 2>/dev/null | jq -e --arg m "$MOUNT/" 'has($m)' >/dev/null 2>&1; then
    echo "FATAL: mount $MOUNT/ is not enabled (run ./scripts/provision-kv.sh)" >&2
    exit 1
fi

walk() {
    # $1 = relative path under the mount (e.g. "production"); "" = mount root
    # NOTE: `bao` requires -format BEFORE the positional path (otherwise the
    # flag is parsed as a second positional arg and fails with "Too many
    # arguments"). JSON list output avoids the human "Keys/----" header.
    local path="$1" entries ver entry
    if [[ -z "$path" ]]; then
        entries="$(bao kv list -format=json -mount="$MOUNT" 2>/dev/null | jq -r '.[]' 2>/dev/null || true)"
    else
        entries="$(bao kv list -format=json -mount="$MOUNT" "$path" 2>/dev/null | jq -r '.[]' 2>/dev/null || true)"
    fi
    [[ -n "$entries" ]] || return 0
    while IFS= read -r entry; do
        [[ -n "$entry" ]] || continue
        if [[ "$entry" == */ ]]; then
            echo "DIR   ${path:+$path/}$entry"
            walk "${path:+$path/}${entry%/}"
        else
            ver="$(bao kv metadata get -format=json -mount="$MOUNT" "${path:+$path/}$entry" 2>/dev/null \
                | jq -r '.data.current_version // "?"' 2>/dev/null || echo '?')"
            echo "SEC   ${path:+$path/}$entry   version=$ver"
        fi
    done <<< "$entries"
}

echo "KV mount: $MOUNT/"
walk ""
