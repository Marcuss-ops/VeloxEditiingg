#!/usr/bin/env bash
# deploy/openbao/scripts/bootstrap-unseal.sh
# ─────────────────────────────────────────────────────────────────────────────
# Unseals the OpenBao node using the key shares saved by bootstrap-init.sh.
# Applies keys one at a time until the node reports Sealed: false.
#
# Reads: <state_dir>/unseal-keys.txt   (override via OPENBAO_STATE_DIR)
# Idempotent: safe to re-run after a reboot or container restart.

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# NOTE: from deploy/openbao/ the repo root is TWO levels up (../../) —
# `deploy/openbao/../.velox` would silently resolve to infra/.velox.
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
THRESHOLD="${OPENBAO_KEY_THRESHOLD:-3}"

KEYS_FILE="$STATE_DIR/unseal-keys.txt"

command -v bao >/dev/null 2>&1 || {
    echo "[unseal] FATAL: 'bao' CLI not found on PATH." >&2
    exit 1
}
command -v jq >/dev/null 2>&1 || {
    echo "[unseal] FATAL: 'jq' not found on PATH." >&2
    exit 1
}

[[ -f "$KEYS_FILE" ]] || {
    echo "[unseal] FATAL: no unseal keys at $KEYS_FILE — run bootstrap-init.sh first." >&2
    exit 1
}

export BAO_ADDR="$ADDR"
TLS_CERT_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
[[ -s "$TLS_CERT_FILE" ]] || {
    echo "[unseal] FATAL: OpenBao TLS CA certificate missing: $TLS_CERT_FILE" >&2
    exit 1
}
export BAO_CACERT="$TLS_CERT_FILE"

# If the node is already unsealed there is nothing to do.
sealed="$(bao status -format=json 2>/dev/null | jq -r .sealed || true)"
if [[ "$sealed" == "false" ]]; then
    echo "[unseal] already unsealed — nothing to do."
    exit 0
fi
if [[ "$sealed" != "true" ]]; then
    echo "[unseal] FATAL: cannot read seal status at $ADDR (container up?)." >&2
    exit 1
fi

mapfile -t KEYS < "$KEYS_FILE"
applied=0
for key in "${KEYS[@]}"; do
    [[ -n "$key" ]] || continue
    bao operator unseal "$key" >/dev/null
    applied=$((applied + 1))
    # `bao status` exits 2 while the node is still sealed — with
    # set -euo pipefail that would abort the loop, so tolerate the exit
    # code (only the parsed .sealed value matters here).
    sealed="$(bao status -format=json 2>/dev/null | jq -r .sealed || true)"
    if [[ "$sealed" == "false" ]]; then
        echo "[unseal] OK — node unsealed after $applied key(s)."
        bao status
        exit 0
    fi
    echo "[unseal] progress: $applied key(s) applied, still sealed (threshold $THRESHOLD)..."
done

echo "[unseal] FATAL: still sealed after ${#KEYS[@]} saved keys." >&2
echo "[unseal]        Expected threshold is $THRESHOLD; check unseal-keys.txt." >&2
exit 1
