#!/usr/bin/env bash
# deploy/openbao/scripts/bootstrap-init.sh
# ─────────────────────────────────────────────────────────────────────────────
# One-time OpenBao initialization: generates the Shamir unseal keys and the
# root token, then writes them ONLY under the gitignored state dir with mode
# 0600. Nothing secret is ever written to the repo.
#
# Output (0600, NEVER committed):
#   <state_dir>/unseal-keys.txt     one base64 unseal key per line
#   <state_dir>/root-token          the root token
#
# state_dir default: <repo>/.velox/openbao   (override via OPENBAO_STATE_DIR)
# Key shares/threshold: OPENBAO_KEY_SHARES (default 5) / OPENBAO_KEY_THRESHOLD (default 3)
#
# Usage:
#   1. docker compose up -d            (deploy/openbao)
#   2. ./scripts/bootstrap-init.sh
#   3. ./scripts/bootstrap-unseal.sh
#   4. Back up unseal-keys.txt + root-token OFFLINE (password manager / safe)
#      and delete them from the workstation afterwards if policy requires.
#
# Refuses to overwrite existing artifacts (safe to re-run).

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# NOTE: from deploy/openbao/ the repo root is TWO levels up (../../) —
# `deploy/openbao/../.velox` would silently resolve to infra/.velox.
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
SHARES="${OPENBAO_KEY_SHARES:-5}"
THRESHOLD="${OPENBAO_KEY_THRESHOLD:-3}"

KEYS_FILE="$STATE_DIR/unseal-keys.txt"
TOKEN_FILE="$STATE_DIR/root-token"

command -v bao >/dev/null 2>&1 || {
    echo "[init] FATAL: 'bao' CLI not found on PATH." >&2
    echo "[init] Install it from the OpenBao release assets (github.com/openbao/openbao/releases)." >&2
    exit 1
}
command -v jq >/dev/null 2>&1 || {
    echo "[init] FATAL: 'jq' not found on PATH." >&2
    exit 1
}

if [[ -f "$KEYS_FILE" || -f "$TOKEN_FILE" ]]; then
    echo "[init] FATAL: init artifacts already exist under $STATE_DIR." >&2
    echo "[init] Refusing to overwrite. Delete them explicitly to re-init (data loss)." >&2
    exit 1
fi

mkdir -p "$STATE_DIR"
chmod 0700 "$STATE_DIR"

export BAO_ADDR="$ADDR"
TLS_CERT_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
[[ -s "$TLS_CERT_FILE" ]] || {
    echo "[init] FATAL: OpenBao TLS CA certificate missing: $TLS_CERT_FILE" >&2
    exit 1
}
export BAO_CACERT="$TLS_CERT_FILE"

# Reachability + initialization check before touching anything.
# /v1/sys/seal-status is the unauthenticated status endpoint and answers 200
# with {initialized, sealed} even on a fresh node — unlike `bao status`, which
# exits non-zero (and may print nothing on stdout) while the node is
# uninitialized. Prefer curl; fall back to `bao status -format=json`.
seal_json=""
if command -v curl >/dev/null 2>&1; then
    seal_json="$(curl --cacert "$TLS_CERT_FILE" -sS -m 5 "$ADDR/v1/sys/seal-status" 2>/dev/null || true)"
else
    seal_json="$(bao status -format=json 2>/dev/null || true)"
fi
# NOTE: do NOT use '.initialized // empty' here — jq's `//` treats `false`
# as falsy, so an initialized:false response would collapse to empty and
# the node would be reported unreachable. Use plain '.initialized'.
if [[ -z "$seal_json" ]]; then
    echo "[init] FATAL: cannot reach OpenBao at $ADDR — is the container up?" >&2
    echo "[init]        docker compose -f $OPENBAO_DIR/compose.yml up -d" >&2
    exit 1
fi
if [[ "$(printf '%s' "$seal_json" | jq -r '.initialized')" != "false" ]]; then
    echo "[init] FATAL: OpenBao at $ADDR is already initialized." >&2
    echo "[init]        Unseal with ./scripts/bootstrap-unseal.sh instead." >&2
    exit 1
fi

echo "[init] Initializing OpenBao at $ADDR (shares=$SHARES threshold=$THRESHOLD) ..."
init_json="$(bao operator init \
    -key-shares="$SHARES" \
    -key-threshold="$THRESHOLD" \
    -format=json
    )"

umask 077
printf '%s\n' "$init_json" | jq -r '.unseal_keys_b64[]' > "$KEYS_FILE"
printf '%s\n' "$init_json" | jq -r '.root_token' > "$TOKEN_FILE"
chmod 0600 "$KEYS_FILE" "$TOKEN_FILE"

echo "[init] OK — OpenBao initialized."
echo "[init]   unseal keys  -> $KEYS_FILE ($(wc -l < "$KEYS_FILE" | tr -d ' ') keys, 0600)"
echo "[init]   root token   -> $TOKEN_FILE (0600)"
echo
echo "[init] NEXT STEPS:"
echo "[init]   1. ./scripts/bootstrap-unseal.sh   (unseal with $THRESHOLD keys)"
echo "[init]   2. Copy unseal-keys.txt + root-token to OFFLINE storage now."
echo "[init]   3. Optionally delete them from this machine (recovery needs them)."
