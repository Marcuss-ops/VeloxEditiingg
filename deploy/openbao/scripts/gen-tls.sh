#!/usr/bin/env bash
# deploy/openbao/scripts/gen-tls.sh
# ─────────────────────────────────────────────────────────────────────────────
# Generates the self-signed TLS certificate used by the OpenBao API listener.
#
# Output (NEVER committed — gitignored state dir, mode 0600/0644):
#   <state_dir>/tls/server.crt
#   <state_dir>/tls/server.key
#
# state_dir default: <repo>/.velox/openbao   (override via OPENBAO_STATE_DIR)
#
# Usage:
#   ./scripts/gen-tls.sh
#   OPENBAO_TLS_CN=velox.example.com OPENBAO_TLS_SANS='DNS:velox.example.com,IP:10.0.0.5' ./scripts/gen-tls.sh
#
# Regenerate: delete the existing cert/key pair and re-run.

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# NOTE: from deploy/openbao/ the repo root is TWO levels up (../../) —
# `deploy/openbao/../.velox` would silently resolve to infra/.velox.
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
TLS_DIR="$STATE_DIR/tls"

DAYS="${OPENBAO_TLS_DAYS:-730}"
CN="${OPENBAO_TLS_CN:-velox-openbao.local}"
# Default SANs cover loopback access (compose binds 127.0.0.1). Extend when
# OpenBao is reachable from other hosts: DNS:<real-hostname>,IP:<real-ip>.
SANS="${OPENBAO_TLS_SANS:-DNS:localhost,DNS:velox-openbao.local,IP:127.0.0.1}"

command -v openssl >/dev/null 2>&1 || {
    echo "[gen-tls] FATAL: openssl not found on PATH" >&2
    exit 1
}

# Docker auto-creates missing bind-mount source dirs AS ROOT at `compose up`
# time. If that happened to $TLS_DIR (or a parent), mkdir fails with a bare
# "Permission denied" — catch it and give a remediation instead.
if ! mkdir -p "$TLS_DIR" 2>/dev/null; then
    echo "[gen-tls] FATAL: cannot create $TLS_DIR (likely created as root by a previous 'docker compose up')." >&2
    echo "[gen-tls]        Fix: docker compose down && sudo chown -R \"$(id -un)\" $STATE_DIR && re-run (or run gen-tls BEFORE compose up)." >&2
    exit 1
fi
chmod 0755 "$TLS_DIR" 2>/dev/null || true
chmod 0700 "$STATE_DIR" 2>/dev/null || true

if [[ ! -w "$TLS_DIR" ]]; then
    echo "[gen-tls] FATAL: $TLS_DIR is not writable by $(id -un)." >&2
    echo "[gen-tls]        Fix: sudo chown -R \"$(id -un)\" $STATE_DIR" >&2
    exit 1
fi

if [[ -f "$TLS_DIR/server.crt" && -f "$TLS_DIR/server.key" ]]; then
    echo "[gen-tls] certs already exist at $TLS_DIR (delete both files to regenerate)"
    exit 0
fi

# NOTE: keep openssl stderr visible — a silent 2>/dev/null here made the
# first container failure (root-owned dir) impossible to diagnose.
openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days "$DAYS" \
    -keyout "$TLS_DIR/server.key" \
    -out "$TLS_DIR/server.crt" \
    -subj "/CN=$CN/O=Velox" \
    -addext "subjectAltName=$SANS" \
    -addext "keyUsage=digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=serverAuth"

chmod 0644 "$TLS_DIR/server.crt"

# The OpenBao container runs as the non-root `openbao` user (uid 100, gid
# 1000) and the image entrypoint does NOT chown /openbao/tls. The listener
# therefore needs server.key readable through the bind mount: make it
# group-readable for gid 1000 (0640). FAIL CLOSED: never silently ship a
# world-readable private key — degrade to 0644 only behind an explicit
# dev opt-in (OPENBAO_ALLOW_INSECURE_KEY_MODE=1).
if chgrp 1000 "$TLS_DIR/server.key" 2>/dev/null; then
    chmod 0640 "$TLS_DIR/server.key"
    echo "[gen-tls] key permissions: 0640, group 1000 (readable by container openbao, gid 1000)"
elif [[ "${OPENBAO_ALLOW_INSECURE_KEY_MODE:-0}" == "1" ]]; then
    chmod 0644 "$TLS_DIR/server.key"
    echo "[gen-tls] WARNING: degraded to 0644 (OPENBAO_ALLOW_INSECURE_KEY_MODE=1) — dev/bootstrap only, world-readable key." >&2
else
    echo "[gen-tls] FATAL: cannot set group 1000 on server.key — the non-root container user (uid 100/gid 1000) could not read it otherwise." >&2
    echo "[gen-tls]        Fix: sudo chgrp 1000 $TLS_DIR/server.key && sudo chmod 0640 $TLS_DIR/server.key, then re-run." >&2
    echo "[gen-tls]        (Dev-only escape hatch: OPENBAO_ALLOW_INSECURE_KEY_MODE=1 .)" >&2
    exit 1
fi

echo "[gen-tls] wrote:"
echo "  cert: $TLS_DIR/server.crt"
echo "  key:  $TLS_DIR/server.key"
echo "[gen-tls] CN=$CN days=$DAYS SANs=[$SANS]"
echo "[gen-tls] restart the container so the listener picks up the new cert:"
echo "          docker compose -f $OPENBAO_DIR/compose.yml restart openbao"
