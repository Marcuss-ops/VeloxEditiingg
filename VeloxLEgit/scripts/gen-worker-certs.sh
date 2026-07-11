#!/usr/bin/env bash
# scripts/gen-worker-certs.sh
#
# Generate ephemeral self-signed mTLS certificates for Velox CI testing.
# Produces a CA root, a server cert (for the master), and a client cert
# (for the worker) — all valid for 10 years from generation.
#
# This is the shell-script equivalent of the Go generateTestCertsDir function
# in RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream_test.go.
# The existing Go test code generates certs in-memory; this script lets CI
# pre-provision the same mTLS triple WITHOUT requiring Go to be installed
# on the runner, using openssl only.
#
# Output directory structure:
#   <OUT_DIR>/
#     ca.crt           CA root certificate (PEM)
#     ca.key           CA private key (PEM, no passphrase)
#     server.crt       Server certificate (PEM)
#     server.key       Server private key (PEM)
#     worker.crt       Worker/client certificate (PEM)
#     worker.key       Worker/client private key (PEM)
#
# Usage:
#   ./scripts/gen-worker-certs.sh <OUT_DIR> [worker-cn]
#
#   worker-cn   CommonName for the worker client cert (default: e2e-worker-1).
#               Must match the worker_id in the CI test job.
#
# Environment:
#   OPENSSL     openssl binary path (default: openssl)
#
# Exit codes:
#   0   certs generated
#   1   openssl missing
#   2   output dir creation failed

set -euo pipefail

OUT_DIR="${1:?usage: $0 <OUT_DIR> [worker-cn]}"
WORKER_CN="${2:-e2e-worker-1}"
OPENSSL="${OPENSSL:-openssl}"

command -v "$OPENSSL" >/dev/null 2>&1 || {
  echo "[gen-certs] FATAL: openssl not found at '$OPENSSL'. Install openssl and retry." >&2
  exit 1
}

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

DAYS=3650  # 10 years — comfortable for CI; never use long-lived bags in prod

# ─── 1. CA key + self-signed cert ────────────────────────────────────────────
"$OPENSSL" req -x509 -new -newkey rsa:2048 -nodes -sha256 \
  -keyout ca.key -out ca.crt -days "$DAYS" \
  -subj '/CN=Velox-E2E-CA/OU=CI/O=Velox' \
  2>/dev/null

echo "[gen-certs] CA:     $OUT_DIR/ca.crt"

# ─── 2. Server key + CSR + cert (signed by CA) ───────────────────────────────
"$OPENSSL" genrsa -out server.key 2048 2>/dev/null
"$OPENSSL" req -new -key server.key -out server.csr -sha256 \
  -subj '/CN=localhost/OU=CI/O=Velox' \
  2>/dev/null

"$OPENSSL" x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days "$DAYS" -sha256 \
  -extfile <(cat <<SAN
basicConstraints       = CA:FALSE
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
keyUsage               = digitalSignature, keyEncipherment
extendedKeyUsage       = serverAuth, clientAuth
subjectAltName         = DNS:localhost,IP:127.0.0.1
SAN
) 2>/dev/null

rm -f server.csr ca.srl
echo "[gen-certs] Server: $OUT_DIR/server.crt"

# ─── 3. Worker/client key + CSR + cert (signed by CA) ────────────────────────
"$OPENSSL" genrsa -out worker.key 2048 2>/dev/null
"$OPENSSL" req -new -key worker.key -out worker.csr -sha256 \
  -subj "/CN=${WORKER_CN}/OU=CI/O=Velox" \
  2>/dev/null

"$OPENSSL" x509 -req -in worker.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out worker.crt -days "$DAYS" -sha256 \
  -extfile <(cat <<SAN
basicConstraints       = CA:FALSE
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
keyUsage               = digitalSignature, keyEncipherment
extendedKeyUsage       = clientAuth
SAN
) 2>/dev/null

rm -f worker.csr ca.srl
echo "[gen-certs] Worker: $OUT_DIR/worker.crt (CN=${WORKER_CN})"

# ─── 4. Summary / env-var dump ───────────────────────────────────────────────
cat <<SUMMARY

  Export these for the master (DataServer) env:
    export VELOX_GRPC_TLS_CERT_FILE=${OUT_DIR}/server.crt
    export VELOX_GRPC_TLS_KEY_FILE=${OUT_DIR}/server.key
    export VELOX_GRPC_TLS_CA_FILE=${OUT_DIR}/ca.crt

  Export these for the worker config (worker.json):
    "tls_cert_file": "${OUT_DIR}/worker.crt"
    "tls_key_file":  "${OUT_DIR}/worker.key"
    "tls_ca_file":   "${OUT_DIR}/ca.crt"

  worker_id in the worker JSON must match CN="${WORKER_CN}".
  The master CN (localhost) and the DNS/IP SANs above allow loopback test.
  These certs are valid for ${DAYS} days from now.
SUMMARY
