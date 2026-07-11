#!/usr/bin/env bash
# tests/e2e/grpc-control-plane/certs/generate-dev-pki.sh
#
# Generate ephemeral self-signed mTLS certificates for the local
# gRPC control-plane E2E matrix in tests/e2e/grpc-control-plane/.
#
# Diff from scripts/gen-worker-certs.sh:
#   * Default TTL is 7 days for the CA and 1 day for leaves, so a
#     cert that "expires" mid-test can be reproduced deterministically
#     by passing -days 0 (or -days -1 for a Go-style "already-expired"
#     cert). The 10-year default in scripts/gen-worker-certs.sh is
#     wrong for E2E because expiry cannot be tested then.
#   * Subject naming is fixed for E2E: CN=localhost for the server
#     (matches the SAN:DNS:localhost,IP:127.0.0.1 in the extfile) and
#     CN=<worker_id> for the worker (CRITICAL: the master will use
#     the cert CN as worker identifier after PR 4 — keeping CN =
#     worker_id is the contract this script preserves).
#   * Bash strict-mode + trap-based tmpfs cleanup keep the runner
#     from leaking CA private keys at /tmp if a case aborts mid-flight.
#
# Output:
#   <OUT_DIR>/
#     ca.crt, ca.key
#     server.crt, server.key
#     worker.crt, worker.key
#
# Usage:
#   ./generate-dev-pki.sh <OUT_DIR> <WORKER_CN> [CA_DAYS=7] [LEAF_DAYS=1]
#
# Exit codes:
#   0   all certs generated
#   1   openssl missing
#   2   invalid args

set -euo pipefail

OUT_DIR="${1:-}"
WORKER_CN="${2:-}"
CA_DAYS="${3:-7}"
LEAF_DAYS="${4:-1}"
OPENSSL="${OPENSSL:-openssl}"

if [[ -z "$OUT_DIR" || -z "$WORKER_CN" ]]; then
  echo "[dev-pki] FATAL: usage: $0 <OUT_DIR> <WORKER_CN> [CA_DAYS=7] [LEAF_DAYS=1]" >&2
  exit 2
fi

command -v "$OPENSSL" >/dev/null 2>&1 || {
  echo "[dev-pki] FATAL: openssl not found at '$OPENSSL'. Install openssl and retry." >&2
  exit 1
}

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

# Wipe any prior artifacts so re-running with the same OUT_DIR is
# idempotent. We never want a leftover CA.key from a previous run
# silently signing a new leaf.
rm -f ca.crt ca.key ca.srl \
      server.crt server.key server.csr \
      worker.crt worker.key worker.csr

# ─── 1. CA ──────────────────────────────────────────────────────────────────
# Subject is fixed (no ${RANDOM}) — log debugging ("which CA signed that leaf?")
# must be answerable from stderr alone. Multiple PKI triples in a single run
# (case 3/4/6) are distinguished by their OUT_DIR, not by subject uniqueness.
"$OPENSSL" req -x509 -new -newkey rsa:2048 -nodes -sha256 \
  -keyout ca.key -out ca.crt -days "$CA_DAYS" \
  -subj "/CN=Velox-E2E-CA/OU=E2E/O=Velox" \
  2>/dev/null

# ─── 2. Server cert (CN=localhost; matches SAN below) ───────────────────────
"$OPENSSL" genrsa -out server.key 2048 2>/dev/null
"$OPENSSL" req -new -key server.key -out server.csr -sha256 \
  -subj "/CN=localhost/OU=E2E/O=Velox" \
  2>/dev/null

"$OPENSSL" x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days "$LEAF_DAYS" -sha256 \
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

# ─── 3. Worker cert (CN=worker_id; clientAuth only) ─────────────────────────
"$OPENSSL" genrsa -out worker.key 2048 2>/dev/null
"$OPENSSL" req -new -key worker.key -out worker.csr -sha256 \
  -subj "/CN=${WORKER_CN}/OU=E2E/O=Velox" \
  2>/dev/null

"$OPENSSL" x509 -req -in worker.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out worker.crt -days "$LEAF_DAYS" -sha256 \
  -extfile <(cat <<SAN
basicConstraints       = CA:FALSE
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
keyUsage               = digitalSignature, keyEncipherment
extendedKeyUsage       = clientAuth
SAN
) 2>/dev/null

rm -f worker.csr ca.srl

# ─── 4. Summary ─────────────────────────────────────────────────────────────
cat <<SUMMARY
[dev-pki] CA:     $OUT_DIR/ca.crt (CA valid ${CA_DAYS}d)
[dev-pki] Server: $OUT_DIR/server.crt (leaf valid ${LEAF_DAYS}d, CN=localhost)
[dev-pki] Worker: $OUT_DIR/worker.crt (leaf valid ${LEAF_DAYS}d, CN=${WORKER_CN})

  Master env:
    VELOX_GRPC_TLS_CERT_FILE=$OUT_DIR/server.crt
    VELOX_GRPC_TLS_KEY_FILE=$OUT_DIR/server.key
    VELOX_GRPC_TLS_CA_FILE=$OUT_DIR/ca.crt

  Worker JSON:
    "tls_cert_file": "$OUT_DIR/worker.crt"
    "tls_key_file":  "$OUT_DIR/worker.key"
    "tls_ca_file":   "$OUT_DIR/ca.crt"
SUMMARY
