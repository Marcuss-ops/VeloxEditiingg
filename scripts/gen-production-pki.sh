#!/usr/bin/env bash
# scripts/gen-production-pki.sh
# =============================================================================
# Production PKI generator — 3-tier certificate hierarchy.
#
# Diff from scripts/gen-worker-certs.sh (CI, self-signed, 10y) and
# tests/e2e/grpc-control-plane/certs/generate-dev-pki.sh (E2E, dev, 7d/1d):
#
#   * 3-TIER: root CA → intermediate CA → leaf (server/worker)
#   * Root CA is OFFLINE — the `intermediate` and `leaf` commands
#     take --root-ca-cert/--root-ca-key paths pointing to the
#     air-gapped root CA material. The root key is NEVER read for
#     daily leaf rotation (only intermediate signs leaves).
#   * Default TTLs match the production runbook:
#       root     = 3650 days (10 years)
#       intermediate = 270 days (9 months)
#       server   = 90 days
#       worker   = 14 days
#   * Serial numbers are tracked via `ca.srl` so each certificate
#     issued by the same CA has a unique, monotonically-increasing
#     serial number.
#   * SANs for server certs enforce DNS + IP.
#   * Output directories auto-create and wipe previous artifacts
#     on leaf certs (idempotent re-generation).
#
# Usage:
#   ./scripts/gen-production-pki.sh root-ca \\
#     --out-dir /secure/velox-root-ca --cn "Velox Root CA" [--days 3650]
#
#   ./scripts/gen-production-pki.sh intermediate \\
#     --out-dir /opt/velox/certs/intermediate \\
#     --cn "Velox Intermediate CA v1" [--days 270] \\
#     --root-ca-cert /secure/velox-root-ca/ca.crt \\
#     --root-ca-key /secure/velox-root-ca/ca.key
#
#   ./scripts/gen-production-pki.sh server \\
#     --out-dir /opt/velox/certs/master --cn "velox-master.example.com" \\
#     --san "DNS:velox-master.example.com,DNS:localhost,IP:127.0.0.1" \\
#     [--days 90] --intermediate-dir /opt/velox/certs/intermediate
#
#   ./scripts/gen-production-pki.sh worker \\
#     --out-dir /opt/velox/certs/workers --cn "worker-01" \\
#     [--days 14] --intermediate-dir /opt/velox/certs/intermediate
#
# Exit codes:
#   0   OK
#   1   openssl missing or openssl failure
#   2   invalid arguments
# =============================================================================

set -euo pipefail

OPENSSL="${OPENSSL:-openssl}"
command -v "$OPENSSL" >/dev/null 2>&1 || {
  echo "[gen-prod-pki] FATAL: openssl not found at '$OPENSSL'" >&2; exit 1; }

# ─── Argument parsing ───────────────────────────────────────────────────────
CMD="${1:-}"; shift 2>/dev/null || true
if [[ -z "$CMD" ]]; then
  echo "usage: $0 {root-ca|intermediate|server|worker} [options]" >&2
  exit 2
fi

OUT_DIR=""
CN=""
DAYS=""
ROOT_CA_CERT=""
ROOT_CA_KEY=""
INTERMEDIATE_DIR=""
SAN=""

while [[ $# -gt 0 ]]; do case "$1" in
  --out-dir)          OUT_DIR="$2"; shift 2 ;;
  --cn)               CN="$2"; shift 2 ;;
  --days)             DAYS="$2"; shift 2 ;;
  --root-ca-cert)     ROOT_CA_CERT="$2"; shift 2 ;;
  --root-ca-key)      ROOT_CA_KEY="$2"; shift 2 ;;
  --intermediate-dir) INTERMEDIATE_DIR="$2"; shift 2 ;;
  --san)              SAN="$2"; shift 2 ;;
  *) echo "[gen-prod-pki] unknown flag: $1" >&2; exit 2 ;;
esac; done

# ─── Default TTLs (production runbook §3) ───────────────────────────────────
case "$CMD" in
  root-ca)      DAYS="${DAYS:-3650}" ;;
  intermediate) DAYS="${DAYS:-270}"  ;;
  server)       DAYS="${DAYS:-90}"   ;;
  worker)       DAYS="${DAYS:-14}"   ;;
  *) echo "[gen-prod-pki] unknown command: $CMD" >&2; exit 2 ;;
esac

if [[ -z "$OUT_DIR" || -z "$CN" ]]; then
  echo "[gen-prod-pki] --out-dir and --cn are required" >&2; exit 2
fi

mkdir -p "$OUT_DIR"

# ─── OpenSSL config (minimal, generated per-run) ────────────────────────────
# We use openssl.cnf for the `ca` subcommand (revocation support) but for
# ad-hoc x509 signing we rely on inline -extfile. The config is generated
# per intermediate CA directory.
gen_openssl_cnf() {
  local dir="$1"
  cat > "$dir/openssl.cnf" <<CNF
[ ca ]
default_ca = velox_ca

[ velox_ca ]
dir               = $dir
certs             = \$dir/certs
new_certs_dir     = \$dir
database          = \$dir/index.txt
serial            = \$dir/ca.srl
private_key       = \$dir/ca.key
certificate       = \$dir/ca.crt
default_days      = ${DAYS}
default_md        = sha256
policy            = velox_policy

[ velox_policy ]
countryName             = optional
stateOrProvinceName     = optional
organizationName        = optional
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional

[ req ]
default_bits            = 2048
default_md              = sha256
distinguished_name      = req_distinguished_name
prompt                  = no

[ req_distinguished_name ]
CN = ${CN}

[ server_ext ]
basicConstraints       = CA:FALSE
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
keyUsage               = digitalSignature, keyEncipherment
extendedKeyUsage       = serverAuth, clientAuth
subjectAltName         = ${SAN}

[ worker_ext ]
basicConstraints       = CA:FALSE
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
keyUsage               = digitalSignature, keyEncipherment
extendedKeyUsage       = clientAuth

[ intermediate_ext ]
basicConstraints       = CA:TRUE,pathlen:0
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
keyUsage               = keyCertSign, cRLSign
CNF
}

# ═══════════════════════════════════════════════════════════════════════════════
# ROOT CA
# ═══════════════════════════════════════════════════════════════════════════════
cmd_root_ca() {
  cd "$OUT_DIR"
  # Wipe prior artifacts only for root — we NEVER want two root CAs in the same dir.
  rm -f ca.crt ca.key ca.srl

  echo "[gen-prod-pki] ROOT CA: CN=$CN days=$DAYS dir=$OUT_DIR"

  "$OPENSSL" req -x509 -new -newkey rsa:4096 -nodes -sha512 \
    -keyout ca.key -out ca.crt -days "$DAYS" \
    -subj "/CN=${CN}/OU=Production/O=Velox" \
    -extensions v3_ca \
    -config <(cat <<X
[ req ]
distinguished_name = req_distinguished_name
x509_extensions    = v3_ca
prompt             = no
[ req_distinguished_name ]
CN = ${CN}
[ v3_ca ]
basicConstraints       = critical,CA:TRUE,pathlen:1
keyUsage               = critical,keyCertSign,cRLSign
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
X
) 2>&1

  echo "[gen-prod-pki] root CA serial: $("$OPENSSL" x509 -in ca.crt -serial -noout | cut -d= -f2)"
  echo "[gen-prod-pki] root CA fingerprint: $("$OPENSSL" x509 -in ca.crt -fingerprint -sha256 -noout | cut -d= -f2)"
  echo "[gen-prod-pki] ROOT CA generated: $OUT_DIR/ca.crt"
  echo ""
  echo "  STORE ca.key OFFLINE — do NOT commit or leave on disk after intermediate generation."
}

# ═══════════════════════════════════════════════════════════════════════════════
# INTERMEDIATE CA
# ═══════════════════════════════════════════════════════════════════════════════
cmd_intermediate() {
  if [[ -z "$ROOT_CA_CERT" || -z "$ROOT_CA_KEY" ]]; then
    echo "[gen-prod-pki] --root-ca-cert and --root-ca-key are required for intermediate CA" >&2
    exit 2
  fi
  cd "$OUT_DIR"
  rm -f ca.crt ca.key ca.csr ca.srl index.txt index.txt.old

  gen_openssl_cnf "$OUT_DIR"
  touch index.txt
  echo 1000 > ca.srl  # Start serial numbers at 1000 (production convention)

  echo "[gen-prod-pki] INTERMEDIATE CA: CN=$CN days=$DAYS dir=$OUT_DIR"

  # Generate intermediate key + CSR
  "$OPENSSL" genrsa -out ca.key 4096 2>/dev/null
  "$OPENSSL" req -new -key ca.key -out ca.csr -sha512 \
    -subj "/CN=${CN}/OU=Production/O=Velox" 2>/dev/null

  # Sign with root CA
  "$OPENSSL" x509 -req -in ca.csr \
    -CA "$ROOT_CA_CERT" -CAkey "$ROOT_CA_KEY" -CAcreateserial \
    -out ca.crt -days "$DAYS" -sha512 \
    -extfile <(cat <<EXT
basicConstraints       = critical,CA:TRUE,pathlen:0
keyUsage               = critical,keyCertSign,cRLSign
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
EXT
) 2>&1

  rm -f ca.csr

  # Verify the chain
  "$OPENSSL" verify -CAfile "$ROOT_CA_CERT" ca.crt >/dev/null 2>&1 || {
    echo "[gen-prod-pki] FATAL: intermediate cert does NOT chain to root CA" >&2
    exit 1
  }

  echo "[gen-prod-pki] intermediate CA serial: $("$OPENSSL" x509 -in ca.crt -serial -noout | cut -d= -f2)"
  echo "[gen-prod-pki] intermediate CA fingerprint: $("$OPENSSL" x509 -in ca.crt -fingerprint -sha256 -noout | cut -d= -f2)"
  echo "[gen-prod-pki] INTERMEDIATE CA generated: $OUT_DIR/ca.crt (verify OK)"
  echo ""
  echo "  Set master env:"
  echo "    VELOX_GRPC_TLS_CA_FILE=$OUT_DIR/ca.crt"
}

# ═══════════════════════════════════════════════════════════════════════════════
# SERVER CERTIFICATE
# ═══════════════════════════════════════════════════════════════════════════════
cmd_server() {
  if [[ -z "$INTERMEDIATE_DIR" ]]; then
    echo "[gen-prod-pki] --intermediate-dir is required for server certs" >&2
    exit 2
  fi
  if [[ -z "$SAN" ]]; then
    SAN="DNS:localhost,IP:127.0.0.1"
    echo "[gen-prod-pki] WARN: --san not set, defaulting to DNS:localhost,IP:127.0.0.1" >&2
  fi
  cd "$OUT_DIR"
  rm -f server.crt server.key server.csr

  echo "[gen-prod-pki] SERVER: CN=$CN days=$DAYS SAN=$SAN"

  "$OPENSSL" genrsa -out server.key 2048 2>/dev/null
  "$OPENSSL" req -new -key server.key -out server.csr -sha256 \
    -subj "/CN=${CN}/OU=Production/O=Velox" 2>/dev/null
  "$OPENSSL" x509 -req -in server.csr \
    -CA "$INTERMEDIATE_DIR/ca.crt" -CAkey "$INTERMEDIATE_DIR/ca.key" \
    -CAserial "$INTERMEDIATE_DIR/ca.srl" \
    -out server.crt -days "$DAYS" -sha256 \
    -extfile <(cat <<EXT
basicConstraints       = CA:FALSE
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
keyUsage               = digitalSignature, keyEncipherment
extendedKeyUsage       = serverAuth, clientAuth
subjectAltName         = ${SAN}
EXT
) 2>&1

  rm -f server.csr
  echo "[gen-prod-pki] server serial: $("$OPENSSL" x509 -in server.crt -serial -noout | cut -d= -f2)"
  echo "[gen-prod-pki] server fingerprint: $("$OPENSSL" x509 -in server.crt -fingerprint -sha256 -noout | cut -d= -f2)"
  echo "[gen-prod-pki] SERVER generated: $OUT_DIR/server.crt"
  echo ""
  echo "  Set master env:"
  echo "    VELOX_GRPC_TLS_CERT_FILE=$OUT_DIR/server.crt"
  echo "    VELOX_GRPC_TLS_KEY_FILE=$OUT_DIR/server.key"
}

# ═══════════════════════════════════════════════════════════════════════════════
# WORKER LEAF CERTIFICATE
# ═══════════════════════════════════════════════════════════════════════════════
cmd_worker() {
  if [[ -z "$INTERMEDIATE_DIR" ]]; then
    echo "[gen-prod-pki] --intermediate-dir is required for worker certs" >&2
    exit 2
  fi

  local worker_crt="$OUT_DIR/${CN}.crt"
  local worker_key="$OUT_DIR/${CN}.key"
  local worker_csr="$OUT_DIR/${CN}.csr"

  rm -f "$worker_crt" "$worker_key" "$worker_csr"

  echo "[gen-prod-pki] WORKER: CN=$CN days=$DAYS"

  "$OPENSSL" genrsa -out "$worker_key" 2048 2>/dev/null
  "$OPENSSL" req -new -key "$worker_key" -out "$worker_csr" -sha256 \
    -subj "/CN=${CN}/OU=Production/O=Velox" 2>/dev/null
  "$OPENSSL" x509 -req -in "$worker_csr" \
    -CA "$INTERMEDIATE_DIR/ca.crt" -CAkey "$INTERMEDIATE_DIR/ca.key" \
    -CAserial "$INTERMEDIATE_DIR/ca.srl" \
    -out "$worker_crt" -days "$DAYS" -sha256 \
    -extfile <(cat <<EXT
basicConstraints       = CA:FALSE
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
keyUsage               = digitalSignature, keyEncipherment
extendedKeyUsage       = clientAuth
EXT
) 2>&1

  rm -f "$worker_csr"

  # Verify the chain: intermediate → worker leaf.
  # Root CA cert may not be on this machine (it's air-gapped).
  # Operators should verify manually:
  #   openssl verify -CAfile /path/to/root-ca.crt -untrusted $INTERMEDIATE_DIR/ca.crt $worker_crt
  if [[ -n "${ROOT_CA_CERT:-}" && -f "$ROOT_CA_CERT" ]]; then
    if "$OPENSSL" verify -CAfile "$ROOT_CA_CERT" -untrusted "$INTERMEDIATE_DIR/ca.crt" "$worker_crt" >/dev/null 2>&1; then
      echo "[gen-prod-pki] chain verification OK (root → intermediate → worker)"
    else
      echo "[gen-prod-pki] WARN: chain verification FAILED — check your CA chain" >&2
    fi
  else
    echo "[gen-prod-pki] chain verification skipped (--root-ca-cert not provided, root CA is offline)"
  fi

  echo "[gen-prod-pki] worker serial: $("$OPENSSL" x509 -in "$worker_crt" -serial -noout | cut -d= -f2)"
  echo "[gen-prod-pki] worker fingerprint: $("$OPENSSL" x509 -in "$worker_crt" -fingerprint -sha256 -noout | cut -d= -f2)"
  # RW-PROD-001 A9: identity-binding guard (re-inserted INSIDE cmd_worker)
  if [[ -n "${WORKER_ID:-}" ]]; then
    actual_cn="$("$OPENSSL" x509 -in "$worker_crt" -noout -subject | sed -n "s/.*CN *= *\([^,/]*\).*/\1/p" | tr -d " ")"
    if [[ -z "$actual_cn" ]]; then
      echo "[gen-prod-pki] FATAL: cert subject has no CN; cannot verify A9 binding." >&2
      rm -f "$worker_crt" "$worker_key"; exit 1
    fi
    if [[ "$actual_cn" != "$WORKER_ID" ]]; then
      echo "[gen-prod-pki] FATAL: WORKER_ID=$WORKER_ID but cert CN=$actual_cn (RW-PROD-001 A9 binding mismatch)." >&2
      rm -f "$worker_crt" "$worker_key"; exit 1
    fi
    echo "[gen-prod-pki] CN binding verified: cert CN=$actual_cn == WORKER_ID=$WORKER_ID"
  fi

  echo "[gen-prod-pki] WORKER generated: $worker_crt"
  echo ""
  echo "  Worker config JSON:"
  echo "    \"tls_cert_file\": \"$worker_crt\""
  echo "    \"tls_key_file\":  \"$worker_key\""
  echo "    \"tls_ca_file\":   \"$INTERMEDIATE_DIR/ca.crt\""
}

case "$CMD" in
  root-ca)      cmd_root_ca      ;;
  intermediate) cmd_intermediate ;;
  server)       cmd_server       ;;
  worker)       cmd_worker       ;;
esac
