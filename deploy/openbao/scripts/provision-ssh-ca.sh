#!/usr/bin/env bash
# deploy/openbao/scripts/provision-ssh-ca.sh
# ─────────────────────────────────────────────────────────────────────────────
# Idempotent provisioning of the OpenBao SSH secrets engine as the Velox SSH
# Certificate Authority (signed user certificates, `key_type=ca`):
#
#   1. enables the `ssh` secrets engine (if not already enabled);
#   2. generates the CA signing key (if missing) — the private key lives ONLY
#      inside OpenBao; it is NEVER exported, and an existing CA is NEVER
#      overwritten (rotating a CA would invalidate every deployed certificate);
#   3. creates the role `velox-operator` (key_type=ca, allowed_users +
#      valid_principals = velox-admin,velox-deploy, ttl 30m / max_ttl 24h);
#   4. exports the CA PUBLIC key to $STATE_DIR/ssh-ca.pub (0644 — public, not
#      a secret) for TrustedUserCAKeys on the worker/master nodes.
#
# The signing private key stays in OpenBao; operators only ever touch the
# public key and the signed certificates (TTL breve, principals limitati).
#
# Usage:
#   ./scripts/provision-ssh-ca.sh                    # enable + CA + role + export
#   ./scripts/provision-ssh-ca.sh --role deploy-ca   # nome role custom
#   ./scripts/provision-ssh-ca.sh --dry-run          # piano senza scrivere
#
# Env: OPENBAO_SSH_ROLE (default velox-operator), OPENBAO_SSH_TTL (30m),
#      OPENBAO_SSH_MAX_TTL (24h), OPENBAO_SSH_ALLOWED_USERS,
#      OPENBAO_SSH_PRINCIPALS, OPENBAO_SSH_CA_OUT (default state-dir/ssh-ca.pub)
#
# Auth: root token dallo state dir (o BAO_TOKEN). Serve WRITE su ssh/*:
# la policy `admin` (o root) lo copre; la policy `ssh-operator` è SOLO firma.

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
TOKEN_FILE="$STATE_DIR/root-token"

ROLE="${OPENBAO_SSH_ROLE:-velox-operator}"
TTL="${OPENBAO_SSH_TTL:-30m}"
MAX_TTL="${OPENBAO_SSH_MAX_TTL:-24h}"
ALLOWED_USERS="${OPENBAO_SSH_ALLOWED_USERS:-velox-admin,velox-deploy}"
PRINCIPALS="${OPENBAO_SSH_PRINCIPALS:-velox-admin,velox-deploy}"
DEFAULT_USER="${OPENBAO_SSH_DEFAULT_USER:-velox-deploy}"
CA_OUT="${OPENBAO_SSH_CA_OUT:-$STATE_DIR/ssh-ca.pub}"

DRY_RUN=0

usage() {
    sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --role)    ROLE="${2:-}"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done
[[ -n "$ROLE" ]] || { echo "FATAL: empty role name" >&2; exit 1; }
[[ "$ROLE" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || {
    echo "FATAL: invalid role name: $ROLE" >&2; exit 1
}

command -v curl >/dev/null 2>&1 || { echo "FATAL: 'curl' not found on PATH" >&2; exit 1; }
command -v jq   >/dev/null 2>&1 || { echo "FATAL: 'jq' not found on PATH" >&2; exit 1; }

# ── Auth (root token o BAO_TOKEN) ────────────────────────────────────────────
if [[ -z "${BAO_TOKEN:-}" ]]; then
    [[ -f "$TOKEN_FILE" ]] || {
        echo "FATAL: no BAO_TOKEN and $TOKEN_FILE missing — run bootstrap-init.sh first" >&2
        exit 1
    }
    BAO_TOKEN="$(cat "$TOKEN_FILE")"
    export BAO_TOKEN
fi

# ── TLS verification (fail-closed; never use -k) ────────────────────────────
TLS_CERT_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
[[ -s "$TLS_CERT_FILE" ]] || {
    echo "FATAL: OpenBao TLS CA certificate missing: $TLS_CERT_FILE" >&2
    exit 1
}
curl_tls=(--cacert "$TLS_CERT_FILE")
export BAO_CACERT="$TLS_CERT_FILE"

api() {
    # api METHOD PATH [BODY] — REST verso OpenBao; stdout = body JSON
    local method="$1" path="$2" body="${3:-}"
    if [[ -n "$body" ]]; then
        curl -fsS "${curl_tls[@]}" -X "$method" \
            -H "X-Vault-Token: $BAO_TOKEN" -H 'Content-Type: application/json' \
            --data-binary "$body" "$ADDR/v1/$path"
    else
        curl -fsS "${curl_tls[@]}" -X "$method" \
            -H "X-Vault-Token: $BAO_TOKEN" "$ADDR/v1/$path"
    fi
}

api_code() {
    # api_code METHOD PATH [BODY] — come api ma stdout = HTTP code (000 su errore)
    local method="$1" path="$2" body="${3:-}" code
    if [[ -n "$body" ]]; then
        code="$(curl -sS "${curl_tls[@]}" -X "$method" \
            -H "X-Vault-Token: $BAO_TOKEN" -H 'Content-Type: application/json' \
            --data-binary "$body" -o /dev/null -w '%{http_code}' \
            "$ADDR/v1/$path" 2>/dev/null || echo 000)"
    else
        code="$(curl -sS "${curl_tls[@]}" -X "$method" \
            -H "X-Vault-Token: $BAO_TOKEN" -o /dev/null -w '%{http_code}' \
            "$ADDR/v1/$path" 2>/dev/null || echo 000)"
    fi
    printf '%s' "$code"
}

# ── 1. Enable ssh secrets engine (idempotente) ───────────────────────────────
mounts="$(api GET sys/mounts 2>/dev/null || echo '{}')"
if echo "$mounts" | jq -e 'has("ssh/")' >/dev/null 2>&1; then
    echo "[ssh-ca] ssh engine already enabled"
else
    echo "[ssh-ca] enabling ssh secrets engine ..."
    [[ "$DRY_RUN" == "1" ]] || api POST sys/mounts/ssh '{"type":"ssh"}' >/dev/null
fi

# ── 2. CA signing key (generate ONCE, never overwrite) ───────────────────────
ca_pub="$(api GET ssh/config/ca 2>/dev/null | jq -r '.data.public_key // empty' 2>/dev/null || true)"
if [[ -n "$ca_pub" ]]; then
    echo "SKIP  ssh CA signing key (already configured — never regenerate: certificates would break)"
else
    echo "[ssh-ca] generating CA signing key (private key stays inside OpenBao) ..."
    if [[ "$DRY_RUN" == "1" ]]; then
        # dry-run: piano soltanto — non deve fallire perché la CA non esiste ancora
        echo "DRY   CA signing key (would generate — private key stays in OpenBao)"
        ca_pub="__DRY_RUN_NO_CA__"
    else
        api POST ssh/config/ca '{"generate_signing_key": true}' >/dev/null
        ca_pub="$(api GET ssh/config/ca 2>/dev/null | jq -r '.data.public_key // empty' 2>/dev/null || true)"
        [[ -n "$ca_pub" ]] || { echo "FATAL: CA key generation failed (no public_key returned)" >&2; exit 1; }
    fi
fi

# ── 3. Role velox-operator (key_type=ca, least-privilege) ────────────────────
role_body="$(jq -n \
    --arg role "$ROLE" \
    --arg users "$ALLOWED_USERS" \
    --arg princ "$PRINCIPALS" \
    --arg defu "$DEFAULT_USER" \
    --arg ttl "$TTL" \
    --arg mttl "$MAX_TTL" \
    '{key_type: "ca",
      allow_user_certificates: true,
      allowed_users: $users,
      default_user: $defu,
      valid_principals: $princ,
      ttl: $ttl,
      max_ttl: $mttl,
      allowed_user_key_configs: [
        {type: "ssh-rsa", lengths: [2048, 4096]},
        {type: "ed25519", lengths: []}
      ]}')"
code="$(api_code GET "ssh/roles/$ROLE")"
if [[ "$code" == "200" ]]; then
    echo "SKIP  role $ROLE (exists; use OPENBAO_SSH_ROLE or delete it to recreate)"
else
    echo "[ssh-ca] creating role $ROLE (key_type=ca, users=$ALLOWED_USERS, principals=$PRINCIPALS, ttl=$TTL max=$MAX_TTL)"
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY   role $ROLE (would write: $(echo "$role_body" | jq -c .))"
    else
        api POST "ssh/roles/$ROLE" "$role_body" >/dev/null
        code="$(api_code GET "ssh/roles/$ROLE")"
        [[ "$code" == "200" ]] || { echo "FATAL: role creation failed (HTTP $code)" >&2; exit 1; }
    fi
fi

# ── 4. Export CA PUBLIC key per TrustedUserCAKeys ────────────────────────────
if [[ "$DRY_RUN" != "1" ]]; then
    mkdir -p "$(dirname "$CA_OUT")"
    printf '%s\n' "$ca_pub" > "$CA_OUT"
    chmod 0644 "$CA_OUT"   # chiave PUBBLICA — non un segreto
    echo "WROTE CA public key -> $CA_OUT (0644, per TrustedUserCAKeys sui nodi)"
else
    echo "DRY   CA public key (would write to $CA_OUT)"
fi

echo
echo "[ssh-ca] done — firma con ./scripts/sign-operator-ssh.sh --pubkey-file <key.pub>"
echo "[ssh-ca] verifica con ./scripts/verify-ssh-ca.sh"
