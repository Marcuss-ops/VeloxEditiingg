#!/usr/bin/env bash
# deploy/openbao/scripts/provision-pki.sh
# ─────────────────────────────────────────────────────────────────────────────
# Provision the OpenBao PKI engine and one least-privilege signing role per
# worker. This script does not generate worker keys and never stores a worker
# private key. The intermediate/root lifecycle is intentionally separate:
# configure the issuer in OpenBao first, then run this role provisioning.
#
# Usage:
#   ./scripts/provision-pki.sh
#   ./scripts/provision-pki.sh --workers "id1 id2"
#   ./scripts/provision-pki.sh --worker id1
#
# Optional environment:
#   OPENBAO_PKI_MOUNT=pki (canonical; other mount names are rejected)
#   OPENBAO_PKI_ROLE_PREFIX=worker-
#   OPENBAO_PKI_DEFAULT_TTL=168h
#   OPENBAO_PKI_MAX_TTL=336h
#   OPENBAO_PKI_ISSUER_URL=https://...
#   OPENBAO_PKI_CRL_URL=https://...

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-$OPENBAO_DIR/../../.velox/openbao}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
MOUNT="${OPENBAO_PKI_MOUNT:-pki}"
[[ "$MOUNT" == "pki" ]] || { echo "FATAL: OPENBAO_PKI_MOUNT must remain the canonical pki mount" >&2; exit 2; }
ROLE_PREFIX="${OPENBAO_PKI_ROLE_PREFIX:-worker-}"
DEFAULT_TTL="${OPENBAO_PKI_DEFAULT_TTL:-168h}"
MAX_TTL="${OPENBAO_PKI_MAX_TTL:-336h}"
DEFAULT_WORKERS=(host_57_129_132_133 host_57_131_20_173 velox-worker-13197 velox-worker-523925eb)
WORKERS=()
DRY_RUN=0

usage() { sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers) mapfile -t WORKERS < <(tr ' ' '\n' <<< "${2:-}"); shift 2 ;;
        --worker) WORKERS+=("${2:-}"); shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done
[[ ${#WORKERS[@]} -gt 0 ]] || WORKERS=("${DEFAULT_WORKERS[@]}")

command -v bao >/dev/null 2>&1 || { echo "FATAL: bao CLI not found" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "FATAL: jq not found" >&2; exit 1; }
export BAO_ADDR="$ADDR"
TLS_CERT_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
[[ -s "$TLS_CERT_FILE" ]] || { echo "FATAL: OpenBao TLS CA missing: $TLS_CERT_FILE" >&2; exit 1; }
export BAO_CACERT="$TLS_CERT_FILE"
if [[ -z "${BAO_TOKEN:-}" ]]; then
    [[ -s "$STATE_DIR/root-token" ]] || { echo "FATAL: root token missing: $STATE_DIR/root-token" >&2; exit 1; }
    BAO_TOKEN="$(cat "$STATE_DIR/root-token")"
    export BAO_TOKEN
fi

if ! bao secrets list -format=json 2>/dev/null | jq -e --arg m "$MOUNT/" 'has($m)' >/dev/null 2>&1; then
    echo "[pki] enabling PKI engine at $MOUNT/"
    [[ "$DRY_RUN" == "1" ]] || bao secrets enable -path="$MOUNT" pki >/dev/null
else
    echo "[pki] mount $MOUNT/ already enabled"
fi

if [[ "$DRY_RUN" != "1" ]]; then
    if ! bao read "$MOUNT/issuer/default" >/dev/null 2>&1 && ! bao read "$MOUNT/config/ca" >/dev/null 2>&1; then
        echo "FATAL: ROOT_CA_MATERIAL_UNAVAILABLE: initialize the OpenBao intermediate with initialize-pki-intermediate.sh before provisioning roles" >&2
        exit 1
    fi
    bao secrets tune -max-lease-ttl="$MAX_TTL" "$MOUNT" >/dev/null
    declare -a config_args=()
    [[ -n "${OPENBAO_PKI_ISSUER_URL:-}" ]] && config_args+=("issuing_certificates=${OPENBAO_PKI_ISSUER_URL}")
    [[ -n "${OPENBAO_PKI_CRL_URL:-}" ]] && config_args+=("crl_distribution_points=${OPENBAO_PKI_CRL_URL}")
    # config/urls accepts publication URLs only; lease TTL belongs to the
    # mount tune above and must not be sent to this endpoint.
    if [[ ${#config_args[@]} -gt 0 ]]; then
        bao write "$MOUNT/config/urls" "${config_args[@]}" >/dev/null
    fi
fi

if [[ "$DRY_RUN" != "1" ]]; then
    issuer_present=0
    if bao read "$MOUNT/issuer/default" >/dev/null 2>&1 || bao read "$MOUNT/config/ca" >/dev/null 2>&1; then issuer_present=1; fi
    if [[ "$issuer_present" != "1" ]]; then
        echo "FATAL: PKI mount $MOUNT/ has no configured issuer; import or initialize the OpenBao intermediate before creating worker roles" >&2
        exit 1
    fi
    echo "[pki] issuer readiness verified"
fi

for worker in "${WORKERS[@]}"; do
    [[ "$worker" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]] || { echo "FATAL: invalid worker id: $worker" >&2; exit 1; }
    role="${ROLE_PREFIX}${worker}"
    spiffe_uri="spiffe://velox/worker/$worker"
    echo "[pki] role $role (CN=$worker, URI SAN=$spiffe_uri, ttl=$DEFAULT_TTL max=$MAX_TTL)"
    if [[ "$DRY_RUN" == "1" ]]; then continue; fi
    # The role is CSR-signing only: OpenBao signs the public key in the CSR;
    # no issue endpoint and no generated private key are used.
    bao write "$MOUNT/roles/$role" \
        allowed_domains="$worker" \
        allow_bare_domains=true \
        allow_subdomains=false \
        enforce_hostnames=false \
        require_cn=true \
        use_csr_common_name=true \
        use_csr_sans=true \
        allowed_uri_sans="$spiffe_uri" \
        key_usage="Digital Signature,Key Encipherment" \
        ext_key_usage="ClientAuth" \
        ttl="$DEFAULT_TTL" \
        max_ttl="$MAX_TTL" \
        >/dev/null
    # Read back the complete role contract before rollout. This prevents a
    # partially configured role (or a server-side normalization surprise) from
    # silently issuing a certificate for the wrong worker identity.
    role_json="$(bao read -format=json "$MOUNT/roles/$role")" || {
        echo "FATAL: cannot read back PKI role $role" >&2
        exit 1
    }
    jq -e --arg worker "$worker" --arg uri "$spiffe_uri" \
        '.data.allowed_domains == [$worker]' <<<"$role_json" >/dev/null || {
        echo "FATAL: PKI role $role allowed_domains is not exactly [$worker]" >&2
        exit 1
    }
    jq -e --arg uri "$spiffe_uri" \
        '.data.allowed_uri_sans == [$uri] and .data.use_csr_sans == true and .data.use_csr_common_name == true' \
        <<<"$role_json" >/dev/null || {
        echo "FATAL: PKI role $role identity constraints are not exact (allowed_uri_sans/use_csr_*)" >&2
        exit 1
    }
 done

echo "[pki] done: mount=$MOUNT workers=${#WORKERS[@]}"
