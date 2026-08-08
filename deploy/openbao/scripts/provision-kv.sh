#!/usr/bin/env bash
# deploy/openbao/scripts/provision-kv.sh
# ─────────────────────────────────────────────────────────────────────────────
# Idempotent provisioning of the OpenBao KV v2 mount `velox/` with the Velox
# secret hierarchy:
#
#   velox/production/master/...
#   velox/production/workers/<worker-id>/credential
#
# mTLS certificates are issued by the dedicated PKI engine, not stored in KV.
# The worker private key is generated locally and must never be provisioned here.
#
# mTLS certificates are issued by the dedicated PKI engine, not stored in KV.
# The worker private key is generated locally and must never be provisioned here.
#   velox/production/services/registry/...
#
# SECURITY RULES (hard requirements):
#   * Secret values NEVER come from committed files. Sources, in precedence:
#       1. environment variable  OPENBAO_VALUE_<NAME>
#       2. gitignored values file $OPENBAO_VALUES_FILE (must be mode 0600 and
#          NOT tracked by git — the script refuses otherwise), lines like
#          ADMIN_TOKEN=..., INSTAEDIT_JWT=..., SOCIAL_API_TOKEN=...
#       3. interactive prompt (read -s) when stdin is a TTY
#   * A secret that already exists is SKIPPED (idempotent). Use --force to
#     overwrite (creates a new KV version).
#   * The script NEVER prints secret values.
#
# Auth: uses the root token from the state dir (<repo>/.velox/openbao/root-token)
# because provisioning needs WRITE on velox/* (AppRole `admin` covers it — to be
# wired when operator tooling adopts AppRole, phase 4, see README §9).
# Override with BAO_TOKEN.
#
# Usage:
#   ./scripts/provision-kv.sh                          # all master+services
#   ./scripts/provision-kv.sh --worker <worker-id>     # + worker credential
#   ./scripts/provision-kv.sh --force                  # overwrite existing
#   ./scripts/provision-kv.sh --dry-run                # show plan only
#   ./scripts/provision-kv.sh --mount <name>           # default: velox
#
# Environment (examples):
#   OPENBAO_VALUE_ADMIN_TOKEN=... \
#   OPENBAO_VALUE_INSTAEDIT_JWT=... \
#   OPENBAO_VALUE_SOCIAL_API_TOKEN=... \
# Worker mTLS material is intentionally absent: use provision-pki.sh and the
# runtime CSR flow. Never pass worker.key to this script.
#   OPENBAO_VALUES_FILE=.velox/openbao/values.env \
#   ./scripts/provision-kv.sh --worker host_57_129_132_133

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
MOUNT="${OPENBAO_KV_MOUNT:-velox}"
TOKEN_FILE="$STATE_DIR/root-token"
REPO_ROOT="$(cd "$OPENBAO_DIR/../.." && pwd)"

FORCE=0
DRY_RUN=0
WORKER_ID=""

usage() {
    sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)          FORCE=1; shift ;;
        --dry-run)        DRY_RUN=1; shift ;;
        --worker)         WORKER_ID="${2:-}"; shift 2 ;;
        --mount)          MOUNT="${2:-}"; shift 2 ;;
        -h|--help)        usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done

command -v bao >/dev/null 2>&1 || { echo "FATAL: 'bao' CLI not found on PATH" >&2; exit 1; }
command -v jq  >/dev/null 2>&1 || { echo "FATAL: 'jq' not found on PATH" >&2; exit 1; }

# ── Auth ─────────────────────────────────────────────────────────────────────
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

# ── Values-file safety (gitignored + 0600 only) ──────────────────────────────
VALUES_FILE="${OPENBAO_VALUES_FILE:-}"
if [[ -n "$VALUES_FILE" ]]; then
    # Normalize to an ABSOLUTE path first: git -C resolves pathspecs against
    # the repo root while stat/grep resolve against the invocation CWD — a
    # relative OPENBAO_VALUES_FILE could pass the tracked-check and then read
    # a DIFFERENT file. realpath makes all three agree on the same file.
    VALUES_FILE="$(realpath -m "$VALUES_FILE")"
    [[ -f "$VALUES_FILE" ]] || { echo "FATAL: values file not found: $OPENBAO_VALUES_FILE" >&2; exit 1; }
    if git -C "$REPO_ROOT" ls-files --error-unmatch "$VALUES_FILE" >/dev/null 2>&1; then
        echo "FATAL: values file is TRACKED by git — secrets must never be committed: $VALUES_FILE" >&2
        exit 1
    fi
    mode="$(stat -c '%a' "$VALUES_FILE" 2>/dev/null || echo '?')"
    if [[ "$mode" != "600" ]]; then
        echo "FATAL: values file must be mode 0600 (got $mode): $VALUES_FILE" >&2
        exit 1
    fi
fi

# ── Manifest: relative path | env suffix | required ──────────────────────────
# Mirrors docs/secrets-audit.md §2.1/§2.2. Required secrets MUST be resolved;
# optional ones are skipped with a note when no value is available.
MANIFEST=(
    "production/master/admin-token|ADMIN_TOKEN|1"
    "production/master/instaedit-control-jwt-secret|INSTAEDIT_JWT|1"
    "production/master/social-api-token|SOCIAL_API_TOKEN|1"
    "production/master/social-webhook-secret|SOCIAL_WEBHOOK_SECRET|0"
    "production/master/commit-hmac-key|COMMIT_HMAC_KEY|0"
    "production/services/registry/username|REGISTRY_USERNAME|0"
    "production/services/registry/token|REGISTRY_TOKEN|0"
)
if [[ -n "$WORKER_ID" ]]; then
    MANIFEST+=(
        "production/workers/$WORKER_ID/credential|WORKER_CREDENTIAL|1"
        # mTLS is deliberately not part of KV. The worker uses its AppRole
        # plus pki/sign/worker-<id> with a locally generated key and CSR.
    )
fi

# ── Helpers ──────────────────────────────────────────────────────────────────
mount_state() {
    bao secrets list -format=json 2>/dev/null || echo '{}'
}

resolve_value() {
    # $1 = env suffix, $2 = relative path; echoes the resolved value.
    local name="$1" path="$2" val=""
    local envvar="OPENBAO_VALUE_${name}"
    if [[ -n "${!envvar:-}" ]]; then
        val="${!envvar}"
    elif [[ -n "${VALUES_FILE:-}" && -f "$VALUES_FILE" ]]; then
        # Single-line, LF-only entries; strip CR so CRLF files don't corrupt
        # the secret with a trailing \r (multi-line values are not supported).
        val="$(grep -E "^${name}=" "$VALUES_FILE" | head -1 | cut -d= -f2- | tr -d '\r' || true)"
    elif [[ -t 0 ]]; then
        read -r -s -p "OpenBao secret $path: " val >&2 || true
        echo >&2
    fi
    printf '%s' "$val"
}

secret_exists() {
    # $1 = relative path
    bao kv metadata get -mount="$MOUNT" "$1" >/dev/null 2>&1
}

# ── TLS verification for REST writes (fail-closed; never use -k) ────────────
curl_tls=(--cacert "$TLS_CERT_FILE")

# ── 1. Ensure KV v2 mount ────────────────────────────────────────────────────
mounts="$(mount_state)"
if echo "$mounts" | jq -e --arg m "$MOUNT/" 'has($m)' >/dev/null 2>&1; then
    mount_ver="$(echo "$mounts" | jq -r --arg m "$MOUNT/" '.[$m].options.version // "1"')"
    if [[ "$mount_ver" != "2" ]]; then
        echo "FATAL: mount $MOUNT/ exists but is KV v$mount_ver — expected KV v2 (remove/re-enable the mount)" >&2
        exit 1
    fi
    echo "[kv] mount $MOUNT/ already enabled (KV v2)"
else
    echo "[kv] enabling KV v2 at $MOUNT/ ..."
    bao secrets enable -path="$MOUNT" kv-v2
fi

# ── 2. Provision secrets ─────────────────────────────────────────────────────
written=0; skipped=0; optional=0
for entry in "${MANIFEST[@]}"; do
    path="${entry%%|*}"; rest="${entry#*|}"; name="${rest%%|*}"; required="${rest##*|}"
    if secret_exists "$path"; then
        if [[ "$FORCE" != "1" ]]; then
            echo "SKIP  $path (exists; use --force to overwrite)"
            skipped=$((skipped + 1)); continue
        fi
        echo "FORCE $path (overwriting — new version)"
    fi
    val="$(resolve_value "$name" "$path")"
    if [[ -z "$val" ]]; then
        if [[ "$required" == "1" ]]; then
            echo "FATAL: required secret $path has no value (set OPENBAO_VALUE_$name, use OPENBAO_VALUES_FILE, or run interactively)" >&2
            exit 1
        fi
        echo "SKIP  $path (optional, no value provided)"
        optional=$((optional + 1)); continue
    fi
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY   $path  <- OPENBAO_VALUE_$name"
        continue
    fi
    # KV v2 write via REST API: the value is fed to jq from the ENVIRONMENT
    # (not argv) and the JSON body from stdin. `bao kv put value=...` would
    # put the secret in the process argv, world-readable via
    # /proc/<pid>/cmdline; the env is readable only by the same user/root,
    # same protection as the state-dir files.
    if ! BAO_KV_SECRET="$val" \
        jq -n 'env.BAO_KV_SECRET | {data: {value: .}}' |
        curl -fsS -o /dev/null "${curl_tls[@]}" -X POST \
            -H "X-Vault-Token: $BAO_TOKEN" \
            --data-binary @- \
            "$ADDR/v1/$MOUNT/data/$path"; then
        echo "FATAL: failed to write $path" >&2
        exit 1
    fi
    echo "WROTE $path"
    written=$((written + 1))
done

echo
echo "[kv] done: written=$written skipped=$skipped optional_skipped=$optional"
echo "[kv] verify with ./scripts/verify-kv.sh"
