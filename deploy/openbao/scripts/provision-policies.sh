#!/usr/bin/env bash
# deploy/openbao/scripts/provision-policies.sh
# ─────────────────────────────────────────────────────────────────────────────
# Idempotent provisioning of the OpenBao ACL policies for the Velox identities:
#
#   deploy/openbao/policies/
#   ├── master.hcl        → policy `master`       (read master/* + workers/*)
#   ├── admin.hcl         → policy `admin`        (CRUD velox/* + approle + policy)
#   └── worker.hcl.tmpl   → policy `worker-<id>`  (read SOLO il proprio ramo)
#
# The worker template is rendered per worker id (default: the canonical fleet
# in scripts/ops/align-worker-digest.sh; override with --workers / --worker).
# Idempotent: a policy already on the server with identical content is
# SKIPPED; only changes are written (change-detection via `bao policy read`).
# No secret material is ever involved — policies are NOT secrets.
#
# Usage:
#   ./scripts/provision-policies.sh                      # fleet canonica
#   ./scripts/provision-policies.sh --workers "id1 id2"  # lista custom
#   ./scripts/provision-policies.sh --worker host_x      # singolo
#   ./scripts/provision-policies.sh --dry-run            # mostra solo il piano
#
# Env: OPENBAO_WORKERS (space-separated, default fleet canonica)

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
POLICIES_DIR="$OPENBAO_DIR/policies"
TOKEN_FILE="$STATE_DIR/root-token"
# Fleet canonica (scripts/ops/align-worker-digest.sh / runtime-cert.sh)
DEFAULT_WORKERS=(host_57_129_132_133 host_57_131_20_173 velox-worker-13197 velox-worker-523925eb)

DRY_RUN=0
WORKERS=()

usage() {
    sed -n '2,32p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers)      mapfile -t WORKERS < <(tr ' ' '\n' <<< "${2:-}"); shift 2 ;;
        --worker)       WORKERS+=("${2:-}"); shift 2 ;;
        --dry-run)      DRY_RUN=1; shift ;;
        -h|--help)      usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done
if [[ ${#WORKERS[@]} -eq 0 ]]; then WORKERS=("${DEFAULT_WORKERS[@]}"); fi

command -v bao >/dev/null 2>&1 || { echo "FATAL: 'bao' CLI not found on PATH" >&2; exit 1; }

export BAO_ADDR="$ADDR"
if [[ -z "${BAO_TOKEN:-}" ]]; then
    [[ -f "$TOKEN_FILE" ]] || {
        echo "FATAL: no BAO_TOKEN and $TOKEN_FILE missing — run bootstrap-init.sh first" >&2
        exit 1
    }
    BAO_TOKEN="$(cat "$TOKEN_FILE")"
    export BAO_TOKEN
fi
export BAO_SKIP_VERIFY="${BAO_SKIP_VERIFY:-true}"   # self-signed loopback cert

# policy_write <name> <rendered-hcl-file> — idempotente con change-detection
policy_write() {
    local name="$1" file="$2" current
    current="$(bao policy read "$name" 2>/dev/null || true)"
    if [[ -n "$current" && "$current" == "$(cat "$file")" ]]; then
        echo "SKIP  policy $name (already current)"
        return 0
    fi
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY   policy $name (would write $(wc -l < "$file") lines)"
        return 0
    fi
    bao policy write "$name" "$file" >/dev/null
    echo "WROTE policy $name"
}

echo "[policies] writing policies from $POLICIES_DIR ..."

# ── static policies ──────────────────────────────────────────────────────────
for f in "$POLICIES_DIR"/*.hcl; do
    [[ -e "$f" ]] || continue
    name="$(basename "$f" .hcl)"
    policy_write "$name" "$f"
done

# ── per-worker policies (rendered from template) ─────────────────────────────
tmpl="$POLICIES_DIR/worker.hcl.tmpl"
[[ -f "$tmpl" ]] || { echo "FATAL: template not found: $tmpl" >&2; exit 1; }
tmp_rendered="$(mktemp)"; trap 'rm -f "$tmp_rendered"' EXIT

for w in "${WORKERS[@]}"; do
    [[ "$w" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]] || {
        echo "FATAL: invalid worker id: $w" >&2; exit 1
    }
    sed "s|{{ WORKER_ID }}|$w|g" "$tmpl" > "$tmp_rendered"
    policy_write "worker-$w" "$tmp_rendered"
done

echo "[policies] done: ${#WORKERS[@]} worker policies + static policies"
