#!/usr/bin/env bash
# deploy/openbao/scripts/provision-approle.sh
# ─────────────────────────────────────────────────────────────────────────────
# Idempotent provisioning of AppRole machine identities (one DISTINCT identity
# per principal) on the OpenBao approle auth method:
#
#   worker-<id>  → policy `worker-<id>`  (read SOLO il proprio ramo)
#   master       → policy `master`       (read master/* + workers/*)
#   admin        → policy `admin`        (CRUD velox/* + approle + policies)
#
# For every principal it:
#   1. enables the `approle` auth method (if not already enabled);
#   2. creates the role bound to the principal policy (if missing);
#   3. reads the role-id and generates a secret-id, storing BOTH under the
#      gitignored state dir with mode 0600:
#        <repo>/.velox/openbao/approle/<principal>/{role-id,secret-id}
#      The role-id is a public identifier; the secret-id is a SECRET — it is
#      written ONLY to disk (0600, gitignored) and NEVER printed or committed.
#      Idempotent: existing material is SKIPPED; --force rotates the secret-id
#      (and rewrites the role config) — old secret-ids are then invalidated.
#
# Usage:
#   ./scripts/provision-approle.sh                       # fleet + master + admin
#   ./scripts/provision-approle.sh --workers "id1 id2"   # SOLO quei worker + master + admin
#   ./scripts/provision-approle.sh --principal master    # solo master (vince su --workers)
#   ./scripts/provision-approle.sh --force               # ruota i secret-id
#   ./scripts/provision-approle.sh --dry-run             # piano
#
# Nota: --workers SOSTITUISCE la fleet di default (come provision-policies.sh);
# master/admin vengono sempre inclusi a meno di --principal.
#
# Env: OPENBAO_WORKERS (space-separated), OPENBAO_TOKEN_TTL (default 1h),
#      OPENBAO_TOKEN_MAX_TTL (default 24h),
#      OPENBAO_SECRET_ID_TTL (default 0 = mai), OPENBAO_SECRET_ID_NUM_USES (0)

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
TOKEN_FILE="$STATE_DIR/root-token"
DEFAULT_WORKERS=(host_57_129_132_133 host_57_131_20_173 velox-worker-13197 velox-worker-523925eb)

TOKEN_TTL="${OPENBAO_TOKEN_TTL:-1h}"
TOKEN_MAX_TTL="${OPENBAO_TOKEN_MAX_TTL:-24h}"
SECRET_ID_TTL="${OPENBAO_SECRET_ID_TTL:-0}"
SECRET_ID_NUM_USES="${OPENBAO_SECRET_ID_NUM_USES:-0}"

FORCE=0
DRY_RUN=0
PRINCIPALS=()
WORKERS=()

usage() {
    sed -n '2,38p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --principal)    PRINCIPALS+=("${2:-}"); shift 2 ;;
        --workers)      mapfile -t WORKERS < <(tr ' ' '\n' <<< "${2:-}"); shift 2 ;;
        --force)        FORCE=1; shift ;;
        --dry-run)      DRY_RUN=1; shift ;;
        -h|--help)      usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done
# Risoluzione principal: --principal vince; altrimenti --workers (o fleet di
# default) + master + admin. Stessa semantica sostitutiva di provision-policies.sh.
if [[ ${#PRINCIPALS[@]} -eq 0 ]]; then
    [[ ${#WORKERS[@]} -eq 0 ]] && WORKERS=("${DEFAULT_WORKERS[@]}")
    for w in "${WORKERS[@]}"; do PRINCIPALS+=("worker-$w"); done
    PRINCIPALS+=(master admin)
fi
trap 'find "$STATE_DIR/approle" -name "*.tmp" -delete 2>/dev/null || true' EXIT

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
export BAO_SKIP_VERIFY="${BAO_SKIP_VERIFY:-true}"

# ── 1. abilita auth method approle (idempotente) ─────────────────────────────
if ! bao auth list -format=json 2>/dev/null | jq -e 'has("approle/")' >/dev/null 2>&1; then
    echo "[approle] enabling approle auth method ..."
    [[ "$DRY_RUN" == "1" ]] || bao auth enable approle
else
    echo "[approle] approle auth method already enabled"
fi

# ── 2. role + materiale per principal ────────────────────────────────────────
for principal in "${PRINCIPALS[@]}"; do
    [[ "$principal" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]] || {
        echo "FATAL: invalid principal name: $principal" >&2; exit 1
    }
    role_path="auth/approle/role/$principal"
    dir="$STATE_DIR/approle/$principal"
    role_id_file="$dir/role-id"
    secret_id_file="$dir/secret-id"

    # guardia: la policy del principal deve esistere (evita role con sola
    # policy `default` per errori di battitura / policy non ancora provisionate)
    if ! bao policy read "$principal" >/dev/null 2>&1; then
        echo "FATAL: policy '$principal' non esiste su OpenBao — esegui prima ./scripts/provision-policies.sh" >&2
        exit 1
    fi

    role_exists=0
    if bao read -format=json "$role_path" >/dev/null 2>&1; then role_exists=1; fi

    # role config: crea se manca, riscrivi solo con --force
    if [[ "$role_exists" == "1" && "$FORCE" != "1" ]]; then
        echo "SKIP  role $principal (exists; use --force to rewrite)"
    else
        action="WROTE"
        [[ "$role_exists" == "1" ]] && action="FORCE"
        echo "$action role $principal (policy=$principal ttl=$TOKEN_TTL max_ttl=$TOKEN_MAX_TTL secret_id_ttl=$SECRET_ID_TTL)"
        if [[ "$DRY_RUN" != "1" ]]; then
            bao write "$role_path" \
                token_policies="$principal" \
                token_ttl="$TOKEN_TTL" \
                token_max_ttl="$TOKEN_MAX_TTL" \
                secret_id_ttl="$SECRET_ID_TTL" \
                secret_id_num_uses="$SECRET_ID_NUM_USES" >/dev/null
        fi
    fi

    # role-id + secret-id → state dir (0600, gitignored), MAI stdout.
    # Scrittura ATOMICA (tmp + mv) e check non-vuoto: un crash a metà non
    # lascia mai file parziali che la run successiva prenderebbe per validi.
    if [[ -s "$role_id_file" && -s "$secret_id_file" && "$FORCE" != "1" ]]; then
        echo "SKIP  material $principal (role-id + secret-id already in $dir)"
        continue
    fi
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY   material $principal (would write role-id + secret-id to $dir)"
        continue
    fi
    mkdir -p "$dir"
    chmod 700 "$dir"
    bao read -field=role_id "$role_path/role-id" > "$dir/.role-id.tmp"
    mv "$dir/.role-id.tmp" "$role_id_file"
    bao write -f -field=secret_id "$role_path/secret-id" > "$dir/.secret-id.tmp"
    mv "$dir/.secret-id.tmp" "$secret_id_file"
    chmod 600 "$role_id_file" "$secret_id_file"
    echo "WROTE material $principal -> $dir (0600, gitignored — copia i secret-id sui nodi, MAI in repo)"
done

echo "[approle] done: ${#PRINCIPALS[@]} principals (role-id/secret-id in $STATE_DIR/approle/)"
echo "[approle] verify with ./scripts/verify-approle.sh"
