#!/usr/bin/env bash
# deploy/openbao/scripts/verify-approle.sh
# ─────────────────────────────────────────────────────────────────────────────
# End-to-end verification of the AppRole machine identities:
#
#   for each principal (worker-<id>, master, admin):
#     1. login REALE con role-id + secret-id dallo state dir;
#     2. token lookup  → la policy attesa è applicata;
#     3. capabilities (REST /v1/sys/capabilities) → check POSITIVI (read/list
#        sul proprio ramo) e NEGATIVI (worker A NON legge il ramo di worker B;
#        master NON scrive; admin SÌ scrive).
#
# FAIL-CLOSED: un errore di verifica (curl/jq falliti) fa FALLIRE i check,
# mai passare — i check negativi sono validi SOLO se la verifica è riuscita
# e la capability risulta esplicitamente assente.
# Non stampa mai valori di secret. Esce con 1 al primo check fallito.
#
# Usage:
#   ./scripts/verify-approle.sh                     # tutti i principal
#   ./scripts/verify-approle.sh --principal master  # solo master

set -euo pipefail

OPENBAO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${OPENBAO_STATE_DIR:-"$OPENBAO_DIR/../../.velox/openbao"}"
ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
TOKEN_FILE="$STATE_DIR/root-token"
DEFAULT_WORKERS=(host_57_129_132_133 host_57_131_20_173 velox-worker-13197 velox-worker-523925eb)

PRINCIPALS=()
FAILED=0

usage() {
    echo "usage: $0 [--principal <name>]..." >&2
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --principal) PRINCIPALS+=("${2:-}"); shift 2 ;;
        -h|--help)   usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done
if [[ ${#PRINCIPALS[@]} -eq 0 ]]; then
    for w in "${DEFAULT_WORKERS[@]}"; do PRINCIPALS+=("worker-$w"); done
    PRINCIPALS+=(master admin)
fi

command -v bao >/dev/null 2>&1 || { echo "FATAL: 'bao' CLI not found on PATH" >&2; exit 1; }
command -v jq  >/dev/null 2>&1 || { echo "FATAL: 'jq' not found on PATH" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "FATAL: 'curl' not found on PATH" >&2; exit 1; }

export BAO_ADDR="$ADDR"
export BAO_SKIP_VERIFY="${BAO_SKIP_VERIFY:-true}"
# Caller per /v1/sys/capabilities: serve sudo → root token di bootstrap. Il
# token AppRole del principal va nel BODY della richiesta, mai in env.
if [[ -z "${BAO_TOKEN:-}" ]]; then
    [[ -f "$TOKEN_FILE" ]] || {
        echo "FATAL: no BAO_TOKEN and $TOKEN_FILE missing — run bootstrap-init.sh first" >&2
        exit 1
    }
    BAO_TOKEN="$(cat "$TOKEN_FILE")"
    export BAO_TOKEN
fi

check() {
    # $1 = descrizione, $2 = esito (0 ok / 1 ko)
    if [[ "$2" == "0" ]]; then
        echo "  PASS  $1"
    else
        echo "  FAIL  $1"
        FAILED=1
    fi
}

# has_cap: 0 = capability PRESENTE, 1 = assente (denied), 2 = verifica fallita.
has_cap() {
    local tok="$1" path="$2" cap="$3" body out rc
    body="$(jq -n --arg t "$tok" --arg p "$path" '{token: $t, path: $p}')"
    out="$(curl -fsS -k -X POST \
            -H "X-Vault-Token: $BAO_TOKEN" \
            --data-binary "$body" \
            "$ADDR/v1/sys/capabilities" 2>/dev/null)" || return 2
    jq -e --arg c "$cap" '.data.capabilities | index($c)' >/dev/null 2>&1 <<<"$out"
    rc=$?
    if [[ "$rc" == "0" ]]; then return 0; fi
    if [[ "$rc" == "1" ]]; then return 1; fi
    return 2   # parse error → verifica fallita
}

expect_cap() {
    # positiva: la capability DEVE esserci; qualsiasi errore → FAIL
    local desc="$1" tok="$2" path="$3" cap="$4" rc
    if has_cap "$tok" "$path" "$cap"; then rc=0; else rc=$?; fi
    if [[ "$rc" == "0" ]]; then check "$desc" 0; else check "$desc" 1; fi
}

expect_no_cap() {
    # negativa: la capability DEVE essere esplicitamente assente; se la
    # verifica fallisce (rc=2) o la cap è presente (rc=0) → FAIL (fail-closed)
    local desc="$1" tok="$2" path="$3" cap="$4" rc
    if has_cap "$tok" "$path" "$cap"; then rc=0; else rc=$?; fi
    case "$rc" in
        1) check "$desc" 0 ;;
        2) check "$desc (verifica non riuscita)" 1 ;;
        *) check "$desc (capability PRESENTE)" 1 ;;
    esac
}

for principal in "${PRINCIPALS[@]}"; do
    dir="$STATE_DIR/approle/$principal"
    echo "── $principal ──"

    if [[ ! -f "$dir/role-id" || ! -f "$dir/secret-id" ]]; then
        check "material present in $dir" 1
        continue
    fi

    # 1. login reale
    tok="$(bao write -field=token auth/approle/login \
        role_id="$(cat "$dir/role-id")" secret_id="$(cat "$dir/secret-id")" 2>/dev/null || true)"
    if [[ -z "$tok" ]]; then
        check "login with role-id + secret-id" 1
        continue
    fi
    check "login with role-id + secret-id" 0

    # 2. policy attesa applicata al token
    policies="$(BAO_TOKEN="$tok" bao token lookup -format=json 2>/dev/null \
        | jq -r '.data.policies[]' 2>/dev/null | tr '\n' ' ')"
    if [[ " $policies " == *" $principal "* ]]; then
        check "token policies include '$principal' (got: ${policies:-none})" 0
    else
        check "token policies include '$principal' (got: ${policies:-none})" 1
    fi

    # 3. autorizzazioni per classe di principal
    case "$principal" in
        worker-*)
            wid="${principal#worker-}"
            expect_cap "read cap on own branch (velox/data/.../$wid/*)" \
                "$tok" "velox/data/production/workers/$wid/credential" read
            expect_cap "list cap on own branch root (velox/metadata/.../$wid/)" \
                "$tok" "velox/metadata/production/workers/$wid/" list
            expect_no_cap "NO write cap on own branch" \
                "$tok" "velox/data/production/workers/$wid/credential" update
            other=""
            for w in "${DEFAULT_WORKERS[@]}"; do
                [[ "$w" == "$wid" ]] || { other="$w"; break; }
            done
            if [[ -n "$other" ]]; then
                expect_no_cap "NO read cap on OTHER worker ($other)" \
                    "$tok" "velox/data/production/workers/$other/credential" read
            fi
            expect_no_cap "NO read cap on master branch" \
                "$tok" "velox/data/production/master/admin-token" read
            ;;
        master)
            expect_cap "read cap on master branch" \
                "$tok" "velox/data/production/master/admin-token" read
            expect_cap "read cap on workers branch" \
                "$tok" "velox/data/production/workers/${DEFAULT_WORKERS[0]}/credential" read
            expect_cap "read cap on services/registry (token master migrati)" \
                "$tok" "velox/data/production/services/registry/token" read
            expect_no_cap "NO write cap on velox/data/*" \
                "$tok" "velox/data/production/master/admin-token" create
            expect_no_cap "NO approle management" \
                "$tok" "auth/approle/role/x" create
            ;;
        admin)
            expect_cap "write cap on velox/data/*" \
                "$tok" "velox/data/production/master/admin-token" update
            expect_cap "approle management cap" \
                "$tok" "auth/approle/role/x" create
            expect_cap "policy management cap" \
                "$tok" "sys/policies/acl/x" update
            expect_cap "ssh CA management cap" \
                "$tok" "ssh/config/ca" update
            ;;
        ssh-operator)
            expect_cap "sign cap on ssh/sign/velox-operator" \
                "$tok" "ssh/sign/velox-operator" update
            expect_cap "read CA pubkey cap (TrustedUserCAKeys)" \
                "$tok" "ssh/config/ca" read
            expect_no_cap "NO ssh role management" \
                "$tok" "ssh/roles/x" create
            expect_no_cap "NO KV write" \
                "$tok" "velox/data/production/master/admin-token" update
            ;;
    esac
    # revoca del token di test
    BAO_TOKEN="$tok" bao token revoke-self >/dev/null 2>&1 || true
done

echo
if [[ "$FAILED" == "1" ]]; then
    echo "verify-approle: FAIL (uno o più check non passati)" >&2
    exit 1
fi
echo "verify-approle: OK — ${#PRINCIPALS[@]} identità verificate (least privilege)"
