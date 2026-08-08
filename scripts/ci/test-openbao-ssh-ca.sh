#!/usr/bin/env bash
# scripts/ci/test-openbao-ssh-ca.sh
# ─────────────────────────────────────────────────────────────────────────────
# Test della CA SSH OpenBao (fase 7):
#   A. sintassi: bash -n + shellcheck sui 3 script nuovi;
#   B. check strutturali: policy ssh-operator, admin.hcl con ssh/*, docs
#      openbao-ssh-ca.md con TrustedUserCAKeys, vault.yml.example con la var;
#   C. smoke LIVE (solo se un OpenBao è raggiungibile con il root token dallo
#      state dir, es. istanza locale): provision-ssh-ca idempotente, verifica
#      AppRole ssh-operator, firma di prova con TTL breve, verify-ssh-ca. In
#      CI GitHub (nessun OpenBao) i check A+B restano il gate e C viene skippato.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROVISION="$ROOT/deploy/openbao/scripts/provision-ssh-ca.sh"
SIGN="$ROOT/deploy/openbao/scripts/sign-operator-ssh.sh"
VERIFY="$ROOT/deploy/openbao/scripts/verify-ssh-ca.sh"
STATE_DIR="$ROOT/.velox/openbao"

fail() { printf 'openbao-ssh-ca: FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'openbao-ssh-ca: %s\n' "$*"; }

# ── A. Sintassi ──────────────────────────────────────────────────────────────
bash -n "$PROVISION" "$SIGN" "$VERIFY"
if command -v shellcheck >/dev/null 2>&1; then
    shellcheck -x "$PROVISION" "$SIGN" "$VERIFY"
fi
pass 'sintassi OK (bash -n, shellcheck)'

# ── B. Check strutturali ─────────────────────────────────────────────────────
grep -q 'ssh/sign/\*' "$ROOT/deploy/openbao/policies/ssh-operator.hcl" \
    || fail 'policy ssh-operator.hcl non copre ssh/sign/*'
grep -q 'ssh/\*' "$ROOT/deploy/openbao/policies/admin.hcl" \
    || fail 'admin.hcl non copre ssh/* (gestione CA)'
grep -q 'ssh CA management cap' "$ROOT/deploy/openbao/scripts/verify-approle.sh" \
    || fail 'verify-approle.sh non verifica la gestione ssh per admin'
grep -q 'TrustedUserCAKeys' "$ROOT/docs/openbao-ssh-ca.md" \
    || fail 'docs/openbao-ssh-ca.md non documenta TrustedUserCAKeys sui nodi'
grep -q 'trusted-user-ca-keys.pem' "$ROOT/docs/openbao-ssh-ca.md" \
    || fail 'docs/openbao-ssh-ca.md non documenta /etc/ssh/trusted-user-ca-keys.pem'
grep -q 'ssh-ca.pub' "$ROOT/docs/openbao-ssh-ca.md" \
    || fail 'docs/openbao-ssh-ca.md non documenta la CA public key (ssh-ca.pub)'
grep -q 'ssh-ca.pub' "$PROVISION" \
    || fail 'provision-ssh-ca.sh non esporta la CA public key'
# Strict TLS regression guard: operational OpenBao paths must always verify
# the server certificate. HTTP is permitted only by the explicit mock-test
# opt-ins in the dedicated master/worker test scripts.
if grep -RInE --exclude='test-openbao-ssh-ca.sh' --exclude='test-openbao-master-tokens.sh' \
    --exclude='test-openbao-worker-secrets.sh' \
    'curl[^[:cntrl:]]*(-k|--insecure)|BAO_SKIP_VERIFY=true|tls-skip-verify|CURL_TLS=\( -k|curl_tls=\( -k' \
    "$ROOT/deploy/openbao" "$ROOT/deploy/runtime" \
    "$ROOT/deploy/openbao/README.md" "$ROOT/docs/openbao-ssh-ca.md" \
    >/dev/null 2>&1; then
    fail 'strict TLS regression: insecure OpenBao transport flag found'
fi
grep -q 'BAO_CACERT' "$ROOT/deploy/openbao/compose.yml" \
    || fail 'compose.yml non configura BAO_CACERT'
pass 'check strutturali OK (policy, docs, vault, export CA, strict TLS)'

# ── C. Smoke live (solo se OpenBao raggiungibile con root token) ─────────────
LIVE=0
BAO_ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
TLS_CERT_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
if [[ -s "$STATE_DIR/root-token" || -n "${OPENBAO_ADDR:-}" ]]; then
    [[ -s "$STATE_DIR/root-token" ]] || fail "OpenBao live address is configured but root-token is missing: $STATE_DIR/root-token"
    [[ -s "$TLS_CERT_FILE" ]] || fail "OpenBao live material exists but TLS CA certificate is missing: $TLS_CERT_FILE"
    if curl --cacert "$TLS_CERT_FILE" -fsS \
        -H "X-Vault-Token: $(cat "$STATE_DIR/root-token")" \
        "$BAO_ADDR/v1/sys/health" >/dev/null 2>&1; then
        LIVE=1
    fi
fi
if [[ "$LIVE" != "1" ]]; then
    pass "SKIP smoke live (nessun materiale live OpenBao configurato) — gate A+B verdi"
    pass 'OK'
    exit 0
fi

export PATH="$HOME/.local/bin:$PATH"
export BAO_ADDR="$BAO_ADDR"
[[ -s "$TLS_CERT_FILE" ]] || fail "OpenBao TLS CA certificate missing: $TLS_CERT_FILE"
export BAO_CACERT="$TLS_CERT_FILE"
curl_tls=(--cacert "$TLS_CERT_FILE")

pass '=== 1. provision-ssh-ca (prima run) ==='
"$PROVISION" >/dev/null || fail 'provision-ssh-ca prima run fallita'
pass 'provision-ssh-ca run 1 OK'

pass '=== 2. idempotenza ==='
out="$("$PROVISION" 2>&1)"
echo "$out" | grep -q 'SKIP  ssh CA signing key' \
    || fail "la CA è stata rigenerata (rotazione!) — output: $out"
echo "$out" | grep -q 'SKIP  role velox-operator' \
    || fail "il role è stato riscritto — output: $out"
pass 'idempotenza OK (CA e role SKIP sulla seconda run)'

pass '=== 3. CA public key esportata ==='
CA_PUB="$STATE_DIR/ssh-ca.pub"
[[ -s "$CA_PUB" ]] || fail "CA public key mancante: $CA_PUB"
head -1 "$CA_PUB" | grep -qE '^(ssh-rsa|ssh-ed25519|ecdsa-|sk-)' \
    || fail "CA public key non sembra una chiave: $(head -c 40 "$CA_PUB")"
pass "CA public key OK ($CA_PUB, $(wc -l < "$CA_PUB") righe)"

pass '=== 4. verify-ssh-ca (firma di prova + negativo) ==='
"$VERIFY" || fail 'verify-ssh-ca fallita'

pass '=== 5. AppRole ssh-operator + sign con TTL custom ==='
TMP="$(mktemp -d)"
cleanup_approle_token() {
    if [[ -n "${APPROLE_TOKEN:-}" ]]; then
        curl -sS "${curl_tls[@]}" -X POST -H "X-Vault-Token: $APPROLE_TOKEN" \
            "$BAO_ADDR/v1/auth/token/revoke-self" >/dev/null 2>&1 || true
    fi
    rm -rf "$TMP"
}
trap cleanup_approle_token EXIT
ROLE_ID_FILE="$STATE_DIR/approle/ssh-operator/role-id"
SECRET_ID_FILE="$STATE_DIR/approle/ssh-operator/secret-id"
[[ -s "$ROLE_ID_FILE" && -s "$SECRET_ID_FILE" ]] \
    || fail 'materiale AppRole ssh-operator mancante'
login_body="$(jq -n --arg r "$(cat "$ROLE_ID_FILE")" --arg s "$(cat "$SECRET_ID_FILE")" \
    '{role_id:$r,secret_id:$s}')"
login_response="$TMP/approle-login.json"
curl -fsS "${curl_tls[@]}" -X POST -H 'Content-Type: application/json' \
    --data-binary "$login_body" "$BAO_ADDR/v1/auth/approle/login" > "$login_response" \
    || fail 'login AppRole ssh-operator fallito'
APPROLE_TOKEN="$(jq -r '.auth.client_token // empty' "$login_response")"
[[ -n "$APPROLE_TOKEN" ]] || fail 'login AppRole senza client token'
if ! curl -fsS "${curl_tls[@]}" -H "X-Vault-Token: $APPROLE_TOKEN" \
    "$BAO_ADDR/v1/auth/token/lookup-self" \
    | jq -e '(.data.policies | index("ssh-operator") != null)
        and (.data.meta.role_name == "ssh-operator")' >/dev/null; then
    fail 'token AppRole senza role metadata/policy ssh-operator'
fi
pass 'AppRole ssh-operator login + policy OK'
if command -v ssh-keygen >/dev/null 2>&1; then
    ssh-keygen -q -t ed25519 -N '' -f "$TMP/op" >/dev/null 2>&1
    if env -u BAO_TOKEN "$SIGN" --pubkey-file "$TMP/op.pub" \
        --out "$TMP/no-token-cert.pub" >/dev/null 2>&1; then
        fail 'sign senza BAO_TOKEN è riuscito'
    fi
    [[ ! -e "$TMP/no-token-cert.pub" ]] || fail 'sign senza token ha creato output'
    pass 'sign senza BAO_TOKEN rifiutato fail-closed'
    if BAO_TOKEN="$(cat "$STATE_DIR/root-token")" "$SIGN" \
        --pubkey-file "$TMP/op.pub" --out "$TMP/root-token-cert.pub" >/dev/null 2>&1; then
        fail 'sign con root-token è riuscito'
    fi
    [[ ! -e "$TMP/root-token-cert.pub" ]] || fail 'root-token ha creato output'
    pass 'sign con root-token rifiutato fail-closed'
    BAO_TOKEN="$APPROLE_TOKEN" "$SIGN" --pubkey-file "$TMP/op.pub" \
        --principals velox-admin --ttl 45m --out "$TMP/op-cert.pub" >/dev/null \
        || fail 'sign velox-admin via AppRole fallita'
    ssh-keygen -L -f "$TMP/op-cert.pub" | grep -A3 'Principals:' | grep -q 'velox-admin' \
        || fail 'cert senza principal velox-admin'
    pass 'sign velox-admin TTL 45m via AppRole OK (principal verificato)'
else
    pass 'SKIP sign test (ssh-keygen non disponibile)'
fi
pass 'OK'
