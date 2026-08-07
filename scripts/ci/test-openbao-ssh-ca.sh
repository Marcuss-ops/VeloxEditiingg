#!/usr/bin/env bash
# scripts/ci/test-openbao-ssh-ca.sh
# ─────────────────────────────────────────────────────────────────────────────
# Test della CA SSH OpenBao (fase 7):
#   A. sintassi: bash -n + shellcheck sui 3 script nuovi;
#   B. check strutturali: policy ssh-operator, admin.hcl con ssh/*, playbook
#      bootstrap-ssh.yml con TrustedUserCAKeys, vault.yml.example con la var;
#   C. smoke LIVE (solo se un OpenBao è raggiungibile con il root token dallo
#      state dir, es. istanza locale): provision-ssh-ca idempotente, firma di
#      prova con TTL breve, verify-ssh-ca. In CI GitHub (nessun OpenBao) i
#      check A+B restano il gate e C viene skippato con notice.
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
grep -q 'TrustedUserCAKeys' "$ROOT/deploy/playbooks/bootstrap-ssh.yml" \
    || fail 'bootstrap-ssh.yml non configura TrustedUserCAKeys'
grep -q 'trusted-user-ca-keys.pem' "$ROOT/deploy/playbooks/bootstrap-ssh.yml" \
    || fail 'bootstrap-ssh.yml non usa /etc/ssh/trusted-user-ca-keys.pem'
grep -q 'vault_velox_ssh_ca_pubkey' "$ROOT/deploy/group_vars/vault.yml.example" \
    || fail 'vault.yml.example non documenta vault_velox_ssh_ca_pubkey'
grep -q 'ssh-ca.pub' "$PROVISION" \
    || fail 'provision-ssh-ca.sh non esporta la CA public key'
pass 'check strutturali OK (policy, playbook, vault, export CA)'

# ── C. Smoke live (solo se OpenBao raggiungibile con root token) ─────────────
LIVE=0
if [[ -f "$STATE_DIR/root-token" ]]; then
    BAO_ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"
    if curl -fsS -k -H "X-Vault-Token: $(cat "$STATE_DIR/root-token")" \
        "$BAO_ADDR/v1/sys/health" >/dev/null 2>&1; then
        LIVE=1
    fi
fi
if [[ "$LIVE" != "1" ]]; then
    pass "SKIP smoke live (nessun OpenBao raggiungibile) — gate A+B verdi"
    pass 'OK'
    exit 0
fi

export PATH="$HOME/.local/bin:$PATH"
export BAO_ADDR="${BAO_ADDR:-https://127.0.0.1:8200}"

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

pass '=== 5. sign con TTL custom e principals velox-admin ==='
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
if command -v ssh-keygen >/dev/null 2>&1; then
    ssh-keygen -q -t ed25519 -N '' -f "$TMP/op" >/dev/null 2>&1
    "$SIGN" --pubkey-file "$TMP/op.pub" --principals velox-admin --ttl 45m \
        --out "$TMP/op-cert.pub" >/dev/null || fail 'sign velox-admin fallita'
    ssh-keygen -L -f "$TMP/op-cert.pub" | grep -A3 'Principals:' | grep -q 'velox-admin' \
        || fail 'cert senza principal velox-admin'
    pass 'sign velox-admin TTL 45m OK (principal verificato con ssh-keygen -L)'
else
    pass 'SKIP sign test (ssh-keygen non disponibile)'
fi

pass 'OK'
