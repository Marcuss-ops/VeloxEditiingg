#!/usr/bin/env bash
# deploy/scripts/test-validate-master-env.sh
#
# Smoke test for deploy/validate-master-env.sh. Generates fixture env files
# in /tmp/, asserts the validator's exit code matches the expected outcome
# (PASS vs FAIL) per case. No system mutation beyond /tmp/ scratch.
#
# Run via: bash /path/to/repo/deploy/scripts/test-validate-master-env.sh
# Exit:    0 if every case matched expectation, 1 otherwise.
#
# Style: pure exit-code assertions. No output format dependency, no parsing.
# This is deliberate so a future validator rewrite cannot silently break the
# test by changing output strings — the contract is binary (PASS / FAIL).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VALIDATOR="${REPO_ROOT}/deploy/validate-master-env.sh"

if [[ ! -r "$VALIDATOR" ]]; then
    printf '[test][FAIL] validator not found at %s — refusing to run\n' "$VALIDATOR" >&2
    exit 1
fi

WORK="$(mktemp -d /tmp/velox-validate-master-env-test.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

PASS=0
FAIL=0
TOTAL=0

# Make a "happy path" env file; tests then mutate it per case.
make_vanilla() {
    local path="$1"
    cat > "$path" <<'EOF'
GIN_MODE=release
VELOX_MASTER_PORT=8000
MASTER_PUBLIC_URL=https://master.example.com
VELOX_ADMIN_TOKEN=this-is-a-strong-test-token-with-32-char-length-x
VELOX_ALLOWED_WORKERS=velox-worker-1,velox-worker-2
VELOX_GRPC_PORT=9000
VELOX_GRPC_TLS_CERT_FILE=/etc/velox/certs/server.crt
VELOX_GRPC_TLS_KEY_FILE=/etc/velox/certs/server.key
VELOX_GRPC_TLS_CA_FILE=/etc/velox/certs/ca.crt
VELOX_DB_PATH=/var/lib/velox/data/velox.db
EOF
}

# check_case <label> <expected_rc (0|1|2)> <fixture>
check_case() {
    local label="$1"
    local expected_rc="$2"
    local fixture="$3"
    local actual_rc=0
    set +e
    bash "$VALIDATOR" "$fixture" >/dev/null 2>&1
    actual_rc=$?
    set -e
    TOTAL=$((TOTAL + 1))
    if [[ "$actual_rc" == "$expected_rc" ]]; then
        PASS=$((PASS + 1))
        printf '  [OK]   %-50s (rc=%d)\n' "$label" "$actual_rc"
    else
        FAIL=$((FAIL + 1))
        printf '  [FAIL] %-50s (expected rc=%d, got rc=%d)\n' "$label" "$expected_rc" "$actual_rc"
    fi
}

printf '\n[test] running %s\n' "$VALIDATOR"
printf '[test] fixtures scratch dir: %s\n' "$WORK"

# ── Happy path: every field valid → PASS ─────────────────────────────────────
F="$WORK/01_valid.env"
make_vanilla "$F"
check_case "01 valid (happy path)"                  0 "$F"

# ── CHANGE_ME left in active line → FAIL ─────────────────────────────────────
F="$WORK/02_changeme_workers.env"
make_vanilla "$F"
printf '\nVELOX_ALLOWED_WORKERS=CHANGE_ME_ALLOWED_WORKERS\n' >> "$F"
# Replace the original line entirely:
sed -i 's|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=CHANGE_ME_ALLOWED_WORKERS|' "$F"
check_case "02 VELOX_ALLOWED_WORKERS=CHANGE_ME_*"  1 "$F"

# ── VELOX_ADMIN_TOKEN empty → FAIL ──────────────────────────────────────────
F="$WORK/03_no_admin.env"
make_vanilla "$F"
sed -i 's|^VELOX_ADMIN_TOKEN=.*|VELOX_ADMIN_TOKEN=|' "$F"
check_case "03 VELOX_ADMIN_TOKEN empty"             1 "$F"

# ── VELOX_ALLOWED_WORKERS empty → FAIL ───────────────────────────────────────
F="$WORK/04_no_workers.env"
make_vanilla "$F"
sed -i 's|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=|' "$F"
check_case "04 VELOX_ALLOWED_WORKERS empty"         1 "$F"

# ── VELOX_ALLOWED_WORKERS has '*' → FAIL ─────────────────────────────────────
F="$WORK/05_workers_wildcard.env"
make_vanilla "$F"
sed -i 's|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=*|' "$F"
check_case "05 VELOX_ALLOWED_WORKERS=*"             1 "$F"

# ── VELOX_ALLOWED_WORKERS has duplicate IDs → FAIL ──────────────────────────
F="$WORK/06_workers_duplicates.env"
make_vanilla "$F"
sed -i 's|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=velox-worker-1,velox-worker-1|' "$F"
check_case "06 duplicate worker IDs"               1 "$F"

# ── TLS triple incomplete (key missing) → FAIL ─────────────────────────────
F="$WORK/07_partial_tls.env"
make_vanilla "$F"
sed -i '/^VELOX_GRPC_TLS_KEY_FILE=/d' "$F"
check_case "07 TLS key missing (incomplete triple)" 1 "$F"

# ── No TLS at all, no insecure-dev opt-in → FAIL ────────────────────────────
F="$WORK/08_no_tls_no_dev.env"
make_vanilla "$F"
sed -i '/^VELOX_GRPC_TLS_/d' "$F"
check_case "08 TLS unset AND no insecure-dev flag"  1 "$F"

# ── MASTER_PUBLIC_URL malformed → FAIL ──────────────────────────────────────
F="$WORK/09_bad_url.env"
make_vanilla "$F"
sed -i 's|^MASTER_PUBLIC_URL=.*|MASTER_PUBLIC_URL=not-a-url|' "$F"
check_case "09 MASTER_PUBLIC_URL malformed"         1 "$F"

# ── VELOX_DB_PATH empty → FAIL ─────────────────────────────────────────────
F="$WORK/10_no_db_path.env"
make_vanilla "$F"
sed -i 's|^VELOX_DB_PATH=.*|VELOX_DB_PATH=|' "$F"
check_case "10 VELOX_DB_PATH empty"                 1 "$F"

# ── VELOX_GRPC_PORT non-numeric → FAIL ──────────────────────────────────────
F="$WORK/11_bad_grpc_port.env"
make_vanilla "$F"
sed -i 's|^VELOX_GRPC_PORT=.*|VELOX_GRPC_PORT=abc|' "$F"
check_case "11 VELOX_GRPC_PORT non-numeric"         1 "$F"

# ── http URL with rest ok → PASS (warning, not hard fail) ───────────────────
F="$WORK/12_http_url_warn.env"
make_vanilla "$F"
sed -i 's|^MASTER_PUBLIC_URL=.*|MASTER_PUBLIC_URL=http://master.example.com|' "$F"
check_case "12 http:// MASTER_URL (warning only)"   0 "$F"

# ── Summary ─────────────────────────────────────────────────────────────────
printf '\n'
if (( FAIL == 0 )); then
    printf '[test] PASS: %d/%d cases behaved as expected\n' "$PASS" "$TOTAL"
    exit 0
fi
printf '[test] FAIL: %d/%d cases mismatched expected exit code\n' "$FAIL" "$TOTAL"
exit 1
