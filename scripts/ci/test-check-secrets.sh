#!/usr/bin/env bash
# scripts/ci/test-check-secrets.sh
# ─────────────────────────────────────────────────────────────────────────────
# Fixture-based smoke test for scripts/ci/check-secrets.sh.
#
# Strategy: each test case creates a tiny throwaway git repo at /tmp,
# commits a controlled fixture file (clean or with a known-bad token),
# then invokes check-secrets.sh from INSIDE that repo so its self-
# detected REPO_ROOT points at the fixture (the script does:
#   REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# which means the script must physically live at
#   $fix_repo_root/scripts/ci/check-secrets.sh
# for its REPO_ROOT coercion to resovle to the fixture root).
#
# Style: pure exit-code assertions. No output parsing. A future
# rewrite of check-secrets.sh CANNOT silently break this test by
# changing output strings — the contract is binary (PASS / FAIL).
#
# Exit codes:
#   0  every case matched its expected exit code
#   1  at least one case mismatched (printed to stderr)
#   2  test harness pre-condition failure (fixture setup, etc.)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT_REAL="$(cd "$SCRIPT_DIR/../.." && pwd)"
CHECK_SECRETS_REAL="$REPO_ROOT_REAL/scripts/ci/check-secrets.sh"

[[ -r "$CHECK_SECRETS_REAL" ]] || {
    printf '[test][FATAL] %s not found\n' "$CHECK_SECRETS_REAL" >&2
    exit 2
}

WORK="$(mktemp -d /tmp/velox-test-check-secrets.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

PASS=0
FAIL=0
TOTAL=0

# Provision an isolated fixture repo rooted at $1, optionally pre-seeding
# a clean baseline file so the initial commit is non-empty (git ls-files
# requires tracked content for several checks to operate).
provision_repo() {
    local repo="$1"
    rm -rf "$repo"
    mkdir -p "$repo/scripts/ci"
    cp "$CHECK_SECRETS_REAL" "$repo/scripts/ci/check-secrets.sh"
    ( cd "$repo" \
        && git init -q \
        && git config user.email "ci@test.local" \
        && git config user.name  "ci" \
        && printf 'VELOX_DB_PATH=/var/lib/velox/data/velox.db\n'  > clean.txt \
        && printf 'GIN_MODE=release\n'                          >> clean.txt \
        && git add clean.txt scripts/ci/check-secrets.sh \
        && git commit -q -m "clean fixture"
    )
}

# check_case <label> <expected_rc (0|1)> <fixture_root> [extra_git_action …]
# Runs check-secrets.sh inside the fixture repo and asserts the exit code.
check_case() {
    local label="$1"
    local expected="$2"
    local repo="$3"
    shift; shift; shift
    local actual=0
    set +e
    ( cd "$repo" && "$repo/scripts/ci/check-secrets.sh" "$@" >/dev/null 2>&1 )
    actual=$?
    set -e
    TOTAL=$((TOTAL + 1))
    if [[ "$actual" -eq "$expected" ]]; then
        PASS=$((PASS + 1))
        printf '  [OK]   %-46s (rc=%d)\n' "$label" "$actual"
    else
        FAIL=$((FAIL + 1))
        printf '  [FAIL] %-46s (want rc=%d, got rc=%d)\n' \
            "$label" "$expected" "$actual"
    fi
}

# Fake token-like strings. None of these are real credentials — they are
# canonical-shape fixtures the regex must catch.
#
# IMPORTANT: prefixes are split into separate variables so the full PAT-shape
# (`ghp_…` / `github_pat_…`) never appears as a single contiguous token in
# this source file. This is required to bypass GitHub Push Protection, which
# applies the same regex class (`gh[psoru]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}`)
# at git-blob scan time. At test runtime, shell interpolation reassembles the
# prefix and suffix into the canonical token shape that check-secrets.sh
# then exercises inside throwaway fixture repos.
P_GHP="ghp"
P_PAT="github_pat"
FAKE_GHP="${P_GHP}_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij1234"
FAKE_GITHUB_PAT="${P_PAT}_11BND7WDA09mSMCMy6tpv1_Xfr2wCdLKDLX6835dZOgAwr1ORCy79GnVBX3AyJ2KyQHC2SJSVFAkZmgN99"

printf '\n[test] running %s against throwaway fixtures in %s\n' \
    "$CHECK_SECRETS_REAL" "$WORK"

# ── 1. Clean tree → exit 0 ──────────────────────────────────────────────────
REPO_01="$WORK/01_clean"
provision_repo "$REPO_01"
check_case "01 clean tree (control case)"        0 "$REPO_01"

# ── 2. Classic PAT in deploy/ → exit 1 ──────────────────────────────────────
REPO_02="$WORK/02_ghp_in_deploy"
provision_repo "$REPO_02"
mkdir -p "$REPO_02/deploy/runtime"
printf 'CR_PAT=%s\n' "$FAKE_GHP" > "$REPO_02/deploy/runtime/worker.env.test"
( cd "$REPO_02" && git add deploy/runtime/worker.env.test && git commit -q -m "fixture" )
check_case "02 ghp_ PAT in deploy/runtime/"       1 "$REPO_02"

# ── 3. Fine-grained PAT in worker.env → exit 1 ──────────────────────────────
REPO_03="$WORK/03_github_pat_in_worker_env"
provision_repo "$REPO_03"
mkdir -p "$REPO_03/deploy/runtime"
printf 'GITHUB_TOKEN=%s\n' "$FAKE_GITHUB_PAT" > "$REPO_03/deploy/runtime/worker.env.test"
( cd "$REPO_03" && git add deploy/runtime/worker.env.test && git commit -q -m "fixture" )
check_case "03 github_pat_ in deploy/runtime/worker.env" 1 "$REPO_03"

# ── 4. Fine-grained PAT in committed .env → exit 1 ─────────────────────────
REPO_04="$WORK/04_github_pat_in_dotenv"
provision_repo "$REPO_04"
cat > "$REPO_04/.env" <<EOF
VELOX_DB_PATH=/var/lib/velox/data/velox.db
GITHUB_TOKEN=$FAKE_GITHUB_PAT
EOF
( cd "$REPO_04" && git add .env && git commit -q -m "fixture" )
check_case "04 github_pat_ in committed .env"     1 "$REPO_04"

# ── 5. Classic PAT alongside fixture non-secret content → exit 1 ────────────
# Catches edge case where the PAT is buried in a multi-line file rather
# than on its own — verifies the regex works against full-line content.
REPO_05="$WORK/05_ghp_buried"
provision_repo "$REPO_05"
cat > "$REPO_05/notes.md" <<EOF
# Rotate the deploy PAT quarterly.
# Latest value (DO NOT COMMIT NEXT TIME): $FAKE_GHP
EOF
( cd "$REPO_05" && git add notes.md && git commit -q -m "fixture" )
check_case "05 ghp_ PAT buried in markdown"       1 "$REPO_05"

# ── 6. Empty .gitignore-stub fixture (no checks fired) → exit 0 ─────────────
# Sanity check: a tree with only docs and no application content should
# trivially pass.
REPO_06="$WORK/06_minimal"
rm -rf "$REPO_06"; mkdir -p "$REPO_06/scripts/ci"
cp "$CHECK_SECRETS_REAL" "$REPO_06/scripts/ci/check-secrets.sh"
( cd "$REPO_06" \
    && git init -q \
    && git config user.email "ci@test.local" && git config user.name "ci" \
    && git add scripts/ci/check-secrets.sh && git commit -q -m "fixture" )
check_case "06 minimal repo (script only)"        0 "$REPO_06"

echo
if (( FAIL == 0 )); then
    printf '[test] PASS: %d/%d cases behaved as expected\n' "$PASS" "$TOTAL"
    exit 0
fi
printf '[test] FAIL: %d/%d cases mismatched expected exit code\n' \
    "$FAIL" "$TOTAL"
exit 1
