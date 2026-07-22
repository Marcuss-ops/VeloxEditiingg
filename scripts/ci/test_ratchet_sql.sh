#!/usr/bin/env bash
# scripts/ci/test_ratchet_sql.sh
#
# Self-test for scripts/ci/ratchet-sql.sh. Verifies:
#
#   1. The ratchet passes against the committed baseline.
#   2. A new file with a SQL violation is rejected.
#   3. An increased per-file violation count is rejected.
#   4. A decreased per-file violation count is reported (but does not
#      fail the ratchet; the developer must run --update to shrink the
#      baseline).
#
# This test does NOT modify the committed repository; it uses a temporary
# directory for synthetic scopes and baselines.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RATCHET="$REPO_ROOT/scripts/ci/ratchet-sql.sh"

PASS=0
FAIL_CNT=0

expect() {
  local label="$1" actual="$2" want="$3"
  if [[ "$actual" == "$want" ]]; then
    printf '  [PASS] %s\n' "$label"
    PASS=$((PASS + 1))
  else
    printf '  [FAIL] %s  got=%q want=%q\n' "$label" "$actual" "$want"
    FAIL_CNT=$((FAIL_CNT + 1))
  fi
}

# ── Test 1: ratchet passes against committed baseline ────────────────────────
if bash "$RATCHET" >/dev/null 2>&1; then
  expect "ratchet passes against committed baseline" "ok" "ok"
else
  expect "ratchet passes against committed baseline" "fail" "ok"
fi

# ── Synthetic-scope tests ───────────────────────────────────────────────────
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT
FAKE_SCOPE="$TMPDIR/DataServer/internal"
mkdir -p "$FAKE_SCOPE"

# Copy the ratchet and the committed baseline so we can point them at the
# synthetic scope without touching the repository.
cp "$RATCHET" "$TMPDIR/ratchet-sql.sh"
cp "$REPO_ROOT/scripts/ci/sql-baseline.txt" "$TMPDIR/sql-baseline.txt"
chmod +x "$TMPDIR/ratchet-sql.sh"

# ── Test 2: new file with SQL coupling is rejected ──────────────────────────
cat > "$FAKE_SCOPE/fake_new.go" <<'EOF'
package fake

import "database/sql"

func foo(db *sql.DB) {
    _, _ = db.Exec("INSERT INTO t VALUES (1)")
}
EOF

if bash "$TMPDIR/ratchet-sql.sh" --scope "$FAKE_SCOPE" --baseline "$TMPDIR/sql-baseline.txt" >/dev/null 2>&1; then
  expect "new file with SQL coupling is rejected" "fail" "ok"
else
  expect "new file with SQL coupling is rejected" "ok" "ok"
fi

# ── Test 3: increased per-file count is rejected ───────────────────────────
# Remove the previous new file so only the controlled fake remains.
rm -f "$FAKE_SCOPE/fake_new.go"
# This file has 3 SQL-coupled lines: the import, a db.Exec call, and a
# db.Query call. A baseline that claims only 2 is a regression.
cat > "$FAKE_SCOPE/fake_existing.go" <<'EOF'
package fake

import "database/sql"

func foo(db *sql.DB) {
    _, _ = db.Exec(q)
    _, _ = db.Query(q)
}
EOF

FAKE_BASELINE="$TMPDIR/sql-baseline-increased.txt"
printf '2 %s\n' "$FAKE_SCOPE/fake_existing.go" > "$FAKE_BASELINE"
if bash "$TMPDIR/ratchet-sql.sh" --scope "$FAKE_SCOPE" --baseline "$FAKE_BASELINE" >/dev/null 2>&1; then
  expect "increased per-file count is rejected" "fail" "ok"
else
  expect "increased per-file count is rejected" "ok" "ok"
fi

# ── Test 4: decreased per-file count is reported (not a failure) ──────────────
# Baseline claims the file has 4 violations, but the file has 3.
printf '4 %s\n' "$FAKE_SCOPE/fake_existing.go" > "$FAKE_BASELINE"
if bash "$TMPDIR/ratchet-sql.sh" --scope "$FAKE_SCOPE" --baseline "$FAKE_BASELINE" >/dev/null 2>&1; then
  expect "decreased per-file count is reported without failing" "ok" "ok"
else
  expect "decreased per-file count is reported without failing" "fail" "ok"
fi

# ── Summary ────────────────────────────────────────────────────────────────
echo
printf 'PASS=%d  FAIL=%d\n' "$PASS" "$FAIL_CNT"
[[ "$FAIL_CNT" -eq 0 ]] || exit 1
echo "test_ratchet_sql: OK"
