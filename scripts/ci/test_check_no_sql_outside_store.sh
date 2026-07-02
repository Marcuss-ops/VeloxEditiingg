#!/usr/bin/env bash
# scripts/ci/test_check_no_sql_outside_store.sh
#
# Unit test for scripts/ci/check-no-sql-outside-store.sh's method-call
# regex and Coordinator tx-lifecycle carve-out. Verifies:
#
#   1. The new `(db\.BeginTx|db\.Exec(Context)?|db\.Query(Context)?|...)`
#      regex matches BOTH plain AND Context-variant method calls on
#      db.* / tx.* receivers. The `(Context)?` GROUP is required:
#      bare `Context?` would only make the FINAL `t` optional per ERE
#      semantics and miss the plain-variant `db.Exec(` shape.
#   2. The receiver-form guard (TX_LIFECYCLE_ONLY + content literal
#      `c\.db\.BeginTx\(`) correctly exempts the coordinator's
#      tx-lifecycle line even though the broader regex matches it.
#   3. Pathological mixed lines (c.db.BeginTx on same line as
#      tx.ExecContext) are NOT exempted by the carve-out — they get
#      flagged.
#   4. Real-lint integration: the actual lint, run against the
#      canonical coordinator.go, must not flag any c.db.BeginTx line.
#
# Exit codes: 0 OK; 1 at least one assertion failed.
#
# This is a standalone unit test — it does NOT mutate the repo, does
# NOT add fixtures to DataServer/, and does NOT touch make verify. Run
# it directly: `./scripts/ci/test_check_no_sql_outside-store.sh`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

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

# ─── Test group 1: NEW regex coverage ──────────────────────────────────────
NEW_REGEX='(db\.BeginTx|db\.Exec(Context)?|db\.Query(Context)?|db\.QueryRow(Context)?|tx\.Exec(Context)?|tx\.Query(Context)?|tx\.QueryRow(Context)?)\('

# Each entry: "<want> <line>" — want=y means regex must match, want=n means it must NOT.
match_cases=(
  # plain methods on db.
  'y _ = db.Exec(ctx, q)'
  'y _ = db.Query(ctx, q)'
  'y _ = db.QueryRow(ctx, q)'
  # Context variants on db.
  'y _ = db.ExecContext(ctx, q)'
  'y _ = db.QueryContext(ctx, q)'
  'y _ = db.QueryRowContext(ctx, q)'
  # plain methods on tx.
  'y _ = tx.Exec(ctx, q)'
  'y _ = tx.Query(ctx, q)'
  'y _ = tx.QueryRow(ctx, q)'
  # Context variants on tx.
  'y _ = tx.ExecContext(ctx, q)'
  'y _ = tx.QueryContext(ctx, q)'
  'y _ = tx.QueryRowContext(ctx, q)'
  # BeginTx (no Context variant — bare alternative only).
  'y tx, err := db.BeginTx(ctx, nil)'
  # Receiver-form variants: the regex matches the receiver.method(
  # substring inside them. The lint decides EXEMPTION separately —
  # only the Coordinator's literal `c.db.BeginTx(` form is opted out
  # of flagging (its exemption path is verified in group 2 / group 3
  # below). ALL other receiver-form calls get flagged.
  'y _ = c.db.BeginTx(ctx, nil)'      # matches: substring `db.BeginTx(`
  'y _ = x.db.ExecContext(ctx, q)'    # matches: substring `db.ExecContext(`
  # Things the regex MUST NOT match (verbatim receiver-free or out-
  # of-list methods; NOTE: a string literal containing an in-list
  # method name will still match — the regex is syntax-only, not Go
  # AST-aware. False-positive exposure for SQL-shaped strings is a
  # known property of this regex, documented here to keep the test
  # honest about what it asserts).
  'n _ = tx.Commit()'                  # tx.Commit not in receiver-prefix list
  'n _ = tx.Rollback()'                # tx.Rollback not in receiver-prefix list
  'n _ = db.Begin()'                   # db.Begin ≠ db.BeginTx (no `T` suffix)
  'n _ := "tx.Commit()"'               # string literal of op name not in list
  'n var _ sql.DB = nil'               # type token, no receiver.method(
)
for pair in "${match_cases[@]}"; do
  want=$(echo "$pair" | awk '{print $1}')
  line=$(echo "$pair" | cut -d' ' -f2-)
  actual='n'
  echo "$line" | grep -qE "$NEW_REGEX" && actual='y'
  expect "regex $(printf '%-30s' "$line")" "$actual" "$want"
done

# ─── Test group 2: receiver-form guard logic in isolation ─────────────────
TX_LIFECYCLE_ONLY='^db\.BeginTx\($'

run_carveout() {
  local content="$1" sql_match_field="$2"
  # $sql_match_field is a newline-separated list of SQL_REGEX hits
  # (the same shape that `grep -oE "$NEW_REGEX"` would emit).
  local line_matches non_lifecycle
  line_matches="$(printf '%s\n' "$sql_match_field" 2>/dev/null || true)"
  non_lifecycle="$(printf '%s\n' "$line_matches" 2>/dev/null \
                    | grep -vE "${TX_LIFECYCLE_ONLY}" 2>/dev/null || true)"
  if [[ -z "${non_lifecycle:-}" ]] \
     && [[ -n "${line_matches:-}" ]] \
     && [[ "$content" =~ c\.db\.BeginTx\( ]]; then
    echo "EXEMPT"
  else
    echo "FLAGGED"
  fi
}

# Pure-tx-lifecycle line: ONLY db.BeginTx( extracted + content has c.db.BeginTx(
expect "carveout pure tx-lifecycle" \
  "$(run_carveout \
       'tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})' \
       'db.BeginTx(')" "EXEMPT"

# Pathological: c.db.BeginTx( AND tx.ExecContext( on same line → FLAG (mixed line).
expect "carveout pathological mixed line" \
  "$(run_carveout \
       'c.db.BeginTx(ctx, nil); _, _ = tx.ExecContext(ctx, "x")' \
       'db.BeginTx(
tx.ExecContext(')" "FLAGGED"

# Someone else's db.BeginTx — guard prohibits piggy-back.
expect "carveout someone.db.BeginTx" \
  "$(run_carveout \
       'tx, err := someone.db.BeginTx(ctx, nil)' \
       'db.BeginTx(')" "FLAGGED"

# Body-level tx.ExecContext only — NOT exempt, NOT in OUT-OF-UoW.
expect "carveout body-level tx.ExecContext" \
  "$(run_carveout \
       '_, _ = tx.ExecContext(ctx, q)' \
       'tx.ExecContext(')" "FLAGGED"

# db.QueryContext only — body-level read, no BeginTx at all.
expect "carveout db.QueryContext only" \
  "$(run_carveout \
       '_, _ = db.QueryContext(ctx, q)' \
       'db.QueryContext(')" "FLAGGED"

# ─── Test group 3: real-lint integration check on canonical coordinator.go ─
# Run the actual lint against main. The integration check verifies that the
# broader regex does NOT spuriously flag the coordinator's c.db.BeginTx
# line — that's the only line the carve-out protects. Other coordinator
# direct-SQL access (tx.ExecContext inside CompleteUpload, etc.) IS supposed
# to be flagged by the new stricter regex (those are body-level offenders we
# want surfaced).

LINT="$REPO_ROOT/scripts/ci/check-no-sql-outside-store.sh"
if [[ ! -x "$LINT" ]]; then
  chmod +x "$LINT"
fi

LINT_STDERR="$(mktemp)"
trap 'rm -f "$LINT_STDERR"' EXIT
( bash "$LINT" > /dev/null 2> "$LINT_STDERR" ) || true

coord_blocks="$(awk '
  /^Direct SQL access.*coordinator\.go/ { in_block=1; print; next }
  in_block==1 && /^check-no-sql-outside-store:.*file violation/ { in_block=0; next }
  in_block==1 { print }
' "$LINT_STDERR")"

if [[ -z "$coord_blocks" ]]; then
  expect "real lint: zero coordinator.go flagged lines" "0" "0"
else
  coord_begin_hits="$(printf '%s\n' "$coord_blocks" | grep -cE 'c\.db\.BeginTx\(' || true)"
  expect "real lint: c.db.BeginTx exempted in coordinator.go (count=0)" "$coord_begin_hits" "0"
fi

# ─── Summary ──────────────────────────────────────────────────────────────
echo
printf 'PASS=%d  FAIL=%d\n' "$PASS" "$FAIL_CNT"
[[ "$FAIL_CNT" -eq 0 ]] || exit 1
echo "test_check_no_sql_outside_store: OK"
