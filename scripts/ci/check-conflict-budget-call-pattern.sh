#!/usr/bin/env bash
# scripts/ci/check-conflict-budget-call-pattern.sh
#
# CI guard: enforce the contract documented in
# DataServer/internal/completion/conflict_budget.go's Record(...) method
# doc-comment. The method has a NIL-RETURN AMBIGUITY (returns nil both on
# reset AND on under-threshold ErrTransitionConflict), so naive callers
# that bind Record's return to a variable named `err` will SHADOW the
# caller's input and lose the ability to surface the original error
# unchanged.
#
# The CORRECT shape, per the doc-comment, is "keep the input err
# separate: if Record returns non-nil, escalate; otherwise surface the
# input err unchanged." The Coordinator encodes that shape in
# (*coordinator).recordAttemptCommitsCAS — the SOLE legitimate
# production caller of (*ConflictBudget).Record. New callers MUST funnel
# through that helper (or another helper that preserves the contract).
#
# ── Forbidden in production code ────────────────────────────────────────
#   1. Any direct call to a `*ConflictBudget`-typed value's `.Record(...)`
#      method in a file that is NOT in the allowlist below. Direct calls
#      outside the wrapper's owner file silently reintroduce the
#      nil-return ambiguity without going through the wrap-pattern.
#   2. The footgun shadow pattern: assigning Record's return value to a
#      variable named exactly `err`, which shadows the caller's input.
#      The wrapper deliberately uses `budgetErr` for the bind-side and
#      keeps the caller's `err` referenceable for the surface path.
#
# ── Allowed by exception ───────────────────────────────────────────────
#   - conflict_budget.go — owns the method DEFINITION (not a call).
#   - coordinator.go     — owns the wrapper; the wrapper has exactly one
#                          direct call inside (*coordinator).recordAttemptCommitsCAS,
#                          which is the SOLE legitimate production call
#                          site of (*ConflictBudget).Record. Any future
#                          direct call in this file MUST also live inside
#                          a wrapper function and MUST NOT shadow `err`.
#   - any _test.go file — tests legitimately exercise the primitive to
#                          verify the wrap-pattern semantics (see
#                          conflict_budget_test.go and the 3
#                          TestCoordinator_RecordAttemptCommitsCAS_*
#                          tests in coordinator_test.go).
#
# ── Exit codes ──────────────────────────────────────────────────────────
# 0 OK
# 1 violation detected (single failure mode matches
#   scripts/ci/check-compute-outcome-labels.sh convention).
#
# ── See also ────────────────────────────────────────────────────────────
# - DataServer/internal/completion/conflict_budget.go (Record method
#   doc-comment + ErrConflictBudgetExhausted typed sentinel)
# - DataServer/internal/completion/coordinator.go (recordAttemptCommitsCAS
#   helper that does the "keep input err separate" dance)
# - scripts/ci/check-compute-outcome-labels.sh (peer lint, sibling style)
# - scripts/ci/check-no-legacy.sh (peer lint, sibling style)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

OWNER_DIR="DataServer/internal/completion"

# Allowed files for direct (*ConflictBudget).Record(...) calls. Each
# entry corresponds to a single legitimate production call site:
#   conflict_budget.go — method DEFINITION (no direct call invocations
#                        of any identifier, the line `func (b *ConflictBudget)
#                        Record(err error) error` is the only Record(...)
#                        match — and it does NOT match the [id].Record(
#                        regex because `Record(err error)` has no `.`
#                        in front)
#   coordinator.go     — owns the wrapper; the wrapper has exactly
#                        ONE direct call, inside recordAttemptCommitsCAS
ALLOWED_FILES=(
  "${OWNER_DIR}/conflict_budget.go"
  "${OWNER_DIR}/coordinator.go"
)

violations=0

# ----------------------------------------------------------------------
# 1. Direct .Record(...) calls on a typed receiver in production Go
#    files outside the allowlist. The regex requires an identifier (Go
#    variable name) immediately followed by `.Record(` — the method
#    DEFINITION `Record(err error)` does NOT match because there is
#    no `.` immediately in front of `Record(`. Failure modes caught:
#      - c.budget.Record(...)         (canonical coordinator var)
#      - budget.Record(...)           (alternate var name)
#      - b.Record(...)                (single-letter var name)
#      - confBudget.Record(...)       (descriptive var name)
#    All of these re-trigger the nil-return ambiguity unless funneled
#    through the wrap-pattern.
# ----------------------------------------------------------------------
hits="$(grep -RInE '[a-zA-Z_][a-zA-Z0-9_]*\.Record\(' \
        "$OWNER_DIR" --include='*.go' --exclude='*_test.go' 2>/dev/null || true)"
if [[ -n "$hits" ]]; then
  filtered="$hits"
  for owner in "${ALLOWED_FILES[@]}"; do
    filtered="$(printf '%s\n' "$filtered" | grep -v "^${owner}:" || true)"
  done
  if [[ -n "$filtered" ]]; then
    printf 'GUARD ERROR: direct (*ConflictBudget).Record(...) call(s) in production code outside the wrapper owner file:\n' >&2
    printf '%s\n' "$filtered" >&2
    printf '\nAll production callers MUST go through (*coordinator).recordAttemptCommitsCAS or another helper that preserves the "keep input err separate" contract. See DataServer/internal/completion/conflict_budget.go::Record doc-comment for the rationale.\n' >&2
    violations=$((violations + 1))
  fi
fi

# ----------------------------------------------------------------------
# 2. Footgun shadow pattern: variable named exactly `err` on the LHS
#    of `:=`, where the RHS contains a `.Record(...)` invocation.
#    Examples that trip the guard:
#      if err := budget.Record(input); err != nil { escalate() }
#      else err := confBudget.Record(input)
#    Note that `budgetErr := c.budget.Record(err)` — the wrapper's
#    canonical bind — does NOT trip the guard because the LHS name is
#    `budgetErr`, not `err`.
# ----------------------------------------------------------------------
# We tighten the leading context with `[^a-zA-Z_]` so identifier
# prefixes (e.g. `myErr`, `callerErr`) are NOT flagged — only the
# exact-name shadow on `err` is the footgun.
footgun="$(grep -RInE '[^a-zA-Z_]err[[:space:]]*:=[[:space:]]*[A-Za-z_][A-Za-z0-9_]*\.Record\(' \
            "$OWNER_DIR" --include='*.go' --exclude='*_test.go' \
            2>/dev/null \
            | grep -vE '://[[:space:]]*err[[:space:]]*:=' || true)"
if [[ -n "$footgun" ]]; then
  printf 'GUARD ERROR: footgun shadow pattern — assigning Record(...) return to a variable named "err" hides the input err at the caller side:\n' >&2
  printf '%s\n' "$footgun" >&2
  printf '\nThe CORRECT shape is to bind a DISTINCT name (e.g. budgetErr) and surface the input err unchanged on the no-escalation path. See DataServer/internal/completion/coordinator.go::recordAttemptCommitsCAS for the canonical example.\n' >&2
  violations=$((violations + 1))
fi

if [[ "$violations" -gt 0 ]]; then
  printf '%d conflict-budget call-pattern violation(s) detected — see above (exit 1).\n' "$violations" >&2
  exit 1
fi

echo "check-conflict-budget-call-pattern: OK"
