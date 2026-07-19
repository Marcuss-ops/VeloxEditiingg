#!/usr/bin/env bash
# =============================================================================
# scripts/ci/inspect-branch-protection.sh
# =============================================================================
# Read-only inspector for the Phase 0 branch-protection payload on a branch.
# Prints the canonical fields:
#   * required_status_checks.{strict, contexts[]}
#   * required_pull_request_reviews.{dismiss_stale_reviews,
#     require_code_owner_reviews, required_approving_review_count}
#   * enforce_admins, required_linear_history, allow_force_pushes,
#     required_conversation_resolution
#
# Used by:
#   * Operators before re-running enable-branch-protection.sh (verify state
#     before re-applying).
#   * Auditors checking whether the four Phase-0 required checks are wired.
#
# Exit codes:
#   0   branch protection present, all seven canonical checks are required
#   1   branch protection present, but one or more required checks missing
#   2   usage error
#   3   gh not authenticated
#   4   branch has no protection configured at all (or not on default branch)
#
# Usage:
#   ./scripts/ci/inspect-branch-protection.sh
#   BRANCH=release/1.x ./scripts/ci/inspect-branch-protection.sh
# =============================================================================

set -uo pipefail

BRANCH="${BRANCH:-main}"

# ─── Pre-flight ───────────────────────────────────────────────────────────
if ! command -v gh >/dev/null 2>&1; then
  printf '::error::gh CLI missing\n' >&2; exit 2
fi
if ! gh auth status >/dev/null 2>&1; then
  printf '::error::gh not authenticated — run "gh auth login"\n' >&2; exit 3
fi

REMOTE_URL="$(gh repo view --json url -q .url 2>/dev/null || true)"
OWNER="$(printf '%s' "$REMOTE_URL" | sed -E 's#https?://github.com/([^/]+)/.*#\1#')"
REPO="$(printf  '%s' "$REMOTE_URL" | sed -E 's#https?://github.com/[^/]+/([^/.]+)(\.git)?/?$#\1#')"

PROTECT_PATH="/repos/${OWNER}/${REPO}/branches/${BRANCH}/protection"

# ─── Fetch ────────────────────────────────────────────────────────────────
RAW="$(gh api \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "$PROTECT_PATH" 2>/dev/null || true)"

if [[ -z "$RAW" ]]; then
  printf '::error::no protection on %s/%s:%s (404 from GitHub)\n' \
    "$OWNER" "$REPO" "$BRANCH" >&2
  printf '  Run ./scripts/ci/enable-branch-protection.sh to apply Phase 0.\n' >&2
  exit 4
fi

printf '→ target: %s/%s  branch: %s\n\n' "$OWNER" "$REPO" "$BRANCH"

# ─── Pretty-print ────────────────────────────────────────────────────────
printf '%s' "$RAW" | python3 -m json.tool

# ─── Audit ────────────────────────────────────────────────────────────────
CANONICAL_REQUIRED=(
  "CI / make verify"
  "E2E gRPC control plane / make e2e-grpc (6-case matrix)"
  "E2E workload (real) / make e2e-workload (Hello→Artifact→SUCCEEDED)"
  "E2E workload-mTLS (PR 7) / make e2e-workload-mtls (mTLS, channel=staging)"
  "Pre-existing Test Watchlist / Pre-existing Test Watchlist"
  "no-youtube-regression / YouTube regression guard"
  "check-canonical-names / Canonical names guard"
)

ACTUAL="$(printf '%s' "$RAW" | python3 -c '
import json, sys
d = json.load(sys.stdin)
ctx = d.get("required_status_checks", {}).get("contexts", []) or []
for c in ctx: print(c)
' 2>/dev/null || true)"

missing=0
printf '\n→ required_status_checks audit:\n'
for c in "${CANONICAL_REQUIRED[@]}"; do
  if grep -Fxq "$c" <<<"$ACTUAL"; then
    printf '   ✓ %s\n' "$c"
  else
    printf '   ✗ MISSING: %s\n' "$c"
    missing=$((missing + 1))
  fi
done

if (( missing == 0 )); then
  printf '\n✓ all four Phase-0 required checks present.\n'
  exit 0
else
  printf '\n::error::%d canonical check(s) missing — re-run enable-branch-protection.sh\n' \
    "$missing" >&2
  exit 1
fi
