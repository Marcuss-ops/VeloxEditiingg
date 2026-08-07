#!/usr/bin/env bash
# =============================================================================
# scripts/ci/enable-branch-protection.sh
# =============================================================================
# Phase 0 (100% certification plan) — branch-protection enforcer.
#
# Configures GitHub branch protection on `main` so that:
#   * Every PR MUST pass all SEVEN canonical required checks:
#       1. CI / make verify
#       2. E2E gRPC control plane / make e2e-grpc (6-case matrix)
#       3. E2E workload (real) / make e2e-workload (Hello→Artifact→SUCCEEDED)
#       4. E2E workload-mTLS (PR 7) / make e2e-workload-mtls (mTLS, channel=staging)
#       5. Pre-existing Test Watchlist / Pre-existing Test Watchlist
#          ↑ Tier-2 follow-up (added 2026-07-03): surfaces the 4
#            known-previously-flaky tests as a NAMED PR status check
#            rather than buried in the aggregate workspace-tests.yml
#            log. Workflow: `.github/workflows/pre-existing-test-watchlist.yml`.
#       6. no-youtube-regression / YouTube regression guard
#          ↑ Tier-2 follow-up (added PR-15.16): forbids re-introduction
#            of any direct Velox-side YouTube integration after the
#            YouTube → Social API closure. Workflow:
#            `.github/workflows/no-youtube-regression.yml`.
#       7. check-canonical-names / Canonical names guard
#          ↑ Tier-2 follow-up (added PR-15.17): mirror of
#            no-youtube-regression. Forbids re-introduction of the
#            deprecated `SocialDestinationID` typed alias (dropped
#            in Residuo 5) and asserts `external_destination_id`
#            canonical findability in the 5 typed-source layers.
#            Workflow: `.github/workflows/check-canonical-names.yml`.
#   * strict=true        — branches MUST be green-up-to-date with main
#   * enforce_admins=true — even admins cannot bypass
#   * required_linear_history=true — no merge commits on main
#   * allow_force_pushes=false, allow_deletions=false — immutable history
#   * required_conversation_resolution=true — PR comments must resolve
#   * require_code_owner_reviews=true, required_approving_review_count=1
#
# OUT-OF-REQUIREMENT (currently advisory only, see §11 of
# docs/100-percent-plan/ci-required-checks.md):
#   - `Workspace Tests / Workspace Tests`       (.github/workflows/workspace-tests.yml)
#   - `Routing Invariants / Routing Invariants` (.github/workflows/routing-invariants.yml)
#   - `Typed Metrics Must-Pass / Typed Metrics Must-Pass` (.github/workflows/typed-metrics-must-pass.yml)
#   - `Deploy / Deploy (resolve digests + verify signatures + Ansible)` (.github/workflows/deploy.yml)
#   - `ci-opaque-wire / ci-opaque-wire`         (.github/workflows/ci-opaque-wire.yml)
#   - `no-youtube-regression / no-youtube-regression` (.github/workflows/no-youtube-regression.yml — single-job variant of the Phase-0 entry above; the canonical status check listed in #6 above uses the `YouTube regression guard` job name)
#
# These four additional workflows run in parallel with the canonical
# 5 but are NOT required for merge today. They are the Tier-2
# promotion target once the watchlist addition has soaked for one
# release cycle. Promotion is the same script with the contexts[]
# array widened — this file is the single source of truth.
#
# NOTE on the user's reference to a `release-gates` workflow: the
# `.github/workflows/release-gates.yml` file does NOT exist in the
# repo as of this commit. The user-named release-gates slot is
# currently occupied by `deploy.yml` (whose single job renders as
# `Deploy / Deploy (resolve digests + verify signatures + Ansible)`).
# If a dedicated release-gates.yml workflow should replace or sit
# beside deploy.yml, that is a separate decision tracked in §11 of
# the operator runbook.
#
# Idempotent: re-running with the same payload is a no-op (GitHub's PUT
# semantics). The script reads the current remote via `gh repo view`
# so it works from any local clone as long as `gh` is authenticated.
#
# Prerequisites (one-time):
#   gh auth login                                 # authenticate
#   gh auth status                                # confirm
#
# Usage:
#   ./scripts/ci/enable-branch-protection.sh            # apply
#   ./scripts/ci/enable-branch-protection.sh --dry-run  # print JSON, no PUT
#   BRANCH=release/1.x ./scripts/ci/enable-branch-protection.sh --dry-run
#
# Companion script: scripts/ci/disable-branch-protection.sh (escape hatch).
# See docs/100-percent-plan/ci-required-checks.md for the operator runbook.
# =============================================================================

set -euo pipefail

BRANCH="${BRANCH:-main}"
DRY_RUN=0

# ─── Args ─────────────────────────────────────────────────────────────────
if [[ $# -gt 0 ]]; then
  case "$1" in
    --dry-run|-n) DRY_RUN=1 ;;
    --help|-h)
      sed -n '2,40p' "$0" | sed 's/^# //; s/^#//'
      exit 0
      ;;
    *)
      printf 'unknown arg: %s\n' "$1" >&2
      exit 2
      ;;
  esac
fi

# ─── Pre-flight: gh auth ─────────────────────────────────────────────────
if ! command -v gh >/dev/null 2>&1; then
  printf '::error::gh CLI missing — install from https://cli.github.com\n' >&2
  exit 2
fi
if ! gh auth status >/dev/null 2>&1; then
  printf '::error::gh not authenticated — run "gh auth login" first\n' >&2
  exit 2
fi

# ─── Resolve OWNER / REPO from local remote ─────────────────────────────
REMOTE_URL="$(gh repo view --json url -q .url 2>/dev/null || true)"
if [[ -z "$REMOTE_URL" ]]; then
  printf '::error::could not resolve repo via gh (auth OK?) — aborting\n' >&2
  exit 3
fi
OWNER="$(printf '%s' "$REMOTE_URL" | sed -E 's#https?://github.com/([^/]+)/.*#\1#')"
REPO="$(printf  '%s' "$REMOTE_URL" | sed -E 's#https?://github.com/[^/]+/([^/.]+)(\.git)?/?$#\1#')"

PROTECT_PATH="/repos/${OWNER}/${REPO}/branches/${BRANCH}/protection"

printf '→ target: %s\n' "${OWNER}/${REPO}"
printf '→ branch: %s\n' "$BRANCH"
printf '→ endpoint: PUT %s\n' "$PROTECT_PATH"

# ─── Payload ─────────────────────────────────────────────────────────────
# IMPORTANT — the `contexts[]` strings below are derived from
# `<github.workflow> / <jobs.<id>.name>` for the 5 canonical gates.
# If any of those workflows or jobs is RENAMED, this contexts[] array
# must be updated IN PARALLEL with the workflow change AND the
# CANONICAL_REQUIRED array in scripts/ci/inspect-branch-protection.sh.
# The 5th context (`Pre-existing Test Watchlist / Pre-existing Test
# Watchlist`) was added on 2026-07-03 as the Tier-2 follow-up
# described in §11 of the operator runbook.
read -r -d '' PAYLOAD <<'JSON' || true
{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "CI / make verify",
      "E2E gRPC control plane / make e2e-grpc (6-case matrix)",
      "E2E workload (real) / make e2e-workload (Hello→Artifact→SUCCEEDED)",
      "E2E workload-mTLS (PR 7) / make e2e-workload-mtls (mTLS, channel=staging)",
      "Pre-existing Test Watchlist / Pre-existing Test Watchlist",
      "no-youtube-regression / YouTube regression guard",
      "check-canonical-names / Canonical names guard"
    ]
  },
  "required_pull_request_reviews": {
    "dismissal_restrictions": {},
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": true,
    "required_approving_review_count": 1,
    "require_last_push_approval": false
  },
  "required_linear_history": true,
  "required_conversation_resolution": true,
  "enforce_admins": true,
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false,
  "lock_branch": false,
  "allow_fork_syncing": false
}
JSON

# ─── Dry-run: print + exit ───────────────────────────────────────────────
if (( DRY_RUN )); then
  printf '\n--- DRY RUN: would PUT the following JSON to %s ---\n\n' "$PROTECT_PATH"
  printf '%s\n' "$PAYLOAD" | python3 -m json.tool
  printf '\n(dry run: no PUT issued)\n'
  exit 0
fi

# ─── Apply ───────────────────────────────────────────────────────────────
printf '%s' "$PAYLOAD" | gh api \
  --method PUT \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "$PROTECT_PATH" \
  --input - \
  >/tmp/velox-branch-protection-resp.json

printf '✓ branch protection applied on %s\n' "$BRANCH"
printf '  Verify: gh api /repos/%s/%s/branches/%s/protection | python3 -m json.tool\n' \
  "$OWNER" "$REPO" "$BRANCH"
printf '  Or:    ./scripts/ci/inspect-branch-protection.sh\n'
