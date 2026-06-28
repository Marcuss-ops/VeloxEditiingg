#!/usr/bin/env bash
# =============================================================================
# scripts/ci/disable-branch-protection.sh
# =============================================================================
# Phase 0 escape hatch — REMOVES branch protection on `main`.
#
# Use ONLY for incident response (e.g. blocked hotfix when all four
# required checks are broken infrastructure-wide). The 100% certification
# plan DEMANDS branch protection as a release gate; stripping it is a
# security event. The script prints a loud warning + fails closed if the
# operator declines to confirm by typing "yes" within 10s of the prompt.
#
# Companion: scripts/ci/enable-branch-protection.sh (re-applies the
# canonical Phase 0 protection — ALWAYS re-run after this script).
#
# Usage:
#   ./scripts/ci/disable-branch-protection.sh
#   BRANCH=release/1.x ./scripts/ci/disable-branch-protection.sh
# =============================================================================

set -euo pipefail

BRANCH="${BRANCH:-main}"

# ─── Pre-flight ───────────────────────────────────────────────────────────
if ! command -v gh >/dev/null 2>&1; then
  printf '::error::gh CLI missing\n' >&2; exit 2
fi
if ! gh auth status >/dev/null 2>&1; then
  printf '::error::gh not authenticated\n' >&2; exit 2
fi

REMOTE_URL="$(gh repo view --json url -q .url 2>/dev/null || true)"
if [[ -z "$REMOTE_URL" ]]; then
  printf '::error::could not resolve repo via gh\n' >&2; exit 3
fi
OWNER="$(printf '%s' "$REMOTE_URL" | sed -E 's#https?://github.com/([^/]+)/.*#\1#')"
REPO="$(printf  '%s' "$REMOTE_URL" | sed -E 's#https?://github.com/[^/]+/([^/.]+)(\.git)?/?$#\1#')"

PROTECT_PATH="/repos/${OWNER}/${REPO}/branches/${BRANCH}/protection"

# ─── Operator confirm ────────────────────────────────────────────────────
printf '\n'
printf '╔════════════════════════════════════════════════════════════════╗\n'
printf '║  WARNING: removing branch protection on %s/%s:%s\n' \
       "$OWNER" "$REPO" "$BRANCH"
printf '║\n'
printf '║  This violates Phase 0 of the 100%% certification plan.\n'
printf '║  All 4 required checks (verify / e2e-grpc / e2e-workload /\n'
printf '║  e2e-workload-mtls) will be removed from the merge gate.\n'
printf '║\n'
printf '║  Use ONLY for incident response. Restore via\n'
printf '║  ./scripts/ci/enable-branch-protection.sh IMMEDIATELY after.\n'
printf '╚════════════════════════════════════════════════════════════════╝\n'
printf '\n'

if [[ "${VELOX_FORCE_REMOVE_PROTECTION:-}" != "1" ]]; then
  # Interactive confirm — safe in TTY, silent in CI.
  if [[ -t 0 ]]; then
    printf 'Type "yes" (lowercase) within 10s to proceed: '
    if ! read -r -t 10 reply || [[ "$reply" != "yes" ]]; then
      printf 'aborted (reply was %q)\n' "${reply:-<empty>}" >&2
      exit 4
    fi
  else
    printf 'non-tty session: set VELOX_FORCE_REMOVE_PROTECTION=1 to acknowledge.\n' >&2
    exit 4
  fi
fi

# ─── Apply ────────────────────────────────────────────────────────────────
printf '→ DELETE %s\n' "$PROTECT_PATH"
gh api \
  --method DELETE \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "$PROTECT_PATH" \
  -o /tmp/velox-branch-protection-resp.json \
  -w '%{http_code}' 2>/dev/null) || http_code="000"

# Capture HTTP status. Expected responses:
#   204 No Content — successful DELETE
#   404 Not Found — protection was not configured (idempotent no-op)
# Anything else (401 / 403 / 5xx / network) bubbles up HARD so the
# operator knows the protection state is uncertain.
case "${http_code}" in
  204|404) ;;  # expected
  *)
    printf '::error::DELETE returned HTTP %s — branch protection state is UNCERTAIN\n' \
      "$http_code" >&2
    cat /tmp/velox-branch-protection-resp.json 2>/dev/null >&2 || true
    printf '\nInspect manually: gh api /repos/%s/%s/branches/%s/protection\n' \
      "$OWNER" "$REPO" "$BRANCH" >&2
    exit 5
    ;;
esac

printf '✓ branch protection REMOVED on %s (http=%s)\n' "$BRANCH" "$http_code"
printf '  REMINDER: re-apply via ./scripts/ci/enable-branch-protection.sh\n' — both fine.

printf '✓ branch protection REMOVED on %s\n' "$BRANCH"
printf '  REMINDER: re-apply via ./scripts/ci/enable-branch-protection.sh\n'
