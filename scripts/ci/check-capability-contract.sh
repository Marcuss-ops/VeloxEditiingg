#!/usr/bin/env bash
# scripts/ci/check-capability-contract.sh
#
# Enforces the architectural capability rule (AGENTS.md §6): every
# capability is DISABLED, READY or MISCONFIGURED — never "enabled but
# with a hidden nil/noop/stub".
#
# Two layers (defence in depth):
#   1. Forbidden fail-open symbols in PRODUCTION code (full-tree,
#      _test.go + docs excluded). Noop/test doubles are test-only; the
#      only production mentions allowed are the explicit dev-mode wiring
#      and the fail-closed rejection sites, listed per pattern.
#   2. Readiness pairing: every AddReadinessCapability("X", ...) in
#      cmd/server/bootstrap_readiness.go MUST be paired with a
#      fail-closed AddReadinessCheck("X-capability", ...) so a
#      MISCONFIGURED dependency flips /ready red (readiness rossa su
#      dipendenze mancanti). The same invariant is pinned at runtime by
#      TestReadiness_CapabilityExposuresHaveFailClosedGates in
#      cmd/server/bootstrap_readiness_test.go.
#
# Full-tree (not diff-scoped): work goes directly on main (AGENTS.md §3),
# so regressions anywhere in the tree must fail CI.
#
# NOTE: patterns are written WITHOUT backslashes on purpose — BRE
# character classes ([.], [(]) and grep -F fixed strings survive every
# shell/JSON escaping layer, where a backslash-escaped metacharacter has
# repeatedly been doubled and silently broken the match.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

violations=0

fail() { printf 'CAPABILITY-CONTRACT ERROR: %s\n' "$*" >&2; exit 1; }

# ── Layer 1: forbidden fail-open symbols in production code ──────────────
#
# Pattern scans cover PRODUCTION code only. Documentation, CI workflows,
# deploy templates and this script may quote the forbidden symbols while
# describing the rule (historical references are intentional, mirrors
# check-no-legacy.sh).
exclusions=(
  ':!*_test.go'
  ':!docs/**'
  ':!AGENTS.md'
  ':!README.md'
  ':!.github/**'
  ':!deploy/**'
  ':!scripts/ci/check-capability-contract.sh'
)

# NoopOperationExecutor is test-only (NewTestExecutorRegistry / _test.go).
# A production registry must start empty; a missing executor yields
# EXECUTOR_NOT_CONFIGURED, never a noop success.
hits="$(git grep -nE 'NoopOperationExecutor' -- "${exclusions[@]}" || true)"
if [[ -n "$hits" ]]; then
  printf 'NoopOperationExecutor must not appear in production code (test-only double):\n%s\n\n' "$hits" >&2
  violations=$((violations + 1))
fi

# StubAssetResolver is development-only. The three allow-listed files are
# the type/constructor definition, the fail-closed production rejection
# (MisconfiguredSmokeCapability) and the VELOX_SMOKE_MODE=development
# wiring — every other production mention is a hidden-stub regression.
stub_hits="$(git grep -nE 'NewStubAssetResolver' -- \
  "${exclusions[@]}" \
  ':!DataServer/internal/fleet/level_d_smoke_deps.go' \
  ':!DataServer/internal/fleet/level_d_smoke_bootstrap.go' \
  ':!DataServer/cmd/server/bootstrap_wiring.go' || true)"
if [[ -n "$stub_hits" ]]; then
  printf 'NewStubAssetResolver is development-only; production mention outside the allow-list:\n%s\n\n' "$stub_hits" >&2
  violations=$((violations + 1))
fi

# Constructors must fail closed on missing dependencies: a nil/typed-nil
# datasource passed to opsalerts.NewEngine in production would produce a
# silently-degraded (MISCONFIGURED) capability instead of a boot error.
# The qualifier is intentionally wildcarded (opsalerts.NewEngine or an
# import alias) so an aliased call cannot slip past the gate.
nil_hits="$(git grep -nE '[.]NewEngine[(][^)]*nil' -- "${exclusions[@]}" || true)"
if [[ -n "$nil_hits" ]]; then
  printf 'opsalerts.NewEngine must not receive a nil dependency in production code:\n%s\n\n' "$nil_hits" >&2
  violations=$((violations + 1))
fi

# ── Layer 2: readiness pairing in bootstrap_readiness.go ─────────────────

readiness_file="DataServer/cmd/server/bootstrap_readiness.go"
[[ -f "$readiness_file" ]] || fail "missing $readiness_file"

cap_names="$(grep -o 'AddReadinessCapability("[^"]*"' "$readiness_file" \
  | sed 's/.*("//; s/"$//' || true)"
for name in $cap_names; do
  needle="$(printf 'AddReadinessCheck("%s-capability"' "$name")"
  if ! grep -qF "$needle" "$readiness_file"; then
    printf 'capability "%s" has no fail-closed readiness gate "AddReadinessCheck("%s-capability")" in %s\n' \
      "$name" "$name" "$readiness_file" >&2
    violations=$((violations + 1))
  fi
done

if [[ "$violations" -gt 0 ]]; then
  printf '%d capability-contract violation(s) -- see above\n' "$violations" >&2
  exit 1
fi

echo "check-capability-contract: OK"
