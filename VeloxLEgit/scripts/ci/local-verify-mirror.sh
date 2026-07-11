#!/usr/bin/env bash
# =============================================================================
# scripts/ci/local-verify-mirror.sh
# =============================================================================
# Reproduce the GitHub Actions pyramid for `main` locally.
#
# This script mirrors what the four required checks run on PR-open:
#   * ci.yml               → step 2: `make verify`
#   * e2e-grpc.yml         → step 2: `make e2e-grpc`
#   * e2e-workload.yml     → step 2: `E2E_EXPECTED_SHA256=$PIN make e2e-workload`
#   * e2e-workload-mtls.yml → step 2: `E2E_EXPECTED_SHA256=$PIN make e2e-workload-mtls`
#
# The script does NOT touch application code — it's a defensive
# pre-PR gate: developers run this on their laptop BEFORE pushing,
# side-stepping the CI round-trip if any of the four checks is broken.
#
# SHA-pinning model (fail-closed):
#   The two workload scripts hard-fail when E2E_EXPECTED_SHA256 is
#   unset — see tests/e2e/workload/run.sh and tests/e2e/workload-mtls/run.sh.
#   Resolution order at the top of this script:
#     1. E2E_EXPECTED_SHA256 already exported by the operator.
#     2. .e2e-expected-sha256.local in the repo root (gitignored;
#        populated by this capture flow below).
#   If neither is set AND E2E_CAPTURE_SHA256=1, this script runs the
#   workload tests once with a placeholder SHA, harvests the rendered
#   artifact's real SHA from $E2E_WORKDIR/storage/artifact.sha256, and
#   writes it to .e2e-expected-sha256.local so subsequent runs use it
#   as the pin. The expected SHA mismatch in capture mode is NOT
#   recorded as a failure.
#
# Usage:
#   ./scripts/ci/local-verify-mirror.sh
#   E2E_CAPTURE_SHA256=1 ./scripts/ci/local-verify-mirror.sh   # first-run pin
#   E2E_EXPECTED_SHA256=$(cat .e2e-expected-sha256.local) ./scripts/ci/local-verify-mirror.sh
#
# Fast variants (developer feedback loops):
#   SKIP_HEAVY=1 ./scripts/ci/local-verify-mirror.sh           # skip cmake + docker
#   SKIP_E2E=1   ./scripts/ci/local-verify-mirror.sh           # make verify only
# =============================================================================

set -uo pipefail  # NOT -e: continue across phase failures so all failures report

# ─── Paths ────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

SHA_PIN_FILE=".e2e-expected-sha256.local"  # gitignored — see .gitignore

# ─── Resolve E2E_EXPECTED_SHA256 ─────────────────────────────────────────
# Precedence: caller env > .e2e-expected-sha256.local.
if [[ -z "${E2E_EXPECTED_SHA256:-}" && -r "$SHA_PIN_FILE" ]]; then
  E2E_EXPECTED_SHA256="$(tr -d '[:space:]' < "$SHA_PIN_FILE" 2>/dev/null || true)"
  if [[ -n "$E2E_EXPECTED_SHA256" ]]; then
    printf '→ resolved E2E_EXPECTED_SHA256 from %s\n' "$SHA_PIN_FILE"
  fi
fi

if [[ -z "${E2E_EXPECTED_SHA256:-}" && "${E2E_CAPTURE_SHA256:-0}" != "1" ]]; then
  printf '::warning::E2E_EXPECTED_SHA256 is not set.\n' >&2
  printf '  Both workload scripts will fail-closed.\n' >&2
  printf '  Resolution options:\n' >&2
  printf '    1. First-run capture:    E2E_CAPTURE_SHA256=1 %s\n' "$0" >&2
  printf '    2. Remote pin (CI):      set $GITHUB repo secret E2E_EXPECTED_SHA256\n' >&2
  printf '    3. Hand-set:             E2E_EXPECTED_SHA256=<sha> %s\n' "$0" >&2
fi

# ─── State tracking ──────────────────────────────────────────────────────
PASS=0
FAIL_COUNT=0
declare -a STEP_VERDICTS=()

# run_step NAME CMD… — runs CMD, records pass/fail, continues on fail.
# When E2E_CAPTURE_SHA256=1 and the failing step is one of the two
# workload tests, inspect $E2E_WORKDIR/storage/artifact.sha256 — if it
# was written, the run reached Verification 3 and produced a real SHA,
# which is exactly what capture mode wants. Mark such steps as
# "PASS (captured)" so the operator is not misled into "first-run
# pin failed!" exit 1.
run_step() {
  local name="$1"; shift
  printf '\n--- step: %s ---\n' "$name"
  local rc=0
  "$@" || rc=$?
  if (( rc == 0 )); then
    STEP_VERDICTS+=("$name: PASS")
    PASS=$((PASS + 1))
    return 0
  fi

  if [[ "${E2E_CAPTURE_SHA256:-0}" == "1" ]]; then
    local step_workdir=""
    case "$name" in
      "make e2e-workload")      step_workdir="${E2E_WORKDIR:-/tmp/velox-e2e-workload}" ;;
      "make e2e-workload-mtls") step_workdir="${E2E_WORKDIR_MTLS:-/tmp/velox-e2e-workload-mtls}" ;;
    esac
    if [[ -n "$step_workdir" && -s "$step_workdir/storage/artifact.sha256" ]]; then
      printf '→ capture-mode: V3 produced %s/artifact.sha256 → expected SHA mismatch tolerated\n' "$step_workdir"
      STEP_VERDICTS+=("$name: PASS (captured)")
      PASS=$((PASS + 1))
      return 0
    fi
  fi

  STEP_VERDICTS+=("$name: FAIL (exit $rc)")
  FAIL_COUNT=$((FAIL_COUNT + 1))
  return 0  # never abort the batch
}

# ─── 1. Pull origin main (mirrors CI start state) ─────────────────────────
# Refuse to pull/rebase when the working tree is dirty — matches the
# policy scripts/ci/verify.sh already enforces for the integrity suite.
# CI runs on a detached SHA regardless, so this guard only fires on
# local developer invocations.
run_step "git pull --ff-only origin main" bash -c '
  set -euo pipefail
  if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
    printf "::error::working tree NOT clean — commit/stash before running local-verify-mirror\n" >&2
    printf "  (git status --porcelain returned non-empty)\n" >&2
    exit 1
  fi
  git fetch origin
  if git rev-parse --abbrev-ref --symbolic-full-name HEAD 2>/dev/null \
       | grep -qx "main" 2>/dev/null; then
    git pull --ff-only origin main
  else
    printf "→ HEAD is on a detached SHA — skipping auto-pull (CI-style)\n"
  fi
'

# ─── 2. Required check: CI / make verify ─────────────────────────────────
run_step "make verify" make verify

# ─── 2b. Phase 1.5: completion-protocol invariant gate (mirrors ci.yml) ──
# Read-only probe: seeds a temp DB through the canonical migration
# runner (DataServer/cmd/seed-velox-db-fixture) and runs the 4
# invariant queries. Always sub-100 ms on an empty fixture; ceiling
# ≤4 s. Always runs even with SKIP_E2E=1 — it's a different concern
# from the workload gates (it exercises schema + invariants, not
# the rendering pipeline).
run_step "completion-protocol invariants (Phase 1.5)" bash -c '
  set -euo pipefail
  DB="$(mktemp /tmp/velox-cp-fixture.XXXXXX.db)"
  trap 'rm -f "$DB"' EXIT
  (cd DataServer && go run ./cmd/seed-velox-db-fixture "$DB")
  ./scripts/ci/check-completion-protocol-invariants.sh "$DB"
'

# ─── 3-5. Required checks: E2E gRPC + 2 workload variants ─────────────────
if [[ "${SKIP_E2E:-0}" != "1" ]]; then
  # Capture mode: workload scripts REQUIRE the var. Pin a known
  # placeholder so they reach Verification 3 (SHA compute + write
  # artifact.sha256 BEFORE failing), then harvest the real SHA
  # post-mortem. Subsequent runs use it.
  if [[ "${E2E_CAPTURE_SHA256:-0}" == "1" && -z "${E2E_EXPECTED_SHA256:-}" ]]; then
    export E2E_EXPECTED_SHA256="0000000000000000000000000000000000000000000000000000000000000000"
    printf '→ E2E_CAPTURE_SHA256=1: workload tests will fail at SHA check; run_step tolerates the expected mismatch.\n'
  fi

  run_step "make e2e-grpc"          make e2e-grpc
  run_step "make e2e-workload"      make e2e-workload
  run_step "make e2e-workload-mtls" make e2e-workload-mtls

  # ─── Post-run SHA harvest ─────────────────────────────────────────────
  if [[ "${E2E_CAPTURE_SHA256:-0}" == "1" ]]; then
    wl_sha="$(awk '{print $1}' "${E2E_WORKDIR:-/tmp/velox-e2e-workload}/storage/artifact.sha256" 2>/dev/null || true)"
    mtls_sha="$(awk '{print $1}' "${E2E_WORKDIR_MTLS:-/tmp/velox-e2e-workload-mtls}/storage/artifact.sha256" 2>/dev/null || true)"
    if [[ -n "$wl_sha" && "$wl_sha" != "0" && "$wl_sha" =~ ^[a-f0-9]{64}$ ]]; then
      printf '%s\n' "$wl_sha" > "$SHA_PIN_FILE"
      printf '✓ wrote E2E_EXPECTED_SHA256=%s to %s\n' "$wl_sha" "$SHA_PIN_FILE"
      if [[ -n "$mtls_sha" && "$mtls_sha" != "$wl_sha" ]]; then
        printf '::warning::mTLS SHA (%s) differs from plaintext run (%s) — cross-platform fixture drift\n' \
          "$mtls_sha" "$wl_sha" >&2
      fi
    else
      printf '::error::E2E_CAPTURE_SHA256: could not harvest SHA from %s\n' \
        "${E2E_WORKDIR:-/tmp/velox-e2e-workload}/storage/artifact.sha256" >&2
      FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
  fi
fi

# ─── Summary ──────────────────────────────────────────────────────────────
printf '\n══════════════════════════════════════════════════════════════\n'
printf '  local-verify-mirror summary\n'
printf '══════════════════════════════════════════════════════════════\n'
for v in "${STEP_VERDICTS[@]:-}"; do
  printf '  %s\n' "$v"
done
printf '\n  Result: %d PASS, %d FAIL\n' "$PASS" "$FAIL_COUNT"
printf '══════════════════════════════════════════════════════════════\n'

if (( FAIL_COUNT > 0 )); then
  exit 1
fi
exit 0
