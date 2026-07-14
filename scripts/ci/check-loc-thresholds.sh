#!/usr/bin/env bash
# scripts/ci/check-loc-thresholds.sh
#
# Project LOC gate. Reference: docs/metrics/loc-baseline.md §11.
# Exits 1 if any tracked source file exceeds the project-policy threshold,
# UNLESS the file appears in KNOWN_VIOLATIONS (annotated baseline carry-over).
#
# Thresholds per §11 (refactor-required boundary):
#   prod Go (non-test, non-generated)   > 900  LOC
#   test Go (*_test.go)                 > 1200 LOC
#   shell (.sh)                         >  700 LOC
#   docs (.md, excl. ./docs/archive)    > 1200 LOC
#   yaml (.yml, excl. workflows/)       >  800 LOC
#
# Generated code (e.g. *.pb.go), archived docs, and CI workflows
# themselves are exempt per §10 / §11 policy annotations.
#
# KNOWN_VIOLATIONS list format:
#   <repo-relative-path>|<kind>|<approx-loc>|<tracking-ref>
# Paths are repo-relative WITHOUT a leading "./". The script normalises
# `find`'s "./X" output to "X" before matching so a single entry covers
# both `./X` and `X` invocations.
# Update ONLY when:
#   (a) a file is fixed (remove the entry), or
#   (b) you explicitly accept a new long-file carry-over (add the entry +
#       the tracking-ref in docs/metrics/loc-baseline.md §10c).
set -u

THRESH_PROD_GO=900
THRESH_TEST_GO=1200
THRESH_SH=700
THRESH_MD=1200
THRESH_YML=800

# KNOWN_VIOLATIONS — annotated baseline carry-over (loc-baseline.md §10c).
# The gate stays green for these; CI must not block on day-1.
KNOWN_VIOLATIONS=(
  "docs/architecture/CURRENT-TO-TARGET-ARCHITECTURE.md|docs|1492|loc-baseline.md §10c + §13 roadmap step 2 (deferred)"
  "deploy/runtime/checklist-verify.sh|shell|1067|loc-baseline.md §10c (deferred)"
  "scripts/cert/certify-worker-2c-2d.sh|shell|794|loc-baseline.md §10c (deferred)"
)

  # === baseline violators emerged from prior refactor rounds ===
  "DataServer/internal/store/sqlite_task_atomic.go|prod-go|939|loc-baseline.md u00a710c + u00a713 roadmap step 8 (split residue; de-dup target)"
  "DataServer/internal/grpcserver/handler.go|prod-go|936|loc-baseline.md u00a710c + u00a713 roadmap step 4 (deferred)"
  "DataServer/internal/jobs/enqueue/enqueue_test.go|test-go|1331|loc-baseline.md u00a710c + u00a713 roadmap step 7 (deferred)"
  "DataServer/internal/store/sqlite_task_atomic_test.go|test-go|1521|loc-baseline.md u00a710c + u00a713 roadmap step 8 (deferred)"
  "DataServer/internal/store/sqlite_youtube_entities_test.go|test-go|1283|loc-baseline.md u00a710c (deferred)"
  "RemoteCodex/native/worker-agent-go/pkg/config/config_test.go|test-go|1201|loc-baseline.md u00a710c (deferred)"
VIOLATIONS=0
KNOWN_HITS=0

# Anchor at the repository root regardless of how/where this script is invoked.
# `git rev-parse --show-toplevel` finds the root from whichever submodule the
# caller cd'd into; the fallback is two `dirname ../..` levels up from the
# canonical location `scripts/ci/check-loc-thresholds.sh`.
ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"

is_known() {
  # Strip leading ./ emitted by `find .` so the path matches against the
  # bare repo-relative entry in KNOWN_VIOLATIONS.
  local path="${1#./}"
  for entry in "${KNOWN_VIOLATIONS[@]}"; do
    local p="${entry%%|*}"
    if [ "$p" = "$path" ]; then return 0; fi
  done
  return 1
}

scan_dir() {
  local kind="$1" threshold="$2"; shift 2
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    loc=$(wc -l < "$f" | tr -d ' ')
    if [ "$loc" -gt "$threshold" ]; then
      if is_known "$f"; then
        # GitHub Actions workflow command → warning annotation in PR UI.
        printf '::warning file=%s::%s LOC %d exceeds %d (KNOWN carry-over, tracked in loc-baseline.md §10c)\n' \
          "$f" "$kind" "$loc" "$threshold"
        KNOWN_HITS=$((KNOWN_HITS + 1))
      else
        # ::error annotation → CI fails on new violations.
        printf '::error file=%s::%s LOC %d exceeds refactor-required threshold %d\n' \
          "$f" "$kind" "$loc" "$threshold"
        VIOLATIONS=$((VIOLATIONS + 1))
      fi
    fi
  done < <(find . "$@" 2>/dev/null)
}

scan_dir prod-go "$THRESH_PROD_GO" \
  -type f -name '*.go' \
  -not -name '*_test.go' \
  -not -path './.git/*' \
  -not -path './shared/controltransport/pb/*.pb.go' \
  -not -path '*/.pb-cache/*'

scan_dir test-go "$THRESH_TEST_GO" \
  -type f -name '*_test.go' \
  -not -path './.git/*'

scan_dir shell "$THRESH_SH" \
  -type f -name '*.sh' \
  -not -path './.git/*'

scan_dir docs "$THRESH_MD" \
  -type f -name '*.md' \
  -not -path './.git/*' \
  -not -path './docs/archive/*'

scan_dir yaml "$THRESH_YML" \
  -type f \( -name '*.yml' -o -name '*.yaml' \) \
  -not -path './.github/workflows/*' \
  -not -path './.git/*'

if [ "$VIOLATIONS" -gt 0 ]; then
  printf '\n❌ LOC gate: %d NEW violation(s); %d annotated known carryover(s) still tracked (loc-baseline.md §10c).\n' \
    "$VIOLATIONS" "$KNOWN_HITS"
  printf 'Add new long-files to KNOWN_VIOLATIONS in this script AND to docs/metrics/loc-baseline.md §10c.\n'
  exit 1
fi
printf '\n✅ LOC gate: %d annotated known carryover(s) tracked; no new violations.\n' "$KNOWN_HITS"
