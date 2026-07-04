#!/usr/bin/env bash
# scripts/ci/check-payload-canonical-form.sh
#
# Step 8/8 of the canonical-purity action plan: final hard gate against
# writer regressions of the V2 single-shape contract.
#
# Detects forbidden patterns in MASTER-write paths (the canonical
# ingestion pipeline that produces process_video job payloads persisted
# by DataServer):
#
#   1. Top-level LEGACY ALIASES — writing these is a writer bug:
#        "id", "run_id", "title", "voiceover_path", "audio_path"
#      These are tolerated on the READ path by NewJobPayloadV2 fallbacks
#      for legacy SQLite rows, but writers must NEVER emit them.
#      Source of truth: shared/contract/canonical_payload.go::LegacyAliasKeys.
#
#   2. The "parameters" sub-map MIRROR — shared/contract/payload_v2.go
#      commits to ONE flat canonical shape. Writers must NOT nest a
#      "parameters" map under the top level.
#
# MASTER-write paths covered (the ingest boundary under DataServer):
#   - DataServer/internal/jobs/enqueue/**      (the canonical enqueue preflight gate)
#   - DataServer/internal/jobs/ingress/**      (ingress / webhook layer)
#   - DataServer/internal/jobs/*.go            (model, repository, status, transitions,
#                                                view — top-level job package writers)
#
# Out of scope (the gate deliberately does NOT scan):
#   - shared/contract/**                       (source-of-truth + readers + tests)
#   - shared/payload/**                        (legacy-tolerance helpers)
#   - DataServer/internal/store/**             (persists whatever the writer produced;
#                                                a storage layer scan would catch the
#                                                reader's "id" *struct field* markers,
#                                                not the writer contract.)
#   - DataServer/internal/artifacts/**         (post-finalization, not ingest)
#   - Anything outside DataServer/internal/jobs (controllers/handlers are documented
#                                                not to bypass the enqueue contract)
#
# Allowlist (where these tokens legitimately appear):
#   * shared/contract/canonical_payload.go         (denylist source-of-truth)
#   * shared/contract/canonical_payload_test.go    (gate self-tests)
#   * shared/contract/payload_v2.go                (V2 reader; lists the legacy fallbacks
#                                                   inside FirstString())
#   * shared/contract/contract.go                  (ExtractRenderJobParams tolerates
#                                                   "audio_path" as a legacy fallback)
#   * shared/contract/contract_test.go             (legacy-fallback test fixtures)
#   * shared/payload/payload.go                    (legacy alias traversal helpers)
#   * docs/*                                       (canonical-purity plan reference)
#   * this script itself
#
# Run: ./scripts/ci/check-payload-canonical-form.sh
# Exit codes: 0 clean (no forbidden patterns in MASTER-write paths)
#             1 forbidden pattern(s) found (printed below)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.."; pwd)"
cd "$REPO_ROOT"

# Pattern: top-level legacy alias emitted as a JSON-like map key.
# Tight enough to NOT match struct field tags like `json:"id,omitempty"`
# (which has no colon after the closing quote); the pattern requires
# `"<alias>":` directly, so it catches Go map literals like
# `{"id": "v"}` and JSON literal strings but NOT `json:"id"`.
LEGACY_ALIAS_REGEX='"id":|"run_id":|"title":|"voiceover_path":|"audio_path":'

# Pattern: "parameters" sub-map mirror at the writer. Distinguishes from
# comments (which never have `map[` after the colon) and from JSON tags
# (no colon before `map[`).
PARAMETERS_MAP_REGEX='"parameters":[[:space:]]*map\['

# Build file list (recursive scan over MASTER-write dirs + top-level jobs/*).
FILE_LIST=()

scan_tree() {
  local root="$1"
  [ -d "$root" ] || return 0
  while IFS= read -r f; do
    FILE_LIST+=("$f")
  done < <(find "$root" -type f \( -name '*.go' -o -name '*.sql' \) 2>/dev/null)
}

scan_tree "DataServer/internal/jobs/enqueue"
scan_tree "DataServer/internal/jobs/ingress"
for top in DataServer/internal/jobs/*.go; do
  [ -f "$top" ] || continue
  FILE_LIST+=("$top")
done

# Apply the in-script allowlist:
#   - *_test.go under the scanned dirs are AUDIT fixtures, not writers.
#     They legitimately mock legacy keys to assert reader tolerance for
#     legacy SQLite rows; the gate audits WRITER behaviour, not TEST
#     coverage.
#   - enqueue/http_response_compat.go is the PR15.6 HTTP-edge back-compat
#     surface that intentionally dual-writes legacy aliases. See the
#     top-of-file docstring in that file for the full rationale; the
#     gate exposes the discipline by listing the file in this allowlist
#     rather than silencing the alert silently.
FILTERED=()
for f in "${FILE_LIST[@]}"; do
  case "$f" in
    *_test.go)                                                continue ;;
    DataServer/internal/jobs/enqueue/http_response_compat.go) continue ;;
  esac
  FILTERED+=("$f")
done
FILE_LIST=("${FILTERED[@]}")

if [ "${#FILE_LIST[@]}" -eq 0 ]; then
  echo "[check-payload-canonical-form] no MASTER-write files found (after allowlist) — vacuously PASS"
  exit 0
fi

echo "[check-payload-canonical-form] scanning ${#FILE_LIST[@]} MASTER-write file(s) for canonical-purity regressions…"

LEGACY_HITS=$(grep -nE "$LEGACY_ALIAS_REGEX" "${FILE_LIST[@]}" 2>/dev/null || true)
PARAM_HITS=$(grep -nE "$PARAMETERS_MAP_REGEX" "${FILE_LIST[@]}" 2>/dev/null || true)

found=0

if [ -n "$LEGACY_HITS" ]; then
  printf '\nFAIL: forbidden LEGACY ALIAS(es) emitted by a MASTER-write path:\n%s\n' "$LEGACY_HITS" >&2
  printf '\nSee shared/contract/canonical_payload.go::LegacyAliasKeys for the binding denylist.\n' >&2
  printf 'If a writer legitimately falls back to a legacy alias, route it through\n' >&2
  printf 'NewJobPayloadV2 (reader pattern) and re-emit the canonical key, OR\n' >&2
  printf 'add the file to scripts/ci/check-payload-canonical-form.sh with justification.\n' >&2
  found=1
fi

if [ -n "$PARAM_HITS" ]; then
  printf '\nFAIL: forbidden "parameters" sub-map MIRROR(s) emitted by a MASTER-write path:\n%s\n' "$PARAM_HITS" >&2
  printf '\nSee shared/contract/payload_v2.go::ToMap — the canonical shape is\n' >&2
  printf 'flat (no nested "parameters" map). Lift the inner fields to the\n' >&2
  printf 'top level and re-marshal via JobPayloadV2.ToMap().\n' >&2
  found=1
fi

if [ "$found" -ne 0 ]; then
  exit 1
fi

echo "[check-payload-canonical-form] OK — writers emit canonical flat shape"
