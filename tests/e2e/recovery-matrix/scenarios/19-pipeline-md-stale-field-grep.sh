#!/usr/bin/env bash
# =============================================================================
# Scenario 19 — docs/pipeline.md structural-drift gate
# =============================================================================
# Purpose: catch future regressions of the YouTube → Social closure
#   narrative on docs/pipeline.md. The closure removed three pre-closure
#   columns (account_id / channel_id / language) and the platform-parsing
#   helper function parsePlatformAndAccount; canonicalised the opaque
#   identifier (Residuo 4: SocialDestinationID → external_destination_id
#   via migration 092); and added cross-references to PR-15.13 + PR-15.14.
#
#   If any of those four invariants drifts back into the doc, this gate
#   exits non-zero, the recovery matrix records FAIL, CI breaks pre-merge.
#
# Type: STRUCTURAL — read-only, no DB mutation, no artifact state change.
#       Pure grep-driven regression gate on a single doc.
# Invariants (must all hold for the script to exit 0):
#   G1 — docs/pipeline.md has 0 lines containing the literal substring
#        '\| `channel_id` \|' (a Markdown table cell with a backticked
#        channel_id token, sandwiched between pipe delimiters).
#        Capture mode: grep -cF (FIXED-STRING mode) — without -F, default
#        BRE interprets `\|` as alternation and would false-flag every
#        table-row pipe delimiter.
#   G2 — docs/pipeline.md has 0 lines containing the legacy function
#        name parsePlatformAndAccount (Residuo 2 closure).
#   G3a — docs/pipeline.md references PR-15.13 (Residuo 3 closure —
#         opaque-mode wire contract, socialclient.DeliverArtifactRequest
#         refactor).
#   G3b — docs/pipeline.md references PR-15.14 (Residuo 4 closure —
#         canonical rename SocialDestinationID → ExternalDestinationID
#         via migration 092).
#   G3c — docs/pipeline.md references external_destination_id (the
#         canonical opaque identifier post-Residuo 4) at least once.
#
# Invocation (standalone, no recovery-matrix orchestrator required):
#   bash tests/e2e/recovery-matrix/scenarios/19-pipeline-md-stale-field-grep.sh
#
# Override DOC_PATH for local counter-testing (negative path):
#   DOC_PATH=/tmp/tampered.md bash tests/e2e/recovery-matrix/scenarios/19-pipeline-md-stale-field-grep.sh
#
# Integration note: tests/e2e/recovery-matrix/run.sh's default loop
# iterates scenarios 01..17; this gate is intended to be invoked with
#   bash tests/e2e/recovery-matrix/run.sh --scenario 19
# independently of the default-all path, or wired in by extending the
# orchestrator's loop range in a follow-up commit.
# =============================================================================
set -uo pipefail

SCENARIO_ID="19-pipeline-md-stale-field-grep"
# Default evidence dir for standalone runs; run.sh overrides this via
# EVIDENCE_DIR export when invoked via --scenario 19.
EVIDENCE_DIR="${EVIDENCE_DIR:-/tmp/velox-recovery-matrix/$(date -u +%Y-%m-%d)/scenarios/19}"
mkdir -p "$EVIDENCE_DIR"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"

DOC="${DOC_PATH:-docs/pipeline.md}"
DOC="$(cd "$(dirname "$DOC")" && pwd)/$(basename "$DOC")"   # resolve to absolute path
DOC_HASH=$(sha256sum "$DOC" | cut -d' ' -f1)

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
rm_info "[$SCENARIO_ID] doc             = $DOC"
rm_info "[$SCENARIO_ID] doc_sha256      = $DOC_HASH"
rm_info "[$SCENARIO_ID] evidence_dir    = $EVIDENCE_DIR"

# ─── G1 — no `\| `channel_id` \|` literal table cell ─────────────────────────
# Use grep -cF (FIXED-STRING). Without -F, default BRE parses `\|` as
# alternation meta-character and the pattern matches almost every table
# row (false-positive).
G1=$(grep -cF '\| `channel_id` \|' "$DOC" || true)
rm_info "[$SCENARIO_ID] G1 (no `\| \`channel_id\` \|` literal) = $G1"
if [[ "$G1" -ne 0 ]]; then
  rm_fail "[$SCENARIO_ID] G1 REGRESSION: $G1 line(s) contain the literal table cell '\\| `channel_id` \\|'. Inspect: grep -nF '\\| \`channel_id\` \\|' $DOC"
  rm_record_verdict "$SCENARIO_ID" "FAIL" "G1 channel_id_literal=$G1"
  echo "doc_sha256=$DOC_HASH g1=$G1" >"$EVIDENCE_DIR/19-verdict.txt"
  exit 1
fi

# ─── G2 — no parsePlatformAndAccount function reference ──────────────────────
G2=$(grep -cF 'parsePlatformAndAccount' "$DOC" || true)
rm_info "[$SCENARIO_ID] G2 (no parsePlatformAndAccount) = $G2"
if [[ "$G2" -ne 0 ]]; then
  rm_fail "[$SCENARIO_ID] G2 REGRESSION: $G2 line(s) still reference the legacy parsePlatformAndAccount. Inspect: grep -nF 'parsePlatformAndAccount' $DOC"
  rm_record_verdict "$SCENARIO_ID" "FAIL" "G2 parsePlatformAndAccount=$G2"
  echo "doc_sha256=$DOC_HASH g2=$G2" >"$EVIDENCE_DIR/19-verdict.txt"
  exit 1
fi

# ─── G3a — PR-15.13 cross-reference present ─────────────────────────────────
if ! grep -qF 'PR-15.13' "$DOC"; then
  rm_fail "[$SCENARIO_ID] G3a REGRESSION: no PR-15.13 cross-reference in $DOC. Residuo 3 closure (opaque-mode wire contract) MUST be referenced."
  rm_record_verdict "$SCENARIO_ID" "FAIL" "G3a no_PR-15.13"
  echo "doc_sha256=$DOC_HASH g3a=missing" >"$EVIDENCE_DIR/19-verdict.txt"
  exit 1
fi
rm_info "[$SCENARIO_ID] G3a (PR-15.13 present) = OK"

# ─── G3b — PR-15.14 cross-reference present ─────────────────────────────────
if ! grep -qF 'PR-15.14' "$DOC"; then
  rm_fail "[$SCENARIO_ID] G3b REGRESSION: no PR-15.14 cross-reference in $DOC. Residuo 4 closure (canonical rename) MUST be referenced."
  rm_record_verdict "$SCENARIO_ID" "FAIL" "G3b no_PR-15.14"
  echo "doc_sha256=$DOC_HASH g3b=missing" >"$EVIDENCE_DIR/19-verdict.txt"
  exit 1
fi
rm_info "[$SCENARIO_ID] G3b (PR-15.14 present) = OK"

# ─── G3c — external_destination_id canonical identifier present ─────────────
G3C=$(grep -cF 'external_destination_id' "$DOC" || true)
rm_info "[$SCENARIO_ID] G3c (external_destination_id refs) = $G3C"
if [[ "$G3C" -le 0 ]]; then
  rm_fail "[$SCENARIO_ID] G3c REGRESSION: 0 references to external_destination_id in $DOC. Canonical identifier MUST be anchored."
  rm_record_verdict "$SCENARIO_ID" "FAIL" "G3c no_external_destination_id"
  echo "doc_sha256=$DOC_HASH g3c=0" >"$EVIDENCE_DIR/19-verdict.txt"
  exit 1
fi

# ─── Evidence snapshot ───────────────────────────────────────────────────────
cat >"$EVIDENCE_DIR/19-gate-summary.txt" <<EOF
scenario               : $SCENARIO_ID
doc                    : $DOC
doc_sha256             : $DOC_HASH
G1_channel_id_literal  : $G1 (must be 0)
G2_parsePlatformAndAcct: $G2 (must be 0)
G3a_PR-15.13           : present
G3b_PR-15.14           : present
G3c_external_dest_id_ct: $G3C (must be > 0)
verdict                : PASS
EOF

cat >"$EVIDENCE_DIR/19-grep-evidence.txt" <<EOF
# Raw grep evidence for the four invariant checks.
# (Pinned to the doc_sha256 above — re-running on a different sha will produce
# a fresh evidence file.)
$G1 $(grep -nF '\| `channel_id` \|' "$DOC" | head -3 || true)
$G2 $(grep -nF 'parsePlatformAndAccount' "$DOC" | head -3 || true)
present $(grep -nF 'PR-15.13' "$DOC" | head -3)
present $(grep -nF 'PR-15.14' "$DOC" | head -3)
$G3C $(grep -nF 'external_destination_id' "$DOC" | head -3)
EOF

rm_record_verdict "$SCENARIO_ID" "PASS" "doc_sha256=$DOC_HASH g1=0 g2=0 g3a=present g3b=present g3c=$G3C"
# Note: do NOT also call rm_end_scenario here — lib.sh's auto-routing would
# record a second PASS and skew RM_PASS_COUNT by +1 for this scenario.
rm_info "[$SCENARIO_ID] done"
exit 0
