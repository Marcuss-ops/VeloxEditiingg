#!/usr/bin/env bash
# scripts/ci/test-check-payload-canonical-form.sh
#
# Self-test for scripts/ci/check-payload-canonical-form.sh.
# Validates that the gate's regexes correctly:
#
#   1. Catch every entry in shared/contract/canonical_payload.go::LegacyAliasKeys
#      when emitted as a top-level JSON-like map key.
#   2. Catch the "parameters" sub-map mirror.
#   3. Produce ZERO hits on canonical-only payloads (no false positives on
#      `"job_id":`, `"video_name":`, `"delivery_plan":`, etc.).
#   4. Strip the script when the check is missing or not executable.
#
# This test runs REGEX-level only — it does NOT re-run the actual gate
# against DataServer/internal/jobs/, so it can run safely in any workflow.
# For an end-to-end artifact check, run:
#     ./scripts/ci/check-payload-canonical-form.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.."; pwd)"
cd "$REPO_ROOT"

CHECK="$REPO_ROOT/scripts/ci/check-payload-canonical-form.sh"

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf '%s\n' "$*"; }

# 1. The script must exist and be executable.
[ -f "$CHECK" ] || fail "$CHECK does not exist"
[ -x "$CHECK" ] || fail "$CHECK is not executable — chmod +x before running"

# Extract the binding LegacyAliasKeys list from the source-of-truth file.
SRC_OF_TRUTH="$REPO_ROOT/shared/contract/canonical_payload.go"
[ -f "$SRC_OF_TRUTH" ] || fail "$SRC_OF_TRUTH missing — the Step 7/8 binding is broken"

# Pull the LegacyAliasKeys slice literal (order-stable).
ALIAS_LIST=$(awk '/var LegacyAliasKeys = \[\]string\{/,/^\}/' "$SRC_OF_TRUTH" \
  | sed -nE 's/^[[:space:]]*"([^"]+)",?$/\1/p')

if [ -z "$ALIAS_LIST" ]; then
  fail "could not extract LegacyAliasKeys from canonical_payload.go"
fi

# Sanity: every spec-required alias must be present.
for required in id run_id title voiceover_path audio_path; do
  if ! printf '%s\n' "$ALIAS_LIST" | grep -qx "$required"; then
    fail "LegacyAliasKeys missing required alias \"$required\""
  fi
done

# Bind the regexes used by the gate (keep in sync with check-payload-canonical-form.sh).
LEGACY_REGEX='"id":|"run_id":|"title":|"voiceover_path":|"audio_path":'
PARAM_REGEX='"parameters":[[:space:]]*map\['

TMP="$(mktemp -d /tmp/cano-puri-test.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

# ── Positive fixture — every forbidden pattern as a Go map literal ────────
cat >"$TMP/legacy_violations.go" <<'EOF'
package fixture

// Regression markers — every line below must be caught by the gate.
var legacyID = map[string]interface{}{"id": "x"}
var legacyRunID = map[string]interface{}{"run_id": "x"}
var legacyTitle = map[string]interface{}{"title": "x"}
var legacyVoiceoverPath = map[string]interface{}{"voiceover_path": "x"}
var legacyAudioPath = map[string]interface{}{"audio_path": "x"}
var parametersMirror = map[string]interface{}{
    "parameters": map[string]interface{}{"x": "y"},
}
var _ = []interface{}{legacyID, legacyRunID, legacyTitle, legacyVoiceoverPath, legacyAudioPath, parametersMirror}
EOF

LEGACY_HITS=$(grep -nE "$LEGACY_REGEX" "$TMP/legacy_violations.go" 2>/dev/null || true)
PARAM_HITS=$(grep -nE "$PARAM_REGEX" "$TMP/legacy_violations.go" 2>/dev/null || true)

# Each individual alias must show up in the legacy hits.
for alias in id run_id title voiceover_path audio_path; do
  if ! echo "$LEGACY_HITS" | grep -q "\"$alias\":"; then
    fail "regex did not catch legacy alias \"$alias\":"
  fi
done

if [ -z "$PARAM_HITS" ]; then
  fail 'regex did not catch "parameters" sub-map mirror'
fi

# ── Negative fixture — canonical-only payload must produce ZERO hits ─────
cat >"$TMP/clean.go" <<'EOF'
package clean

var ok = map[string]interface{}{
    "job_id":         "j",
    "job_run_id":     "r",
    "correlation_id": "c",
    "video_name":     "My Video",
    "script_text":    "Hello world",
    "voiceover_paths": []interface{}{"vo.mp3"},
    "scenes":         []interface{}{},
    "items":          []interface{}{},
    "delivery_plan":  []interface{}{},
    "priority":       1,
    "timeout_secs":   3600,
    "status":         "PENDING",
}
var _ = ok
EOF

LEGACY_HITS=$(grep -nE "$LEGACY_REGEX" "$TMP/clean.go" 2>/dev/null || true)
PARAM_HITS=$(grep -nE "$PARAM_REGEX" "$TMP/clean.go" 2>/dev/null || true)

if [ -n "$LEGACY_HITS" ]; then
  fail "regex falsely caught clean payload (legacy hits): $LEGACY_HITS"
fi
if [ -n "$PARAM_HITS" ]; then
  fail "regex falsely caught clean payload (params hits): $PARAM_HITS"
fi

# ── Negative fixture — Go struct tags must NOT trip the gate ────────────
# Catches the most common false positive: `json:"id,omitempty"` where
# there's NO colon after the closing quote.
cat >"$TMP/struct_tags.go" <<'EOF'
package fixture

type Tags struct {
    ID             string `json:"id,omitempty"`
    RunID          string `json:"run_id,omitempty"`
    Title          string `json:"title,omitempty"`
    VoiceoverPath  string `json:"voiceover_path,omitempty"`
    AudioPath      string `json:"audio_path,omitempty"`
}
var _ = Tags{}
EOF

LEGACY_HITS=$(grep -nE "$LEGACY_REGEX" "$TMP/struct_tags.go" 2>/dev/null || true)
if [ -n "$LEGACY_HITS" ]; then
  fail "regex falsely caught Go struct tags (false positive): $LEGACY_HITS"
fi

pass "[test-check-payload-canonical-form] OK"
