#!/usr/bin/env bash
# scripts/ci/check-render-plan-can-inicality.sh
#
# Asserts render plan CANONICITY (the filename keeps the original spelling
# as requested; the correct term is "canonicity" — invariant of canonical-purity
# for RenderPlan.Timeline).
#
# Rule: for each scene in the input payload, the final clip (role=scene_clip)
# MUST be the LAST visible resource. A scene ends whenever role=scene_clip
# appears; the next item either starts a new scene (role=voiceover_bed) or
# the timeline ends. If a scene ends with any role other than scene_clip
# (i.e., a voiceover_bed / stock segment is left as the last item of its
# scene), the canonical-purity contract is violated and the script exits 1.
#
# This guards against the regression: "alla fine della scena deve vedersi la
# clip associata, non lo stock" (at the end of each scene the final clip must
# be visible, not the stock).
#
# Usage:
#   check-render-plan-can-inicality.sh <payload.json>
#
# Payload format (JSON):
#   {
#     "items": [
#       {"role": "voiceover_bed", "url": "<stock>"},
#       {"role": "scene_clip",    "url": "<final>"},
#       ... one or more scene pairs ...
#     ]
#   }
#
# Scene model: a scene is the run of items between two consecutive
# `scene_clip` boundaries. The first `scene_clip` closes scene 0; the
# next `scene_clip` closes scene 1; etc. A/B-roll patterns
# (`voiceover_bed → scene_clip → voiceover_bed → scene_clip`) are
# therefore treated as TWO adjacent scenes, each independently required
# to end with `scene_clip`. The TIMELINE must not end with a trailing
# `voiceover_bed` or any non-`scene_clip` role.
#
# Exit codes:
#   0 — PASS: every scene ends with role=scene_clip
#   1 — FAIL: at least one scene ends with a non-clip (canonicity violation)
#   2 — usage error: missing arg, missing jq, missing/invalid payload

set -euo pipefail

PAYLOAD="${1:-}"

if [[ -z "$PAYLOAD" ]]; then
  echo "Usage: $0 <payload.json>" >&2
  echo "  Reads a JSON payload with top-level 'items' array (each item" >&2
  echo "  having {role, url}); asserts each scene ends with role=scene_clip." >&2
  exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required but not installed (apt: jq, brew: jq)" >&2
  exit 2
fi

if [[ ! -f "$PAYLOAD" ]]; then
  echo "ERROR: payload file not found: $PAYLOAD" >&2
  exit 2
fi

if ! jq -e '.items | type == "array"' "$PAYLOAD" >/dev/null 2>&1; then
  echo "ERROR: payload must have a top-level 'items' array" >&2
  exit 2
fi

item_count=$(jq '.items | length' "$PAYLOAD")
echo "Checking $item_count item(s) in $PAYLOAD"
echo

violations=0
scene_idx=0
scene_roles=()
scene_urls=()

flush_scene() {
  if [[ ${#scene_roles[@]} -eq 0 ]]; then
    return
  fi
  local last_role="${scene_roles[-1]}"
  if [[ "$last_role" != "scene_clip" ]]; then
    echo "  FAIL scene $scene_idx: last item role='$last_role', expected 'scene_clip'" >&2
    echo "         scene contents: [${scene_roles[*]}]" >&2
    echo "         scene urls:     [${scene_urls[*]}]" >&2
    violations=$((violations + 1))
  else
    echo "  OK scene $scene_idx: [${scene_roles[*]}] ends with scene_clip"
  fi
  scene_idx=$((scene_idx + 1))
  scene_roles=()
  scene_urls=()
}

for ((i=0; i<item_count; i++)); do
  role=$(jq -r ".items[$i].role // \"\"" "$PAYLOAD")
  url=$(jq -r ".items[$i].url  // \"\"" "$PAYLOAD")
  scene_roles+=("$role")
  scene_urls+=("$url")
  if [[ "$role" == "scene_clip" ]]; then
    flush_scene
  fi
done

# Trailing scene that did not end with scene_clip: delegate to flush_scene
# (which prints the FAIL line and increments the violation counter). The
# "trailing" context is implicit in that no subsequent scene_clip arrived
# to flush the buffer; the FAIL message names the offending role.
if [[ ${#scene_roles[@]} -gt 0 ]]; then
  flush_scene
fi

echo
if [[ $violations -gt 0 ]]; then
  echo "FAIL: $violations canonicity violation(s) detected" >&2
  echo "      (one or more scenes ended with a non-clip segment; the final" >&2
  echo "       clip must be the LAST visible resource per scene)" >&2
  exit 1
fi

echo "PASS: final clip is the last visible resource per scene ($scene_idx scene(s) checked)"
exit 0
