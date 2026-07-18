#!/usr/bin/env bash
# scripts/ci/check-size-band-policy.sh
#
# Per-file byte-band policy hard-fail (PR-15.7 follow-up).
#
# Rule (universal — file-size independent for band-token validation):
#   (1) Size window: any tracked source file (.go / .sh / .bash / .py)
#       with bytes > 50 000 OR bytes < 1 000 triggers a hard ::error
#       UNLESS it carries an explicit
#           `// size-benchmark: <band>`  (Go)
#           `# size-benchmark: <band>`  (bash)
#       header.
#   (2) Band-token validation: ANY file carrying the above-style header
#       (regardless of size) must reference a band token listed in the
#       `### Known size-bands` manifest at docs/CHANGELOG.md, OR be in
#       the baseline allow-list scripts/ci/check-size-band-policy.known-violations.
#
# Threshold convention: DECIMAL bytes (matches the manifest notation
# `42 000 – 42 200` for the band token `42-42,2 KB`).
#
# Canonical band-token form: `<low>-<high> KB` with ASCII hyphen-minus
# and no spaces. The lint script normalises BOTH sides of the
# comparison (strip whitespace + replace Unicode en-dash U+2013 with
# hyphen-minus) so contributors can use either ASCII or typographic
# dash form in their file headers without breaking the gate.
#
# Exit codes:
#   0  -- no violations
#   1  -- at least one source file violates the policy
#   2  -- configuration error (missing manifest, broken baseline, ...)
#
# Legacy/known violations live in
# `scripts/ci/check-size-band-policy.known-violations` -- one absolute
# path per line. Pattern mirrors scripts/ci/check-loc-thresholds.sh
# `KNOWN_VIOLATIONS_BASELINE` allow-list.
#
# Annotations are GitHub Actions `::error file=...` / `::warning file=...`
# so the gate ties in to PR checks. Baseline paths surface as `::warning`
# (auditable, NOT fail-loud-until-resolved). Fresh violations emit
# `::error` and tally toward the final exit-1.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MANIFEST_PATH="docs/CHANGELOG.md"
BASELINE_PATH="scripts/ci/check-size-band-policy.known-violations"

THRESH_MAX=50000   # 50 KB  (decimal)
THRESH_MIN=1000    #  1 KB  (decimal)

[[ -f "$MANIFEST_PATH" ]] || {
  printf '::error::Size-band manifest %s missing\n' "$MANIFEST_PATH" >&2
  exit 2
}
[[ -f "$BASELINE_PATH" ]] || {
  printf '::error::Size-band baseline %s missing\n' "$BASELINE_PATH" >&2
  exit 2
}

# Canonicalise a band token:
#   1. Strip leading + trailing whitespace.
#   2. Replace Unicode en-dash (–, U+2013) with ASCII hyphen-minus.
#   3. Collapse interior whitespace around the central hyphen to nothing, e.g.
#      `42 - 42,2 KB` (hyphen-minus bracketed by spaces) becomes `42-42,2 KB`.
# All three steps are idempotent. Result form: `<low>-<high> KB` with
# ASCII hyphen-minus and zero interior spaces around the central hyphen.
normalise_band() {
  local s="$1"
  printf '%s' "$s" \
    | sed 's/`//g' \
    | sed -E 's|^[ \t]+||; s|[ \t]+$||' \
    | sed 's|–|-|g' \
    | sed -E 's|[ \t]+-[ \t]+|-|g'
}

# Manifest parser: extract ONLY the rows from `### Known size-bands`
# table (skip header row, skip neighbours' sections). Column 2 of each
# `| ... | ... | ... | ...` line is the band token; we canonicalise it
# via normalise_band before storing it in KNOWN_BANDS.
KNOWN_BANDS_RAW=$(awk -F'|' '
  /^### Known size-bands/        { in_sec = 1; next }
  in_sec && /^### /              { in_sec = 0 }
  in_sec && /^\|/ {
    band = $2
    gsub(/^[ \t]+|[ \t]+$/, "", band)
    if (band != "Band token" && band !~ /^-+$/ && band != "") print band
  }
' "$MANIFEST_PATH" | sed -E 's|^[ \t]+||; s|[ \t]+$||' | grep -v '^$' || true)

KNOWN_BANDS=""
while IFS= read -r b; do
  [[ -z "$b" ]] && continue
  norm=$(normalise_band "$b")
  [[ -z "$norm" ]] && continue
  KNOWN_BANDS+="${norm}"$'\n'
done <<<"$KNOWN_BANDS_RAW"

if [[ -z "$(printf '%s' "$KNOWN_BANDS" | sed '/^$/d')" ]]; then
  printf '::error::No bands parsed from %s `### Known size-bands` table\n' "$MANIFEST_PATH" >&2
  exit 2
fi

BASELINE=$(grep -v -E '^[[:space:]]*$|^#' "$BASELINE_PATH" || true)

VIOLATIONS=0
WARNINGS=0
TRACKED_FILES=0
SIZE_VIOLATIONS=0
SIZE_BASELINE_WARNS=0
SIZE_BAND_OK=0
BAND_HITS_UNIVERSAL=0
BAND_BOGUS=0

is_basenamed() {
  local f="$1"
  printf '%s\n' "$BASELINE" | grep -F -x -q "$f"
}

band_in_manifest() {
  local b="$1"
  local n
  n=$(normalise_band "$b")
  printf '%s\n' "$KNOWN_BANDS" | sed '/^$/d' | grep -F -x -q "$n"
}

while IFS= read -r file; do
  [[ "$file" =~ \.(go|sh|bash|py)$ ]] || continue
  TRACKED_FILES=$(( TRACKED_FILES + 1 ))

  bytes=$(wc -c < "$file")

  # Locate band token IF one exists (in first 5 lines of file).
  band_token=$(head -n 5 "$file" | grep -E -m 1 '^(//|#) size-benchmark: ' \
    | sed -E 's/^(\/\/|#) size-benchmark: //' \
    | sed 's|–|-|g' \
    | sed -E 's|^[ \t]+||; s|[ \t]+$||' \
    || true)

  # ---- (1) Universal band-token check (file-size independent) ----
  if [[ -n "$band_token" ]]; then
    if ! band_in_manifest "$band_token"; then
      if is_basenamed "$file"; then
        printf '::warning file=%s,line=1::band token `%s` not in manifest (%s) -- BASELINE allow-list (schedule manifest update OR refactor)\n' \
          "$file" "$band_token" "$MANIFEST_PATH"
        WARNINGS=$(( WARNINGS + 1 ))
        continue
      fi
      printf '::error file=%s,line=1::band token `%s` not in manifest (%s `### Known size-bands`)\n' \
        "$file" "$band_token" "$MANIFEST_PATH" >&2
      VIOLATIONS=$(( VIOLATIONS + 1 ))
      BAND_BOGUS=$(( BAND_BOGUS + 1 ))
      continue
    fi
    BAND_HITS_UNIVERSAL=$(( BAND_HITS_UNIVERSAL + 1 ))
  fi

  # ---- (2) Size-threshold check ----
  if (( bytes > THRESH_MAX || bytes < THRESH_MIN )); then
    SIZE_VIOLATIONS=$(( SIZE_VIOLATIONS + 1 ))
    if [[ -z "$band_token" ]]; then
      if is_basenamed "$file"; then
        printf '::warning file=%s,line=1::size=%d bytes outside [%d, %d], no size-benchmark header (BASELINE -- schedule refactor)\n' \
          "$file" "$bytes" "$THRESH_MIN" "$THRESH_MAX"
        WARNINGS=$(( WARNINGS + 1 ))
        SIZE_BASELINE_WARNS=$(( SIZE_BASELINE_WARNS + 1 ))
        continue
      fi
      printf '::error file=%s,line=1::size=%d bytes outside [%d, %d], no `// size-benchmark:` / `# size-benchmark:` header\n' \
        "$file" "$bytes" "$THRESH_MIN" "$THRESH_MAX" >&2
      VIOLATIONS=$(( VIOLATIONS + 1 ))
      continue
    fi
    # A manifest-approved benchmark header is an explicit exemption from
    # the universal byte window.  The policy is about requiring an audited
    # size classification, not rejecting intentionally tiny helper scripts.
    SIZE_BAND_OK=$(( SIZE_BAND_OK + 1 ))
    continue
  fi
  # In-band tagged file.
  if [[ -n "$band_token" ]]; then
    SIZE_BAND_OK=$(( SIZE_BAND_OK + 1 ))
  fi
done < <(git ls-files)

cat <<SUMMARY
Size-band policy summary:
  source-tracked files scanned:    ${TRACKED_FILES}
  files in size-overrun window:    ${SIZE_VIOLATIONS}
  files correctly size-band-tagged: ${SIZE_BAND_OK}
  baseline-known size warns:       ${SIZE_BASELINE_WARNS}
  files carrying band header:      ${BAND_HITS_UNIVERSAL}
  band-token bogus violations:     ${BAND_BOGUS}
  baseline-known warns (total):    ${WARNINGS}
  hard-fail violations (total):    ${VIOLATIONS}
  manifest bands recognised:       $(printf '%s\n' "$KNOWN_BANDS" | sed '/^$/d' | wc -l | tr -d ' ')
SUMMARY

if (( VIOLATIONS > 0 )); then
  printf 'Size-band policy: %d violation(s) -- see ::error annotations above\n' "$VIOLATIONS" >&2
  exit 1
fi

echo "Size-band policy: OK (${SIZE_BAND_OK} size-bands in range; ${BAND_HITS_UNIVERSAL} band-headers validated; ${WARNINGS} baseline-known legacy warns)"
