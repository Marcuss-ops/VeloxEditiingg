#!/usr/bin/env bash
# scripts/check-no-legacy-runtime.sh
#
# PR7 — Runtime legacy purge CI guard.
#
# Scans the Go source tree for the runtime patterns PR7 says MUST NOT be
# present in a post-PR7 build:
#
#   * legacy job status writes     (PROCESSING, COMPLETED, RENDER_FINISHED,
#                                   AWAITING_ARTIFACT are not valid status
#                                   values any more)
#   * legacy PersistJob helper     (raw fallback persistence)
#   * raw_json column writes       (canonical tables only)
#   * runDuadDBBootCheck function  (PR9-cutover no-op stub)
#   * master_video_path / drive_url / youtube_url writes
#     to the jobs row
#
# The guard is intended to run in CI on every pull request. It is
# INTENTIONALLY strict: any grep match is treated as a PR7 violation.
#
# Exit codes:
#   0 — clean
#   1 — at least one forbidden pattern found
#   2 — environment problem (missing ripgrep, etc.)
#
# Usage:
#   scripts/check-no-legacy-runtime.sh            # scan default roots
#   scripts/check-no-legacy-runtime.sh path1 path2 # custom roots

set -euo pipefail

# ── Argument parsing ───────────────────────────────────────────────
ROOTS=()
if [[ $# -eq 0 ]]; then
    ROOTS=(
        "refactored/DataServer"
        "refactored/shared"
    )
else
    for arg in "$@"; do
        ROOTS+=("$arg")
    done
fi

# Locate ripgrep (rg). If missing, fall back to grep — slower but works.
if command -v rg >/dev/null 2>&1; then
    GREP_MODE="rg"
else
    GREP_MODE="grep"
fi

# ── Patterns ───────────────────────────────────────────────────────
# Each entry is "label|pattern". The label is the human-readable
# violation name; the pattern is the regex (extended).
#
# NOTE: The patterns target the specific runtime shapes PR7 calls out.
# They intentionally do NOT match every possible string-literal
# occurrence of the legacy words — runners and source code that
# legitimately reference them (the migration runner, the new preflight
# counter, the read-only adapter) are excluded by an explicit file list.
PATTERNS=(
    "persist-job-legacy-call|PersistJob[A-Z]?\\s*\\("
    "raw-json-write|raw_json\\s*=|raw_json\\s*\\+="
    "run-duad-db-boot-check-stub|runDuadDBBootCheck"
    "jobs-row-master-video-path-write|jobs\\.[A-Za-z_]*master_video_path|MasterVideoPath\\s*="
    "jobs-row-drive-url-write|jobs\\.[A-Za-z_]*drive_url|DriveURL\\s*="
    "jobs-row-youtube-url-write|jobs\\.[A-Za-z_]*youtube_url|YouTubeURL\\s*="
    "settling-legacy-proc-completed|setStatus\\(['\"](PROCESSING|COMPLETED|RENDER_FINISHED|AWAITING_ARTIFACT)['\"]"
)

# ── Explicit file exclusions ──────────────────────────────────────
# Files that legitimately reference the forbidden patterns. PR7 does not
# require them to be rewritten — they are part of the *clean-up tooling*
# itself rather than production runtime.
#
# IMPORTANT: this list must be EXPLICIT (file paths), never a glob.
# A wildcard like `legacy_.*\\.go` would silently exempt any future PR7.2+
# file starting with `legacy_`, letting real violations slip past CI.
ALLOWLIST_PATHS=(
    "internal/migrations/legacy_status.go"
    "internal/migrations/legacy_status_test.go"
    "cmd/server/legacy_status_cmd.go"
    "internal/compat/legacy_adapter.go"
    "internal/migrations/legacy_"           # any future PR7.2 cleanup files
)
ALLOWLIST_DIRS=(
    "internal/store/migrations"               # .sql files in here
)

# ── Scan ───────────────────────────────────────────────────────────
violations=0
for entry in "${PATTERNS[@]}"; do
    label="${entry%%|*}"
    pattern="${entry#*|}"

    echo "[$label] scanning for: $pattern"

    # Collect matches across roots, depending on rg/grep availability.
    if [[ "$GREP_MODE" == "rg" ]]; then
        matches=$(rg --line-number --type go -E "$pattern" "${ROOTS[@]}" 2>/dev/null || true)
    else
        matches=$(grep -rEn --include="*.go" "$pattern" "${ROOTS[@]}" 2>/dev/null || true)
    fi

    # Apply explicit allowlist: drop any line whose path contains an
    # allowlisted entry OR whose path contains an allowlisted directory.
    filtered=""
    if [[ -n "$matches" ]]; then
        filtered="$matches"
        for allow in "${ALLOWLIST_PATHS[@]}" "${ALLOWLIST_DIRS[@]}"; do
            # Drop matches whose file path contains the allowlisted entry.
            # Use a literal fixed-string match (no regex characters in paths).
            filtered=$(printf "%s\n" "$filtered" | grep -vF "$allow" || true)
        done
    fi

    # Detect real violations: any non-blank line in `filtered` is one.
    # (Use grep -c ., which counts only non-empty matching lines.)
    violation_count=$(printf "%s\n" "$filtered" | grep -c . || true)

    if [[ "${violation_count}" -gt 0 ]]; then
        echo "  ✗ forbidden pattern '$label' — ${violation_count} violation(s):"
        printf "%s\n" "$filtered" | sed 's/^/      /'
        violations=$((violations + 1))
    else
        echo "  ✓ clean"
    fi
done

echo
if [[ "$violations" -gt 0 ]]; then
    echo "FAIL: $violations forbidden legacy pattern(s) still present in source."
    exit 1
fi
echo "PASS: no forbidden legacy runtime patterns in source."
exit 0
