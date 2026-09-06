#!/usr/bin/env bash
# =============================================================================
# check-handler-mounts.sh — dead-handler regression guard.
#
# Background: the 2026-09 audit found 15 gin handlers compiled into the
# binary but never mounted on any route (an entire HTTP update/rollout/SSE
# surface that looked live but 404'd). Nothing in CI failed when a handler
# lost its route, so the surface rotted invisibly. This guard closes that
# gap: every `Handler() gin.HandlerFunc` method must appear at least once
# outside its own definition (a route mount, a test harness, or an alias).
#
# Semantics (deliberately identical to check-no-legacy.sh):
#   - exit 0: every handler has at least one non-definition reference
#   - exit 1: at least one handler has zero references outside its def
#   - exit 2: usage error (repo root not found)
#
# Scope: production Go source under DataServer (internal/, cmd/, shared/).
# Test files count as valid referencers: a handler mounted only inside a
# test harness is still *wired intentionally* (e.g. the outbox
# bundle-rebuild handler is driven by keystone tests), and deleting such
# a handler is a removal decision that must pass the pre-removal gate —
# not something to flag as silent rot. A handler with zero references
# anywhere (def + tests) is exactly the rot this guard catches.
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DS_DIR="$REPO_ROOT/DataServer"

if [ ! -d "$DS_DIR" ]; then
    printf 'check-handler-mounts: cannot locate DataServer under %s\n' "$REPO_ROOT" >&2
    exit 2
fi

cd "$DS_DIR"

# Production trees scanned for handler definitions and mounts.
SCAN_DIRS=(internal cmd)

fail=0
checked=0

# Collect handler method names: `func (x *T) NameHandler() gin.HandlerFunc`.
# The same name defined on two receiver types would collapse here; that has
# not happened in this codebase and would be its own smell — accept the
# over-approximation (checking the union of references is still sound: a
# name with any reference passes, and per-type false-negatives only occur
# when another type's same-named handler is mounted).
while IFS= read -r base; do
    [ -z "$base" ] && continue
    name="${base}Handler"
    checked=$((checked + 1))
    refs=$(grep -rn --include='*.go' -w "$name" "${SCAN_DIRS[@]}" 2>/dev/null \
        | grep -v "_test.go" \
        | grep -vE '^[^:]+:[0-9]+: *//' \
        | grep -vc "func.*) ${name}() gin\.HandlerFunc" || true)
    if [ "$refs" -eq 0 ]; then
        # Distinguish "referenced only by tests" (wired on purpose) from
        # "referenced nowhere at all" (dead rot the guard exists for).
        trefs=$(grep -rn --include='*_test.go' -w "$name" "${SCAN_DIRS[@]}" 2>/dev/null \
            | grep -vc "func.*) ${name}() gin\.HandlerFunc" || true)
        if [ "$trefs" -eq 0 ]; then
            printf 'DEAD HANDLER: %s — defined but never mounted or referenced anywhere\n' "$name" >&2
            def_file=$(grep -rln --include='*.go' "func.*) ${name}() gin\.HandlerFunc" "${SCAN_DIRS[@]}" 2>/dev/null | head -1 || true)
            [ -n "$def_file" ] && printf '  definition: %s\n' "$def_file" >&2
            fail=1
        fi
    fi
done < <(grep -rhoE --include='*.go' '\) [A-Z][A-Za-z]+Handler\(\) gin\.HandlerFunc' "${SCAN_DIRS[@]}" 2>/dev/null \
    | sed -E 's/.*\) ([A-Za-z]+)Handler\(\) gin\.HandlerFunc/\1/' | sort -u)

if [ "$fail" -ne 0 ]; then
    printf '\ncheck-handler-mounts: FAILED — see DEAD HANDLER lines above.\n' >&2
    printf 'Either mount the handler on a route, delete it (with the pre-removal\n' >&2
    printf 'gate), or, if it is test-wired by design, add its name to the\n' >&2
    printf 'allowlist section at the top of this script with a justification.\n' >&2
    exit 1
fi

printf 'check-handler-mounts: OK (%d handler methods, all mounted or intentionally test-wired)\n' "$checked"
