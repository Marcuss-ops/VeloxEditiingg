#!/usr/bin/env bash
# scripts/ci/check-no-binaries.sh
#
# Rejects compiled binary files (ELF, PE, Mach-O) that are tracked
# in the git repository. Binaries must never be committed — they
# belong in build artifacts, container images, or release pipelines.
#
# This guard runs against ALL tracked files (full tree, not diff-scoped)
# because a single committed binary is a permanent regression regardless
# of which branch or commit introduced it.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

violations=0

# List every file tracked by git and check for ELF / PE / Mach-O signatures.
# `file` output for the types we reject:
#   ELF 64-bit LSB executable
#   ELF 32-bit LSB executable
#   PE32 executable
#   PE32+ executable
#   Mach-O 64-bit executable
while IFS= read -r tracked_file; do
  if [[ ! -f "$tracked_file" ]]; then
    continue
  fi
  file_type="$(file -b "$tracked_file" 2>/dev/null || true)"
  if echo "$file_type" | grep -qE '^ELF |^PE32|\bELF\b.*\bexecutable\b|\bPE32.*\bexecutable\b|\bMach-O\b.*\bexecutable\b'; then
    printf 'FORBIDDEN (tracked binary): %s  [%s]\n' \
      "$tracked_file" "$file_type" >&2
    violations=$((violations + 1))
  fi
done < <(git ls-files --cached)

if [[ "$violations" -gt 0 ]]; then
  printf '\n%d compiled binary file(s) tracked in git — remove with:\n' \
    "$violations" >&2
  printf '  git rm --cached <file>\n' >&2
  printf 'and add the path(s) to .gitignore.\n' >&2
  exit 1
fi

echo "check-no-binaries: OK"
