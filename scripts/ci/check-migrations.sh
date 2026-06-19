#!/usr/bin/env bash
# scripts/ci/check-migrations.sh
#
# Migration invariants. ALL checks are scoped to files changed in the
# current branch vs BASE_REF (default origin/main) so HEAD main is
# trivially green and PRs surface NEW regressions only.
#
# Rules enforced (only on files ADDED or MODIFIED in the PR):
#   1. Forward-only: forbid new `DROP TABLE` statements.
#   2. No SQLite-unsafe `ALTER TABLE ... RENAME COLUMN`
#      (mattn/go-sqlite3 driver compat).
#   3. Sequential numbering: any new `NNN_*.sql` MUST be greater than
#      the last existing sequence number, and NOT collide with siblings.
#
# Exit: 0 ok -- 1 violation.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source "$(dirname "${BASH_SOURCE[0]}")/lib/diff-scope.sh"

fail_if_no_base_ref

# Locate ALL migration files for context (sequencer needs every file,
# not just the diff).
all_migrations=()
while IFS= read -r f; do
  all_migrations+=("$f")
done < <(find DataServer RemoteCodex \
            -path '*/vendor/*' -prune -o \
            -type f -name '*.sql' -print | sort)

# Locate NEW + MODIFIED migrations only.
new_sql=()
while IFS= read -r f; do
  new_sql+=("$f")
done < <(git diff --name-only --diff-filter=ACMR "$BASE_REF"...HEAD \
            -- '*.sql' 2>/dev/null || true)

violations=0

# 1. DROP TABLE / ALTER RENAME COLUMN -- only on NEW files.
for sql in "${new_sql[@]}"; do
  [[ -e "$sql" ]] || continue  # file was deleted in the diff
  if matches="$(
         grep -nE '^[[:space:]]*DROP[[:space:]]+TABLE' "$sql" || true
       )"; [[ -n "$matches" ]]; then
    printf 'DROP TABLE in NEW migration %s:\n%s\n\n' \
      "$sql" "$matches" >&2
    violations=$((violations + 1))
  fi
  if matches="$(
         grep -nE \
           'ALTER[[:space:]]+TABLE[[:space:]]+[A-Za-z0-9_]+[[:space:]]+RENAME[[:space:]]+COLUMN' \
           "$sql" || true
       )"; [[ -n "$matches" ]]; then
    printf 'SQLite-unsafe ALTER RENAME COLUMN in NEW %s:\n%s\n\n' \
      "$sql" "$matches" >&2
    violations=$((violations + 1))
  fi
done

# 2. Filename sequencer over the full set. We keep the whole set so
# we surface anomalies created by renames or non-monotone additions,
# but we only FAIL on issues that involve a NEW file. Existing
# historical sequencing remains a follow-up.
seq_files=()
for f in "${all_migrations[@]}"; do
  bn="$(basename "$f")"
  if [[ "$bn" =~ ^([0-9]{3})_(.+)\.sql$ ]]; then
    seq_files+=("${BASH_REMATCH[1]}:$bn:$f")
  fi
done

if [[ ${#seq_files[@]} -gt 0 ]]; then
  prev=0
  prev_name=""
  declare -A seen_nums=()
  while IFS=: read -r num name path; do
    # Force base-10 -- `008` would otherwise be parsed as octal.
    if (( 10#$num <= 10#$prev )); then
      # Only fail if the LOWER number is itself a NEW file. Existing
      # historical non-monotone sequences are grandfathered.
      if [[ " ${new_sql[*]} " == *" $path "* ]]; then
        printf 'Non-monotone migration sequence (NEW): %s after %s\n' \
          "$name" "$prev_name" >&2
        violations=$((violations + 1))
      fi
    fi
    if [[ -n "${seen_nums[$num]:-}" ]]; then
      if [[ " ${new_sql[*]} " == *" $path "* ]]; then
        printf 'Duplicate migration number %s (NEW file %s, prior %s)\n' \
          "$num" "$name" "${seen_nums[$num]}" >&2
        violations=$((violations + 1))
      fi
    fi
    seen_nums[$num]="$path"
    prev="$num"
    prev_name="$name"
  done < <(printf '%s\n' "${seq_files[@]}" | sort -t: -k1n)
fi

if [[ "$violations" -gt 0 ]]; then
  printf '%d migration violation(s) -- see above\n' "$violations" >&2
  exit 1
fi

echo "check-migrations: OK ($(printf '%d' "${#all_migrations[@]}") files)"
