#!/usr/bin/env bash
# scripts/ci/ratchet-sql.sh
#
# Per-file SQL-ownership ratchet.
#
# Replaces the previous directory-allowlist gates with a file-level
# ratchet. Rules:
#
#   1. The only unrestricted SQL gateway is DataServer/internal/store/**.
#   2. Every other production file under DataServer/internal is
#      checked for direct SQL coupling.
#   3. A baseline file (sql-baseline.txt) records the number of
#      violations per file. The total can only decrease.
#   4. A file not in the baseline must have zero violations.
#   5. A file in the baseline cannot have more violations than its
#      baseline count.
#   6. A file in the baseline with zero current violations must be
#      removed from the baseline.
#
# Usage:
#   ./scripts/ci/ratchet-sql.sh              # check mode
#   ./scripts/ci/ratchet-sql.sh --update     # regenerate baseline
#
# Exit codes: 0 OK, 1 regression detected.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

SCOPE="DataServer/internal"
BASELINE="${REPO_ROOT}/scripts/ci/sql-baseline.txt"
UPDATE_MODE=0

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --update) UPDATE_MODE=1 ;;
    --baseline) BASELINE="$2"; shift ;;
    --scope) SCOPE="$2"; shift ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
  shift
done

# Forbidden method-call tokens (db.* / tx.* receivers).
METHOD_REGEX='(db\.BeginTx|db\.Exec(Context)?|db\.Query(Context)?|db\.QueryRow(Context)?|tx\.Exec(Context)?|tx\.Query(Context)?|tx\.QueryRow(Context)?)\('
# Forbidden shape-level tokens: import, sql.DB/Tx usage, raw DML strings.
IMPORT_REGEX='"database/sql"'
TYPE_REGEX='\bsql\.(DB|Tx)\b'
DML_REGEX='\bINSERT[[:space:]]+INTO\b|\bUPDATE[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]+SET\b|\bDELETE[[:space:]]+FROM\b'
SHAPE_REGEX="${IMPORT_REGEX}|${TYPE_REGEX}|${DML_REGEX}"
COMBINED_REGEX="(?i)${METHOD_REGEX}|${SHAPE_REGEX}"

# Gather all production Go files under scope, excluding tests and the
# canonical store gateway.
mapfile -t files < <(
  find "$SCOPE" -type f -name '*.go' \
    -not -name '*_test.go' \
    | sort
)

# filter_comments strips Go line comments and block comments from
# grep -n output (format: <path>:<lineno>:<content>). Used to keep
# doc-string prose from inflating the SQL-coupling baseline.
filter_comments() {
  awk '
    BEGIN { in_block = 0 }
    {
      if (in_block == 1) {
        if (index($0, "*/") > 0) { in_block = 0 }
        next
      }
      if ($0 ~ /^[^:]+:[0-9]+:[[:space:]]*\/\*/) {
        if ($0 !~ /\*\//) { in_block = 1 }
        next
      }
      if ($0 ~ /^[^:]+:[0-9]+:[[:space:]]*\/\//) { next }
      print
    }
  '
}

count_violations() {
  local f="$1"
  local hits=0
  local method_hits shape_hits all_hits
  method_hits="$(grep -nE "${METHOD_REGEX}" "$f" 2>/dev/null | filter_comments || true)"
  shape_hits="$(grep -nP "${SHAPE_REGEX}" "$f" 2>/dev/null | filter_comments || true)"
  all_hits="$(printf '%s\n' "$method_hits" "$shape_hits" | grep -v '^$' || true)"
  if [[ -n "$all_hits" ]]; then
    hits="$(printf '%s\n' "$all_hits" | cut -d: -f1 | sort -u | wc -l)"
  fi
  echo "$hits"
}

# Build current counts map.
declare -A current
for f in "${files[@]}"; do
  # Skip canonical store gateway and test helpers.
  case "$f" in
    ${SCOPE}/store/*) continue ;;
  esac
  current["$f"]=$(count_violations "$f")
done

# If update mode, write baseline and exit.
if [[ "$UPDATE_MODE" -eq 1 ]]; then
  : > "$BASELINE"
  for f in "${!current[@]}"; do
    cnt="${current[$f]}"
    if [[ "$cnt" -gt 0 ]]; then
      printf '%d %s\n' "$cnt" "$f" >> "$BASELINE"
    fi
  done
  sort -k2 "$BASELINE" > "${BASELINE}.tmp"
  mv "${BASELINE}.tmp" "$BASELINE"
  echo "ratchet-sql: baseline updated at ${BASELINE#$REPO_ROOT/}"
  exit 0
fi

# Parse baseline.
declare -A baseline
if [[ -f "$BASELINE" ]]; then
  while IFS=' ' read -r cnt path; do
    [[ -z "$path" ]] && continue
    baseline["$path"]="$cnt"
  done < "$BASELINE"
fi

# Evaluate rules.
regressions=0
removed_zero=()
new_files=()
reduced_files=()

for f in "${!current[@]}"; do
  cnt="${current[$f]}"
  base="${baseline[$f]:-0}"
  if [[ "$cnt" -gt 0 && -z "${baseline[$f]+x}" ]]; then
    new_files+=("$f")
    regressions=$((regressions + 1))
  elif [[ "$cnt" -gt "$base" ]]; then
    printf 'REGRESSION: %s has %d violations (baseline %d)\n' "$f" "$cnt" "$base" >&2
    regressions=$((regressions + 1))
  elif [[ "$cnt" -lt "$base" ]]; then
    reduced_files+=("$f ($base -> $cnt)")
  fi
done

# Detect baseline files that are now zero or whose file no longer exists.
for f in "${!baseline[@]}"; do
  if [[ ! -f "$f" ]]; then
    # File removed; baseline entry should be removed too.
    removed_zero+=("$f (file removed)")
  elif [[ -z "${current[$f]+x}" ]]; then
    # File not scanned (e.g. moved to store) -- treat as removed.
    removed_zero+=("$f (no longer scanned)")
  elif [[ "${current[$f]}" -eq 0 ]]; then
    removed_zero+=("$f (now zero)")
  fi
done

# Report.
if [[ ${#reduced_files[@]} -gt 0 ]]; then
  echo "ratchet-sql: the following files have fewer violations than baseline; run with --update to reduce the baseline:" >&2
  for r in "${reduced_files[@]}"; do
    printf '  %s\n' "$r" >&2
  done
fi

if [[ ${#removed_zero[@]} -gt 0 ]]; then
  echo "ratchet-sql: the following baseline entries must be removed (file missing or count reached zero); run with --update to clean up:" >&2
  for r in "${removed_zero[@]}"; do
    printf '  %s\n' "$r" >&2
  done
  regressions=$((regressions + ${#removed_zero[@]}))
fi

if [[ ${#new_files[@]} -gt 0 ]]; then
  echo "ratchet-sql: new files with SQL violations (must be added via --update after review):" >&2
  for f in "${new_files[@]}"; do
    printf '  %s (%d violations)\n' "$f" "${current[$f]}" >&2
  done
fi

if [[ "$regressions" -gt 0 ]]; then
  printf '\nratchet-sql: FAIL (%d issue(s))\n' "$regressions" >&2
  exit 1
fi

echo "ratchet-sql: OK (total files with violations: $(grep -c . "$BASELINE" 2>/dev/null || echo 0))"
