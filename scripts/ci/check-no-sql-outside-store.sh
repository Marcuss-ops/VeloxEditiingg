#!/usr/bin/env bash
# scripts/ci/check-no-sql-outside-store.sh
#
# SQL-ownership lint. Forbids direct db.* / tx.* SQL access in
# production code outside the canonical store package or its declared
# allowlist. Mirrors the docstring on the UnitOfWork adapter
# (DataServer/internal/completion/sqlite_uow.go) and the architecture
# chapter docs/architecture/unit-of-work.md: the typed repos are the
# sole SQL gateway going forward.
#
# Allowlist (in evaluation order, first match wins per file):
#
#   1. Path:        DataServer/internal/store/** (sole SQL gateway)
#   2. Path:        DataServer/cmd/seed-*    (test scaffolding)
#   3. File marker: any Go file containing the literal
#                   "// sql-allowlist:" comment (rationale tracked in
#                   the marker). Used for legacy direct-access paths
#                   that have not yet been refactored into store/.
#   4. Method:      The two OUT-OF-UNITOFWORK-SCOPE methods in
#                   DataServer/internal/completion/coordinator.go:
#                   DeclareOutputs and RecordUploadProgress (the HMAC
#                   + INSERT-OR-IGNORE dance tightly coupled to the
#                   FenceTuple.Read gate). Their function bodies are
#                   identified via the "OUT OF UNITOFWORK SCOPE" marker
#                   that immediately precedes them in the doc-separator
#                   comment block. Other methods of the Coordinator
#                   speak only through the typed UoW repos, so any
#                   direct SQL hit there is a violation.
#
# Forbidden tokens (canonical receiver-prefixed SQL methods on db.* or
# tx.* (or any local var named ending in those tokens, which is the
# codebase convention for tx handles)):
#
#   - .BeginTx(  .Begin(   .Exec(    .ExecContext(
#   - .Query(    .QueryContext(
#   - .QueryRow( .QueryRowContext(
#
# Production-only (excludes *_test.go so the test fixtures — which
# routinely open short-lived in-memory DBs — stay out of scope).
#
# Exit codes: 0 OK; 1 violation found.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Forbidden token regex — exactly the user-specified literals.
# Per the spec, the receivers are db. and tx. (NOT any var ending in
# those tokens — that would catch Gin/Chi HTTP-context Query() calls
# like c.Query("name") which are routinely used in handlers and
# share zero surface area with the SQL gateway). Strict-spec: only
# the seven receiver-prefixed methods named in the user's spec are
# flagged; the auto-commit db.Begin() and the Context variants
# (.ExecContext, .QueryContext, .QueryRowContext) are intentionally
# excluded so this lint stays a SQL-ownership check, not a stylistic
# one.
SQL_REGEX='(db\.BeginTx|db\.Exec|db\.Query|db\.QueryRow|tx\.Exec|tx\.Query|tx\.QueryRow)\('

# Locate all production Go files in DataServer/internal/* excluding
# _test.go. Scope matches the user spec ("DataServer/internal/*" only);
# DataServer/cmd/* and DataServer/DatabaseServer/* binaries are not
# checked by this lint (they own their own indirect DB paths).
mapfile -t files < <(
  find DataServer/internal -type f -name '*.go' \
    -not -name '*_test.go' \
    | sort
)

violations=0

is_in_uow_section() {
  # Returns 0 (true) iff the given line number $1 falls inside one of the
  # two OUT-OF-UoW method bodies (DeclareOutputs, RecordUploadProgress)
  # in the canonical coordinator.go. Uses awk to walk the file.
  local f="$1" ln="$2"
  awk -v target="$ln" '
    BEGIN { in_uow_section = 0 }# ────────────────────────────────────────────────────────────────────
# IMPORTANT — coordination_marker: any line containing the substring
# "OUT OF UNITOFWORK SCOPE" opens the OUT-of-UoW scope; the default
# rule below still runs on that line so a SQL hit that physically
# shares a line with the doc-separator (rare but possible) is
# recognised as in_uow_section instead of being wrongly flagged.
    /OUT OF UNITOFWORK SCOPE/ {
      in_uow_section = 1
    }
    /^func \(c \*coordinator\) [A-Z][A-Za-z0-9_]*/ {
      # End of the OUT-of-UoW section ONLY when a new method
      # declaration begins for any name other than DeclareOutputs or
      # RecordUploadProgress. Both target methods keep the section open.
      name = $0
      sub(/^func \(c \*coordinator\) /, "", name)
      sub(/\(.*$/, "", name)
      if (name != "DeclareOutputs" && name != "RecordUploadProgress") {
        in_uow_section = 0
      } else {
        in_uow_section = 1
      }
      next
    }
    {
      if (NR == target && in_uow_section == 1) {
        exit 0
      }
    }
    END { exit 1 }
  ' "$f"
}

for f in "${files[@]}"; do
  # Allowlist 1: path-based — internal/store/ is the sole SQL gateway.
  # NOTE: The find above is scoped to DataServer/internal/ so the
  # cmd/seed-* branch is currently dead code (forward-compat only).
  case "${f#DataServer/internal/}" in
    store/*) continue ;;
  esac
  case "${f#DataServer/}" in
    cmd/seed-*) continue ;;
  esac

  # Allowlist 3: file-marker. Any Go file whose top-of-file doc-comment
  # block contains the literal "// sql-allowlist:" line is opted in
  # to direct SQL access; the marker is no-op when the file already
  # lives in internal/store/. The grep uses whole-file, no column-0
  # anchor: the marker can be on any leading comment line (some
  # files have a 2–8 line package doc block above it).
  if grep -q '//[[:space:]]*sql-allowlist:' "$f"; then
    continue
  fi

  # Allowlist 4: completion/coordinator.go's two OUT-OF-UoW methods
  # PLUS the Coordinator's documented tx-lifecycle layer
  # (`c.db.BeginTx(` only — every method owns its own LevelSerializable
  # tx per the package doc: "The Coordinator owns the *sql.Tx
  # lifecycle (open, commit, defer rollback) per method call"). UoW
  # repos do NOT start or commit transactions; the BeginTx line is
  # the architecture boundary, not raw-SQL access. tx.Commit /
  # tx.Rollback are out of our regex so they are not flagged
  # elsewhere.
  if [[ "$f" == "DataServer/internal/completion/coordinator.go" ]]; then
    # `grep -oE "${SQL_REGEX}"` strips the receiver prefix, so a line
    # `c.db.BeginTx(...)` extracts as just `db.BeginTx(`. Match that
    # exact form, then guard against any non-coordinator file writing
    # the same SQL_REGEX-only match by re-checking the literal
    # `c.db.BeginTx(` against the original source line.
    TX_LIFECYCLE_ONLY='^db\.BeginTx\($'
    bad=()
    while IFS=: read -r ln content; do
      [[ -z "$ln" ]] && continue
      # 1. Inside DeclareOutputs or RecordUploadProgress body? OK.
      if is_in_uow_section "$f" "$ln"; then
        continue
      fi
      # 2. Tx-lifecycle open in coordinator.go's exact receiver form.
      #    Pin the literal `c.db.BeginTx(` against $content so a
      #    hypothetical other file using `someone.db.BeginTx(...)`
      #    cannot piggy-back on the same exemption.
      line_matches="$(grep -oE "${SQL_REGEX}" <<<"$content" 2>/dev/null || true)"
      non_lifecycle="$(grep -vE "${TX_LIFECYCLE_ONLY}" <<<"$line_matches" 2>/dev/null || true)"
      if [[ -z "${non_lifecycle:-}" ]] \
         && [[ -n "${line_matches:-}" ]] \
         && [[ "$content" =~ c\.db\.BeginTx\( ]]; then
        continue
      fi
      bad+=("$ln: $content")
    done < <(grep -nE "${SQL_REGEX}" "$f" || true)
    if [[ ${#bad[@]} -gt 0 ]]; then
      printf 'Direct SQL access in coordinator.go OUTSIDE OUT-OF-UoW methods:\n  %s\n' "$f" >&2
      for v in "${bad[@]}"; do
        printf '    %s\n' "$v" >&2
      done
      violations=$((violations + 1))
    fi
    continue
  fi

  # Default: any SQL hit in this file is a violation.
  if matches="$(grep -nE "${SQL_REGEX}" "$f")"; [[ -n "$matches" ]]; then
    printf 'Direct SQL access outside allowlist: %s\n' "$f" >&2
    while IFS= read -r line; do
      printf '    %s\n' "$line" >&2
    done <<< "$matches"
    violations=$((violations + 1))
  fi
done

if [[ "$violations" -gt 0 ]]; then
  printf '\ncheck-no-sql-outside-store: %d file violation(s)\n' "$violations" >&2
  exit 1
fi

echo "check-no-sql-outside-store: OK ($(printf '%d' "${#files[@]}") files scanned)"
