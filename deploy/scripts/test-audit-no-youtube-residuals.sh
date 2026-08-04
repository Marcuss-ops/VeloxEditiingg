#!/usr/bin/env bash
# test-audit-no-youtube-residuals.sh
# Focused regression test for audit-no-youtube-residuals.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUDIT_SCRIPT="$SCRIPT_DIR/audit-no-youtube-residuals.sh"

[[ -x "$AUDIT_SCRIPT" ]] || {
  printf '[test][FATAL] audit script is missing or not executable: %s\n' "$AUDIT_SCRIPT" >&2
  exit 2
}
command -v sqlite3 >/dev/null 2>&1 || {
  printf '[test][FATAL] sqlite3 CLI is required\n' >&2
  exit 2
}

WORK="$(mktemp -d -t velox-audit-test.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

run_case() {
  local label="$1" expected="$2" db="$3"
  shift 3
  local actual=0
  set +e
  "$AUDIT_SCRIPT" "$db" "$@" >/dev/null 2>&1
  actual=$?
  set -e
  if [[ "$actual" -ne "$expected" ]]; then
    printf '[FAIL] %-38s (want rc=%d, got rc=%d)\n' "$label" "$expected" "$actual"
    return 1
  fi
  printf '[OK]   %-38s (rc=%d)\n' "$label" "$actual"
}

make_velox_db() {
  local db="$1"
  sqlite3 "$db" <<'SQL'
CREATE TABLE jobs (id TEXT);
CREATE TABLE artifacts (id TEXT);
CREATE TABLE job_deliveries (id TEXT);
CREATE TABLE calendar_events (id TEXT);
SQL
}

printf '[test] audit script: %s\n' "$AUDIT_SCRIPT"

# A valid Velox schema has only the four permanent canonical tables. In
# particular, no Dark Editor table is required for the audit to run.
CLEAN_DB="$WORK/clean.db"
make_velox_db "$CLEAN_DB"
run_case 'four-table Velox schema is clean' 0 "$CLEAN_DB"

RESIDUAL_TABLE_DB="$WORK/residual-table.db"
make_velox_db "$RESIDUAL_TABLE_DB"
sqlite3 "$RESIDUAL_TABLE_DB" 'CREATE TABLE youtube_channels (id TEXT);'
run_case 'YouTube table is reported' 1 "$RESIDUAL_TABLE_DB"

RESIDUAL_COLUMN_DB="$WORK/residual-column.db"
make_velox_db "$RESIDUAL_COLUMN_DB"
sqlite3 "$RESIDUAL_COLUMN_DB" 'ALTER TABLE calendar_events ADD COLUMN youtube_channel_id TEXT;'
run_case 'YouTube column is reported' 1 "$RESIDUAL_COLUMN_DB"

NOT_SCHEMA_DB="$WORK/not-velox.db"
sqlite3 "$NOT_SCHEMA_DB" 'CREATE TABLE unrelated (id TEXT);'
run_case 'non-Velox schema returns NOT_VELOX_SCHEMA' 3 "$NOT_SCHEMA_DB"

INVALID_DB="$WORK/invalid.db"
printf 'not a SQLite database\n' > "$INVALID_DB"
run_case 'invalid SQLite returns NOT_VELOX_SCHEMA' 3 "$INVALID_DB"

EMPTY_DB="$WORK/empty.db"
: > "$EMPTY_DB"
run_case 'empty DB returns DB_NOT_FOUND' 2 "$EMPTY_DB"
run_case 'missing DB returns DB_NOT_FOUND' 2 "$WORK/missing.db"

actual=0
set +e
"$AUDIT_SCRIPT" >/dev/null 2>&1
actual=$?
set -e
if [[ "$actual" -ne 4 ]]; then
  printf '[FAIL] missing argument returns ARGV_OR_TOOL (want rc=4, got rc=%d)\n' "$actual"
  exit 1
fi
printf '[OK]   missing argument returns ARGV_OR_TOOL (rc=4)\n'

actual=0
set +e
"$AUDIT_SCRIPT" "$CLEAN_DB" unexpected >/dev/null 2>&1
actual=$?
set -e
if [[ "$actual" -ne 4 ]]; then
  printf '[FAIL] extra argument returns ARGV_OR_TOOL (want rc=4, got rc=%d)\n' "$actual"
  exit 1
fi
printf '[OK]   extra argument returns ARGV_OR_TOOL (rc=4)\n'

# Exercise the tool exit code without changing the host installation: provide
# a PATH containing bash but no sqlite3, then invoke the script by absolute path.
NO_SQLITE_BIN="$WORK/no-sqlite-bin"
mkdir -p "$NO_SQLITE_BIN"
ln -s "$(command -v bash)" "$NO_SQLITE_BIN/bash"
actual=0
set +e
PATH="$NO_SQLITE_BIN" "$AUDIT_SCRIPT" "$CLEAN_DB" >/dev/null 2>&1
actual=$?
set -e
if [[ "$actual" -ne 4 ]]; then
  printf '[FAIL] sqlite3 missing returns ARGV_OR_TOOL (want rc=4, got rc=%d)\n' "$actual"
  exit 1
fi
printf '[OK]   sqlite3 missing returns ARGV_OR_TOOL (rc=4)\n'

printf '[test] PASS: audit exit codes and YouTube probes are stable\n'
