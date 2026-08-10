#!/usr/bin/env bash
# audit-no-youtube-residuals.sh
# ────────────────────────────────────────────────────────────────────────────
# Read-only YouTube-residue audit for an existing Velox SQLite database.
#
# Velox delegated the YouTube domain (channels, groups, OAuth tokens,
# metrics, cache) to the external Social API repository. Migration
# 090_drop_youtube_domain.sql (sqlite) is the source-of-truth DROP
# that closes out that separation.
#
# This script probes a Velox SQLite DB and reports any leftover
# YouTube_* tables or youtube_* columns on the historically-leaking
# table `calendar_events`. It NEVER writes to the database; it is safe
# to invoke on the live production DB.
#
# Exit codes
#   0  CLEAN              — no YouTube tables or columns remain
#   1  RESIDUAL_FOUND     — see reported residuals; remediation hint printed
#   2  DB_NOT_FOUND       — path missing / unreadable / empty
#   3  NOT_VELOX_SCHEMA   — DB exists but is missing canonical Velox tables
#   4  ARGV_OR_TOOL       — sqlite3 CLI missing or wrong invocation
#
# Usage
#   ./deploy/scripts/audit-no-youtube-residuals.sh /var/lib/velox/data/velox.db
#
# Cross-references
#   - DataServer/internal/store/migrations/sqlite/090_drop_youtube_domain.sql
#   - DataServer/internal/store/migrations/testdata/090_drop_youtube_domain.sql
#   - DataServer/internal/store/migrations/migrations_schema_test.go (test
#     pinning that 090 drops the tables + the historical columns)
#   - DataServer/internal/store/migrations/migrations_integration_test.go
#     (end-to-end test asserting zero YouTube state on a fresh DB)
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <path-to-velox.db>" >&2
  exit 4
fi

DB_PATH="$1"

fatal_schema() {
  echo "FATAL: DB at $DB_PATH is not a readable SQLite Velox database" >&2
  exit 3
}

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "FATAL: sqlite3 CLI not on PATH (install sqlite3 ≥ 3.16)" >&2
  exit 4
fi

if [[ ! -r "$DB_PATH" || ! -s "$DB_PATH" ]]; then
  echo "FATAL: DB not readable or empty: $DB_PATH" >&2
  exit 2
fi

# Sanity: must look like a Velox schema. We probe for the canonical
# permanent tables; if any are missing the DB is either corrupt, a
# different product, or a non-Velox SQLite file. Dark Editor tables are
# intentionally not part of this contract: that domain is no longer owned
# by Velox and must not gate a YouTube audit.
if ! canonical_found=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('jobs','artifacts','job_deliveries','calendar_events');"); then
  fatal_schema
fi
if [[ "$canonical_found" -lt 4 ]]; then
  echo "FATAL: DB at $DB_PATH does not look like a Velox schema" >&2
  echo "(found $canonical_found/4 canonical tables; expected all of: jobs, artifacts," >&2
  echo " job_deliveries, calendar_events)" >&2
  exit 3
fi

# Probe 1: youtube_* tables. Anchored prefix (lowercase comparison) to
# avoid false positives on any future `social_youtube_*` style names.
# Uses pragma_table_info as a table-valued function so we can filter
# inline (SQLite ≥ 3.16).
if ! residual_tables=$(sqlite3 -separator $'\n' "$DB_PATH" \
  "SELECT name FROM sqlite_master WHERE type='table' AND lower(name) LIKE 'youtube\_%' ESCAPE '\\' ORDER BY name;"); then
  fatal_schema
fi

# Probe 2: youtube_* columns on the historically-leaking table.
if ! ce_columns=$(sqlite3 -separator $'\n' "$DB_PATH" \
  "SELECT name FROM pragma_table_info('calendar_events') WHERE lower(name) LIKE 'youtube\_%' ESCAPE '\\' ORDER BY name;"); then
  fatal_schema
fi

# Compose report.
residual_count=0
report=""

if [[ -n "$residual_tables" ]]; then
  blk="RESIDUAL TABLES (should have been dropped by migration 090_drop_youtube_domain.sql):\n"
  while IFS= read -r t; do
    blk+="  - ${t}\n"
  done <<< "$residual_tables"
  report+="$blk"
  residual_count=$((residual_count + $(printf '%s\n' "$residual_tables" | grep -c . || true)))
fi

if [[ -n "$ce_columns" ]]; then
  blk="RESIDUAL COLUMNS on calendar_events (should have been dropped by migration 090_drop_youtube_domain.sql):\n"
  while IFS= read -r c; do
    blk+="  - calendar_events.${c}\n"
  done <<< "$ce_columns"
  report+="$blk"
  residual_count=$((residual_count + $(printf '%s\n' "$ce_columns" | grep -c . || true)))
fi

if [[ "$residual_count" -gt 0 ]]; then
  printf '%s' "$report"
  printf '\nTOTAL RESIDUALS: %d\n' "$residual_count"
  cat <<'REMEDIATION'

- Velox migration 090_drop_youtube_domain.sql is forward-only and
  idempotent (checksum-pinned by the migrations runner). If you see
  residuals on an existing install, it means the migration was skipped
  or interrupted at boot. Restarting the Velox server re-applies any
  pending migrations, including 090.
- For a FRESH DB, the migration chain rolls through 090 during the very
  first boot of the server; an end-to-end test for this lives at
  DataServer/internal/store/migrations/migrations_integration_test.go
  (TestIntegration_MigrationRunner_EndToEnd, phase 4) and the schema
  test at migrations_schema_test.go (TestMigration090_YouTubeDomainDropped)
  which additionally asserts the historical columns are gone.

REMEDIATION
  exit 1
fi

echo "CLEAN: $DB_PATH has no YouTube residual tables or columns."
exit 0
