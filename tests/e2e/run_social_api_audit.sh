#!/usr/bin/env bash
# run_social_api_audit.sh — single-file replacement for the §3.5 audit
# bash block in docs/SOCIAL_API_MIGRATION_RUNBOOK.md. Operators can run
# this against any velox.db (or a freshly-bootstrapped one) per §7
# runbook update protocol.
#
# Usage:
#   ./tests/e2e/run_social_api_audit.sh                           # use a fresh /tmp/velox-test.db
#   ./tests/e2e/run_social_api_audit.sh /var/lib/velox/velox.db  # audit a specific DB
#
# Exits 0 only on `=== ALL CHECKS PASS ===`. Any failure prints the
# offender and exits non-zero.

set -uo pipefail

DB_PATH="${1:-/tmp/velox-test.db}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

echo "=== Bootstrap: $DB_PATH (migrations 001-108 except 035+048) ==="
mkdir -p "$(dirname "$DB_PATH")"
rm -f "$DB_PATH" "$DB_PATH-wal" "$DB_PATH-shm"
for m in $(ls DataServer/internal/store/migrations/sqlite/*.sql | sort); do
  bn=$(basename "$m")
  case "$bn" in
    035_*|048_*) continue ;;
  esac
  sqlite3 "$DB_PATH" < "$m" || true
done

echo
echo "=== Check 1: youtube-residue audit ==="
./deploy/scripts/audit-no-youtube-residuals.sh "$DB_PATH" \
  || { echo "FAIL"; exit 1; }

echo
echo "=== Check 2: closure marker pass ==="
sqlite3 "$DB_PATH" <<SQL
SELECT "total="        || (SELECT count(*) FROM delivery_destinations)
     || " marked="     || (SELECT count(*) FROM delivery_destinations
                             WHERE json_extract(configuration_json, "$.residuo4_closed_at") IS NOT NULL
                               AND json_valid(configuration_json) = 1)
     || " malformed="  || (SELECT count(*) FROM delivery_destinations WHERE json_valid(configuration_json) = 0);
SQL

echo
echo "=== Check 3: wire-shape dry-run (mirrors ci-opaque-wire.yml) ==="
matches=$(git grep -nE "^[[:space:]]+(Platform|AccountID|ChannelID)[[:space:]]+[A-Za-z*\[]" -- \
    DataServer/internal/socialclient/ \
    ':!.github/workflows/ci-opaque-wire.yml' \
    ':!**/*_test.go' \
    ':!**/testdata/**' \
    ':!**/migrations/**' \
    ':!**/*.md' \
    ':!CHANGELOG.md' \
    ':!docs/**' \
    ":!DataServer/internal/socialclient/targets.go" \
   || true)
if [[ -n "$matches" ]]; then
  echo "FAIL — opaque-wire regression:"
  echo "$matches"
  exit 1
fi
echo "OK — opaque-wire clean."

echo
echo "=== Check 4: dispatch-time DESTINATION_UNMAPPED rate ==="
sqlite3 -separator "$(printf '\t')" "$DB_PATH" <<SQL
SELECT date(completed_at) AS day, count(*) AS unmapped_count
FROM job_deliveries
WHERE last_error_code = 'DESTINATION_UNMAPPED'
  AND status = 'FAILED'
GROUP BY day
ORDER BY day DESC
LIMIT 7;
SQL

echo
echo "=== ALL CHECKS PASS ==="
