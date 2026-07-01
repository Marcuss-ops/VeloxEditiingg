#!/usr/bin/env bash
# scripts/ci/check-completion-protocol-invariants.sh
#
# Artifact Commit Protocol — SQL invariant assertions (Fase 1.5 of
# docs/completion-protocol.md). Four queries cover the dangerous
# combinations the new protocol must make structurally impossible:
#
#  (1) Job SUCCEEDED with NO artifact READY
#  (2) Task SUCCEEDED with required outputs still NOT READY
#  (3) >1 winning READY artifact per (job_id, output_kind)
#  (4) Delivery row pointing to a non-READY artifact
#
# Two run modes:
#
#   (a) CI GATE (default — no DB_PATH). The script invokes the Go
#       migration runner via `go test -run TestCompletionProtocolInvariants`
#       on the canonical migration-test file. That test applies ALL
#       production migrations to a fresh in-memory SQLite DB (the
#       runner's per-statement tolerance handles ALTER duplicate-
#       column / DROP COLUMN edge cases that the sqlite3 CLI does
#       not survive in migrations like 035_drop_legacy_delivery_bridge),
#       then executes the four queries and asserts zero rows. This is
#       THE in-CI proof.
#
#       Why not the sqlite3 CLI? Migration 035 ships
#         ALTER TABLE job_deliveries DROP COLUMN delivery_target_id;
#       which raises `no such column` on a fresh DB (legitimate for
#       legacy-import DBs but the Go runner's `applyMigration`
#       swallows that string with the same tolerance production
#       relies on at boot). Reusing the Go runner keeps mode (a) in
#       lock-step with the production boot path so any future
#       tolerance the runner grows is mirrored automatically.
#
#   (b) PRODUCTION DOCTOR (DB_PATH=/path/to/velox.db). The script
#       reads an existing, populated SQLite DB snapshot and runs the
#       four queries against it. Read-only — never mutates the input
#       DB. Operators can wire this into a periodic cron
#       (docs/SECURITY_RUNBOOK.md) to catch drift between the
#       protocol's formal invariants and the actual DB state.
#
# Exit 0 on no violations, 1 if any query returns non-empty (the
# offending rows are echoed to stderr for triage).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

DB_PATH="${DB_PATH:-}"

# ── The four invariant queries — shared across both run modes ──────────
# We deliberately inline them (not a here-doc) so this script can be
# sourced or sourced-after-cd'd without escaping concerns. Each query
# is its own variable so the production-doctor path can iterate them
# cleanly.
QUERY_JOB_SUCCEEDED_NO_READY='SELECT j.job_id FROM jobs j
  WHERE j.status=''SUCCEEDED''
    AND NOT EXISTS (SELECT 1 FROM artifacts a
                    WHERE a.job_id = j.job_id
                      AND a.status = ''READY'');'

QUERY_TASK_SUCCEEDED_REQUIRED_NOT_READY='SELECT t.task_id FROM tasks t
  WHERE t.status=''SUCCEEDED''
    AND EXISTS (
      SELECT 1 FROM task_output_declarations d
      LEFT JOIN artifacts a
        ON a.id = d.artifact_id AND a.status = ''READY''
      WHERE d.task_id = t.task_id
        AND d.required = 1
        AND a.id IS NULL
    );'

QUERY_DUP_WINNER_ARTIFACTS='SELECT job_id, output_kind
  FROM artifacts
  WHERE status = ''READY''
  GROUP BY job_id, output_kind
  HAVING COUNT(*) > 1;'

QUERY_DELIVERY_ON_NON_READY='SELECT d.delivery_id
  FROM job_deliveries d
  JOIN artifacts a ON a.id = d.artifact_id
  WHERE a.status != ''READY'';'

CHECKS=(
  "job_succeeded_without_ready_artifact|${QUERY_JOB_SUCCEEDED_NO_READY}"
  "task_succeeded_required_not_ready|${QUERY_TASK_SUCCEEDED_REQUIRED_NOT_READY}"
  "dup_winning_artifacts|${QUERY_DUP_WINNER_ARTIFACTS}"
  "delivery_on_non_ready_artifact|${QUERY_DELIVERY_ON_NON_READY}"
)

# ── Mode (a): CI gate — go test via the migration runner ──────────────
if [[ -z "$DB_PATH" ]]; then
  echo "[invariant] CI gate — running TestCompletionProtocolInvariants"
  ( cd "$REPO_ROOT/DataServer" && \
      go test -run TestCompletionProtocolInvariants \
              -count=1 -timeout 120s \
              ./internal/store/migrations/... )
  echo "[invariant] CI gate OK"
  exit 0
fi

# ── Mode (b): production doctor — run queries against an existing DB ───
if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "FATAL: sqlite3 binary not found on PATH" >&2
  exit 2
fi

if [[ ! -f "$DB_PATH" ]]; then
  echo "FATAL: DB_PATH=$DB_PATH does not exist" >&2
  exit 1
fi

violations=0
for entry in "${CHECKS[@]}"; do
  label="${entry%%|*}"
  query="${entry#*|}"

  result="$(sqlite3 -separator '|' "$DB_PATH" "$query")"
  if [[ -z "$result" ]]; then
    echo "[invariant] $label OK"
    continue
  fi
  echo "[invariant] $label VIOLATIONS:" >&2
  echo "$result" >&2
  violations=$((violations + 1))
done

if [[ "$violations" -gt 0 ]]; then
  echo "[invariant] $violations violation(s) — see above" >&2
  exit 1
fi

echo "[invariant] production-doctor OK"
