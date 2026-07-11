-- migrations/sqlite/065_artifacts_output_kind.sql
--
-- Artifact Commit Protocol (Phase 1.5 follow-up) — add the
-- `output_kind` column to the artifacts table.
--
-- Invariant query #3 in scripts/ci/check-completion-protocol-invariants.sh
-- groups WINNING READY artifacts by (job_id, output_kind) and asserts
-- each group has at most one row. Without this column the query is a
-- SQL error (`no such column: output_kind`) on a fresh DB and a CI
-- regression at the master layer would silently slip past the guard.
--
-- default '' (empty string) keeps every pre-existing artifact's
-- output_kind a sentinel that groups together as a single (job_id, '')
-- bucket — invariant query #3 then returns zero rows on a freshly
-- migrated DB (no artifact is READY at fresh seed regardless). The
-- master stamps output_kind to a non-empty value at finalize-time,
-- pulled from the matching task_output_declarations.row. Phase 2
-- wires the master-side stamping; until then the column is
-- informational only.
--
-- ADD COLUMN with NOT NULL DEFAULT '' is a SQLite-supported online
-- ALTER that does not require a table rewrite; existing rows are
-- back-filled atomically with the empty string.

ALTER TABLE artifacts
    ADD COLUMN output_kind TEXT NOT NULL DEFAULT '';
