// Package artifacts / finalize_phases.go
//
// Steps 1–3 (and the 2.5 sweep) of the FinalizeVerified atomic tx:
// the per-table CAS flips. Each method receives the caller's *sql.Tx,
// performs ONE logical CAS, and NEVER opens/commits/rolls back its own
// transaction — tx lifecycle stays exclusively in the orchestrator
// (sqlite_finalize_writer.go).
package artifacts

import (
	"context"
	"database/sql"
	"fmt"
)

// ── Step 1: validateFinalizingUploadTx ──────────────────────────────────

// validateFinalizingUploadTx enforces the auth + state precondition on
// the artifact_uploads row. This is the *only* place in the writer
// where identity is checked at the SQL layer: the subsequent job and
// artifact CASes are identity-free, so any drift here MUST be caught
// before we start flipping other tables.
//
// Tightened to 'FINALIZING' only — accepting 'RECEIVED' here would
// mask a missing orchestration step with a misleading late-stage
// ErrTransitionConflict at the COMPLETED flip below; rejecting here
// surfaces the precondition failure with the correct
// ErrUploadStateInvalid sentinel so the caller can retry.
func (w *SQLiteFinalizeWriter) validateFinalizingUploadTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand) error {
	pre, err := loadUploadSessionForCASInTx(ctx, tx, cmd.UploadID)
	if err != nil {
		return err
	}
	if pre.Status != "FINALIZING" {
		return fmt.Errorf("%w: upload=%s status=%s (expected FINALIZING — Service.Finalize must CAS RECEIVED->FINALIZING first)",
			ErrUploadStateInvalid, cmd.UploadID, pre.Status)
	}
	if pre.WorkerID != cmd.WorkerID || pre.LeaseID != cmd.LeaseID || pre.AttemptNumber != cmd.AttemptNumber {
		return fmt.Errorf("%w: auth upload=%s worker=%s->%s lease=%s->%s attempt=%d->%d",
			ErrTransitionConflict, cmd.UploadID,
			pre.WorkerID, cmd.WorkerID, pre.LeaseID, cmd.LeaseID,
			pre.AttemptNumber, cmd.AttemptNumber)
	}
	return nil
}

// ── Step 2: markJobSucceededTx ──────────────────────────────────────────

// markJobSucceededTx is the sole writer of jobs.status='SUCCEEDED'.
// The audit contract enforced by scan_test.go pivots on this query
// being anchored in this file.
//
// WHERE allows status IN ('RUNNING', 'AWAITING_ARTIFACT'). The
// AWAITING_ARTIFACT branch is the post-task-completion state
// written elsewhere once all tasks succeed; this writer closes
// the loop to SUCCEEDED. RUNNING → SUCCEEDED is preserved for
// legacy workers without an artifact contract (defensive backward
// compat).
//
// Optional cmd.ExpectedRevision>0 adds an extra CAS guard
// (optimistic concurrency) — when zero, the guard is omitted to
// match the legacy "any in-flight run" semantic.
func (w *SQLiteFinalizeWriter) markJobSucceededTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string) error {
	jobQuery := `
		UPDATE jobs
		SET status = 'SUCCEEDED',
		    completed_at = ?,
		    updated_at   = ?,
		    revision     = revision + 1
		WHERE job_id = ?
		  AND status IN ('RUNNING', 'AWAITING_ARTIFACT')`
	jobArgs := []interface{}{nowStr, nowStr, cmd.JobID}
	if cmd.ExpectedRevision != 0 {
		jobQuery += ` AND revision = ?`
		jobArgs = append(jobArgs, cmd.ExpectedRevision)
	}
	jobRes, err := tx.ExecContext(ctx, jobQuery, jobArgs...)
	if err != nil {
		return fmt.Errorf("artifacts: FinalizeVerified jobs CAS: %w", err)
	}
	if n, _ := jobRes.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: jobs affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}
	return nil
}

// ── Step 2.5: markTaskSucceededTx ──────────────────────────────────────
//
// markTaskSucceededTx sweeps the canonical `tasks` row for this job_id
// to SUCCEEDED inside the same tx that flipped jobs.status='SUCCEEDED'
// in Step 2. Closes the well-known desync surfaced by Phase 1.5
// invariant Q5 (scripts/ci/check-completion-protocol-invariants.sh):
// a job that the closure tx commits as SUCCEEDED while a worker
// release / fast-abort-finalization has stranded the corresponding
// task in RUNNING/LEASED/PENDING.
//
// WHERE accepts status IN ('RUNNING','LEASED','PENDING') so we cover:
//   - the common case: task was RUNNING when the worker submitted the
//     verified-finalize RPC (Lease → RUNNING closed cleanly),
//   - the fast-abort case: task was PENDING (master promoted Job =
//     SUCCEEDED before the claimant flip ran),
//   - the defensive case: a still-LEASED task whose offer never
//     produced an AcceptTaskAtomic.
//
// RowsAffected == 0 is INTENTIONALLY not a Tx-fatal condition: not
// every job has a tasks row (legacy job-only ingestion paths pre-
// migration-039 may legitimately have no tasks row at INSERT time).
// The Q5 invariant is the post-fix gate that catches
// real desync; this step enforces it forward.
//
// HARD DEPENDENCY: this writer requires migration 039 (the
// DataServer/internal/store/migrations/sqlite/039_tasks.sql create
// of the `tasks` table). RunMigrations
// (DataServer/internal/store/migrations/runner.go) auto-applies
// every embedded pending migration in version order on the master
// boot path, so a healthy production deploy always has 039 in place
// before any finalize RPC reaches this writer.
//
// On a half-migrated pre-039 DB the UPDATE below surfaces
// "no such table: tasks" and rolls the entire finalization tx back
// — this is the intended fail-fast signal so a half-migrated
// deploy cannot silently land Q5-flagged SUCCEEDED jobs.
//
// TEST FIXTURES: openPropagationDB in retry_budget_propagation_test.go
// bypasses RunMigrations and MUST ship its own `tasks` table mirroring
// DataServer/internal/store/migrations/sqlite/039_tasks.sql, or
// FinalizeVerified will roll back at Step 2.5 with the same
// `no such table: tasks` error. Static fixtures that produce jobs via
// direct INSERT into `jobs` (no `tasks` row) are still safe —
// RowsAffected == 0 at Step 2.5 is non-fatal by design.
func (w *SQLiteFinalizeWriter) markTaskSucceededTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string) error {
	if cmd.AttemptID != "" {
		res, err := tx.ExecContext(ctx, `
			UPDATE tasks
			SET status = ?, completed_at = ?, updated_at = ?,
			    winning_attempt_id = ?, winning_attempt_committed_at = ?,
			    winning_attempt_terminal_pending = 0, revision = revision + 1
			WHERE job_id = ? AND attempt_id = ?
			  AND worker_id = ? AND lease_id = ?
			  AND status IN ('RUNNING', 'LEASED', 'PENDING')`,
			"SUCCEEDED", nowStr, nowStr, cmd.AttemptID, nowStr,
			cmd.JobID, cmd.AttemptID, cmd.WorkerID, cmd.LeaseID,
		)
		if err != nil {
			return fmt.Errorf("artifacts: FinalizeVerified task winner CAS: %w", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: task winner affected=%d attempt=%s", ErrTransitionConflict, n, cmd.AttemptID)
		}

		attemptRes, err := tx.ExecContext(ctx, `
			UPDATE task_attempts
			SET status = ?, completed_at = COALESCE(completed_at, ?), updated_at = ?
			WHERE id = ? AND task_id = (SELECT task_id FROM tasks WHERE job_id = ? AND attempt_id = ?)
			  AND worker_id = ? AND lease_id = ?
			  AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')`,
			"SUCCEEDED", nowStr, nowStr, cmd.AttemptID, cmd.JobID, cmd.AttemptID, cmd.WorkerID, cmd.LeaseID,
		)
		if err != nil {
			return fmt.Errorf("artifacts: FinalizeVerified attempt winner CAS: %w", err)
		}
		if n, _ := attemptRes.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: attempt winner affected=%d attempt=%s", ErrTransitionConflict, n, cmd.AttemptID)
		}
		return nil
	}

	_, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET status = ?,
		    completed_at = ?,
		    updated_at   = ?
		WHERE job_id = ?
		  AND status IN ('RUNNING', 'LEASED', 'PENDING')`,
		"SUCCEEDED", nowStr, nowStr, cmd.JobID,
	)
	if err != nil {
		return fmt.Errorf("artifacts: FinalizeVerified tasks sweep: %w", err)
	}
	// n intentionally not asserted: see doc above. The tx only commits
	// the flip if subsequent steps succeed, so a partial "no task row
	// updated" pass is fine — repair via Q5 / reconciliation if needed.
	return nil
}

// ── Step 3: markArtifactReadyTx ─────────────────────────────────────────

// markArtifactReadyTx flips artifacts.status: STAGING → READY and
// stamps the master-computed (storage_provider, storage_key, sha256,
// size, mime, verified_at) tuple atomically with the job flip. The
// shared tx guarantees a partial state where Job is SUCCEEDED but
// artifacts is still STAGING cannot be observed.
func (w *SQLiteFinalizeWriter) markArtifactReadyTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE artifacts
		SET status = 'READY',
		    storage_provider = ?,
		    storage_key = ?,
		    sha256 = ?, size_bytes = ?, mime_type = ?,
		    verified_at = ?
		WHERE id = ? AND job_id = ? AND status = 'STAGING'`,
		cmd.StorageProvider, cmd.StorageKey,
		cmd.SHA256, cmd.SizeBytes, cmd.MIMEType,
		nowStr,
		cmd.ArtifactID, cmd.JobID,
	)
	if err != nil {
		return fmt.Errorf("artifacts: FinalizeVerified artifacts CAS: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: artifacts affected=%d artifact=%s",
			ErrTransitionConflict, n, cmd.ArtifactID)
	}
	return nil
}
