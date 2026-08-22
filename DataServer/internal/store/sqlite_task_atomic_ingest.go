package store

// sqlite_task_atomic_ingest.go: IngestTaskResultAtomic — the single
// legal entry point for ingesting a worker TaskResult, atomically
// transitioning Task + Attempt to terminal AND persisting typed
// metrics, cache stats, cost basis, segment timings, parallelism,
// phase timings and output artifact declarations in ONE transaction.
// Split out of sqlite_task_atomic.go.

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/renderfingerprintstore"
	"velox-server/internal/taskgraph"

	sharedtelemetry "velox-shared/telemetry"
)

// IngestTaskResultAtomic is the single legal entry point for ingesting
// a worker TaskResult. It atomically transitions Task + Attempt to
// terminal AND persists typed metrics, cache stats, cost basis, AND
// registers output artifact declarations in ONE database transaction.
// No partial writes: if any step fails, everything rolls back.
//
// fix/atomic-ingestion: replaces the 4-step sequence
// (TransitionTaskToTerminalAtomic + PersistMetrics + PersistCacheStats +
// PersistCostBasis + per-artifact Register) with a single atomic call.
//
// Returns ErrTransitionConflict on stale Task CAS; the caller must NOT
// proceed with artifact registration or job roll-up on this error.
// Returns taskattempts.ErrStaleReport when the Task CAS succeeds but
// no active attempt exists for the identity tuple (§9.5 guard).
func (r *SQLiteTaskRepository) IngestTaskResultAtomic(ctx context.Context, cmd taskgraph.IngestResultCommand) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if cmd.TaskID == "" || cmd.WorkerID == "" || cmd.LeaseID == "" {
		return fmt.Errorf("task repository: IngestTaskResultAtomic requires task_id, worker_id, lease_id")
	}
	if !cmd.TaskStatus.IsTerminal() {
		return fmt.Errorf("task repository: IngestTaskResultAtomic requires terminal task status, got %s", cmd.TaskStatus)
	}
	if !cmd.AttemptStatus.IsTerminal() {
		return fmt.Errorf("task repository: IngestTaskResultAtomic requires terminal attempt status, got %s", cmd.AttemptStatus)
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("task ingest atomic begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if cmd.JobID == "" {
		if err := tx.QueryRowContext(ctx, `SELECT job_id FROM tasks WHERE task_id = ?`, cmd.TaskID).Scan(&cmd.JobID); err != nil {
			return fmt.Errorf("task ingest atomic resolve job_id: %w", err)
		}
	}
	ingestStartedAt := time.Now().UTC()

	now := nowRFC3339()

	if err := ingestTaskCAS(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := ingestAttemptCAS(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := persistAttemptVersioning(ctx, tx, cmd, now); err != nil {
		return err
	}
	renderStamp, err := persistAttemptRenderIdentity(ctx, tx, cmd, now)
	if err != nil {
		return err
	}
	if renderStamp.Mismatch {
		// Worker-declared SHA differs from the master-computed authoritative
		// value — potential ARTIFACT_TRANSFER_CORRUPTED. The master-computed
		// value remains authoritative on artifact_sha256; the flag + event
		// make the discrepancy visible in the determinism chain instead of
		// silently dropping the worker hint. The event is part of the atomic
		// audit record for this transition, so a persistence failure must
		// abort the ingest rather than report a partially recorded result.
		log.Printf("[SHA_MISMATCH] attempt=%s task=%s worker=%s worker_sha256=%s master_sha256=%s — potential ARTIFACT_TRANSFER_CORRUPTED",
			cmd.AttemptID, cmd.TaskID, cmd.WorkerID, cmd.ArtifactSHA256, renderStamp.AuthoritativeSHA256)
		if err := persistMasterExecutionEventTx(ctx, tx, masterExecutionEvent{
			EventID:   fmt.Sprintf("master-%s-sha-mismatch", cmd.AttemptID),
			AttemptID: cmd.AttemptID, TaskID: cmd.TaskID,
			Scope: sharedtelemetry.ScopeAttempt, Component: "master.artifact",
			Action: "sha_mismatch", Phase: "finalize", Status: "suspected",
			StartedAt: ingestStartedAt, CompletedAt: time.Now().UTC(),
			MetadataJSON: fmt.Sprintf(`{"worker_sha256":%q,"master_sha256":%q}`, cmd.ArtifactSHA256, renderStamp.AuthoritativeSHA256),
		}); err != nil {
			return fmt.Errorf("task ingest sha mismatch telemetry: %w", err)
		}
	}
	if err := persistAttemptTracing(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := renderfingerprintstore.SaveRenderFingerprint(ctx, tx, cmd.AttemptID, cmd.TaskID, cmd.JobID, cmd.RenderFingerprint, ingestStartedAt); err != nil {
		return err
	}
	if err := persistAttemptMetrics(ctx, tx, cmd); err != nil {
		return err
	}
	if err := persistAttemptCacheStats(ctx, tx, cmd); err != nil {
		return err
	}
	if err := persistAttemptCostBasis(ctx, tx, cmd); err != nil {
		return err
	}
	if err := persistOutputArtifacts(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := persistSegmentTimings(ctx, tx, cmd); err != nil {
		return err
	}
	if err := persistParallelism(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := persistMasterExecutionEventTx(ctx, tx, masterExecutionEvent{
		EventID:   fmt.Sprintf("master-%s-result-ingest-tx", cmd.AttemptID),
		AttemptID: cmd.AttemptID, TaskID: cmd.TaskID,
		Scope: sharedtelemetry.ScopeAttempt, Component: "db", Action: "result_ingest_tx", Phase: "finalize",
		StartedAt: ingestStartedAt, CompletedAt: time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("task ingest master telemetry: %w", err)
	}
	if err := persistPhaseTimingsAndExecutionEvents(ctx, tx, cmd); err != nil {
		return err
	}
	// Older workers (and a few mixed-version reports) persist the detailed
	// phase ledger but leave the flat aggregate columns at zero.  Project the
	// canonical ledger into the typed row in the same transaction so SQL
	// reports, daily rollups and cost dashboards see the same timings that are
	// already visible in task_phase_timings.  Non-zero worker values remain
	// authoritative.
	if err := projectPhaseAggregates(ctx, tx, cmd.AttemptID); err != nil {
		return err
	}
	if err := persistRawReport(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := persistDeadLetter(ctx, tx, cmd, ingestStartedAt); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task ingest atomic commit: %w", err)
	}
	committed = true
	return nil
}
