// Package completion / coordinator_commit.go
//
// CommitAttempt — the canonical atomic final transaction for a
// commit_id. All in ONE BEGIN SERIALIZABLE so commit_id either fully
// ratifies or fully rolls back. Zero raw SQL — every per-table write
// is delegated to the UnitOfWork repositories; the CommitResult
// snapshot is read BEFORE tx.Commit() to fix the previous
// tx-after-commit bug (Verdetto P1 #9).
package completion

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ────────────────────────────────────────────────────────────────────────
// CommitAttempt — UNITOFWORK-DRIVEN. Zero raw SQL. The CommitResult
// snapshot is read BEFORE tx.Commit() to fix the previous
// tx-after-commit bug (Verdetto P1 #9).
// ────────────────────────────────────────────────────────────────────────

// CommitAttempt performs the canonical atomic final transaction for a
// commit_id. All in ONE BEGIN SERIALIZABLE so commit_id either fully
// ratifies or fully rolls back.
//
// Idempotency: a duplicate CommitAttempt on a COMMITTED row is a no-op
// CommitResult return.
//
// Gating: tasks.status must be in ('RUNNING','LEASED'). Note we do
// NOT require winning_attempt_terminal_pending=1 — the worker can
// call CommitAttempt directly without driving through
// IngestTaskResultAtomic first (legacy TaskResult path) and the
// commit protocol ratifies identically.
func (c *coordinator) CommitAttempt(ctx context.Context, commitID string) (*CommitResult, error) {
	if commitID == "" {
		return nil, fmt.Errorf("completion.CommitAttempt: commitID empty")
	}

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	repos := c.uowFactory.WithTx(tx)
	row, err := repos.AttemptCommits().Find(ctx, commitID)
	if err != nil {
		return nil, err
	}
	if row.Status == "COMMITTED" {
		// Idempotent re-call: snapshot already-COMMITTED state via
		// the SAME tx (still open), GetCommitResult is part of the
		// same write lock — no race window.
		res, gerr := repos.AttemptCommits().GetCommitResult(ctx, commitID)
		if gerr != nil {
			return nil, fmt.Errorf("completion.CommitAttempt: snapshot on idempotent re-call: %w", gerr)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("completion.CommitAttempt: commit (idempotent): %w", err)
		}
		committed = true
		return res, nil
	}
	if row.Status != "DECLARED" && row.Status != "UPLOADING" && row.Status != "RECEIVED" && row.Status != "VERIFYING" {
		return nil, fmt.Errorf("%w: attempt_commits.status=%q", ErrTransitionConflict, row.Status)
	}
	if row.ReadyOutputCnt < row.RequiredOutputCnt {
		return nil, fmt.Errorf("%w: ready=%d required=%d (commit blocked)", ErrTransitionConflict, row.ReadyOutputCnt, row.RequiredOutputCnt)
	}

	if err := repos.TaskAttempts().MarkSucceeded(ctx, row.AttemptID, row.WorkerID, row.LeaseID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: task_attempts CAS: %w", err)
	}
	if err := repos.Tasks().MarkSucceeded(ctx, row.TaskID, row.AttemptID, row.WorkerID, row.LeaseID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: tasks CAS: %w", err)
	}
	if err := repos.AttemptCommits().MarkCommitted(ctx, commitID, nowStr); err != nil {
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): MarkCommitted
		// is the third canonical attempt_commits CAS path. Per-key
		// isolation: this commit's streak is independent of any
		// other in-flight commit's conflicts.
		if budgetErr := c.recordAttemptCommitsCAS("commit:"+commitID, err); budgetErr != nil {
			return nil, fmt.Errorf("completion.CommitAttempt: attempt_commits CAS: %w", budgetErr)
		}
		return nil, fmt.Errorf("completion.CommitAttempt: attempt_commits CAS: %w", err)
	}
	if err := repos.Jobs().MarkSucceededIfTasksDone(ctx, row.JobID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: jobs CAS: %w", err)
	}
	if err := repos.Deliveries().InsertDeliveriesForJob(ctx, row.JobID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: job_deliveries insert: %w", err)
	}
	payloadJSON := `{"commit_id":"` + commitID + `","attempt_id":"` + row.AttemptID + `","job_id":"` + row.JobID + `"}`
	if err := repos.Outbox().InsertEvent(ctx, "ce_"+commitID, "task", row.TaskID, "commit_protocol.committed", payloadJSON, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: outbox_events insert: %w", err)
	}

	// Snapshot CommitResult BEFORE commit (Verdetto P1 #9 / tx-after-
	// commit bug fix). The read is part of the same LevelSerializable
	// write lock so the result cannot drift from the just-written
	// SUCCEEDED state under a concurrent writer.
	res, err := repos.AttemptCommits().GetCommitResult(ctx, commitID)
	if err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: snapshot CommitResult: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: commit: %w", err)
	}
	committed = true
	// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): reset the
	// per-commit conflict budget on a successful CommitAttempt.
	c.recordAttemptCommitsCAS("commit:"+commitID, nil)
	return res, nil
}
