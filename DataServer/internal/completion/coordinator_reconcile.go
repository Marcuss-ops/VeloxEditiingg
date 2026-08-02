// Package completion / coordinator_reconcile.go
//
// ReconcileAttempt — the supervisor's repair-forward scan on a single
// commit_id. One LevelSerializable tx; the CommitResult snapshot is
// read BEFORE tx.Commit() (Verdetto P1 #9). Zero raw SQL — all
// writes go through the UnitOfWork repositories.
package completion

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ────────────────────────────────────────────────────────────────────────
// ReconcileAttempt — UNITOFWORK-DRIVEN. Zero raw SQL. The CommitResult
// snapshot is read BEFORE tx.Commit() (Verdetto P1 #9).
// ────────────────────────────────────────────────────────────────────────

// ReconcileAttempt performs the supervisor's repair-forward scan on a
// single commit_id. Phase 2.9 ships only the DECLARED-with-dead-worker
// case: when commit_deadline_at has elapsed mark EXPIRED and emit
// 'commit_protocol.expired'. Other cases (Phase 4.1 wiring).
func (c *coordinator) ReconcileAttempt(ctx context.Context, commitID string) (*CommitResult, error) {
	if commitID == "" {
		return nil, fmt.Errorf("completion.ReconcileAttempt: commitID empty")
	}

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: begin tx: %w", err)
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

	if row.Status != "DECLARED" && row.Status != "UPLOADING" && row.Status != "RECEIVED" {
		// Already terminal or bypass-able — snapshot and return.
		res, gerr := repos.AttemptCommits().GetCommitResult(ctx, commitID)
		if gerr != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: snapshot on non-terminal status: %w", gerr)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: commit (noop): %w", err)
		}
		committed = true
		return res, nil
	}

	deadlineElapsed := false
	if row.CommitDeadlineAt != "" {
		if t, perr := time.Parse(time.RFC3339Nano, row.CommitDeadlineAt); perr == nil {
			deadlineElapsed = now.After(t)
		}
	}
	if !deadlineElapsed {
		res, gerr := repos.AttemptCommits().GetCommitResult(ctx, commitID)
		if gerr != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: snapshot on deadline-not-elapsed: %w", gerr)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: commit (deadline-not-elapsed): %w", err)
		}
		committed = true
		return res, nil
	}

	if err := repos.AttemptCommits().SetExpiredByID(ctx, commitID, nowStr); err != nil {
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): SetExpiredByID
		// is the canonical attempt_commits CAS path on the
		// reconcile side. Per-key: this commit's streak is
		// independent of any other in-flight commit's conflicts.
		if budgetErr := c.recordAttemptCommitsCAS("commit:"+commitID, err); budgetErr != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: attempt_commits CAS: %w", budgetErr)
		}
		return nil, fmt.Errorf("completion.ReconcileAttempt: attempt_commits CAS: %w", err)
	}
	payloadJSON := `{"commit_id":"` + commitID + `","attempt_id":"` + row.AttemptID + `","job_id":"` + row.JobID + `"}`
	if err := repos.Outbox().InsertEvent(ctx, "re_"+commitID, "task", row.TaskID, "commit_protocol.expired", payloadJSON, nowStr); err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: outbox_events insert: %w", err)
	}

	// Snapshot CommitResult BEFORE commit (Verdetto P1 #9 / tx-after-
	// commit bug fix).
	res, err := repos.AttemptCommits().GetCommitResult(ctx, commitID)
	if err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: snapshot CommitResult: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: commit: %w", err)
	}
	committed = true
	// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): reset the
	// per-commit conflict budget on a successful ReconcileAttempt.
	c.recordAttemptCommitsCAS("commit:"+commitID, nil)
	return res, nil
}
