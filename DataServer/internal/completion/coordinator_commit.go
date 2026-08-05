package completion

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/store"
)

func (c *coordinator) CommitAttempt(ctx context.Context, commitID string) (*CommitResult, error) {
	if commitID == "" {
		return nil, fmt.Errorf("completion.CommitAttempt: commitID empty")
	}
	var result *CommitResult
	err := c.store.Run(ctx, func(tx store.CompletionTx) error {
		row, err := tx.FindCompletionAttempt(ctx, commitID)
		if err != nil {
			return mapStoreCompletionError(err)
		}
		if row.Status == "COMMITTED" {
			snapshot, e := tx.GetCompletionResult(ctx, commitID)
			if e != nil {
				return fmt.Errorf("completion.CommitAttempt: snapshot on idempotent re-call: %w", mapStoreCompletionError(e))
			}
			result = completionResultFromStore(snapshot)
			return nil
		}
		if row.Status != "DECLARED" && row.Status != "UPLOADING" && row.Status != "RECEIVED" && row.Status != "VERIFYING" {
			return fmt.Errorf("%w: attempt_commits.status=%q", ErrTransitionConflict, row.Status)
		}
		if row.ReadyOutputCount < row.RequiredOutputCount {
			return fmt.Errorf("%w: ready=%d required=%d (commit blocked)", ErrTransitionConflict, row.ReadyOutputCount, row.RequiredOutputCount)
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if err := tx.MarkCompletionTaskAttemptSucceeded(ctx, row.AttemptID, row.WorkerID, row.LeaseID, now); err != nil {
			return fmt.Errorf("completion.CommitAttempt: task_attempts CAS: %w", mapStoreCompletionError(err))
		}
		if err := tx.MarkCompletionTaskSucceeded(ctx, row.TaskID, row.AttemptID, row.WorkerID, row.LeaseID, now); err != nil {
			return fmt.Errorf("completion.CommitAttempt: tasks CAS: %w", mapStoreCompletionError(err))
		}
		if err := tx.MarkCompletionCommitted(ctx, commitID, now); err != nil {
			if budgetErr := c.recordAttemptCommitsCAS("commit:"+commitID, mapStoreCompletionError(err)); budgetErr != nil {
				return fmt.Errorf("completion.CommitAttempt: attempt_commits CAS: %w", budgetErr)
			}
			return fmt.Errorf("completion.CommitAttempt: attempt_commits CAS: %w", mapStoreCompletionError(err))
		}
		if err := tx.MarkCompletionJobSucceededIfTasksDone(ctx, row.JobID, now); err != nil {
			return fmt.Errorf("completion.CommitAttempt: jobs CAS: %w", mapStoreCompletionError(err))
		}
		if err := tx.InsertCompletionDeliveries(ctx, row.JobID, now); err != nil {
			return fmt.Errorf("completion.CommitAttempt: job_deliveries insert: %w", err)
		}
		payload := `{"commit_id":"` + commitID + `","attempt_id":"` + row.AttemptID + `","job_id":"` + row.JobID + `"}`
		if err := tx.InsertCompletionOutbox(ctx, "ce_"+commitID, "task", row.TaskID, "commit_protocol.committed", payload, now); err != nil {
			return fmt.Errorf("completion.CommitAttempt: outbox_events insert: %w", err)
		}
		snapshot, err := tx.GetCompletionResult(ctx, commitID)
		if err != nil {
			return fmt.Errorf("completion.CommitAttempt: snapshot CommitResult: %w", mapStoreCompletionError(err))
		}
		result = completionResultFromStore(snapshot)
		return nil
	})
	if err != nil {
		return nil, err
	}
	c.recordAttemptCommitsCAS("commit:"+commitID, nil)
	return result, nil
}
