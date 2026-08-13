package completion

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/repository"
)

func (c *coordinator) ReconcileAttempt(ctx context.Context, commitID string) (*CommitResult, error) {
	if commitID == "" {
		return nil, fmt.Errorf("completion.ReconcileAttempt: commitID empty")
	}
	var result *CommitResult
	err := c.store.Run(ctx, func(tx repository.CompletionTx) error {
		row, err := tx.FindCompletionAttempt(ctx, commitID)
		if err != nil {
			return mapStoreCompletionError(err)
		}
		now := time.Now().UTC()
		nowStr := now.Format(time.RFC3339Nano)
		if row.Status != "DECLARED" && row.Status != "UPLOADING" && row.Status != "RECEIVED" {
			snapshot, e := tx.GetCompletionResult(ctx, commitID)
			if e != nil {
				return fmt.Errorf("completion.ReconcileAttempt: snapshot on non-terminal status: %w", mapStoreCompletionError(e))
			}
			result = completionResultFromStore(snapshot)
			return nil
		}
		deadlineElapsed := false
		if row.CommitDeadlineAt != "" {
			if deadline, e := time.Parse(time.RFC3339Nano, row.CommitDeadlineAt); e == nil {
				deadlineElapsed = now.After(deadline)
			}
		}
		if !deadlineElapsed {
			snapshot, e := tx.GetCompletionResult(ctx, commitID)
			if e != nil {
				return fmt.Errorf("completion.ReconcileAttempt: snapshot on deadline-not-elapsed: %w", mapStoreCompletionError(e))
			}
			result = completionResultFromStore(snapshot)
			return nil
		}
		if err := tx.ExpireCompletionAttemptByID(ctx, commitID, nowStr); err != nil {
			mapped := mapStoreCompletionError(err)
			if budgetErr := c.recordAttemptCommitsCAS("commit:"+commitID, mapped); budgetErr != nil {
				return fmt.Errorf("completion.ReconcileAttempt: attempt_commits CAS: %w", budgetErr)
			}
			return fmt.Errorf("completion.ReconcileAttempt: attempt_commits CAS: %w", mapped)
		}
		payload := `{"commit_id":"` + commitID + `","attempt_id":"` + row.AttemptID + `","job_id":"` + row.JobID + `"}`
		if err := tx.InsertCompletionOutbox(ctx, "re_"+commitID, "task", row.TaskID, "commit_protocol.expired", payload, nowStr); err != nil {
			return fmt.Errorf("completion.ReconcileAttempt: outbox_events insert: %w", err)
		}
		snapshot, err := tx.GetCompletionResult(ctx, commitID)
		if err != nil {
			return fmt.Errorf("completion.ReconcileAttempt: snapshot CommitResult: %w", mapStoreCompletionError(err))
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
