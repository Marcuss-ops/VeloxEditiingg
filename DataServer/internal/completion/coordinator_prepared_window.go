package completion

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/repository"
)

func (c *coordinator) CommitPreparedWindow(ctx context.Context, cmd PreparedWindowCommitCommand) (string, error) {
	if err := cmd.Fence.Validate(); err != nil {
		return "", fmt.Errorf("completion prepared-window fence: %w", err)
	}
	if cmd.JobID == "" || cmd.Identity == "" || cmd.AssetID == "" || cmd.SHA256 == "" || cmd.ExtradataSHA256 == "" || cmd.SizeBytes <= 0 {
		return "", fmt.Errorf("completion prepared-window evidence is incomplete")
	}
	commitID := "prepared_" + cmd.Fence.TaskID + "_" + cmd.Fence.AttemptID
	_, tokenHash, err := generateDeterministicCommitToken(c, commitID, cmd.Fence)
	if err != nil {
		return "", fmt.Errorf("prepared-window completion token: %w", err)
	}
	var committedID string
	err = c.store.Run(ctx, func(tx repository.CompletionTx) error {
		var err error
		committedID, err = tx.CommitPreparedWindow(ctx, repository.PreparedWindowCommitParams{
			Fence: completionFence(cmd.Fence), JobID: cmd.JobID, Identity: cmd.Identity,
			AssetID: cmd.AssetID, SHA256: cmd.SHA256, ExtradataSHA256: cmd.ExtradataSHA256,
			SizeBytes: cmd.SizeBytes, TokenHash: tokenHash, Now: time.Now().UTC().Format(time.RFC3339Nano),
		})
		return err
	})
	if err != nil {
		return "", mapStoreCompletionError(err)
	}
	return committedID, nil
}
