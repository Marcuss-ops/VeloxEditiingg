package completion

import (
	"context"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/repository"
)

func (c *coordinator) CommitSourceCertificate(ctx context.Context, cmd SourceCertificateCommitCommand) (string, error) {
	if err := cmd.Fence.Validate(); err != nil {
		return "", fmt.Errorf("completion source-certificate fence: %w", err)
	}
	if strings.TrimSpace(cmd.JobID) == "" || strings.TrimSpace(cmd.CertificateJSON) == "" {
		return "", fmt.Errorf("completion source-certificate evidence is incomplete")
	}
	commitID := "sourcecert_" + cmd.Fence.TaskID + "_" + cmd.Fence.AttemptID
	_, tokenHash, err := generateDeterministicCommitToken(c, commitID, cmd.Fence)
	if err != nil {
		return "", fmt.Errorf("source-certificate completion token: %w", err)
	}
	var committedID string
	err = c.store.Run(ctx, func(tx repository.CompletionTx) error {
		var err error
		committedID, err = tx.CommitSourceCertificate(ctx, repository.SourceCertificateCommitParams{
			Fence: completionFence(cmd.Fence), JobID: cmd.JobID,
			CertificateJSON: cmd.CertificateJSON, TokenHash: tokenHash,
			Now: time.Now().UTC().Format(time.RFC3339Nano),
		})
		return err
	})
	if err != nil {
		return "", mapStoreCompletionError(err)
	}
	return committedID, nil
}
