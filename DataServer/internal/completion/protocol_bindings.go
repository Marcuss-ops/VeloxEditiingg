package completion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

func (c *coordinator) ListUploadBindings(ctx context.Context, commitID string) ([]UploadBinding, error) {
	if strings.TrimSpace(commitID) == "" {
		return nil, fmt.Errorf("completion.ListUploadBindings: commitID empty")
	}
	rows, err := c.store.ListCompletionUploadBindings(ctx, commitID)
	if err != nil {
		return nil, err
	}
	out := make([]UploadBinding, 0, len(rows))
	for _, row := range rows {
		out = append(out, UploadBinding{DeclarationID: row.DeclarationID, CommitID: row.CommitID, UploadID: row.UploadID, ArtifactID: row.ArtifactID, TaskID: row.TaskID, AttemptID: row.AttemptID, WorkerID: row.WorkerID, LeaseID: row.LeaseID, Revision: row.Revision, OutputKind: row.OutputKind, LogicalName: row.LogicalName})
	}
	return out, nil
}

func (c *coordinator) GetUploadBinding(ctx context.Context, uploadID string) (*UploadBinding, error) {
	if strings.TrimSpace(uploadID) == "" {
		return nil, fmt.Errorf("completion.GetUploadBinding: uploadID empty")
	}
	row, err := c.store.GetCompletionUploadBinding(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	return &UploadBinding{DeclarationID: row.DeclarationID, CommitID: row.CommitID, UploadID: row.UploadID, ArtifactID: row.ArtifactID, TaskID: row.TaskID, AttemptID: row.AttemptID, WorkerID: row.WorkerID, LeaseID: row.LeaseID, Revision: row.Revision, OutputKind: row.OutputKind, LogicalName: row.LogicalName}, nil
}

func (c *coordinator) BindUpload(ctx context.Context, declarationID, uploadID, artifactID string) error {
	if declarationID == "" || uploadID == "" || artifactID == "" {
		return fmt.Errorf("completion.BindUpload: declaration, upload and artifact IDs are required")
	}
	return c.store.BindCompletionUpload(ctx, declarationID, uploadID, artifactID)
}

func (c *coordinator) VerifyUploadToken(ctx context.Context, uploadID, token string) error {
	binding, err := c.GetUploadBinding(ctx, uploadID)
	if err != nil {
		return err
	}
	derived, _, err := generateDeterministicCommitToken(c, binding.CommitID, FenceTuple{TaskID: binding.TaskID, AttemptID: binding.AttemptID, WorkerID: binding.WorkerID, LeaseID: binding.LeaseID, Revision: binding.Revision})
	if err != nil {
		return err
	}
	provided, err := hex.DecodeString(token)
	if err != nil {
		return fmt.Errorf("completion.VerifyUploadToken: invalid token encoding")
	}
	providedHash := sha256.Sum256(provided)
	stored, err := c.store.GetCompletionCommitTokenHash(ctx, binding.CommitID)
	if err != nil {
		return err
	}
	derivedBytes, err := hex.DecodeString(derived)
	if err != nil {
		return fmt.Errorf("completion.VerifyUploadToken: derived token encoding: %w", err)
	}
	derivedHash := sha256.Sum256(derivedBytes)
	if !subtleConstantTimeHexEqual(stored, hex.EncodeToString(providedHash[:])) || !subtleConstantTimeHexEqual(stored, hex.EncodeToString(derivedHash[:])) {
		return fmt.Errorf("completion.VerifyUploadToken: invalid token")
	}
	return nil
}

func subtleConstantTimeHexEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

var _ UploadProtocolStore = (*coordinator)(nil)
