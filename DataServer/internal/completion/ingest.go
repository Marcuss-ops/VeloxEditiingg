package completion

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/repository"
)

const commitGraceDefault = 2 * time.Minute

func (c *coordinator) DeclareOutputs(ctx context.Context, cmd DeclareOutputsCommand) (*UploadPlan, error) {
	if err := cmd.Fence.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if len(cmd.OutputManifests) == 0 {
		return nil, fmt.Errorf("completion.DeclareOutputs: at least one OutputManifest required (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}
	candidate, err := newUUIDLowerHex()
	if err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: mint commit_id: %w", err)
	}
	now := time.Now().UTC()
	var plan *UploadPlan
	err = c.store.Run(ctx, func(tx repository.CompletionTx) error {
		state, err := tx.ReadCompletionFence(ctx, completionFence(cmd.Fence), true)
		if err != nil {
			return mapStoreCompletionError(err)
		}
		commitID := candidate
		if state != nil {
			commitID = state.CommitID
		}
		token, tokenHash, err := generateDeterministicCommitToken(c, commitID, cmd.Fence)
		if err != nil {
			return fmt.Errorf("completion.DeclareOutputs: derive commit_token: %w", err)
		}
		if state == nil {
			canonical, err := tx.InsertCompletionAttempt(ctx, repository.CompletionDeclareParams{
				CommitID: commitID, TaskID: cmd.Fence.TaskID, AttemptID: cmd.Fence.AttemptID,
				JobID: cmd.JobID, WorkerID: cmd.Fence.WorkerID.String(), LeaseID: cmd.Fence.LeaseID,
				Revision: cmd.Fence.Revision, RequiredOutputCount: len(cmd.OutputManifests),
				TokenHash: tokenHash, Deadline: now.Add(commitGraceDefault).Format(time.RFC3339Nano), Now: now.Format(time.RFC3339Nano),
			})
			if err != nil {
				return err
			}
			if canonical != commitID {
				commitID = canonical
				token, _, err = generateDeterministicCommitToken(c, commitID, cmd.Fence)
				if err != nil {
					return fmt.Errorf("completion.DeclareOutputs: re-derive commit_token after race: %w", err)
				}
			}
		}
		targets := make([]UploadTarget, 0, len(cmd.OutputManifests))
		for _, manifest := range cmd.OutputManifests {
			if err := validateManifest(&manifest); err != nil {
				return fmt.Errorf("completion.DeclareOutputs: invalid manifest: %w", err)
			}
			declarationID, err := newUUIDLowerHex()
			if err != nil {
				return fmt.Errorf("completion.DeclareOutputs: mint declaration_id: %w", err)
			}
			if err := tx.InsertCompletionDeclaration(ctx, repository.CompletionDeclarationParams{
				DeclarationID: declarationID, CommitID: commitID, TaskID: cmd.Fence.TaskID,
				AttemptID: cmd.Fence.AttemptID, OutputKind: manifest.OutputKind, LogicalName: manifest.LogicalName,
				MimeType: manifest.MimeType, SizeBytes: manifest.SizeBytes, SHA256: manifest.SHA256,
				WorkerSpoolKey: manifest.WorkerSpoolKey, Now: now.Format(time.RFC3339Nano),
			}); err != nil {
				return fmt.Errorf("completion.DeclareOutputs: insert declaration %s: %w", manifest.LogicalName, err)
			}
			resolved, err := tx.GetCompletionDeclarationID(ctx, commitID, manifest.OutputKind, manifest.LogicalName)
			if err != nil {
				return fmt.Errorf("completion.DeclareOutputs: resolve declaration %s: %w", manifest.LogicalName, err)
			}
			targets = append(targets, UploadTarget{DeclarationID: resolved})
		}
		plan = &UploadPlan{CommitID: commitID, CommitToken: token, Targets: targets}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (c *coordinator) RecordUploadProgress(ctx context.Context, cmd RecordUploadProgressCommand) error {
	if err := cmd.Fence.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if cmd.UploadID == "" {
		return fmt.Errorf("completion.RecordUploadProgress: UploadID empty (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}
	now := time.Now().UTC()
	return c.store.Run(ctx, func(tx repository.CompletionTx) error {
		state, err := tx.ReadCompletionFence(ctx, completionFence(cmd.Fence), false)
		if err != nil {
			return mapStoreCompletionError(err)
		}
		nowStr := now.Format(time.RFC3339Nano)
		n, err := tx.UpdateCompletionProgress(ctx, state.CommitID, nowStr, now.Add(commitGraceDefault).Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: status=%q (cannot progress past terminal/rejected state)", ErrTransitionConflict, state.Status)
		}
		if cmd.UploadedBytes > 0 {
			if err := tx.UpdateCompletionUploadedBytes(ctx, completionFence(cmd.Fence), cmd.UploadID, cmd.UploadedBytes, nowStr); err != nil {
				return err
			}
		}
		return nil
	})
}

func completionFence(f FenceTuple) repository.CompletionFence {
	return repository.CompletionFence{TaskID: f.TaskID, AttemptID: f.AttemptID, WorkerID: f.WorkerID.String(), LeaseID: f.LeaseID, Revision: f.Revision}
}

func newUUIDLowerHex() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("completion: entropy failure for UUID (crypto/rand): %w", err)
	}
	const digits = "0123456789abcdef"
	out := make([]byte, 32)
	for i, v := range b {
		out[i*2], out[i*2+1] = digits[v>>4], digits[v&0x0f]
	}
	return string(out), nil
}

func generateDeterministicCommitToken(c *coordinator, commitID string, fence FenceTuple) (string, string, error) {
	if len(c.hmacKey) < commitTokenByteLen {
		return "", "", fmt.Errorf("completion: commit HMAC key not configured (must be >= 32 bytes)")
	}
	mac := hmac.New(sha256.New, c.hmacKey)
	if _, err := fmt.Fprintf(mac, "v1|%s|%s|%s|%s|%s|%d", commitID, fence.TaskID, fence.AttemptID, fence.WorkerID, fence.LeaseID, fence.Revision); err != nil {
		return "", "", err
	}
	sum := mac.Sum(nil)
	token := hex.EncodeToString(sum)
	hash := sha256.Sum256(sum)
	return token, hex.EncodeToString(hash[:]), nil
}

func validateManifest(m *OutputManifest) error {
	if strings.TrimSpace(m.OutputKind) == "" {
		return fmt.Errorf("manifest: OutputKind empty")
	}
	if strings.TrimSpace(m.LogicalName) == "" {
		return fmt.Errorf("manifest: LogicalName empty")
	}
	if strings.TrimSpace(m.MimeType) == "" {
		return fmt.Errorf("manifest: MimeType empty")
	}
	if m.SizeBytes <= 0 {
		return fmt.Errorf("manifest: SizeBytes must be > 0 (got %d)", m.SizeBytes)
	}
	if len(m.SHA256) != 64 {
		return fmt.Errorf("manifest: SHA256 must be 64 hex chars (got %d chars)", len(m.SHA256))
	}
	for _, ch := range m.SHA256 {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return fmt.Errorf("manifest: SHA256 must be lowercase hex")
		}
	}
	return nil
}
