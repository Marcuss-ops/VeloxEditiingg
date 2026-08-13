package completion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/artifacts"
	"velox-server/internal/repository"
	"velox-server/internal/telemetry"
	"velox-shared/contract/domain"

	"go.opentelemetry.io/otel/attribute"
)

func (c *coordinator) CompleteUpload(ctx context.Context, cmd CompleteUploadCommand) error {
	ctx, span := telemetry.StartSpan(ctx, "complete_upload",
		attribute.String("velox.upload_id", cmd.UploadID),
		attribute.String("velox.task_id", cmd.Fence.TaskID),
	)
	defer span.End()

	if err := cmd.Fence.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if cmd.UploadID == "" {
		return fmt.Errorf("completion.CompleteUpload: UploadID empty (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}
	now := time.Now().UTC()
	return c.store.Run(ctx, func(tx repository.CompletionTx) error {
		state, err := tx.ReadCompletionFence(ctx, completionFence(cmd.Fence), false)
		if err != nil {
			return mapStoreCompletionError(err)
		}
		upload, err := tx.GetCompletionUploadState(ctx, cmd.UploadID)
		if err != nil {
			return mapStoreCompletionError(err)
		}
		if upload.Status == "COMPLETED" {
			c.recordAttemptCommitsCAS("upload:"+cmd.UploadID, nil)
			return nil
		}
		expected := upload.ExpectedSHA256
		if upload.ReceivedSHA256 != "" {
			expected = upload.ReceivedSHA256
		}
		if cmd.WorkerSHA256 != "" && expected != "" && cmd.WorkerSHA256 != expected {
			return fmt.Errorf("%w: %w: upload=%s worker_sha=%s master_declared=%s", ErrStaleReport, domain.NewStaleReport(nil), cmd.UploadID, cmd.WorkerSHA256, expected)
		}
		if cmd.ServerSHA256 != "" && expected != "" && cmd.ServerSHA256 != expected {
			return fmt.Errorf("%w: %w: upload=%s server_sha=%s master_declared=%s", ErrStaleReport, domain.NewStaleReport(nil), cmd.UploadID, cmd.ServerSHA256, expected)
		}
		verdict := repository.CompletionKeepVerifying
		if cmd.ServerSHA256 != "" && (expected == "" || cmd.ServerSHA256 == expected) {
			verdict = repository.CompletionReady
		}
		nowStr := now.Format(time.RFC3339Nano)
		if err := tx.CompleteCompletionUpload(ctx, verdict, cmd.UploadID, cmd.ServerSHA256, nowStr); err != nil {
			return fmt.Errorf("completion.CompleteUpload: artifact CAS: %w", err)
		}
		if verdict == repository.CompletionReady && c.blobStore != nil && upload.TemporaryStorageKey != "" {
			ext := ".bin"
			if exts, e := mime.ExtensionsByType(upload.MimeType); e == nil && len(exts) > 0 {
				ext = exts[0]
			}
			storageKey, e := artifacts.PromoteToCanonical(c.blobStore, upload.TemporaryStorageKey, cmd.ServerSHA256, ext)
			if e != nil {
				return fmt.Errorf("completion.CompleteUpload: promote artifact: %w", e)
			}
			if e = tx.StampCompletionArtifact(ctx, upload.ArtifactID, storageKey, cmd.ServerSHA256, upload.SizeBytes); e != nil {
				if !errors.Is(e, repository.ErrCompletionCanonicalConflict) {
					return fmt.Errorf("completion.CompleteUpload: stamp canonical artifact: %w", e)
				}
				altKey := duplicateCanonicalKey(storageKey, upload.ArtifactID)
				if e := materializeCanonicalDuplicate(c.blobStore, storageKey, altKey); e != nil {
					return fmt.Errorf("completion.CompleteUpload: materialize duplicate canonical artifact: %w", e)
				}
				if e := tx.StampCompletionArtifact(ctx, upload.ArtifactID, altKey, cmd.ServerSHA256, upload.SizeBytes); e != nil {
					return fmt.Errorf("completion.CompleteUpload: stamp duplicate canonical artifact: %w", e)
				}
			}
		}
		if err := tx.UpdateCompletionReadyCount(ctx, completionFence(cmd.Fence), nowStr); err != nil {
			mapped := mapStoreCompletionError(err)
			if budgetErr := c.recordAttemptCommitsCAS("commit:"+state.CommitID, mapped); budgetErr != nil {
				return fmt.Errorf("completion.CompleteUpload: ready_output_count bump: %w", budgetErr)
			}
			return fmt.Errorf("completion.CompleteUpload: ready_output_count bump: %w", mapped)
		}
		if err := tx.ExpireCompletionAttempt(ctx, completionFence(cmd.Fence), nowStr); err != nil {
			mapped := mapStoreCompletionError(err)
			if budgetErr := c.recordAttemptCommitsCAS("commit:"+state.CommitID, mapped); budgetErr != nil {
				return fmt.Errorf("completion.CompleteUpload: deadline-breach EXPIRED: %w", budgetErr)
			}
			return fmt.Errorf("completion.CompleteUpload: deadline-breach EXPIRED: %w", mapped)
		}
		return nil
	})
}

func duplicateCanonicalKey(storageKey, artifactID string) string {
	ext := filepath.Ext(storageKey)
	return strings.TrimSuffix(storageKey, ext) + ".dup-" + artifactID + ext
}

func materializeCanonicalDuplicate(blobStore interface{ FinalDir() string }, sourceKey, targetKey string) error {
	if blobStore == nil {
		return fmt.Errorf("blob store unavailable")
	}
	source := filepath.Join(blobStore.FinalDir(), filepath.FromSlash(sourceKey))
	target := filepath.Join(blobStore.FinalDir(), filepath.FromSlash(targetKey))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(target); err == nil {
		return nil
	}
	if err := os.Link(source, target); err == nil {
		// The hardlink shares the already-durable source inode, but the NEW
		// directory entry is metadata that must survive a crash too: the
		// completion row will reference targetKey. fsync the parent dir
		// before declaring success.
		syncDirectory(filepath.Dir(target))
		return nil
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(target)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(target)
		return err
	}
	// Sync before Close so "success" means the duplicate bytes are durable,
	// not merely resident in the page cache. The primary canonical is already
	// durable (PromoteToCanonical); a crash between here and the DB stamp
	// must not leave a corrupt/empty duplicate referenced by the row.
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(target)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(target)
		return err
	}
	// fsync the directory entry for the copy fallback as well.
	syncDirectory(filepath.Dir(target))
	return nil
}

// syncDirectory best-effort fsyncs a directory so a link/copy that precedes
// a DB commit survives a crash (POSIX; no-op elsewhere).
func syncDirectory(path string) {
	if dir, err := os.Open(path); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
}
