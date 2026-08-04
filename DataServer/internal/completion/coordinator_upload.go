// Package completion / coordinator_upload.go
//
// CompleteUpload — the upload-time completion path. One
// LevelSerializable tx: verifies the worker-supplied SHA against the
// master-declared expected_sha256, flips artifact_uploads →
// COMPLETED + artifacts STAGING/VERIFYING → READY|VERIFYING, and
// bumps attempt_commits.ready_output_count via a deterministic
// derived count. Zero raw SQL — all writes go through the
// UnitOfWork repositories.
package completion

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/artifacts"
)

// ────────────────────────────────────────────────────────────────────────
// CompleteUpload — UNITOFWORK-DRIVEN. Zero raw SQL.
// ────────────────────────────────────────────────────────────────────────

// CompleteUpload verifies the worker-supplied SHA against the master-
// declared expected_sha256 on artifact_uploads, flips artifact_uploads
// → COMPLETED + artifacts STAGING/VERIFYING → READY|VERIFYING in one
// tx, and bumps attempt_commits.ready_output_count via a
// deterministic derived count.
//
// Returns nil on success; ErrTransitionConflict on stale fence;
// ErrStaleReport on attempted promotion from COMMITTED|FAILED|CANCELLED
// or on a server-vs-declarative SHA mismatch (Branch D, with tx
// rollback). All per-table writes are dispatched to AttemptCommitRepository.
func (c *coordinator) CompleteUpload(ctx context.Context, cmd CompleteUploadCommand) error {
	if err := cmd.Fence.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if cmd.UploadID == "" {
		return fmt.Errorf("completion.CompleteUpload: UploadID empty (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("completion.CompleteUpload: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	state, err := cmd.Fence.Read(ctx, tx)
	if err != nil {
		return err
	}

	repos := c.uowFactory.WithTx(tx)

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	// 1. artifact_uploads read for the four-branch gate.
	uploadState, err := repos.AttemptCommits().GetArtifactUploadState(ctx, cmd.UploadID)
	if err != nil {
		return err
	}
	if uploadState.Status == "COMPLETED" {
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): replay-safe
		// no-op is a successful exit — reset the per-commit
		// budget counter so a previous streak on THIS commit
		// does not poison the next attempt. The key uses the
		// upload_id as a stable per-replay key. (Independent
		// replays of the same upload_id share a streak by
		// design.)
		c.recordAttemptCommitsCAS("upload:"+cmd.UploadID, nil)
		return nil // replay-safe no-op
	}
	effectiveExpected := uploadState.ExpectedSHA256
	if uploadState.ReceivedSHA256 != "" {
		effectiveExpected = uploadState.ReceivedSHA256
	}

	// Worker fabrication early-reject: the worker's local SHA must
	// match the canonical expected_sha256 declared earlier in
	// DeclareOutputs. This protects against a worker that
	// post-Declare rewrites its claimed hash to anything different
	// (e.g., trying to align with a forged file). The ServerSHA256
	// gate below is independent and authoritative for STAGING->READY.
	if cmd.WorkerSHA256 != "" && effectiveExpected != "" && cmd.WorkerSHA256 != effectiveExpected {
		return fmt.Errorf("%w: upload=%s worker_sha=%s master_declared=%s",
			ErrStaleReport, cmd.UploadID, cmd.WorkerSHA256, effectiveExpected)
	}

	// Verdetto P0 #5 — authoritative SHA gate.
	//
	// Four branches determined by ServerSHA256 + effectiveExpected:
	//
	//   A. ServerSHA="" AND effectiveExpected="" — no canonical reference
	//      on either side. Bytes transferred but neither side has a
	//      hash. Stay at VERIFYING.
	//   B. ServerSHA="" AND effectiveExpected!="" — declarative SHA
	//      present, master hasn't verified. Stay at VERIFYING.
	//   C. ServerSHA matches effectiveExpected (or ServerSHA!="" with no
	//      canonical reference) — master agrees with declarative.
	//      Promote artifact STAGING/VERIFYING → READY.
	//   D. ServerSHA!="" AND differs from effectiveExpected — reject
	//      with ErrStaleReport; tx rolls back.
	serverMatches := cmd.ServerSHA256 == "" || effectiveExpected == "" || cmd.ServerSHA256 == effectiveExpected
	if !serverMatches {
		return fmt.Errorf("%w: upload=%s server_sha=%s master_declared=%s",
			ErrStaleReport, cmd.UploadID, cmd.ServerSHA256, effectiveExpected)
	}

	verdict := ArtifactKeepVerifying
	if cmd.ServerSHA256 != "" && (effectiveExpected == "" || cmd.ServerSHA256 == effectiveExpected) {
		verdict = ArtifactReady
	}

	if err := repos.AttemptCommits().CompleteArtifactUpload(ctx, verdict, cmd.UploadID, cmd.ServerSHA256, nowStr); err != nil {
		return fmt.Errorf("completion.CompleteUpload: artifact CAS: %w", err)
	}

	// The typed protocol receives bytes into the artifact staging key rather
	// than through artifacts.Service.Finalize (which is the legacy single-
	// output job-success path). Promote the verified file here, before the
	// commit transaction becomes visible, and stamp the canonical pointer so
	// DeliveryRunner can read the exact bytes after CommitAttempt.
	if verdict == ArtifactReady && c.blobStore != nil && uploadState.TemporaryStorageKey != "" {
		ext := ".bin"
		if exts, merr := mime.ExtensionsByType(uploadState.MimeType); merr == nil && len(exts) > 0 && exts[0] != "" {
			ext = exts[0]
		}
		storageKey, perr := artifacts.PromoteToCanonical(c.blobStore, uploadState.TemporaryStorageKey, cmd.ServerSHA256, ext)
		if perr != nil {
			return fmt.Errorf("completion.CompleteUpload: promote artifact: %w", perr)
		}
		if _, uerr := tx.ExecContext(ctx, `
			UPDATE artifacts
			   SET storage_provider = 'local', storage_key = ?, sha256 = ?, size_bytes = ?
			 WHERE id = ?`, storageKey, cmd.ServerSHA256, uploadState.SizeBytes, uploadState.ArtifactID); uerr != nil {
			// Canonical blobs are content-addressed, so identical outputs can
			// legitimately race for the same physical key. The artifact row
			// still needs its own manifest key; materialize a hardlink/copy
			// and retry the row update rather than leaving the commit blocked.
			if !strings.Contains(uerr.Error(), "UNIQUE constraint failed: artifacts.storage_provider, artifacts.storage_key") {
				return fmt.Errorf("completion.CompleteUpload: stamp canonical artifact: %w", uerr)
			}
			altKey := duplicateCanonicalKey(storageKey, uploadState.ArtifactID)
			if err := materializeCanonicalDuplicate(c.blobStore, storageKey, altKey); err != nil {
				return fmt.Errorf("completion.CompleteUpload: materialize duplicate artifact: %w", err)
			}
			if _, retryErr := tx.ExecContext(ctx, `
				UPDATE artifacts
				   SET storage_provider = 'local', storage_key = ?, sha256 = ?, size_bytes = ?
				 WHERE id = ?`, altKey, cmd.ServerSHA256, uploadState.SizeBytes, uploadState.ArtifactID); retryErr != nil {
				return fmt.Errorf("completion.CompleteUpload: stamp duplicate canonical artifact: %w", retryErr)
			}
		}
	}

	if err := repos.AttemptCommits().UpdateReadyCountExhaustive(ctx, cmd.Fence, nowStr); err != nil {
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): route through
		// the conflict budget under the per-commit key so concurrent
		// independent commits don't aggregate into one streak.
		// Under threshold → propagate the original ErrTransitionConflict
		// unchanged. Over threshold → propagate
		// ErrConflictBudgetExhausted so the caller can escalate.
		if budgetErr := c.recordAttemptCommitsCAS("commit:"+state.CommitID, err); budgetErr != nil {
			return fmt.Errorf("completion.CompleteUpload: ready_output_count bump: %w", budgetErr)
		}
		return fmt.Errorf("completion.CompleteUpload: ready_output_count bump: %w", err)
	}

	if err := repos.AttemptCommits().SetExpired(ctx, cmd.Fence, nowStr); err != nil {
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): same pattern
		// as UpdateReadyCountExhaustive — both are canonical
		// attempt_commits CAS paths that count toward the budget.
		// Per-key: this commit's streak is independent of any
		// other in-flight commit's conflicts.
		if budgetErr := c.recordAttemptCommitsCAS("commit:"+state.CommitID, err); budgetErr != nil {
			return fmt.Errorf("completion.CompleteUpload: deadline-breach EXPIRED: %w", budgetErr)
		}
		return fmt.Errorf("completion.CompleteUpload: deadline-breach EXPIRED: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("completion.CompleteUpload: commit: %w", err)
	}
	committed = true
	// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): reset the
	// per-commit conflict budget on a successful CompleteUpload
	// so a fresh streak starts next time for THIS commit only.
	c.recordAttemptCommitsCAS("commit:"+state.CommitID, nil)
	return nil
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
	return out.Close()
}
