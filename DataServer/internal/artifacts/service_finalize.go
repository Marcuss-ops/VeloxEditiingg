package artifacts

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"velox-server/internal/store"
)

// =====================================================================
// FASE 3 + 4: Finalize (orchestrates CAS RECEIVED->FINALIZING + atomic SUCCEEDED via finRepo)
// =====================================================================

// Finalize orchestrates Fase 3 + Fase 4 of the spec:
//
//  1. Verify the upload session exists + is in a FINISHEABLE state.
//     If it's already COMPLETED (idempotent retry), auth-match success
//     returns the post-Finalize artifact as a no-op (spec: "doppia
//     finalizzazione" → already-completed).
//  2. Detect MIME from the staged blob (best-effort, falls back to the
//     BeginUpload-declared mime).
//  3. Promote the blob to its canonical storage_key (idempotent on
//     retry because the key is content-addressable).
//  4. CAS RECEIVED → FINALIZING on artifact_uploads (serializes
//     concurrent Finalize callers at the SQL layer).
//  5. Hand off to FinalizationRepository.FinalizeVerified — the
//     SINGLE ATOMIC SQL transaction that flips jobs RUNNING →
//     SUCCEEDED + artifacts STAGING → READY + job_attempts →
//     SUCCEEDED + outbox events + delivery + FINALIZING → COMPLETED.
//
// The blob promotion runs BEFORE the SQL tx. If the SQL tx rolls back,
// the spec's reconciler (chunk 5) deletes the orphan blob. This is
// intentional — the spec calls out that "un blob orfano eliminabile è
// preferibile rispetto a (artifact READY con file inesistente)".
func (s *Service) Finalize(ctx context.Context, cmd FinalizeArtifactCommand) (*store.Artifact, error) {
	if cmd.UploadID == "" {
		return nil, fmt.Errorf("artifacts: Finalize: empty uploadID")
	}

	session, err := s.repo.GetUploadSession(ctx, cmd.UploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if session == nil {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, cmd.UploadID)
	}
	if session.JobID != cmd.JobID {
		return nil, fmt.Errorf("%w: session_job=%s cmd_job=%s",
			ErrTransitionConflict, session.JobID, cmd.JobID)
	}

	// ----- idempotent COMPLETED path (spec: "doppia finalizzazione") -----
	// If the previous tx already committed, the session row is COMPLETED;
	// we re-load the post-tx artifact and return it when auth fields
	// match. Different worker/lease/revision still error (a duplicate
	// retry from a *different* worker must NOT silently succeed).
	if session.Status == "COMPLETED" {
		if session.WorkerID != cmd.WorkerID {
			return nil, fmt.Errorf("%w: completed upload=%s worker=%s->%s",
				ErrTransitionConflict, cmd.UploadID, session.WorkerID, cmd.WorkerID)
		}
		if session.LeaseID != cmd.LeaseID {
			return nil, fmt.Errorf("%w: completed upload=%s lease_mismatch",
				ErrTransitionConflict, cmd.UploadID)
		}
		if session.ExpectedRevision != 0 &&
			session.ExpectedRevision != cmd.ExpectedRevision {
			return nil, fmt.Errorf("%w: completed upload=%s revision_mismatch",
				ErrTransitionConflict, cmd.UploadID)
		}
		if cmd.AttemptNumber != 0 && session.AttemptNumber != cmd.AttemptNumber {
			return nil, fmt.Errorf("%w: completed upload=%s attempt=%d->%d",
				ErrAttemptMismatch, cmd.UploadID, session.AttemptNumber, cmd.AttemptNumber)
		}
		art, lerr := loadArtifactByID(ctx, s.db, session.ArtifactID)
		if lerr != nil {
			return nil, lerr
		}
		if art == nil {
			return nil, fmt.Errorf("%w: completed upload=%s but artifact missing",
				ErrTransitionConflict, cmd.UploadID)
		}
		return art, nil
	}

	if session.Status != "RECEIVED" && session.Status != "FINALIZING" {
		return nil, fmt.Errorf("%w: upload_id=%s status=%s",
			ErrUploadStateInvalid, cmd.UploadID, session.Status)
	}
	if session.WorkerID != cmd.WorkerID {
		return nil, fmt.Errorf("%w: upload=%s expected_worker=%s got=%s",
			ErrWrongJobOwner, cmd.UploadID, session.WorkerID, cmd.WorkerID)
	}
	if session.LeaseID != cmd.LeaseID {
		return nil, fmt.Errorf("%w: upload=%s lease_mismatch", ErrLeaseInvalid, cmd.UploadID)
	}
	if session.ExpectedRevision != 0 && session.ExpectedRevision != cmd.ExpectedRevision {
		return nil, fmt.Errorf("%w: upload=%s expected_revision=%d got=%d",
			ErrRevisionMismatch, cmd.UploadID, session.ExpectedRevision, cmd.ExpectedRevision)
	}
	if cmd.AttemptNumber != 0 && session.AttemptNumber != cmd.AttemptNumber {
		return nil, fmt.Errorf("%w: upload=%s expected_attempt=%d got=%d",
			ErrAttemptMismatch, cmd.UploadID, session.AttemptNumber, cmd.AttemptNumber)
	}

	receivedSHA := session.ReceivedSHA256
	receivedSize := session.ReceivedSizeBytes
	if receivedSHA == "" || receivedSize == 0 {
		return nil, fmt.Errorf("%w: upload=%s has empty received snapshot (Receive() not completed?)",
			ErrUploadStateInvalid, cmd.UploadID)
	}

	mimeType := detectMIME(session.TemporaryStorageKey)
	if mimeType == "" || mimeType == "application/octet-stream" {
		mimeType = session.ExpectedMIME
	}
	if mimeType == "" {
		mimeType = http.DetectContentType(nil) // returns "application/octet-stream"
	}

	// Promote BEFORE the SQL tx. If the SQL tx rolls back, the orphan
	// blob is reclaimed by the reconciler (chunk 5).
	storageKey, err := PromoteToCanonical(s.blobStore, session.TemporaryStorageKey, receivedSHA, mimeToExt(mimeType))
	if err != nil {
		return nil, err
	}

	// CAS-gate the RECEIVED → FINALIZING transition. This serializes
	// concurrent Finalize callers at the SQL layer — only the first
	// writer successfully flips status; the loser gets ErrTransitionConflict
	// and the caller retries. After the winner's tx commits + flips
	// FINALIZING → COMPLETED (in-tx, see FinalizeVerified step 7),
	// the loser's retry hits the idempotent COMPLETED short-circuit
	// path above.
	//
	// For status FINALIZING (a peer is mid-flight) we DO NOT skip the
	// CAS — we explicitly reject with ErrTransitionConflict so the
	// caller retries. Without this gate the loser would fall through
	// to PromoteToCanonical and run a SECOND os.Rename to the same
	// canonical path, racing the winner's first promote.
	switch session.Status {
	case string(store.UploadReceived):
		if err := s.repo.TransitionUploadStatus(ctx, cmd.UploadID, string(store.UploadReceived), string(store.UploadFinalizing)); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTransitionConflict, translateStoreErr(err))
		}
	case string(store.UploadFinalizing):
		return nil, fmt.Errorf("%w: upload=%s peer-mid-flight",
			ErrTransitionConflict, cmd.UploadID)
	default:
		// Defence-in-depth: any other status (CREATED, UPLOADING,
		// EXPIRED, FAILED) cannot enter the finalization critical
		// section. The legacy guard above already catches these, but a
		// future refactor without it must not silently fall through.
		return nil, fmt.Errorf("%w: upload=%s status=%s unexpected",
			ErrUploadStateInvalid, cmd.UploadID, session.Status)
	}

	// PR 3.5-a: delegate to the single atomic SUCCEEDED write on
	// finRepo. FinalizeVerified expects:
	//   - upload_id in FINALIZING state (we just flipped it via CAS)
	//   - worker_id + lease_id matching the session
	//   - revision matching the job's expected revision
	// It performs: jobs CAS → SUCCEEDED, artifacts CAS → READY, attempts
	// CAS → SUCCEEDED, outbox events, delivery idempotent creation, and
	// FINALIZING → COMPLETED on artifact_uploads — all in one tx.
	out, err := s.finRepo.FinalizeVerified(ctx, FinalizeVerifiedCommand{
		UploadID:         cmd.UploadID,
		ArtifactID:       session.ArtifactID,
		JobID:            cmd.JobID,
		WorkerID:         cmd.WorkerID,
		LeaseID:          cmd.LeaseID,
		AttemptNumber:    session.AttemptNumber,
		ExpectedRevision: session.ExpectedRevision,

		StorageProvider: "local",
		StorageKey:      storageKey,
		SHA256:          receivedSHA,
		SizeBytes:       receivedSize,
		MIMEType:        mimeType,

		VerifiedAt: s.clock.Now(),
	})
	if err != nil {
		if isArtifactStorageKeyConflict(err) {
			altStorageKey, keyErr := makeDuplicateStorageKey(storageKey, session.ArtifactID)
			if keyErr != nil {
				return nil, err
			}
			if dupErr := s.materializeDuplicateFinalBlob(storageKey, altStorageKey); dupErr != nil {
				return nil, fmt.Errorf("artifacts: duplicate final blob fallback: %w", dupErr)
			}
			out, err = s.finRepo.FinalizeVerified(ctx, FinalizeVerifiedCommand{
				UploadID:         cmd.UploadID,
				ArtifactID:       session.ArtifactID,
				JobID:            cmd.JobID,
				WorkerID:         cmd.WorkerID,
				LeaseID:          cmd.LeaseID,
				AttemptNumber:    session.AttemptNumber,
				ExpectedRevision: session.ExpectedRevision,

				StorageProvider: "local",
				StorageKey:      altStorageKey,
				SHA256:          receivedSHA,
				SizeBytes:       receivedSize,
				MIMEType:        mimeType,

				VerifiedAt: s.clock.Now(),
			})
		}
		if err != nil {
			return nil, err
		}
	}

	// NOTE: the FINALIZING → COMPLETED flip happens INSIDE
	// FinalizeVerified's *sql.Tx (step 7 there). Doing it inside the tx
	// avoids a liveness bug where a process crash between tx-commit and
	// a separate post-commit UPDATE would leave the upload row stuck in
	// FINALIZING forever, blocking retries even though the underlying
	// jobs/artifacts/attempts are already SUCCEEDED.
	return out, nil
}

func isArtifactStorageKeyConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed: artifacts.storage_provider, artifacts.storage_key")
}

func makeDuplicateStorageKey(storageKey, artifactID string) (string, error) {
	storageKey = strings.TrimSpace(storageKey)
	artifactID = strings.TrimSpace(artifactID)
	if storageKey == "" {
		return "", fmt.Errorf("artifacts: duplicate storage key fallback requires storage key")
	}
	if artifactID == "" {
		return "", fmt.Errorf("artifacts: duplicate storage key fallback requires artifact id")
	}
	ext := filepath.Ext(storageKey)
	base := strings.TrimSuffix(storageKey, ext)
	return base + ".dup-" + artifactID + ext, nil
}

func (s *Service) materializeDuplicateFinalBlob(sourceStorageKey, targetStorageKey string) error {
	if s == nil || s.blobStore == nil {
		return fmt.Errorf("blob store unavailable")
	}
	sourcePath := filepath.Join(s.blobStore.FinalDir(), filepath.FromSlash(sourceStorageKey))
	targetPath := filepath.Join(s.blobStore.FinalDir(), filepath.FromSlash(targetStorageKey))
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(targetPath); err == nil {
		return nil
	}
	if err := os.Link(sourcePath, targetPath); err == nil {
		return nil
	}
	src, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, src); err != nil {
		_ = os.Remove(targetPath)
		return err
	}
	if err := dst.Sync(); err != nil {
		_ = os.Remove(targetPath)
		return err
	}
	return nil
}

// detectMIME sniffs the first 512 bytes and returns the canonical mime
// type. Falls back to "" when the file cannot be read.
func detectMIME(path string) string {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return ""
	}
	defer f.Close()
	var sniff [512]byte
	n, _ := io.ReadFull(f, sniff[:])
	return http.DetectContentType(sniff[:n])
}

// NOTE: PR 3.5-a DELETED:
//   - FinalizeArtifactAndCompleteJob (the giant ~250-line tx method)
//     Replaced entirely by FinalizationRepository.FinalizeVerified.
//   - emitOutboxTx                  (orphan)
//     The finalization repo has its own emitOutboxTx inside the tx.
