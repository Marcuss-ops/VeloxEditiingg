package artifacts

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"velox-server/internal/repository"
)

// =====================================================================
// FASE 2: Receive
// =====================================================================

// Receive streams worker bytes into the staging blob, computing SHA-256
// + size on the way. The hasher / counter share the same io.Copy so the
// worker cannot report a size or hash that disagrees with what the
// master observed (the worker is a transport, not a source of truth).
//
// On hash or size mismatch against the expected snapshot (whenever those
// were supplied by BeginUpload) the staging blob is removed and the
// upload is marked FAILED. A worker-declared hash that differs from the
// master-computed one is classified ARTIFACT_TRANSFER_CORRUPTED; the
// master-computed hash of the received bytes is authoritative.
//
// The io.MultiWriter partial-write hazard (a downstream error leaving
// the file with bytes that were hashed + counted but not durably
// written) is closed WITHOUT re-reading the artifact: io.Copy propagates
// every writer error, dst.Sync() + dst.Close() are checked, and the
// post-close os.Stat size invariant proves the on-disk file matches the
// hashed/counted byte count (B2: no second read). This is the trust
// boundary: any mismatch -> FAILED.
func (s *Service) Receive(ctx context.Context, uploadID string, reader io.Reader) (*ReceiveResult, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifacts: Receive: empty uploadID")
	}
	if reader == nil {
		return nil, fmt.Errorf("artifacts: Receive: nil reader")
	}

	session, err := s.repo.GetUploadSession(ctx, uploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if session == nil {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, uploadID)
	}
	// A lost HTTP response can make the worker retry /complete after the
	// server already persisted RECEIVED. Return the persisted master result
	// without consuming the retry body or touching the staging file.
	if session.Status == string(repository.UploadReceived) {
		return receiveResultFromSession(session)
	}
	if session.Status != string(repository.UploadCreated) && session.Status != string(repository.UploadUploading) {
		return nil, fmt.Errorf("%w: upload_id=%s status=%s",
			ErrUploadStateInvalid, uploadID, session.Status)
	}
	if !session.ExpiresAt.IsZero() && s.clock.Now().After(session.ExpiresAt) {
		return nil, fmt.Errorf("%w: upload_id=%s expired_at=%s",
			ErrUploadExpired, uploadID, session.ExpiresAt.Format(time.RFC3339))
	}

	// Move CREATED -> UPLOADING so the reconciler (chunk 5) treats it
	// differently from a row that hasn't started streaming yet.
	if session.Status == string(repository.UploadCreated) {
		uploading := string(repository.UploadUploading)
		if err := s.repo.UpdateUploadStatus(ctx, uploadID, repository.UploadFields{
			Status: &uploading,
		}); err != nil {
			return nil, translateStoreErr(err)
		}
	}

	// ----- stream to a fresh temp file under staging dir -----
	firstByte := s.clock.Now()
	if err := s.repo.UpdateUploadStatus(ctx, uploadID, repository.UploadFields{FirstByteReceivedAt: &firstByte, Status: func() *string { v := string(repository.UploadUploading); return &v }()}); err != nil {
		return nil, translateStoreErr(err)
	}
	dst, err := os.OpenFile(filepath.Clean(session.TemporaryStorageKey),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("%w: create temp: %v", ErrBlobWriteFailed, err)
	}
	cleanup := func() {
		_ = dst.Close()
		_ = os.Remove(session.TemporaryStorageKey)
	}

	hasher := sha256.New()
	counter := &countingWriter{}
	writer := io.MultiWriter(dst, hasher, counter)

	if _, err := io.Copy(writer, reader); err != nil {
		cleanup()
		_ = s.markFailed(ctx, uploadID, "io.Copy error")
		return nil, fmt.Errorf("%w: io.Copy: %v", ErrBlobWriteFailed, err)
	}

	if err := dst.Sync(); err != nil {
		cleanup()
		return nil, fmt.Errorf("%w: fsync: %v", ErrBlobWriteFailed, err)
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(session.TemporaryStorageKey)
		return nil, fmt.Errorf("%w: close: %v", ErrBlobWriteFailed, err)
	}

	receivedSHA := fmt.Sprintf("%x", hasher.Sum(nil))
	receivedSize := counter.n
	lastByte := s.clock.Now()

	// ----- O(1) disk-size invariant (replaces the full-file re-hash) -----
	// The hasher and counter observed EXACTLY the bytes accepted by
	// dst.Write: io.MultiWriter short-circuits on the first writer error
	// and os.File never silently under-writes a buffer, so io.Copy success
	// means hashed == counted == sent-to-disk. With fsync + close already
	// error-checked above, the only residual hazard is a staged file on
	// disk shorter than what we hashed; one stat closes that gap without
	// re-reading the artifact bytes (B2: no second read).
	info, statErr := os.Stat(session.TemporaryStorageKey)
	if statErr != nil {
		_ = os.Remove(session.TemporaryStorageKey)
		return nil, fmt.Errorf("%w: stat staged blob: %v", ErrBlobWriteFailed, statErr)
	}
	if !info.Mode().IsRegular() || info.Size() != receivedSize {
		_ = os.Remove(session.TemporaryStorageKey)
		_ = s.markFailed(ctx, uploadID, "staged blob size mismatch")
		return nil, fmt.Errorf("%w: staged blob size=%d hashed=%d", ErrBlobWriteFailed, info.Size(), receivedSize)
	}

	// ----- compare against worker-declared hints -----
	// The worker-declared SHA (ExpectedSHA256 supplied at BeginUpload) is a
	// transport hint, never authority: when it differs from the
	// master-computed hash of the received bytes the transfer is corrupt
	// (ARTIFACT_TRANSFER_CORRUPTED) and the master-computed hash is the
	// authoritative one for the artifact the master stores.
	if session.ExpectedSHA256 != "" && session.ExpectedSHA256 != receivedSHA {
		_ = os.Remove(session.TemporaryStorageKey)
		_ = s.markFailed(ctx, uploadID, "hash mismatch")
		return nil, fmt.Errorf("%w: %w: worker_declared=%s master_computed=%s",
			ErrArtifactTransferCorrupted, ErrHashMismatch, session.ExpectedSHA256, receivedSHA)
	}
	if session.ExpectedSizeBytes > 0 && session.ExpectedSizeBytes != receivedSize {
		_ = os.Remove(session.TemporaryStorageKey)
		_ = s.markFailed(ctx, uploadID, "size mismatch")
		return nil, fmt.Errorf("%w: expected=%d got=%d",
			ErrSizeMismatch, session.ExpectedSizeBytes, receivedSize)
	}	// ----- mark RECEIVED -----
	now := s.clock.Now()

	received := string(repository.UploadReceived)
	if err := s.repo.UpdateUploadStatus(ctx, uploadID, repository.UploadFields{
		Status:            &received,
		ReceivedSizeBytes: &receivedSize,
		ReceivedSHA256:    &receivedSHA,
		CompletedAt:       &now,
	}); err != nil {
		return nil, translateStoreErr(err)
	}

	return &ReceiveResult{
		UploadID:          uploadID,
		ReceivedSizeBytes: receivedSize,
		ReceivedSHA256:    receivedSHA,
		Status:            string(repository.UploadReceived),
	}, nil
}

func receiveResultFromSession(session *repository.UploadSession) (*ReceiveResult, error) {
	if session == nil || session.Status != string(repository.UploadReceived) || session.ReceivedSHA256 == "" || session.ReceivedSizeBytes < 0 {
		return nil, fmt.Errorf("%w: received session is incomplete", ErrUploadStateInvalid)
	}
	return &ReceiveResult{
		UploadID:          session.UploadID,
		ReceivedSizeBytes: session.ReceivedSizeBytes,
		ReceivedSHA256:    session.ReceivedSHA256,
		Status:            session.Status,
	}, nil
}

// markFailed flips an upload to FAILED on Receive errors so the
// reconciler can clean up the staging blob later.
func (s *Service) markFailed(ctx context.Context, uploadID, reason string) error {
	now := s.clock.Now()
	failed := string(repository.UploadFailed)
	err := s.repo.UpdateUploadStatus(ctx, uploadID, repository.UploadFields{
		Status:      &failed,
		CompletedAt: &now,
	})
	if err != nil {
		return translateStoreErr(err)
	}
	_ = reason // future hook for log enrichment
	return nil
}
