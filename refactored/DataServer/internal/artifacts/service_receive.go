package artifacts

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// =====================================================================
// FASE 2: Receive
// =====================================================================

// Receive streams worker bytes into the staging blob, computing SHA-256
// + size on the way. The hasher / counter share the same io.Copy so the
// worker cannot report a size or hash that disagrees with what the
// master observed (the worker is a transport, not a source of truth).
//
// On hash or size mismatch against expectedSnapshot (whenever those
// were supplied by BeginUpload) the staging blob is removed and the
// upload is marked FAILED.
//
// Post-write verification (verifyStagedBlob) catches the
// io.MultiWriter partial-write hazard where a downstream error could
// leave the file with bytes that were hashed + counted but not actually
// durably written. This is the trust boundary: mismatch -> FAILED.
func (s *Service) Receive(ctx context.Context, uploadID string, reader io.Reader) (*ReceiveResult, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifacts: Receive: empty uploadID")
	}
	if reader == nil {
		return nil, fmt.Errorf("artifacts: Receive: nil reader")
	}

	session, err := s.repo.GetUploadSession(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, uploadID)
	}
	if session.Status != "CREATED" && session.Status != "UPLOADING" {
		return nil, fmt.Errorf("%w: upload_id=%s status=%s",
			ErrUploadStateInvalid, uploadID, session.Status)
	}
	if !session.ExpiresAt.IsZero() && s.clock.Now().After(session.ExpiresAt) {
		return nil, fmt.Errorf("%w: upload_id=%s expired_at=%s",
			ErrUploadExpired, uploadID, session.ExpiresAt.Format(time.RFC3339))
	}

	// Move CREATED -> UPLOADING so the reconciler (chunk 5) treats it
	// differently from a row that hasn't started streaming yet.
	if session.Status == "CREATED" {
		if err := s.repo.UpdateUploadStatus(ctx, uploadID, UploadFields{
			Status: ptrString("UPLOADING"),
		}); err != nil {
			return nil, err
		}
	}

	// ----- stream to a fresh temp file under staging dir -----
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

	// ----- post-write re-verification (io.MultiWriter safety net) -----
	verifiedSHA, verifiedSize, verr := verifyStagedBlob(session.TemporaryStorageKey)
	if verr != nil {
		_ = os.Remove(session.TemporaryStorageKey)
		return nil, fmt.Errorf("%w: post-write verify: %v", ErrBlobWriteFailed, verr)
	}
	if verifiedSHA != receivedSHA || verifiedSize != receivedSize {
		_ = os.Remove(session.TemporaryStorageKey)
		_ = s.markFailed(ctx, uploadID, "post-write verify mismatch")
		return nil, fmt.Errorf("%w: post-write verify mismatch sha=%s/%s size=%d/%d",
			ErrBlobWriteFailed, receivedSHA, verifiedSHA, receivedSize, verifiedSize)
	}

	// ----- compare against worker-declared hints -----
	if session.ExpectedSHA256 != "" && session.ExpectedSHA256 != receivedSHA {
		_ = os.Remove(session.TemporaryStorageKey)
		_ = s.markFailed(ctx, uploadID, "hash mismatch")
		return nil, fmt.Errorf("%w: expected=%s got=%s",
			ErrHashMismatch, session.ExpectedSHA256, receivedSHA)
	}
	if session.ExpectedSizeBytes > 0 && session.ExpectedSizeBytes != receivedSize {
		_ = os.Remove(session.TemporaryStorageKey)
		_ = s.markFailed(ctx, uploadID, "size mismatch")
		return nil, fmt.Errorf("%w: expected=%d got=%d",
			ErrSizeMismatch, session.ExpectedSizeBytes, receivedSize)
	}

	// ----- mark RECEIVED -----
	now := s.clock.Now()
	if err := s.repo.UpdateUploadStatus(ctx, uploadID, UploadFields{
		Status:            ptrString("RECEIVED"),
		ReceivedSizeBytes: &receivedSize,
		ReceivedSHA256:    &receivedSHA,
		CompletedAt:       &now,
	}); err != nil {
		return nil, err
	}

	return &ReceiveResult{
		UploadID:          uploadID,
		ReceivedSizeBytes: receivedSize,
		ReceivedSHA256:    receivedSHA,
		Status:            "RECEIVED",
	}, nil
}

// markFailed flips an upload to FAILED on Receive errors so the
// reconciler can clean up the staging blob later.
func (s *Service) markFailed(ctx context.Context, uploadID, reason string) error {
	now := s.clock.Now()
	err := s.repo.UpdateUploadStatus(ctx, uploadID, UploadFields{
		Status:      ptrString("FAILED"),
		CompletedAt: &now,
	})
	if err != nil {
		return err
	}
	_ = reason // future hook for log enrichment
	return nil
}
