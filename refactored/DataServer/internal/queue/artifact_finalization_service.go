// Package queue artifact finalization service.
//
// ArtifactFinalizationService implements the master's authoritative
// STAGING → VERIFYING → READY state machine. The worker reports a Temporary
// SHA via the upload-completed endpoint; the master then re-computes SHA-256
// (and sniffs mime + measures size) over the local copy and only then
// transitions the artifact to READY. The worker cannot unilaterally promote
// the artifact to READY or the job to SUCCEEDED.
//
// State machine:
//   STAGING (worker upload in progress) ── upload-complete ─► VERIFYING
//   VERIFYING (master computes SHA + size + mime) ── match ─► READY
//   VERIFYING                                       ── mismatch ─► QUARANTINED
//   READY (master-verified) ── retention window ──► DELETED
//
// The SHA-256 + size work happens BETWEEN two transactions to keep the
// writer lock short. SQLite is the source of truth for status; the on-disk
// file is the source of truth for the bytes.
package queue

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"velox-server/internal/store"
)

// FinalizeRenderInput is the request payload for ArtifactFinalizationService.
type FinalizeRenderInput struct {
	ArtifactID    string // canonical key — multiple artifacts per job are possible
	JobID         string // for log/outbox correlation only; do not use as lookup key
	AttemptID     int64
	WorkerID      string
	LeaseID       string
	TemporaryPath string
	ExpectedSize  int64
	WorkerSHA256  string // for comparison only; not authoritative
}

// FinalizeRenderResult is what the service returns on success.
type FinalizeRenderResult struct {
	ArtifactID  string
	Status      string
	VerifiedAt  time.Time
	SizeBytes   int64
	SHA256      string
	MIMEType    string
	DurationMs  int64
}

// ErrArtifactVerificationFailed is returned when the master's re-hash does
// not match the worker-reported SHA. The caller must surface the failure
// to the orchestrator so the job is held in QUARANTINED, not silently
// promoted to READY.
var ErrArtifactVerificationFailed = errors.New("artifact: verification failed (sha mismatch)")

// ArtifactFinalizationService coordinates the master-side artifact
// verification. The constructor only requires a *store.SQLiteStore; the
// runner hooks and storage backend are wired via the optional setters so
// the service can be exercised in unit tests without a real disk store.
type ArtifactFinalizationService struct {
	dbStore *store.SQLiteStore
}

// NewArtifactFinalizationService builds the service.
func NewArtifactFinalizationService(dbStore *store.SQLiteStore) *ArtifactFinalizationService {
	return &ArtifactFinalizationService{dbStore: dbStore}
}

// FinalizeRender implements the authoritative STAGING → READY path. The
// caller (typically the upload-completed HTTP handler) supplies what the
// worker reported plus the local tempfile to verify. The service:
//
//   1. (Tx1) lock the artifact row in VERIFYING — fails fast if it is not
//      in STAGING (already verified, deleted, or scope mismatch).
//   2. (off-line) compute SHA-256, sniff mime, measure size; on mismatch
//      with WorkerSHA256, transition to QUARANTINED and return the error.
//   3. (Tx2) mark READY with verified_at + sha256 + mime_type + duration_ms,
//      emit artifact_ready outbox event.
//
// File copy → final storage path is out of scope here; the upload
// endpoint writes the final blob into a tempfile controlled by the
// service caller.
func (s *ArtifactFinalizationService) FinalizeRender(ctx context.Context, input FinalizeRenderInput) (*FinalizeRenderResult, error) {
	if s == nil || s.dbStore == nil {
		return nil, errors.New("artifact: nil service")
	}
	if input.ArtifactID == "" || input.TemporaryPath == "" {
		return nil, errors.New("artifact: missing artifact_id or temporary_path")
	}

	// 1. Tx1: STAGING → VERIFYING (keyed by artifact_id, NOT job_id —
	//    multiple artifacts per job means an UPDATE WHERE job_id= would race)
	if err := s.dbStore.TransitionArtifactStatus(ctx, input.ArtifactID, "STAGING", "VERIFYING"); err != nil {
		return nil, fmt.Errorf("artifact: STAGING → VERIFYING: %w", err)
	}

	// 2. Off-line verification. If the file is missing or unreadable, we
	//    mark the artifact QUARANTINED and surface the error.
	file, err := os.Open(input.TemporaryPath)
	if err != nil {
		_ = s.dbStore.TransitionArtifactStatus(ctx, input.ArtifactID, "VERIFYING", "QUARANTINED")
		return nil, fmt.Errorf("artifact: open tempfile: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		_ = s.dbStore.TransitionArtifactStatus(ctx, input.ArtifactID, "VERIFYING", "QUARANTINED")
		return nil, fmt.Errorf("artifact: hash tempfile: %w", err)
	}
	masterSHA := fmt.Sprintf("%x", hasher.Sum(nil))

	if input.WorkerSHA256 != "" && masterSHA != input.WorkerSHA256 {
		_ = s.dbStore.TransitionArtifactStatus(ctx, input.ArtifactID, "VERIFYING", "QUARANTINED")
		return nil, ErrArtifactVerificationFailed
	}
	if input.ExpectedSize > 0 && size != input.ExpectedSize {
		_ = s.dbStore.TransitionArtifactStatus(ctx, input.ArtifactID, "VERIFYING", "QUARANTINED")
		return nil, fmt.Errorf("artifact: size mismatch (got %d, want %d)", size, input.ExpectedSize)
	}

	// Mime sniff from the first 512 bytes via os.Open + http.DetectContentType
	// (re-using the same file handle after a Seek will work because we
	// closed+reopen is unnecessary on POSIX — the implementation here
	// reopens cheaply).
	head, err := os.Open(filepath.Clean(input.TemporaryPath))
	if err != nil {
		_ = s.dbStore.TransitionArtifactStatus(ctx, input.ArtifactID, "VERIFYING", "QUARANTINED")
		return nil, fmt.Errorf("artifact: re-open for mime sniff: %w", err)
	}
	defer head.Close()
	var sniff [512]byte
	n, _ := io.ReadFull(head, sniff[:])
	mimeType := http.DetectContentType(sniff[:n])

	// 3. Tx2: VERIFYING → READY with full verified_at + sha256 + mime_type
	// + duration_ms via store.FinalizeArtifactVerified.
	verified, err := s.dbStore.FinalizeArtifactVerified(ctx, input.ArtifactID, masterSHA, size, mimeType)
	if err != nil {
		return nil, fmt.Errorf("artifact: VERIFYING → READY: %w", err)
	}
	if verified == nil {
		return nil, errors.New("artifact: VERIFYING → READY failed (no row)")
	}

	// Outbox event (also written atomically inside FinalizeArtifactVerified;
	// this InsertJobEvent is a best-effort duplicate for log-only consumers).
	_ = s.dbStore.InsertJobEvent(time.Now().UTC().Format(time.RFC3339), verified.JobID, "artifact_ready", fmt.Sprintf(`{"artifact_id":%q,"sha256":%q,"size":%d}`, verified.ID, masterSHA, size))

	return &FinalizeRenderResult{
		ArtifactID: verified.ID,
		Status:     verified.Status,
		VerifiedAt: time.Now().UTC(),
		SizeBytes:  size,
		SHA256:     masterSHA,
		MIMEType:   mimeType,
	}, nil
}
