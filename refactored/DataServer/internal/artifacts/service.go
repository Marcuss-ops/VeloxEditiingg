package artifacts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/store"
)

// Clock abstracts time.Now for testability.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// defaultUploadTTL matches the spec's reconciler rule
// ("blob finale senza riga DB dopo 24h → elimina") so the same window
// sweeps both orphaned upload sessions and orphaned final blobs.
const defaultUploadTTL = 24 * time.Hour

// Service is the master-side authority on artifact state transitions.
//
// The Service exposes four methods:
//   - BeginUpload                      (Fase 1, validation only)
//   - Receive                          (Fase 2, streaming + master hash + post-write verify)
//   - Finalize                         (Fase 3, orchestration: promote blob + delegate to tx)
//   - FinalizeArtifactAndCompleteJob   (Fase 4, single tx w/ CAS guards)
//
// The Service is the only layer that computes SHA-256 / size from the
// actual bytes — workers cannot influence the canonical storage_key,
// the artifact status, or the job status; they can only REQUEST a
// transition that the master authorizes + verifies.
type Service struct {
	repo      Repository
	blobStore store.BlobStore
	db        *sql.DB
	clock     Clock

	uploadTTL time.Duration
}

// NewService composes the dependencies Service needs. The same *sql.DB
// used by store.SQLiteStore is passed in so FinalizeArtifactAndCompleteJob
// can join the multi-table tx with the artifact_uploads updates.
//
// blobStore is LocalBlobStore in production, NopBlobStore in tests.
//
// Outbox emission is intentionally direct (INSERT into outbox_events
// inside the same *sql.Tx) because chunk 4 has not yet wired the
// upstream outbox.Store dependency; the production outbox dispatcher
// (internal/outbox/registry.go) still polls outbox_events regardless.
func NewService(repo Repository, blobStore store.BlobStore, db *sql.DB, clock Clock) *Service {
	if clock == nil {
		clock = realClock{}
	}
	return &Service{
		repo:      repo,
		blobStore: blobStore,
		db:        db,
		clock:     clock,
		uploadTTL: defaultUploadTTL,
	}
}

// WithUploadTTL adjusts the upload session expiry (tests).
func (s *Service) WithUploadTTL(d time.Duration) *Service {
	s.uploadTTL = d
	return s
}

// =====================================================================
// helpers
// =====================================================================

// newID returns a 128-bit random hex string. crypto/rand.Read on
// Linux's /dev/urandom cannot fail in practice; the time-based fallback
// exists purely so the function never returns the empty string.
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "id_fb_" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	return hex.EncodeToString(b)
}

// stagingTempKey returns the staging blob path used during Receive().
// Lives under blobStore.StagingDir() so removal is trivial on hash /
// size mismatch (just call blobStore.RemoveStaging).
func stagingTempKey(bl store.BlobStore, uploadID string) string {
	return filepath.Join(bl.StagingDir(), "upload-"+uploadID+".tmp")
}

// mimeToExt maps a master-detected MIME to a stable file extension.
// Fallback: ".bin" so the SHA-derived storage_key is still valid for
// unknown mime types. The spec mandates the extension in the
// storage_key; mime alone is not enough (text/plain → .txt,
// application/json → .json, etc.).
//
// The result MUST be applied identically across Finalize,
// ReconcilerCleanup, and any pre-create path so a single sha256 maps
// to a single canonical storage_key. Centralizing this here prevents
// drift across the 3+ call sites.
func mimeToExt(mimeType string) string {
	if mimeType == "" {
		return ".bin"
	}
	exts, err := mime.ExtensionsByType(mimeType)
	if err == nil && len(exts) > 0 && exts[0] != "" {
		ext := exts[0]
		if ext[0] != '.' {
			ext = "." + ext
		}
		return ext
	}
	return ".bin"
}

// countingWriter is the io.Writer side of io.MultiWriter — it counts
// bytes while piping them through to the blob on disk. The spec example
// (writer = io.MultiWriter(temporaryBlobWriter, hasher, counter))
// requires a counter implementation that does not buffer.
type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// jobState captures the columns BeginUpload / Finalize need from `jobs`.
// Loaded with a single SELECT, then checked against the BeginUpload
// auth fields in one place.
type jobState struct {
	status      string
	assignedTo  string
	leaseID     string
	leaseExpiry time.Time
	revision    int
	outputSha   string
}

// loadJob reads the auth-relevant columns of a `jobs` row.
func (s *Service) loadJob(ctx context.Context, jobID string) (*jobState, error) {
	if jobID == "" {
		return nil, fmt.Errorf("artifacts: loadJob: empty jobID")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(assigned_to, ''),
		        COALESCE(lease_id, ''), COALESCE(lease_expiry, ''),
		        COALESCE(revision, 0), COALESCE(output_sha256, '')
		 FROM jobs WHERE job_id = ?`, jobID)
	var j jobState
	var leaseExp string
	if err := row.Scan(&j.status, &j.assignedTo, &j.leaseID, &leaseExp,
		&j.revision, &j.outputSha); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: loadJob: %w", err)
	}
	if leaseExp != "" {
		if ts, perr := time.Parse(time.RFC3339, leaseExp); perr == nil {
			j.leaseExpiry = ts
		}
	}
	return &j, nil
}

// loadAttempt reads auth-relevant columns of a `job_attempts` row.
func (s *Service) loadAttempt(ctx context.Context, jobID string, attemptNumber int) (status, workerID, leaseID string, err error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id, ''), COALESCE(lease_id, '')
		 FROM job_attempts
		 WHERE job_id = ? AND attempt_number = ?`,
		jobID, attemptNumber)
	if scanErr := row.Scan(&status, &workerID, &leaseID); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return "", "", "", fmt.Errorf("%w: job=%s n=%d",
				ErrAttemptMismatch, jobID, attemptNumber)
		}
		return "", "", "", fmt.Errorf("artifacts: loadAttempt: %w", scanErr)
	}
	return status, workerID, leaseID, nil
}

// verifyStagedBlob reads the staged temp file end-to-end and returns
// (sha256 hex, byte count). Used AFTER io.Copy completes in Receive()
// to catch io.MultiWriter partial-write hazards where a downstream
// error would leave the disk with bytes that were hashed + counted but
// never actually durably written. The cost is one extra fs read but
// it is correctness-critical for the trust boundary.
func verifyStagedBlob(path string) (string, int64, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return "", 0, fmt.Errorf("verifyStagedBlob open: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("verifyStagedBlob read: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), n, nil
}

// loadArtifactByID is a small helper used by Finalize's idempotent
// COMPLETED path.
func loadArtifactByID(ctx context.Context, db *sql.DB, id string) (*store.Artifact, error) {
	row := db.QueryRowContext(ctx, `
		SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		       COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		       COALESCE(local_path, ''), COALESCE(sha256, ''),
		       COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		       status, COALESCE(verified_at, ''), created_at
		FROM artifacts WHERE id = ?`, id)
	var a store.Artifact
	var verifiedAt string
	if err := row.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
		&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
		&a.SizeBytes, &a.DurationSeconds, &a.Status, &verifiedAt, &a.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("loadArtifactByID: %w", err)
	}
	return &a, nil
}

// isNoSuchTable returns true when err is the SQLite "no such table"
// / "no such column" error. Used to soft-skip over schema-roll phases
// where outbox_events / job_deliveries may not yet exist.
func isNoSuchTable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no such table") ||
		strings.Contains(msg, "no such column") ||
		strings.Contains(msg, "Error 1")
}

// =====================================================================
// FASE 1: BeginUpload
// =====================================================================

// BeginUpload authorizes a worker-side upload session.
//
// Validation gates (PR 2 spec, Fase 1):
//   * job.status = RUNNING
//   * job.assigned_to = worker_id
//   * job.lease_id    = lease_id, lease not expired
//   * job.revision    = expected_revision
//   * attempt.status  = RENDER_FINISHED, owner + lease match the job's
//   * no other artifact of the requested kind for this job is READY
//
// On success an `artifacts` row in STAGING + an `artifact_uploads`
// row in CREATED are inserted. The temporary storage key is allocated
// in blobStore.StagingDir() but no blob is written yet — Receive() will
// stream bytes into it.
func (s *Service) BeginUpload(ctx context.Context, cmd BeginUploadCommand) (*UploadSession, error) {
	if cmd.JobID == "" || cmd.WorkerID == "" || cmd.LeaseID == "" {
		return nil, fmt.Errorf("artifacts: BeginUpload: job_id, worker_id and lease_id are required")
	}

	// ----- 1. job auth -----
	job, err := s.loadJob(ctx, cmd.JobID)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, fmt.Errorf("%w: job=%s missing", ErrJobNotRunning, cmd.JobID)
	}
	if job.status != "RUNNING" {
		return nil, fmt.Errorf("%w: job=%s status=%s", ErrJobNotRunning, cmd.JobID, job.status)
	}
	if job.assignedTo != cmd.WorkerID {
		return nil, fmt.Errorf("%w: job=%s assigned_to=%s worker=%s",
			ErrWrongJobOwner, cmd.JobID, job.assignedTo, cmd.WorkerID)
	}
	if job.leaseID != cmd.LeaseID {
		return nil, fmt.Errorf("%w: job=%s lease_mismatch", ErrLeaseInvalid, cmd.JobID)
	}
	if !job.leaseExpiry.IsZero() && s.clock.Now().After(job.leaseExpiry) {
		return nil, fmt.Errorf("%w: job=%s expired_at=%s",
			ErrLeaseInvalid, cmd.JobID, job.leaseExpiry.Format(time.RFC3339))
	}
	if cmd.ExpectedRevision != 0 && job.revision != cmd.ExpectedRevision {
		return nil, fmt.Errorf("%w: job=%s revision=%d want=%d",
			ErrRevisionMismatch, cmd.JobID, job.revision, cmd.ExpectedRevision)
	}

	// ----- 2. attempt auth -----
	attStatus, attWorker, attLease, err := s.loadAttempt(ctx, cmd.JobID, cmd.AttemptNumber)
	if err != nil {
		return nil, err
	}
	if attWorker != cmd.WorkerID {
		return nil, fmt.Errorf("%w: attempt_owner job=%s n=%d",
			ErrWrongJobOwner, cmd.JobID, cmd.AttemptNumber)
	}
	if attLease != cmd.LeaseID {
		return nil, fmt.Errorf("%w: attempt_lease job=%s n=%d",
			ErrLeaseInvalid, cmd.JobID, cmd.AttemptNumber)
	}
	if attStatus != "RENDER_FINISHED" {
		return nil, fmt.Errorf("%w: job=%s n=%d current=%s",
			ErrAttemptNotRenderFinished, cmd.JobID, cmd.AttemptNumber, attStatus)
	}

	// ----- 3. uniqueness gate -----
	var existingID string
	if err := s.db.QueryRowContext(ctx,
		`SELECT id FROM artifacts
		 WHERE job_id = ? AND type = ? AND status = 'READY'
		 LIMIT 1`, cmd.JobID, cmd.Kind).Scan(&existingID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("artifacts: BeginUpload existing-ready check: %w", err)
		}
	} else if existingID != "" {
		return nil, fmt.Errorf("%w: job=%s kind=%s existing=%s",
			ErrDuplicateReadyArtifact, cmd.JobID, cmd.Kind, existingID)
	}

	// ----- 4. allocate ids + temp key -----
	now := s.clock.Now()
	uploadID := newID()
	artifactID := newID()
	tempKey := stagingTempKey(s.blobStore, uploadID)

	session := &UploadSession{
		UploadID:            uploadID,
		ArtifactID:          artifactID,
		JobID:               cmd.JobID,
		WorkerID:            cmd.WorkerID,
		LeaseID:             cmd.LeaseID,
		AttemptNumber:       cmd.AttemptNumber,
		ExpectedRevision:    cmd.ExpectedRevision,
		Kind:                cmd.Kind,
		ExpectedMIME:        cmd.MimeType,
		TemporaryStorageKey: tempKey,
		ExpectedSizeBytes:   cmd.ExpectedSizeBytes,
		ExpectedSHA256:      cmd.ExpectedSHA256,
		Status:              "CREATED",
		CreatedAt:           now,
		ExpiresAt:           now.Add(s.uploadTTL),
	}

	// Insert artifacts first so the artifact_uploads FK resolves at
	// write time on systems where foreign_keys pragma is on.
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO artifacts (id, job_id, attempt_id, type,
		                       storage_provider, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		artifactID, cmd.JobID, cmd.AttemptNumber, cmd.Kind,
		"local", "STAGING", now.UTC().Format(time.RFC3339),
	); err != nil {
		return nil, fmt.Errorf("artifacts: BeginUpload insert artifact: %w", err)
	}

	if err := s.repo.CreateUploadSession(ctx, session); err != nil {
		// Best-effort rollback of the artifact row so we don't leak
		// STAGING rows for upload sessions that never made it.
		_, _ = s.db.ExecContext(ctx, `DELETE FROM artifacts WHERE id = ?`, artifactID)
		return nil, fmt.Errorf("artifacts: BeginUpload insert upload: %w", err)
	}

	return session, nil
}

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

// =====================================================================
// FASE 3 + 4: Finalize / FinalizeArtifactAndCompleteJob
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
//  4. Hand off to FinalizeArtifactAndCompleteJob for the single SQL
//     transaction (CAS on jobs/artifacts/attempts + outbox + delivery).
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
		return nil, err
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
		return nil, fmt.Errorf("%w: upload=%s lease_mismatch",
			ErrLeaseInvalid, cmd.UploadID)
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
	// FINALIZING → COMPLETED (in-tx, see FinalizeArtifactAndCompleteJob),
	// the loser's retry hits the idempotent COMPLETED short-circuit
	// path above.
	//
	// For status FINALIZING (a peer is mid-flight) we DO NOT skip the
	// CAS — we explicitly reject with ErrTransitionConflict so the
	// caller retries. Without this gate the loser would fall through
	// to PromoteToCanonical and run a SECOND os.Rename to the same
	// canonical path, racing the winner's first promote.
	switch session.Status {
	case "RECEIVED":
		if err := s.repo.TransitionUploadStatus(ctx, cmd.UploadID, "RECEIVED", "FINALIZING"); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTransitionConflict, err)
		}
	case "FINALIZING":
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

	out, err := s.FinalizeArtifactAndCompleteJob(ctx, FinalizeArtifactAndCompleteJobCommand{
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
		return nil, err
	}

	// NOTE: the FINALIZING → COMPLETED flip happens INSIDE
	// FinalizeArtifactAndCompleteJob's *sql.Tx (see step 8 there).
	// Doing it inside the tx avoids a liveness bug where a process
	// crash between tx-commit and a separate post-commit UPDATE would
	// leave the upload row stuck in FINALIZING forever, blocking
	// retries even though the underlying jobs/artifacts/attempts are
	// already SUCCEEDED.
	return out, nil
}

// FinalizeArtifactAndCompleteJob is the SINGLE TRANSACTION that
// flips jobs RUNNING → SUCCEEDED, artifacts STAGING → READY,
// job_attempts RENDER_FINISHED → SUCCEEDED, emits outbox events,
// and inserts the delivery row (idempotently) — all in one tx.
//
// Each SQL must affect 0 or 1 row (CAS). If any CAS fails, the tx
// rolls back: the promoted blob stays on disk and the reconciler
// (chunk 5) cleans it up after `defaultUploadTTL`. This matches the
// spec's preference: orphan blob > artifact READY with missing file.
//
// Idempotency: re-running with the same upload_id after a previous
// successful commit short-circuits in Finalize() (the COMPLETED path)
// BEFORE entering this tx — so for the no-op success case we never
// even open a new tx here.
func (s *Service) FinalizeArtifactAndCompleteJob(
	ctx context.Context,
	cmd FinalizeArtifactAndCompleteJobCommand,
) (*store.Artifact, error) {
	if cmd.UploadID == "" || cmd.ArtifactID == "" || cmd.JobID == "" {
		return nil, fmt.Errorf("artifacts: FinalizeArtifactAndCompleteJob: upload/artifact/job ids are required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeArtifactAndCompleteJob begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 1. upload session must be RECEIVED or FINALIZING.
	var uploadStatus, uploadWorker, uploadLease string
	var uploadAttempt int
	if err := tx.QueryRowContext(ctx, `
		SELECT status, worker_id, lease_id, attempt_number
		FROM artifact_uploads WHERE upload_id = ?`, cmd.UploadID,
	).Scan(&uploadStatus, &uploadWorker, &uploadLease, &uploadAttempt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, cmd.UploadID)
		}
		return nil, fmt.Errorf("artifacts: tx load upload: %w", err)
	}
	if uploadStatus != "RECEIVED" && uploadStatus != "FINALIZING" {
		return nil, fmt.Errorf("%w: upload=%s status=%s",
			ErrUploadStateInvalid, cmd.UploadID, uploadStatus)
	}
	if uploadWorker != cmd.WorkerID || uploadLease != cmd.LeaseID || uploadAttempt != cmd.AttemptNumber {
		return nil, fmt.Errorf("%w: auth upload=%s worker=%s->%s lease=%s->%s attempt=%d->%d",
			ErrTransitionConflict, cmd.UploadID,
			uploadWorker, cmd.WorkerID, uploadLease, cmd.LeaseID,
			uploadAttempt, cmd.AttemptNumber)
	}

	now := s.clock.Now().UTC().Format(time.RFC3339)

	// 2. jobs CAS: RUNNING + owner + lease + revision → SUCCEEDED.
	jobRes, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'SUCCEEDED',
		    completed_at = ?,
		    updated_at   = ?,
		    lease_id     = NULL,
		    lease_expiry = NULL,
		    revision     = revision + 1,
		    output_sha256 = ?
		WHERE job_id = ?
		  AND status = 'RUNNING'
		  AND assigned_to = ?
		  AND lease_id = ?
		  AND revision = ?`,
		now, now, cmd.SHA256, cmd.JobID,
		cmd.WorkerID, cmd.LeaseID, cmd.ExpectedRevision,
	)
	if err != nil {
		return nil, fmt.Errorf("artifacts: tx jobs CAS: %w", err)
	}
	if n, _ := jobRes.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: jobs affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}

	// 3. artifacts CAS: STAGING → READY, master-stamp metadata.
	artRes, err := tx.ExecContext(ctx, `
		UPDATE artifacts
		SET status = 'READY',
		    storage_provider = ?,
		    storage_key = ?,
		    sha256 = ?, size_bytes = ?, mime_type = ?,
		    verified_at = ?
		WHERE id = ? AND job_id = ? AND status = 'STAGING'`,
		cmd.StorageProvider, cmd.StorageKey,
		cmd.SHA256, cmd.SizeBytes, cmd.MIMEType,
		now,
		cmd.ArtifactID, cmd.JobID,
	)
	if err != nil {
		return nil, fmt.Errorf("artifacts: tx artifacts CAS: %w", err)
	}
	if n, _ := artRes.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: artifacts affected=%d upload=%s artifact=%s",
			ErrTransitionConflict, n, cmd.UploadID, cmd.ArtifactID)
	}

	// 4. job_attempts CAS: RENDER_FINISHED + auth → SUCCEEDED.
	attRes, err := tx.ExecContext(ctx, `
		UPDATE job_attempts
		SET status = 'SUCCEEDED',
		    finished_at = ?
		WHERE job_id = ?
		  AND attempt_number = ?
		  AND worker_id = ?
		  AND lease_id = ?
		  AND status = 'RENDER_FINISHED'`,
		now, cmd.JobID, cmd.AttemptNumber,
		cmd.WorkerID, cmd.LeaseID,
	)
	if err != nil {
		return nil, fmt.Errorf("artifacts: tx job_attempts CAS: %w", err)
	}
	if n, _ := attRes.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: job_attempts affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}

	// 5. outbox ARTIFACT_READY + JOB_SUCCEEDED (transactional outbox).
	if err := s.emitOutboxTx(ctx, tx,
		"artifact", cmd.ArtifactID, "ARTIFACT_READY",
		fmt.Sprintf(`{"artifact_id":%q,"job_id":%q,"sha256":%q,"size_bytes":%d,"mime_type":%q,"storage_key":%q}`,
			cmd.ArtifactID, cmd.JobID, cmd.SHA256, cmd.SizeBytes, cmd.MIMEType, cmd.StorageKey),
	); err != nil {
		return nil, fmt.Errorf("artifacts: tx outbox ARTIFACT_READY: %w", err)
	}
	if err := s.emitOutboxTx(ctx, tx,
		"job", cmd.JobID, "JOB_SUCCEEDED",
		fmt.Sprintf(`{"job_id":%q,"artifact_id":%q,"sha256":%q}`,
			cmd.JobID, cmd.ArtifactID, cmd.SHA256),
	); err != nil {
		return nil, fmt.Errorf("artifacts: tx outbox JOB_SUCCEEDED: %w", err)
	}

	// 6. job_deliveries idempotent creation.
	//
	// PR 2 chunk 1+2+3 placeholder: the SELECT-keys-on-'primary' rule
	// keeps the ON CONFLICT DO NOTHING idempotency exercised. Real
	// destination enumeration belongs to a DeliveryPlanResolver
	// interface (added in chunk 4): a per-job list []string of
	// destination IDs iterated with the same INSERT pattern. The
	// current single-row INSERT is safe (UNIQUE(artifact_id,
	// destination_id) protects against double FINALIZE) but does NOT
	// match the wider destination set that production delivery runs
	// require.
	delRes, err := tx.ExecContext(ctx, `
		INSERT INTO job_deliveries (artifact_id, destination_id, payload, status, created_at)
		SELECT ?, 'primary', ?, 'PENDING', ?
		WHERE NOT EXISTS (
			SELECT 1 FROM job_deliveries
			WHERE artifact_id = ? AND destination_id = 'primary'
		)`,
		cmd.ArtifactID,
		fmt.Sprintf(`{"artifact_id":%q,"storage_key":%q}`, cmd.ArtifactID, cmd.StorageKey),
		now, cmd.ArtifactID,
	)
	if err != nil {
		if !isNoSuchTable(err) {
			return nil, fmt.Errorf("artifacts: tx job_deliveries insert: %w", err)
		}
	} else if delRes != nil {
		if n, _ := delRes.RowsAffected(); n == 1 {
			// 7. outbox DELIVERY_CREATED only when we actually
			// inserted — ON CONFLICT DO NOTHING must NOT fire the event.
			if err := s.emitOutboxTx(ctx, tx,
				"delivery", cmd.ArtifactID+":primary", "DELIVERY_CREATED",
				fmt.Sprintf(`{"artifact_id":%q,"destination_id":"primary"}`,
					cmd.ArtifactID),
			); err != nil {
				return nil, fmt.Errorf("artifacts: tx outbox DELIVERY_CREATED: %w", err)
			}
		}
	}

	// 8. In-tx flip FINALIZING → COMPLETED. The UPDATE joins the same
	// *sql.Tx as the rest of step 1-7 so commit is the single atomicity
	// boundary; a process crash BEFORE commit rolls back all 8 steps
	// together, a process crash AFTER commit persists them together.
	// This eliminates the liveness bug where a separate post-commit
	// UpdateUploadStatus(COMPLETED) call could leave the row stuck in
	// FINALIZING if the process died between commit and the flip.
	upRes, err := tx.ExecContext(ctx, `
		UPDATE artifact_uploads
		SET status = 'COMPLETED',
		    completed_at = ?
		WHERE upload_id = ?
		  AND status = 'FINALIZING'`,
		now, cmd.UploadID)
	if err != nil {
		return nil, fmt.Errorf("artifacts: tx upload COMPLETED flip: %w", err)
	}
	if n, _ := upRes.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: upload affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeArtifactAndCompleteJob commit: %w", err)
	}
	committed = true

	// Re-load the post-update artifact for the caller.
	var out store.Artifact
	row := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		       COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		       COALESCE(local_path, ''), COALESCE(sha256, ''),
		       COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		       status, COALESCE(verified_at, ''), created_at
		FROM artifacts WHERE id = ?`, cmd.ArtifactID)
	var verifiedAt string
	if scanErr := row.Scan(&out.ID, &out.JobID, &out.AttemptID, &out.Type, &out.StorageProvider,
		&out.StorageKey, &out.StorageURL, &out.LocalPath, &out.SHA256,
		&out.SizeBytes, &out.DurationSeconds, &out.Status, &verifiedAt, &out.CreatedAt); scanErr != nil {
		return nil, fmt.Errorf("artifacts: FinalizeArtifactAndCompleteJob post-load: %w", scanErr)
	}
	_ = verifiedAt

	return &out, nil
}

// emitOutboxTx is the transactional-outbox helper: the INSERT joins
// the same *sql.Tx as the rest of the SQL in FinalizeArtifactAndCompleteJob,
// so commit is the single atomicity boundary.
//
// On schemas where outbox_events does not exist yet (pre-migration 026)
// this is a soft-skip so the spec's FK → outbox requirement doesn't
// break older production rolls. Once 026 is applied uniformly we
// drop the soft-skip.
func (s *Service) emitOutboxTx(ctx context.Context, tx *sql.Tx, aggType, aggID, eventType, payload string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status, created_at)
		VALUES (?, ?, ?, ?, 'PENDING', ?)`,
		aggType, aggID, eventType, payload,
		s.clock.Now().UTC().Format(time.RFC3339))
	if isNoSuchTable(err) {
		return nil
	}
	return err
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

func ptrString(s string) *string { return &s }
