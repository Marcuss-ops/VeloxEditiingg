// Package artifacts / service.go
//
// Master-side authority on artifact state transitions
// (BeginUpload → Receive → Finalize). Three phase boundaries:
//
//   - BeginUpload — validation + atomic insert via UploadSessionWriter.
//   - Receive     — streaming + master-computed hash + post-write verify
//                   via the typed store.UploadRepository.
//   - Finalize    — blob promotion + FinalizationWriter atomic tx
//                   (sole jobs.status='SUCCEEDED' writer) +
//                   ArtifactReader post-tx read.
//
// The Service is the only layer that computes SHA-256 / size from the
// actual bytes — workers cannot influence the canonical storage_key,
// the artifact status, or the job status; they can only REQUEST a
// transition that the master authorizes + verifies. The canonical
// atomic SUCCEEDED write lives on FinalizationWriter; this struct
// holds a reference to the writer and cannot itself produce a
// SUCCEEDED without going through the atomic tx.
package artifacts

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/identity"
	"velox-server/internal/platform/clock"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
)

// defaultUploadTTL matches the spec's reconciler rule
// ("blob finale senza riga DB dopo 24h → elimina") so the same window
// sweeps both orphaned upload sessions and orphaned final blobs.
const defaultUploadTTL = 24 * time.Hour

// Service composes three narrow persistence surfaces (uploadWriter +
// finalizeWriter + artifactReader), the typed store.UploadRepository for
// per-session reads + non-finalizing status transitions, and the
// blob store for staging + final-blob IO.
//
// LoadJob / loadAttempt keep raw SQL because the typed store does not
// expose these specific joins yet; they are read-only and audited by
// the SQL-ownership shape guard.
type Service struct {
	repo           store.UploadRepository
	uploadWriter   UploadSessionWriter
	finalizeWriter FinalizationWriter
	artifactReader ArtifactReader
	blobStore      store.BlobStore
	db             *sql.DB
	clock          clock.Clock

	uploadTTL time.Duration
}

// NewService composes the dependencies Service needs.
//
// All six deps are required. Each nil check panics so a misconfigured
// compose fails fast on startup rather than silently producing no
// SUCCEEDED.
//
//   - repo: per-session CRUD (state machine + read loads + chunks).
//   - uploadWriter: atomic paired-insert of artifacts + artifact_uploads.
//   - finalizeWriter: atomic verified-finalization tx; the sole legal
//     writer of jobs.status='SUCCEEDED'.
//   - artifactReader: read-only artifact projection; consumed by the
//     idempotent COMPLETED path and downstream callers.
//   - blobStore: FilesystemBlobStore in production, NopBlobStore in tests.
//
// The four artifacts-package SQLite components share the same *sql.DB
// so the finalize tx can join with concurrent updates on
// artifact_uploads.
func NewService(
	repo store.UploadRepository,
	uploadWriter UploadSessionWriter,
	finalizeWriter FinalizationWriter,
	artifactReader ArtifactReader,
	blobStore store.BlobStore,
	db *sql.DB,
	c clock.Clock,
) *Service {
	if c == nil {
		c = clock.System{}
	}
	if repo == nil {
		panic("artifacts: NewService requires a non-nil UploadRepository")
	}
	if uploadWriter == nil {
		panic("artifacts: NewService requires a non-nil UploadSessionWriter")
	}
	if finalizeWriter == nil {
		panic("artifacts: NewService requires a non-nil FinalizationWriter (sole writer of jobs.status='SUCCEEDED')")
	}
	if artifactReader == nil {
		panic("artifacts: NewService requires a non-nil ArtifactReader")
	}
	if blobStore == nil {
		panic("artifacts: NewService requires a non-nil BlobStore")
	}
	return &Service{
		repo:           repo,
		uploadWriter:   uploadWriter,
		finalizeWriter: finalizeWriter,
		artifactReader: artifactReader,
		blobStore:      blobStore,
		db:             db,
		clock:          c,
		uploadTTL:      defaultUploadTTL,
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

// jobState captures the columns BeginUpload / Finalize need from
// `jobs`. Loaded with a single SELECT, then checked against the
// BeginUpload auth fields in one place.
//
// migration 048 dropped assigned_to, lease_id, lease_expiry from
// `jobs`. Worker / lease identity lives on task_attempts and on the
// artifact_uploads CAS chain. The jobs row only carries status and
// revision in this code path — auth is verified after loadJob at the
// artifact_uploads and task_attempts layers.
type jobState struct {
	status   string
	revision int
}

// loadJob reads the auth-relevant columns of a `jobs` row.
func (s *Service) loadJob(ctx context.Context, jobID string) (*jobState, error) {
	if jobID == "" {
		return nil, fmt.Errorf("artifacts: loadJob: empty jobID")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(revision, 0)
		 FROM jobs WHERE job_id = ?`, jobID)
	var j jobState
	if err := row.Scan(&j.status, &j.revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: loadJob: %w", err)
	}
	return &j, nil
}

// loadAttempt reads auth-relevant columns of a `task_attempts` row.
//
// This is the canonical source of worker_id / lease_id / attempt
// identity for the upload pipeline (migration 048 dropped those
// columns from jobs). Anywhere else still selects assigned_to /
// lease_id from jobs WILL FAIL because those columns are gone.
//
// NOTE: the function name is historical (pre-migration 048 it queried
// job_attempts); the SQL now reads task_attempts joined to tasks.
func (s *Service) loadAttempt(ctx context.Context, jobID string, attemptNumber int) (status, workerID, leaseID string, err error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(ta.status, ''), COALESCE(ta.worker_id, ''), COALESCE(ta.lease_id, '')
		 FROM task_attempts ta
		 JOIN tasks t ON ta.task_id = t.task_id
		 WHERE t.job_id = ? AND ta.attempt_number = ?`,
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

// isNoSuchTable returns true when err is the SQLite "no such table"
// / "no such column" error. Used to soft-skip over schema-roll phases
// where outbox_events / job_deliveries may not yet exist.
//
// The reconciler (reconciler.go) uses this for the cleanup queries
// that have to tolerate partial-migration DBs.
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
// FASE 1: BeginUpload (atomic via finRepo.CreateArtifactAndUploadSession)
// =====================================================================

// BeginUpload authorizes a worker-side upload session.
//
// Validation gates:
//   - job.status = RUNNING
//   - job.assigned_to = worker_id (was removed; checked via loadAttempt)
//   - job.lease_id    = lease_id, lease not expired (was removed;
//                        checked via loadAttempt)
//   - job.revision    = expected_revision
//   - attempt.status  non-terminal (any RENDER_FINISHED / RUNNING / etc.)
//   - no other artifact of the requested kind for this job is READY
//
// On success the artifacts + artifact_uploads rows are inserted
// ATOMICALLY in a single transaction via finRepo.CreateArtifactAndUploadSession.
// The temporary storage key is allocated in blobStore.StagingDir() but
// no blob is written yet — Receive() will stream bytes into it.
func (s *Service) BeginUpload(ctx context.Context, cmd BeginUploadCommand) (*store.UploadSession, error) {
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
	if job.status != string(store.JobStatusRunning) {
		return nil, fmt.Errorf("%w: job=%s status=%s", ErrJobNotRunning, cmd.JobID, job.status)
	}
	// migration 048: assigned_to/lease_id/lease_expiry from jobs were
	// dropped. Worker + lease identity is verified at attempt level
	// via loadAttempt below, and at the artifact_uploads CAS chain
	// in FinalizeVerified. Keeping a lease_expiry check here would
	// reference a column that no longer exists.
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
	attStatus = strings.ToUpper(strings.TrimSpace(attStatus))
	// task_attempts lifecycle is PENDING → RUNNING → SUCCEEDED
	// (closes on ingester via the atomic transition). BeginUpload
	// runs WHILE task_attempts is non-terminal (worker active) and
	// stamps worker_id + lease_id into artifact_uploads. A terminal
	// state on the attempt is the failure signal — accept any
	// non-terminal.
	if attStatus == string(taskattempts.AttemptStatusSucceeded) ||
		attStatus == string(taskattempts.AttemptStatusFailed) ||
		attStatus == string(taskattempts.AttemptStatusCancelled) ||
		attStatus == string(taskattempts.AttemptStatusTimedOut) {
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

	// ----- 4. allocate ids + temp key + atomic insert via uploadWriter -----
	now := s.clock.Now()
	uploadID, err := identity.NewHex128()
	if err != nil {
		return nil, fmt.Errorf("generate upload ID: %w", err)
	}
	artifactID, err := identity.NewHex128()
	if err != nil {
		return nil, fmt.Errorf("generate artifact ID: %w", err)
	}
	tempKey := stagingTempKey(s.blobStore, uploadID)

	// Atomic insert of artifacts + artifact_uploads via UploadSessionWriter.
	if err := s.uploadWriter.CreateArtifactAndUploadSession(ctx, CreateArtifactAndUploadSessionCommand{
		ArtifactID:          artifactID,
		UploadID:            uploadID,
		JobID:               cmd.JobID,
		AttemptID:           int64(cmd.AttemptNumber),
		Kind:                cmd.Kind,
		WorkerID:            cmd.WorkerID,
		LeaseID:             cmd.LeaseID,
		AttemptNumber:       cmd.AttemptNumber,
		ExpectedRevision:    cmd.ExpectedRevision,
		StorageProvider:     "local",
		ExpectedMIME:        cmd.MimeType,
		ExpectedSizeBytes:   cmd.ExpectedSizeBytes,
		ExpectedSHA256:      cmd.ExpectedSHA256,
		TemporaryStorageKey: tempKey,
		CreatedAt:           now,
		ExpiresAt:           now.Add(s.uploadTTL),
	}); err != nil {
		return nil, fmt.Errorf("artifacts: BeginUpload atomic insert: %w", err)
	}

	session := &store.UploadSession{
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
		Status:              string(store.UploadCreated),
		CreatedAt:           now,
		ExpiresAt:           now.Add(s.uploadTTL),
	}

	return session, nil
}
