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
)

// defaultUploadTTL matches the spec's reconciler rule
// ("blob finale senza riga DB dopo 24h → elimina") so the same window
// sweeps both orphaned upload sessions and orphaned final blobs.
const defaultUploadTTL = 24 * time.Hour

// Service is the master-side authority on artifact state transitions.
//
// The Service exposes three methods:
//   - BeginUpload   (Fase 1: validation + atomic insert of artifacts + artifact_uploads via finRepo)
//   - Receive       (Fase 2: streaming + master hash + post-write verify)
//   - Finalize      (Fase 3 + 4: orchestration — promote blob + delegate SUCCEEDED-tx to finRepo)
//
// The Service is the only layer that computes SHA-256 / size from the
// actual bytes — workers cannot influence the canonical storage_key,
// the artifact status, or the job status; they can only REQUEST a
// transition that the master authorizes + verifies.
//
// PR 3.5-a: the canonical atomic SUCCEEDED write lives on
// FinalizationRepository (single SQL transaction across jobs +
// artifacts + job_attempts + outbox + delivery + artifact_uploads
// flip). This struct holds the *reference* to that repo but cannot
// itself produce a SUCCEEDED without going through the atomic tx.
type Service struct {
	repo      Repository
	finRepo   FinalizationRepository // NEW in PR 3.5-a: sole writer of jobs.status='SUCCEEDED'
	blobStore store.BlobStore
	db        *sql.DB
	clock     clock.Clock

	uploadTTL time.Duration
}

// NewService composes the dependencies Service needs.
//
// repo: artifact uploads CRUD (State machine + ReadOnly loads).
// finRepo: atomic single-tx SUCCEEDED write + atomic artifacts+artifact_uploads insert.
// blobStore: FilesystemBlobStore in production, NopBlobStore in tests.
//
// The same *sql.DB is shared so the finalization tx can join with the
// concurrent update on artifact_uploads (step 7 of FinalizeVerified).
//
// PR 3.5-a: finRepo is REQUIRED. Panic if nil so a misconfigured compose
// always flakes on startup instead of silently producing no SUCCEEDED.
func NewService(repo Repository, finRepo FinalizationRepository, blobStore store.BlobStore, db *sql.DB, c clock.Clock) *Service {
	if c == nil {
		c = clock.System{}
	}
	if repo == nil {
		panic("artifacts: NewService requires a non-nil Repository")
	}
	if finRepo == nil {
		panic("artifacts: NewService requires a non-nil FinalizationRepository (sole writer of jobs.status='SUCCEEDED')")
	}
	return &Service{
		repo:      repo,
		finRepo:   finRepo,
		blobStore: blobStore,
		db:        db,
		clock:     c,
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
}

// loadJob reads the auth-relevant columns of a `jobs` row.
func (s *Service) loadJob(ctx context.Context, jobID string) (*jobState, error) {
	if jobID == "" {
		return nil, fmt.Errorf("artifacts: loadJob: empty jobID")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(assigned_to, ''),
		        COALESCE(lease_id, ''), COALESCE(lease_expiry, ''),
		        COALESCE(revision, 0)
		 FROM jobs WHERE job_id = ?`, jobID)
	var j jobState
	var leaseExp string
	if err := row.Scan(&j.status, &j.assignedTo, &j.leaseID, &leaseExp,
		&j.revision); err != nil {
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
//
// PR 3.5-a: KEEP this helper. The reconciler (reconciler.go) uses it
// for the cleanup queries that have to tolerate partial-migration DBs.
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
// Validation gates (PR 2 spec, Fase 1):
//   - job.status = RUNNING
//   - job.assigned_to = worker_id
//   - job.lease_id    = lease_id, lease not expired
//   - job.revision    = expected_revision
//   - attempt.status  = RENDER_FINISHED, owner + lease match the job's
//   - no other artifact of the requested kind for this job is READY
//
// On success the artifacts + artifact_uploads rows are inserted
// ATOMICALLY in a single transaction via finRepo.CreateArtifactAndUploadSession.
// The temporary storage key is allocated in blobStore.StagingDir() but
// no blob is written yet — Receive() will stream bytes into it.
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
	if job.status != string(store.JobStatusRunning) {
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
	attStatus = strings.ToUpper(strings.TrimSpace(attStatus))
	if attStatus != string(AttemptRenderFinished) && attStatus != string(AttemptProcessing) {
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

	// ----- 4. allocate ids + temp key + atomic insert via finRepo -----
	now := s.clock.Now()
	uploadID := identity.NewHex128()
	artifactID := identity.NewHex128()
	tempKey := stagingTempKey(s.blobStore, uploadID)

	// PR 3.5-a: atomic insert of artifacts + artifact_uploads via finRepo.
	// The previous two-step pattern (artifacts INSERT + repo.CreateUploadSession)
	// left STAGING rows orphaned when the upload INSERT failed.
	if err := s.finRepo.CreateArtifactAndUploadSession(ctx, CreateArtifactAndUploadSessionCommand{
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
		Status:              string(UploadCreated),
		CreatedAt:           now,
		ExpiresAt:           now.Add(s.uploadTTL),
	}

	return session, nil
}
