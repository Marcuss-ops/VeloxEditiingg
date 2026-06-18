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
	clock     Clock

	uploadTTL time.Duration
}

// NewService composes the dependencies Service needs.
//
// repo: artifact uploads CRUD (State machine + ReadOnly loads).
// finRepo: atomic single-tx SUCCEEDED write + atomic artifacts+artifact_uploads insert.
// blobStore: LocalBlobStore in production, NopBlobStore in tests.
//
// The same *sql.DB is shared so the finalization tx can join with the
// concurrent update on artifact_uploads (step 7 of FinalizeVerified).
//
// PR 3.5-a: finRepo is REQUIRED. Panic if nil so a misconfigured compose
// always flakes on startup instead of silently producing no SUCCEEDED.
func NewService(repo Repository, finRepo FinalizationRepository, blobStore store.BlobStore, db *sql.DB, clock Clock) *Service {
	if clock == nil {
		clock = realClock{}
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
//   * job.status = RUNNING
//   * job.assigned_to = worker_id
//   * job.lease_id    = lease_id, lease not expired
//   * job.revision    = expected_revision
//   * attempt.status  = RENDER_FINISHED, owner + lease match the job's
//   * no other artifact of the requested kind for this job is READY
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

	// ----- 4. allocate ids + temp key + atomic insert via finRepo -----
	now := s.clock.Now()
	uploadID := newID()
	artifactID := newID()
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
		Status:              "CREATED",
		CreatedAt:           now,
		ExpiresAt:           now.Add(s.uploadTTL),
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
		return nil, err
	}

	// NOTE: the FINALIZING → COMPLETED flip happens INSIDE
	// FinalizeVerified's *sql.Tx (step 7 there). Doing it inside the tx
	// avoids a liveness bug where a process crash between tx-commit and
	// a separate post-commit UPDATE would leave the upload row stuck in
	// FINALIZING forever, blocking retries even though the underlying
	// jobs/artifacts/attempts are already SUCCEEDED.
	return out, nil
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

func ptrString(s string) *string { return &s }
