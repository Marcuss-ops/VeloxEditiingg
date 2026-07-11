// Package artifacts / service_begin.go
//
// Phase 1 of the verified-finalization pipeline: BeginUpload.
//
// Validates job + attempt auth through AuthReader
// (service_auth.go) and produces an upload session by delegating to
// UploadSessionWriter (sqlite_upload_session_writer.go).
//
// On success the artifacts (STAGING) + artifact_uploads (CREATED)
// rows are inserted ATOMICALLY via the writer. The temporary storage
// key is allocated in blobStore.StagingDir() but no blob is written
// yet — Receive() (service_receive.go) streams bytes into it during
// phase 2.
package artifacts

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/identity"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
)

// BeginUpload authorizes a worker-side upload session.
//
// Validation gates, all resolved via AuthReader:
//   - job.status = RUNNING
//   - job.revision = expected_revision (when supplied)
//   - attempt.status non-terminal (worker still active)
//   - attempt.worker_id = cmd.WorkerID
//   - attempt.lease_id = cmd.LeaseID
//   - no other artifact of the requested kind for this job is READY
//
// On success: artifacts + artifact_uploads are inserted ATOMICALLY
// via UploadSessionWriter (sqlite_upload_session_writer.go); no
// other code path mutates either table.
func (s *Service) BeginUpload(ctx context.Context, cmd BeginUploadCommand) (*store.UploadSession, error) {
	if cmd.JobID == "" || cmd.WorkerID == "" || cmd.LeaseID == "" {
		return nil, fmt.Errorf("artifacts: BeginUpload: job_id, worker_id and lease_id are required")
	}

	// ----- 1. job auth -----
	job, err := s.auth.LoadJob(ctx, cmd.JobID)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, fmt.Errorf("%w: job=%s missing", ErrJobNotRunning, cmd.JobID)
	}
	if job.Status != string(store.JobStatusRunning) {
		return nil, fmt.Errorf("%w: job=%s status=%s", ErrJobNotRunning, cmd.JobID, job.Status)
	}
	if cmd.ExpectedRevision != 0 && job.Revision != cmd.ExpectedRevision {
		return nil, fmt.Errorf("%w: job=%s revision=%d want=%d",
			ErrRevisionMismatch, cmd.JobID, job.Revision, cmd.ExpectedRevision)
	}

	// ----- 2. attempt auth -----
	attStatus, attWorker, attLease, err := s.auth.LoadAttempt(ctx, cmd.JobID, cmd.AttemptNumber)
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
	// task_attempts lifecycle: PENDING → RUNNING → SUCCEEDED.
	// BeginUpload runs WHILE non-terminal. A terminal attempt status
	// is the failure signal — accept any non-terminal.
	if attStatus == string(taskattempts.AttemptStatusSucceeded) ||
		attStatus == string(taskattempts.AttemptStatusFailed) ||
		attStatus == string(taskattempts.AttemptStatusCancelled) ||
		attStatus == string(taskattempts.AttemptStatusTimedOut) {
		return nil, fmt.Errorf("%w: job=%s n=%d current=%s",
			ErrAttemptNotRenderFinished, cmd.JobID, cmd.AttemptNumber, attStatus)
	}

	// ----- 3. uniqueness gate -----
	existingID, err := s.auth.FindExistingReadyArtifact(ctx, cmd.JobID, cmd.Kind)
	if err != nil {
		return nil, err
	}
	if existingID != "" {
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
