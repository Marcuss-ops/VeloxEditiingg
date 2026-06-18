// Package artifacts implements the master-side authority over the
// STAGING -> RECEIVED -> READY state machine, the trust boundary that
// prevents workers from unilaterally promoting an artifact or declaring
// a job SUCCEEDED.
//
// PR 2 collapses this state machine into a single transactional step
// (FinalizeArtifactAndCompleteJob) with CAS guards on jobs / artifacts /
// job_attempts / job_deliveries / outbox_events.
//
// The public surface is intentionally small so the gRPC handler in
// internal/grpcserver can be rewritten to call Service.Finalize and
// Service.FinalizeArtifactAndCompleteJob without exposing any
// underlying tx control.
package artifacts

import "errors"

// Sentinel errors returned by Service methods. Callers should compare
// via errors.Is so wrapped variants (fmt.Errorf("%w: ...")) propagate.
var (
	// Fase 1 (BeginUpload) gates.
	ErrJobNotRunning            = errors.New("artifacts: job is not RUNNING")
	ErrWrongJobOwner            = errors.New("artifacts: wrong job owner (worker mismatch)")
	ErrLeaseInvalid             = errors.New("artifacts: lease invalid or expired")
	ErrAttemptMismatch          = errors.New("artifacts: attempt number mismatch")
	ErrRevisionMismatch         = errors.New("artifacts: revision mismatch")
	ErrAttemptNotRenderFinished = errors.New("artifacts: attempt.status != RENDER_FINISHED")
	ErrDuplicateReadyArtifact   = errors.New("artifacts: duplicate READY artifact of same kind for job")

	// Fase 2 (Receive) gates.
	ErrHashMismatch    = errors.New("artifacts: sha256 mismatch (worker-declared != master-computed)")
	ErrSizeMismatch    = errors.New("artifacts: size mismatch")
	ErrBlobWriteFailed = errors.New("artifacts: failed to write blob to staging")

	// Fase 3 (Finalize) + Fase 4 (single-tx CAS) gates.
	ErrUploadNotFound     = errors.New("artifacts: upload session not found")
	ErrUploadStateInvalid = errors.New("artifacts: upload session not in expected state")
	ErrUploadExpired      = errors.New("artifacts: upload session expired")
	ErrTransitionConflict = errors.New("artifacts: state transition conflict (CAS failed)")
	ErrStorageKeyInvalid  = errors.New("artifacts: storage key / sha derivation error")
	ErrBlobPromoteFailed  = errors.New("artifacts: blob promotion to final storage failed")
	ErrOrphanedBlob       = errors.New("artifacts: blob promoted but SQL transaction rolled back")
)
