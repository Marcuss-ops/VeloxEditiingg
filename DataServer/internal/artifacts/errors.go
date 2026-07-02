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

import (
	"errors"
	"fmt"

	"velox-server/internal/store"
)

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

// translateStoreErr re-wraps a store-layer sentinel into the matching
// artifacts sentinel so existing callers using `errors.Is(err,
// artifacts.ErrX)` keep matching without test churn. Multi-%w (Go 1.20+)
// leaves the inner store.ErrX in the wrap chain so future store-side
// unit tests can also target `errors.Is(err, store.ErrX)` directly.
//
// File-1/4 of the migration moved artifact_uploads CRUD to store;
// Service methods return store.Err{X} from s.repo. We translate at
// the Service boundary (in every place that previously returned a raw
// repo error) so the public-facing sentinel is the artifacts one.
//
// Returns nil when err is nil. Returns err unchanged when no sentinel
// matched (so unrelated errors propagate verbatim).
func translateStoreErr(err error) error {
	if err == nil {
		return nil
	}
	// Mapping: store.Err{X} → artifacts.Err{X}. Order is irrelevant —
	// each test branch walks the chain independently.
	switch {
	case errors.Is(err, store.ErrUploadStateInvalid):
		return fmt.Errorf("%w: %w", ErrUploadStateInvalid, err)
	case errors.Is(err, store.ErrTransitionConflict):
		return fmt.Errorf("%w: %w", ErrTransitionConflict, err)
	case errors.Is(err, store.ErrUploadNotFound):
		return fmt.Errorf("%w: %w", ErrUploadNotFound, err)
	case errors.Is(err, store.ErrUploadExpired):
		return fmt.Errorf("%w: %w", ErrUploadExpired, err)
	}
	return err
}
