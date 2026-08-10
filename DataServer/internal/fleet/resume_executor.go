package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// ErrResumeSmokeFailed marks a resume that was refused because the worker's
// Level D smoke gate was not green. The worker remains excluded from
// placement until a later resume succeeds.
var ErrResumeSmokeFailed = errors.New("resume: smoke gate failed")

// ResumeBackend bundles the narrow dependencies required by ResumeExecutor.
// SmokeExecutor must be the real Level D executor, not the read-only
// SmokeRunHealthChecker: Resume owns a fresh smoke run and only clears
// exclusion flags after that executor returns nil.
type ResumeBackend struct {
	Registry      *workersreg.Registry
	SmokeExecutor OperationExecutor
	SmokeAssetID  string
}

// ResumeExecutor is the sole writer that can make a drained or quarantined
// worker eligible again. It runs a fresh Level D smoke first, then clears both
// exclusion flags. A stale persisted smoke result is never sufficient.
type ResumeExecutor struct {
	backend ResumeBackend
}

func NewResumeExecutor(backend ResumeBackend) *ResumeExecutor {
	return &ResumeExecutor{backend: backend}
}

func (e *ResumeExecutor) Execute(ctx context.Context, op *store.Operation) error {
	if op == nil {
		return errors.New("resume: nil operation")
	}
	if op.WorkerID == "" {
		return errors.New("resume: worker_id empty")
	}
	if e == nil || e.backend.Registry == nil {
		return errors.New("resume: worker registry not wired")
	}
	if e.backend.SmokeExecutor == nil {
		// The operation cannot run without a fresh Level D gate. Clear
		// only the transient marker; the original drain/quarantine
		// exclusion remains in force. Surface cleanup failure rather
		// than silently leaving the worker stuck in RESUMING.
		if cleanupErr := e.backend.Registry.ClearWorkerResumingIfOwner(ctx, op.WorkerID, op.OperationID); cleanupErr != nil {
			return fmt.Errorf("%w: Level D smoke executor not wired; cleanup failed: %v", ErrResumeSmokeFailed, cleanupErr)
		}
		return fmt.Errorf("%w: Level D smoke executor not wired", ErrResumeSmokeFailed)
	}
	info := e.backend.Registry.GetWorker(ctx, op.WorkerID)
	if info == nil {
		return fmt.Errorf("resume: worker %q not found", op.WorkerID)
	}
	if !info.Drain && !info.Quarantined && !info.Resuming {
		return nil
	}
	// The nested operation is executed directly so Resume waits for the
	// actual Level D pipeline. Its smoke_runs row is the durable evidence;
	// merely reading an older SUCCEEDED row is intentionally impossible.
	payload, err := json.Marshal(SmokePayload{
		AssetID: smokeAssetID(e.backend.SmokeAssetID),
		Reason:  "resume Level D smoke gate",
	})
	if err != nil {
		return fmt.Errorf("%w: build smoke payload: %v", ErrResumeSmokeFailed, err)
	}
	smokeOp := &store.Operation{
		OperationID: op.OperationID + ":resume-smoke",
		WorkerID:    op.WorkerID,
		Op:          OperationKindSmoke,
		RequestedBy: op.RequestedBy,
		Reason:      "resume Level D smoke gate",
		Payload:     payload,
		Status:      store.OperationStatusRunning,
		QueuedAt:    op.QueuedAt,
	}
	if smokeErr := e.backend.SmokeExecutor.Execute(ctx, smokeOp); smokeErr != nil {
		// Preserve the original drain/quarantine flags and clear only
		// the transient RESUMING marker. Placement therefore remains
		// excluded after any Level D failure. Surface cleanup failure
		// because a durable RESUMING gate must never be silent.
		if cleanupErr := e.backend.Registry.ClearWorkerResumingIfOwner(ctx, op.WorkerID, op.OperationID); cleanupErr != nil {
			return fmt.Errorf("%w: %v; cleanup failed: %v", ErrResumeSmokeFailed, smokeErr, cleanupErr)
		}
		return fmt.Errorf("%w: %v", ErrResumeSmokeFailed, smokeErr)
	}
	// Clear quarantine first, then drain. If either write fails, retain
	// RESUMING and the remaining exclusion flag so placement stays
	// fail-closed and the operation can be retried safely.
	if err := e.backend.Registry.CompleteResume(ctx, op.WorkerID, op.OperationID); err != nil {
		return fmt.Errorf("resume: complete green smoke transition: %w", err)
	}
	return nil
}

func smokeAssetID(configured string) string {
	if trimmed := strings.TrimSpace(configured); trimmed != "" {
		return trimmed
	}
	return "asset-canary-001"
}
