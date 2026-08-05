package fleet

import (
	"context"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// ErrResumeSmokeFailed marks a resume that was refused because the worker's
// Level D smoke gate was not green. The worker remains excluded from
// placement until a later resume succeeds.
var ErrResumeSmokeFailed = errors.New("resume: smoke gate failed")

// ResumeBackend bundles the narrow dependencies required by ResumeExecutor.
// The registry is intentionally the same in-process registry used by
// placement, so clearing the flags is atomic with the read model's writes.
type ResumeBackend struct {
	Registry *workersreg.Registry
	Smoke    BackendSmokeRunner
}

// ResumeExecutor is the sole writer that can make a drained or quarantined
// worker eligible again. It verifies the persisted Level D smoke result first,
// then clears both exclusion flags.
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
	if e.backend.Smoke == nil {
		return fmt.Errorf("%w: smoke runner not wired", ErrResumeSmokeFailed)
	}
	info := e.backend.Registry.GetWorker(ctx, op.WorkerID)
	if info == nil {
		return fmt.Errorf("resume: worker %q not found", op.WorkerID)
	}
	if !info.Drain && !info.Quarantined {
		return nil
	}
	if op.QueuedAt.IsZero() {
		return fmt.Errorf("%w: operation queued_at is required for freshness gate", ErrResumeSmokeFailed)
	}
	freshSmoke, ok := e.backend.Smoke.(interface {
		RunLevelDAfter(context.Context, string, time.Time) (string, error)
	})
	if !ok {
		return fmt.Errorf("%w: smoke runner does not support freshness gate", ErrResumeSmokeFailed)
	}
	_, smokeErr := freshSmoke.RunLevelDAfter(ctx, op.WorkerID, op.QueuedAt)
	if smokeErr != nil {
		return fmt.Errorf("%w: %v", ErrResumeSmokeFailed, smokeErr)
	}
	// Clear quarantine first, then drain. If the second write fails, Drain
	// remains true and placement stays fail-closed until retry.
	if err := e.backend.Registry.SetWorkerQuarantine(ctx, op.WorkerID, false); err != nil {
		return fmt.Errorf("resume: clear quarantine: %w", err)
	}
	if err := e.backend.Registry.SetWorkerDrain(ctx, op.WorkerID, false); err != nil {
		return fmt.Errorf("resume: clear drain: %w", err)
	}
	return nil
}
