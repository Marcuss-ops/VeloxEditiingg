package fleet

import (
	"context"
	"errors"
	"fmt"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// WorkerStateExecutor applies an operator-owned worker scheduling state.
//
// Drain and quarantine are flipped synchronously by the admin handler so
// placement excludes the worker before the operation is queued. The
// executor repeats that idempotent write when the ledger reaches RUNNING;
// this makes the persisted operation complete successfully without using a
// production no-op executor and repairs state if a concurrent read observed
// the old value between publish and dispatch.
type WorkerStateExecutor struct {
	Registry *workersreg.Registry
	Kind     string
}

// NewWorkerStateExecutor constructs the concrete executor for drain or
// quarantine. Other operation kinds must use their dedicated executor.
func NewWorkerStateExecutor(registry *workersreg.Registry, kind string) *WorkerStateExecutor {
	return &WorkerStateExecutor{Registry: registry, Kind: kind}
}

// ValidateProductionBackends keeps the production registry fail-closed when
// the worker registry was not wired into the composition root.
func (e *WorkerStateExecutor) ValidateProductionBackends() error {
	if e == nil || e.Registry == nil {
		return errors.New("worker state registry is not wired")
	}
	if e.Kind != OperationKindDrain && e.Kind != OperationKindQuarantine {
		return fmt.Errorf("unsupported worker state operation kind %q", e.Kind)
	}
	return nil
}

// Execute applies the requested state idempotently. It never clears an
// operator-owned flag; resume owns the only path that clears both flags after
// a green Level-D smoke gate.
func (e *WorkerStateExecutor) Execute(ctx context.Context, op *store.Operation) error {
	if err := e.ValidateProductionBackends(); err != nil {
		return fmt.Errorf("%w: %v", ErrExecutorNotConfigured, err)
	}
	if op == nil || op.WorkerID == "" {
		return errors.New("worker state: worker_id is required")
	}
	if e.Kind != op.Op {
		return fmt.Errorf("worker state: executor kind=%q cannot execute op=%q", e.Kind, op.Op)
	}
	if e.Registry.GetWorker(ctx, op.WorkerID) == nil {
		return fmt.Errorf("worker state: worker %q not found", op.WorkerID)
	}
	switch e.Kind {
	case OperationKindDrain:
		if err := e.Registry.SetWorkerDrain(ctx, op.WorkerID, true); err != nil {
			return fmt.Errorf("worker state: set drain: %w", err)
		}
	case OperationKindQuarantine:
		if err := e.Registry.SetWorkerQuarantine(ctx, op.WorkerID, true); err != nil {
			return fmt.Errorf("worker state: set quarantine: %w", err)
		}
	}
	return nil
}
