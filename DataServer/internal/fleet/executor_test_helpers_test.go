package fleet

import (
	"context"

	"velox-server/internal/store"
)

// NoopOperationExecutor is intentionally test-only. Keeping it in a _test.go
// file prevents a false-success executor from entering the production binary.
type NoopOperationExecutor struct{}

func (NoopOperationExecutor) isNoopExecutor() {}

func (NoopOperationExecutor) Execute(_ context.Context, _ *store.Operation) error {
	return nil
}

// NewTestExecutorRegistry provides the explicit all-kinds no-op fixture used
// by controller tests that are not exercising a concrete fleet side effect.
func NewTestExecutorRegistry() *ExecutorRegistry {
	registry := NewExecutorRegistry()
	registry.executors = make(map[string]OperationExecutor, len(AllOperationKinds))
	noop := NoopOperationExecutor{}
	for _, kind := range AllOperationKinds {
		registry.executors[kind] = noop
	}
	return registry
}
