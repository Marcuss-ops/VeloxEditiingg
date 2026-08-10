package alertengine

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/supervisor"
)

func TestEvaluatePreservesContextCancellation(t *testing.T) {
	engine := New(0, nil)
	engine.AddRule(func(context.Context) (*Alert, error) {
		return nil, context.Canceled
	})

	_, err := engine.Evaluate(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Evaluate error = %v, want context.Canceled", err)
	}
	if errors.Is(err, supervisor.ErrInfrastructure) {
		t.Fatalf("context cancellation must not be classified as infrastructure: %v", err)
	}
}
