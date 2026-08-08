package pipeline

import (
	"context"
	"testing"

	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
)

type noopPlanResolver struct{}

func (noopPlanResolver) ResolvePlan(_ context.Context, _, _ string) (*enqueue.ResolvedPlan, error) {
	return &enqueue.ResolvedPlan{
		JobID: "test-job",
		Destinations: []enqueue.PlanDestination{{
			DestinationID: "destination-main",
			Priority:      0,
			RetryBudget:   5,
		}},
	}, nil
}

// openHandlerTestDB opens a temp SQLite store suitable for pipeline handler
// integration tests. It is shared by asset-progress and submission tests;
// catalog retirement must not own this fixture.
func openHandlerTestDB(t *testing.T) *store.SQLiteStore {
	t.Helper()
	s, err := store.NewSQLiteStore(t.TempDir() + "/handler-test.db")
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	return s
}
