package enqueue

import (
	"context"
	"strconv"
	"testing"

	"velox-server/internal/deliveries"
	"velox-server/internal/store"
)

// newTestEnqueuer creates an Enqueuer backed by an in-memory SQLite store
// for integration-level testing of the atomic creation path. The
// PlanResolver is the happy-path mock so non-precondition tests do not
// have to configure it. Precondition tests use a custom mockPlanResolver
// directly via NewEnqueuer.
//
// Seeds "drive-main" + "destination-main" into delivery_destinations so
// store.validateDeliveryDestinationTx (called inside AtomicJobTaskCreator
// during the per-destination JWT check) has a stable set to query
// against. The seed is also the reason helper-using tests like
// TestEnqueueCreatesJobAndTaskAtomically / TestEnqueueDefaultsPreserved /
// TestEnqueueWithForwardingKey that pass `delivery_plan: [{destination_id:
// "drive-main", ...}]` stop failing with "destination_id \"drive-main\"
// does not exist" — the seeded row makes validateDeliveryDestinationTx
// return nil for those ids.
func newTestEnqueuer(t *testing.T) *Enqueuer {
	t.Helper()
	db, err := store.NewSQLiteStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	seedDestinations(t, db, map[string]bool{
		"drive-main": true,
	})
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	return NewEnqueuer(atomic, jobRepo, nil, newTestPlanResolver())
}

// seedDestinations seeds the delivery_destinations table with the
// given (id, enabled) pairs so the per-destination validator
// (store.validateDeliveryDestinationTx, called inside the atomic
// creator's parse-time plan check) has a stable set to query against.
//
// Mirrors the helper of the same name in
// DataServer/internal/store/atomic_job_task_test.go. Duplicated here
// (not exported) because:
//   - importing store's test internals would couple this file to
//     non-production symbols,
//   - tests in this package need different default seeds than tests
//     in store (e.g. "drive-main" vs the multi-dest IDs the store
//     package tests exercise).
func seedDestinations(t *testing.T, db *store.SQLiteStore, pairs map[string]bool) {
	t.Helper()
	for id, enabled := range pairs {
		if err := db.InsertDeliveryDestination(&store.DeliveryDestination{
			DestinationID: id,
			Provider:      "drive",
			Name:          id,
			Enabled:       enabled,
		}); err != nil {
			t.Fatalf("seed destination %q: %v", id, err)
		}
	}
}

// =============================================================================
// Tests for the enqueue-time plan resolver. The mockPlanResolver is a
// hand-rolled PlanResolver that returns a configured plan/error without
// any DB interaction so the precondition can be unit-tested in isolation
// from the deliveries stack.
// =============================================================================

// mockPlanResolver implements PlanResolver for tests. It returns the
// configured plan or error verbatim, with a defensive copy of the
// destinations slice so tests cannot accidentally mutate shared state.
type mockPlanResolver struct {
	plan *ResolvedPlan
	err  error
}

func (m *mockPlanResolver) ResolvePlan(_ context.Context, _, _ string) (*ResolvedPlan, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.plan == nil {
		return nil, nil
	}
	out := &ResolvedPlan{JobID: m.plan.JobID}
	out.Destinations = append(out.Destinations, m.plan.Destinations...)
	return out, nil
}

// newTestPlanResolver returns a happy-path PlanResolver (single
// destination, retry_budget=5) used by the existing non-precondition
// tests. Precondition tests construct their own mockPlanResolver to
// exercise the rejection paths.
func newTestPlanResolver() PlanResolver {
	return &mockPlanResolver{
		plan: &ResolvedPlan{
			JobID: "test-job",
			Destinations: []PlanDestination{
				{DestinationID: "destination-main", Priority: 0, RetryBudget: 5},
			},
		},
	}
}

// =============================================================================
// Integration test that uses the REAL DB-backed
// *deliveries.SQLiteDeliveryPlanResolver (not a hand-rolled mock) via
// a local planResolverAdapter. This proves the precondition reads
// from the real job_delivery_plans table, propagates max(retry_budget)
// to job.MaxRetries, and surfaces the production ErrNoExplicitPlan
// path when no plan rows exist. The adapter mirrors the production
// one in cmd/server/bootstrap_modules.go to avoid an import cycle
// between enqueue and deliveries.
// =============================================================================

// planResolverAdapter bridges *deliveries.SQLiteDeliveryPlanResolver to
// enqueue.PlanResolver for the integration test. It is the test-side
// twin of deliveryPlanResolverAdapter in cmd/server/bootstrap_modules.go;
// the duplication is intentional (composition-root adapter for prod, local
// adapter for the test) to keep the enqueue package decoupled from
// deliveries.
type planResolverAdapter struct {
	inner *deliveries.SQLiteDeliveryPlanResolver
}

func (a *planResolverAdapter) ResolvePlan(ctx context.Context, jobID, artifactID string) (*ResolvedPlan, error) {
	if a == nil || a.inner == nil {
		return nil, nil
	}
	plan, err := a.inner.ResolvePlan(ctx, jobID, artifactID)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}
	out := &ResolvedPlan{JobID: plan.JobID}
	for _, d := range plan.Destinations {
		out.Destinations = append(out.Destinations, PlanDestination{
			DestinationID: d.DestinationID,
			Priority:      d.Priority,
			RetryBudget:   d.RetryBudget,
		})
	}
	return out, nil
}

// asFloat parses a YAML/JSON-mapped value into float64, accepting the
// transport encodings RoundTrippers produce when the source payload came
// in from JSON, YAML, or a typed struct. Returns 0 on unknown types so
// the caller can still surface "duration not set" via a positive vs.
// zero check rather than crashing the test on map-iteration drift.
//
// Used by clip-payload tests where duration_seconds may arrive as
// int, int64, float64, or string depending on the upstream Codec.
func asFloat(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	default:
		return 0
	}
}
