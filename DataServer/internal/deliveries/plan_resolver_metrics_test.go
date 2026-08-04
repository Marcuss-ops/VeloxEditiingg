package deliveries

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/store"
	"velox-server/internal/telemetry"
)

func TestResolvePlanRecordsExplicitPlanQueriesAndRejectsMissingPlan(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/resolver-metrics.sqlite")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.DB().Exec(`
		INSERT INTO jobs (job_id, status, revision, max_retries, created_at, updated_at, migrated_at)
		VALUES (?, 'PENDING', 0, 3, ?, ?, ?)`, "planned-job", now, now, now); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if _, err := db.DB().Exec(`
		INSERT INTO delivery_destinations
			(destination_id, provider, name, enabled, created_at, updated_at)
		VALUES (?, ?, ?, 1, ?, ?)`, "fallback-destination", "drive", "Fallback", now, now); err != nil {
		t.Fatalf("seed delivery destination: %v", err)
	}
	if _, err := db.DB().Exec(`
		INSERT INTO job_delivery_plans
			(job_id, destination_id, enabled, priority, retry_budget, created_at, updated_at)
		VALUES (?, ?, 1, 0, 3, ?, ?)`, "planned-job", "fallback-destination", now, now); err != nil {
		t.Fatalf("seed job delivery plan: %v", err)
	}

	resolver := NewSQLiteDeliveryPlanResolver(db.DB())
	ctx, metrics := telemetry.WithEnqueueMetrics(context.Background(), false)

	planned, err := resolver.ResolvePlan(ctx, "planned-job", "")
	if err != nil {
		t.Fatalf("ResolvePlan explicit: %v", err)
	}
	if planned == nil || len(planned.Destinations) != 1 {
		t.Fatalf("explicit plan = %#v, want one destination", planned)
	}

	if _, err := resolver.ResolvePlan(ctx, "missing-plan-job", ""); err == nil {
		t.Fatal("missing explicit plan must fail closed")
	}

	snapshot := metrics.Snapshot()
	if snapshot.ResolverQueries != 2 {
		t.Fatalf("resolver query count = %d, want 2", snapshot.ResolverQueries)
	}
	if snapshot.ResolverPlanQueries != 2 {
		t.Fatalf("plan query count = %d, want 2", snapshot.ResolverPlanQueries)
	}
}
