package deliveries

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/store"
	"velox-server/internal/telemetry"
)

func TestResolvePlanRecordsRealPlanAndFallbackQueries(t *testing.T) {
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

	resolver := NewSQLiteDeliveryPlanResolver(db.DB(), true)
	ctx, metrics := telemetry.WithEnqueueMetrics(context.Background(), false)

	planned, err := resolver.ResolvePlan(ctx, "planned-job", "")
	if err != nil {
		t.Fatalf("ResolvePlan explicit: %v", err)
	}
	if planned == nil || len(planned.Destinations) != 1 {
		t.Fatalf("explicit plan = %#v, want one destination", planned)
	}

	fallback, err := resolver.ResolvePlan(ctx, "fallback-job", "")
	if err != nil {
		t.Fatalf("ResolvePlan fallback: %v", err)
	}
	if fallback == nil || len(fallback.Destinations) != 1 {
		t.Fatalf("fallback plan = %#v, want one destination", fallback)
	}

	snapshot := metrics.Snapshot()
	if snapshot.ResolverQueries != 3 {
		t.Fatalf("resolver query count = %d, want 3 (plans + plans + fallback)", snapshot.ResolverQueries)
	}
	if snapshot.ResolverPlanQueries != 2 {
		t.Fatalf("plan query count = %d, want 2", snapshot.ResolverPlanQueries)
	}
	if snapshot.ResolverFallbackQueries != 1 {
		t.Fatalf("fallback query count = %d, want 1", snapshot.ResolverFallbackQueries)
	}
}
