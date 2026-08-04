package deliveries

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/store"
	"velox-server/internal/telemetry"
)

func TestResolvePlanManyDestinationsUsesExplicitPlanQueries(t *testing.T) {
	const destinationCount = 256

	db, resolver := newManyDestinationResolver(t, destinationCount)
	ctx, metrics := telemetry.WithEnqueueMetrics(context.Background(), false)

	explicit, err := resolver.ResolvePlan(ctx, "many-destinations-job", "")
	if err != nil {
		t.Fatalf("ResolvePlan explicit: %v", err)
	}
	if explicit == nil || len(explicit.Destinations) != destinationCount {
		t.Fatalf("explicit destination count = %d, want %d", len(explicit.Destinations), destinationCount)
	}
	for i := 1; i < len(explicit.Destinations); i++ {
		previous := explicit.Destinations[i-1]
		current := explicit.Destinations[i]
		if previous.Priority > current.Priority ||
			(previous.Priority == current.Priority && previous.DestinationID > current.DestinationID) {
			t.Fatalf("explicit plan lost SQL ordering at index %d: previous=%+v current=%+v", i, previous, current)
		}
	}

	_, err = resolver.ResolvePlan(ctx, "job-without-explicit-plan", "")
	if err == nil {
		t.Fatal("missing explicit plan must fail closed")
	}

	snapshot := metrics.Snapshot()
	if snapshot.ResolverPlanQueries != 2 {
		t.Fatalf("plan query count = %d, want 2", snapshot.ResolverPlanQueries)
	}
	if snapshot.ResolverQueries != 2 {
		t.Fatalf("total resolver query count = %d, want 2", snapshot.ResolverQueries)
	}

	_ = db
}

func BenchmarkResolvePlanManyDestinations(b *testing.B) {
	for _, destinationCount := range []int{256, 1000} {
		b.Run(fmt.Sprintf("explicit_%d", destinationCount), func(b *testing.B) {
			_, resolver := newManyDestinationResolver(b, destinationCount)
			ctx := context.Background()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				plan, err := resolver.ResolvePlan(ctx, "many-destinations-job", "")
				if err != nil {
					b.Fatal(err)
				}
				if len(plan.Destinations) != destinationCount {
					b.Fatalf("destination count = %d, want %d", len(plan.Destinations), destinationCount)
				}
			}
		})

		b.Run(fmt.Sprintf("missing_plan_%d", destinationCount), func(b *testing.B) {
			_, resolver := newManyDestinationResolver(b, destinationCount)
			ctx := context.Background()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := resolver.ResolvePlan(ctx, "job-without-explicit-plan", ""); err == nil {
					b.Fatal("missing explicit plan must fail closed")
				}
			}
		})
	}
}

// newManyDestinationResolver prepares a real migrated SQLite database. The
// setup is intentionally outside the benchmark timer: the benchmark measures
// only ResolvePlan. The explicit job has one plan row per destination, while
// the missing-plan job has no plan rows and must fail closed without reading
// the global destination catalog.
func newManyDestinationResolver(tb testing.TB, destinationCount int) (*store.SQLiteStore, *SQLiteDeliveryPlanResolver) {
	tb.Helper()

	db, err := store.NewSQLiteStore(filepath.Join(tb.TempDir(), "many-destinations.sqlite"))
	if err != nil {
		tb.Fatalf("NewSQLiteStore: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.DB().Exec(`
		INSERT INTO jobs (job_id, status, revision, max_retries, created_at, updated_at, migrated_at)
		VALUES (?, 'PENDING', 0, 3, ?, ?, ?)`, "many-destinations-job", now, now, now); err != nil {
		tb.Fatalf("seed job: %v", err)
	}

	tx, err := db.DB().Begin()
	if err != nil {
		tb.Fatalf("begin seed transaction: %v", err)
	}
	for i := 0; i < destinationCount; i++ {
		// Insert in reverse lexical order so the test proves the SQL
		// destination_id tie-breaker instead of accidentally relying on
		// insertion order.
		destinationID := fmt.Sprintf("destination-%04d", destinationCount-1-i)
		if _, err := tx.Exec(`
			INSERT INTO delivery_destinations
				(destination_id, provider, name, enabled, created_at, updated_at)
			VALUES (?, 'test', ?, 1, ?, ?)`, destinationID, destinationID, now, now); err != nil {
			_ = tx.Rollback()
			tb.Fatalf("seed destination %s: %v", destinationID, err)
		}

		// Reverse priorities force the resolver's ORDER BY to be observable;
		// paired priorities also exercise destination_id as the deterministic
		// tie-breaker for equal priorities.
		priority := destinationCount - (i / 2)
		if _, err := tx.Exec(`
			INSERT INTO job_delivery_plans
				(job_id, destination_id, enabled, priority, retry_budget, created_at, updated_at)
			VALUES (?, ?, 1, ?, 3, ?, ?)`,
			"many-destinations-job", destinationID, priority, now, now); err != nil {
			_ = tx.Rollback()
			tb.Fatalf("seed plan %s: %v", destinationID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		tb.Fatalf("commit seed transaction: %v", err)
	}

	return db, NewSQLiteDeliveryPlanResolver(db.DB())
}
