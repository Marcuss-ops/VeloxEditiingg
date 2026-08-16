package metrics

import (
	"context"
	"path/filepath"
	"testing"

	"velox-server/internal/store"
)

func TestSQLiteLabelResolver_RejectsCorruptPhaseTimestamp(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "metrics.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	if _, err := db.DB().Exec(`INSERT INTO task_phase_timings
		(attempt_id, phase, duration_ms, wall_start, wall_end, phase_order, component, action)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"attempt-corrupt", "render", 10, "not-a-timestamp", "2026-08-12T00:00:01Z", 1, "engine", "render"); err != nil {
		t.Fatalf("seed phase timing: %v", err)
	}

	resolver := NewSQLiteLabelResolver(db.DB())
	if timings, err := resolver.GetPhaseTimingsDetailed(context.Background(), "attempt-corrupt"); err == nil || timings != nil {
		t.Fatalf("GetPhaseTimingsDetailed() = (%v, %v), want nil and timestamp error", timings, err)
	}
}

func TestSQLiteLabelResolver_AllowsMissingPhaseTimestamps(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "metrics-missing-time.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	if _, err := db.DB().Exec(`INSERT INTO task_phase_timings
		(attempt_id, phase, duration_ms, wall_start, wall_end, phase_order, component, action)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"attempt-missing-time", "render", 10, "", "", 1, "engine", "render"); err != nil {
		t.Fatalf("seed phase timing: %v", err)
	}

	resolver := NewSQLiteLabelResolver(db.DB())
	timings, err := resolver.GetPhaseTimingsDetailed(context.Background(), "attempt-missing-time")
	if err != nil {
		t.Fatalf("GetPhaseTimingsDetailed: %v", err)
	}
	if len(timings) != 1 {
		t.Fatalf("got %d phase timings, want 1", len(timings))
	}
	if !timings[0].StartedAt.IsZero() || !timings[0].CompletedAt.IsZero() {
		t.Fatalf("missing timestamps were not preserved as zero times: %#v", timings[0])
	}
}

func TestSQLiteLabelResolver_RejectsCorruptDailyRollupValue(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "rollup-corrupt.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	day := "2026-08-12"
	if _, err := db.DB().Exec(`INSERT INTO task_attempts
		(id, task_id, attempt_number, worker_id, lease_id, status, created_at, updated_at)
		VALUES (?, ?, 1, ?, ?, 'SUCCEEDED', ?, ?)`,
		"attempt-rollup-corrupt", "task-rollup-corrupt", "worker-rollup", "lease-rollup",
		day+"T00:00:00Z", day+"T01:00:00Z"); err != nil {
		t.Fatalf("seed attempt: %v", err)
	}
	if _, err := db.DB().Exec(`INSERT INTO task_attempt_metrics (attempt_id, pipeline_resolve_ms) VALUES (?, ?)`,
		"attempt-rollup-corrupt", "not-a-number"); err != nil {
		t.Fatalf("seed corrupt metric: %v", err)
	}

	resolver := NewSQLiteLabelResolver(db.DB())
	if err := resolver.ComputeDailyRollups(context.Background(), day); err == nil {
		t.Fatal("ComputeDailyRollups() accepted a corrupt metric value")
	}
}
