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
