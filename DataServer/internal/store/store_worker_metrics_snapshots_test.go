package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// store_worker_metrics_snapshots_test.go — minimal smoke tests
// for Step 13/15 fleet telemetry store layer. Mirrors the
// existing test patterns for store_smoke_runs / store_deployment_records.
// Coverage is intentionally narrow (the gate's per-commit
// scope is one step at a time per AGENTS.md §1).
//
//   TestCreateWorkerMetricsSnapshotsTableIfNotExists_Idempotent
//     DDL is idempotent across two consecutive calls.
//
//   TestInsertWorkerMetricsSnapshot_GetLatestForWorker_RoundTrip
//     The full insert → get_latest path stores + retrieves
//     snapshotted_at + nullable fields faithfully.
//
//   TestGetLatestWorkerMetricsForWorker_NotFoundReturnsSentinel
//     Empty workers table surfaces ErrWorkerMetricsSnapshotNotFound,
//     so the handler maps it to 404 cleanly (per the handler
//     docblock in admin_workers_metrics_aggregator_handler.go).
//
//   TestInsertWorkerMetricsSnapshot_EmptyWorkerIDFails
//     Empty worker_id surfaces a defensive error rather than
//     letting CHECK(length() > 0) fail with a SQL-level
//     surprise.

// openTestStore spins up an in-memory SQLite store with the
// worker_metrics_snapshots table DDL'd in. Tests that don't
// need the full store layer use this shortcut.
func openTestStoreWithMetricsSnapshots(t *testing.T) *SQLiteStore {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &SQLiteStore{db: db}
	if err := s.CreateWorkerMetricsSnapshotsTableIfNotExists(); err != nil {
		t.Fatalf("create worker_metrics_snapshots table: %v", err)
	}
	return s
}

func TestCreateWorkerMetricsSnapshotsTableIfNotExists_Idempotent(t *testing.T) {
	s := openTestStoreWithMetricsSnapshots(t)
	// Second call must not error + must leave the table usable.
	if err := s.CreateWorkerMetricsSnapshotsTableIfNotExists(); err != nil {
		t.Errorf("second DDL call must be idempotent, got %v", err)
	}
	var name string
	if err := s.db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name='worker_metrics_snapshots'`,
	).Scan(&name); err != nil {
		t.Fatalf("verify table exists: %v", err)
	}
	if name != "worker_metrics_snapshots" {
		t.Errorf("table name = %q, want worker_metrics_snapshots", name)
	}
}

func TestInsertWorkerMetricsSnapshot_GetLatestForWorker_RoundTrip(t *testing.T) {
	s := openTestStoreWithMetricsSnapshots(t)
	now := time.Date(2026, 7, 28, 19, 0, 0, 0, time.UTC)
	want := WorkerMetricsSnapshot{
		WorkerID:            "velox-worker-13197",
		SnapshottedAt:       now,
		AvailabilityPercent: sql.NullFloat64{Float64: 99.7, Valid: true},
		Disconnects:         2,
		JobsSucceeded:       17,
		JobsFailed:          1,
		FailureRate:         sql.NullFloat64{Float64: 5.55, Valid: true},
		Restarts:            2,
		RollbackCount:       1,
		CurrentImageDigest:  sql.NullString{String: "sha256:abc123", Valid: true},
		LastSmokeStatus:     sql.NullString{String: "SUCCEEDED", Valid: true},
		QueueMsAvg:          1200,
		RenderMsAvg:         1850,
		RenderMsP95:         2100,
		DownloadMsAvg:       0, // RESERVED Step 14+
	}
	if err := s.InsertWorkerMetricsSnapshot(context.Background(), want); err != nil {
		t.Fatalf("InsertWorkerMetricsSnapshot: %v", err)
	}
	got, err := s.GetLatestWorkerMetricsForWorker(context.Background(), "velox-worker-13197")
	if err != nil {
		t.Fatalf("GetLatestWorkerMetricsForWorker: %v", err)
	}
	if got.WorkerID != want.WorkerID {
		t.Errorf("WorkerID = %q, want %q", got.WorkerID, want.WorkerID)
	}
	if !got.SnapshottedAt.Equal(want.SnapshottedAt) {
		t.Errorf("SnapshottedAt = %v, want %v", got.SnapshottedAt, want.SnapshottedAt)
	}
	if got.AvailabilityPercent.Float64 != want.AvailabilityPercent.Float64 {
		t.Errorf("AvailabilityPercent = %v, want %v",
			got.AvailabilityPercent.Float64, want.AvailabilityPercent.Float64)
	}
	if got.JobsSucceeded != want.JobsSucceeded {
		t.Errorf("JobsSucceeded = %d, want %d", got.JobsSucceeded, want.JobsSucceeded)
	}
	if got.FailureRate.Float64 != want.FailureRate.Float64 {
		t.Errorf("FailureRate = %v, want %v",
			got.FailureRate.Float64, want.FailureRate.Float64)
	}
	if got.LastSmokeStatus.String != want.LastSmokeStatus.String {
		t.Errorf("LastSmokeStatus = %q, want %q",
			got.LastSmokeStatus.String, want.LastSmokeStatus.String)
	}
	if got.RenderMsP95 != want.RenderMsP95 {
		t.Errorf("RenderMsP95 = %d, want %d", got.RenderMsP95, want.RenderMsP95)
	}
}

func TestGetLatestWorkerMetricsForWorker_NotFoundReturnsSentinel(t *testing.T) {
	s := openTestStoreWithMetricsSnapshots(t)
	_, err := s.GetLatestWorkerMetricsForWorker(context.Background(), "ghost-worker")
	if !errors.Is(err, ErrWorkerMetricsSnapshotNotFound) {
		t.Errorf("want ErrWorkerMetricsSnapshotNotFound, got %v", err)
	}
}

func TestInsertWorkerMetricsSnapshot_EmptyWorkerIDFails(t *testing.T) {
	s := openTestStoreWithMetricsSnapshots(t)
	err := s.InsertWorkerMetricsSnapshot(context.Background(), WorkerMetricsSnapshot{
		WorkerID:      "",
		SnapshottedAt: time.Now().UTC(),
	})
	if err == nil {
		t.Errorf("empty WorkerID must surface a defensive error")
	}
}
