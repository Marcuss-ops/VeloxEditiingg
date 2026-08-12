package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
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
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
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

// TestWorkerCurrentImageDigest_ReadsLastSuccessfulFromReadModel pins the
// migration-152 contract for the fleet metrics aggregation: current_image_digest
// comes from worker_deployment_state.last_successful_digest (the VERIFIED
// digest), NEVER from a SUCCEEDED deployment_records journal scan.
func TestWorkerCurrentImageDigest_ReadsLastSuccessfulFromReadModel(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	digestX := "sha256:" + strings.Repeat("x", 64)
	digestY := "sha256:" + strings.Repeat("y", 64)

	// A SUCCEEDED journal baseline to X (also advances the read model).
	base := time.Now().UTC().Truncate(time.Second)
	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "metrics-base",
		WorkerID:     "wicket",
		TargetDigest: digestX,
		StartedAt:    base,
		FinishedAt:   &base,
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}

	// (a) Read model carries the verified digest → aggregation returns it.
	got, err := workerCurrentImageDigest(ctx, s.db, "wicket")
	if err != nil {
		t.Fatalf("workerCurrentImageDigest: %v", err)
	}
	if !got.Valid || got.String != digestX {
		t.Fatalf("digest = %v, want valid %q (last_successful_digest from read model)", got, digestX)
	}

	// (b) A NEWER FAILED journal row to Y must NOT move current_image_digest:
	// the journal is history, the read model's last_successful stays X.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "metrics-failed",
		WorkerID:       "wicket",
		PreviousDigest: digestX,
		TargetDigest:   digestY,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.UpdateDeploymentStatus(ctx, "metrics-failed", DeployStatusFailed, base.Add(2*time.Minute)); err != nil {
		t.Fatalf("UpdateDeploymentStatus(FAILED): %v", err)
	}
	got, err = workerCurrentImageDigest(ctx, s.db, "wicket")
	if err != nil {
		t.Fatalf("workerCurrentImageDigest after FAILED: %v", err)
	}
	if !got.Valid || got.String != digestX {
		t.Fatalf("digest after FAILED rollout = %v, want %q (read model last_successful, not journal history)", got, digestX)
	}

	// (c) Read model cleared → invalid (UNKNOWN is honest, no journal backfill).
	if _, err := s.db.ExecContext(ctx, `UPDATE worker_deployment_state SET last_successful_digest='' WHERE worker_id='wicket'`); err != nil {
		t.Fatalf("clear read model: %v", err)
	}
	got, err = workerCurrentImageDigest(ctx, s.db, "wicket")
	if err != nil {
		t.Fatalf("workerCurrentImageDigest after clear: %v", err)
	}
	if got.Valid {
		t.Fatalf("digest after read-model clear = %q valid, want invalid (journal SUCCEEDED=X must not be backfilled)", got.String)
	}

	// (d) No state row at all → invalid, never an error.
	got, err = workerCurrentImageDigest(ctx, s.db, "no-such-worker")
	if err != nil {
		t.Fatalf("workerCurrentImageDigest(missing): %v", err)
	}
	if got.Valid {
		t.Fatalf("digest for missing worker = %q valid, want invalid", got.String)
	}
}
