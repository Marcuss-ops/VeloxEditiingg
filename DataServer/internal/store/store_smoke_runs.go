package store

// store_smoke_runs.go: re-export + delegation shim for the smokerunstore
// leaf package (internal/smokerunstore), which owns the smoke_runs analytics
// table. The SQL moved out of this god-package into the leaf; the *SQLiteStore
// methods below stay as thin forwarders so the fleet callers (LevelDSmoke
// executor + smoke dashboard) keep the store.SmokeRun surface unchanged.

import (
	"context"
	"time"

	"velox-server/internal/smokerunstore"
)

// SmokeRun + status constants + sentinel re-exported from the leaf.
type SmokeRun = smokerunstore.SmokeRun

const (
	SmokeStatusPending   = smokerunstore.SmokeStatusPending
	SmokeStatusSucceeded = smokerunstore.SmokeStatusSucceeded
	SmokeStatusFailed    = smokerunstore.SmokeStatusFailed
)

var ErrSmokeRunNotFound = smokerunstore.ErrSmokeRunNotFound

// CreateSmokeRunsTableIfNotExists is the test/dev-only bootstrap path.
func (s *SQLiteStore) CreateSmokeRunsTableIfNotExists() error {
	return smokerunstore.CreateSmokeRunsTableIfNotExists(s.db)
}

// InsertSmokeRun persists a new smoke run row.
func (s *SQLiteStore) InsertSmokeRun(ctx context.Context, rec SmokeRun) error {
	return smokerunstore.InsertSmokeRun(ctx, s.db, rec)
}

// MarkSmokeSucceeded atomically transitions a smoke_runs row to SUCCEEDED.
func (s *SQLiteStore) MarkSmokeSucceeded(ctx context.Context, runID string, finishedAt time.Time, durationMs int64, artifactDriveID string) error {
	return smokerunstore.MarkSmokeSucceeded(ctx, s.db, runID, finishedAt, durationMs, artifactDriveID)
}

// MarkSmokeFailed atomically transitions a smoke_runs row to FAILED.
func (s *SQLiteStore) MarkSmokeFailed(ctx context.Context, runID string, finishedAt time.Time, durationMs int64, errMsg string) error {
	return smokerunstore.MarkSmokeFailed(ctx, s.db, runID, finishedAt, durationMs, errMsg)
}

// GetLatestSmokeForWorker returns the latest row for the worker.
func (s *SQLiteStore) GetLatestSmokeForWorker(ctx context.Context, workerID string) (*SmokeRun, error) {
	return smokerunstore.GetLatestSmokeForWorker(ctx, s.db, workerID)
}

// ListRecentSmokesForWorker returns recent rows in started_at DESC order.
func (s *SQLiteStore) ListRecentSmokesForWorker(ctx context.Context, workerID string, limit int) ([]SmokeRun, error) {
	return smokerunstore.ListRecentSmokesForWorker(ctx, s.db, workerID, limit)
}
