// Package fleet — Step 12/15 SmokeRunHealthChecker.
//
// SmokeRunHealthChecker implements BackendSmokeRunner by reading the
// LATEST smoke run from the smoke_runs analytics table. It does NOT
// trigger a new smoke — the on-demand POST /api/v1/admin/workers/
// {id}/smoke endpoint drives new smoke runs via the FleetController
// and LevelDSmokeExecutor. The health probe (GET .../health?level=D)
// reads the persisted last-run result so the operator sees the real
// smoke state without waiting for a new execution.
//
// Wired into HealthProbeDeps in cmd/server/bootstrap_composition.go
// at Step 12/15; before this step the Smoke field was nil (audit-only
// "smoke runner not wired").
package fleet

import (
	"context"
	"fmt"

	"velox-server/internal/store"
)

// SmokeRunHealthChecker reads the latest smoke run for a worker from
// the smoke_runs table and surfaces it as a BackendSmokeRunner so
// ProbeLevelD can report the real status.
type SmokeRunHealthChecker struct {
	runs BackendSmokeRuns
}

// Compile-time assertion: SmokeRunHealthChecker satisfies BackendSmokeRunner.
var _ BackendSmokeRunner = (*SmokeRunHealthChecker)(nil)

// NewSmokeRunHealthChecker returns a ready-to-use checker.
func NewSmokeRunHealthChecker(runs BackendSmokeRuns) *SmokeRunHealthChecker {
	return &SmokeRunHealthChecker{runs: runs}
}

// RunLevelD implements BackendSmokeRunner by returning the latest
// smoke run's artifact_drive_id on SUCCEEDED, or an error describing
// the terminal state (FAILED / PENDING / never-run).
func (c *SmokeRunHealthChecker) RunLevelD(ctx context.Context, workerID string) (string, error) {
	if c == nil || c.runs == nil {
		return "", fmt.Errorf("smoke health checker: smoke_runs backend not wired")
	}
	run, err := c.runs.GetLatestSmokeForWorker(ctx, workerID)
	if err != nil {
		if err == store.ErrSmokeRunNotFound {
			return "", fmt.Errorf("no smoke runs recorded for worker %q", workerID)
		}
		return "", fmt.Errorf("smoke health checker: query: %w", err)
	}
	switch run.Status {
	case store.SmokeStatusSucceeded:
		if run.ArtifactDriveID != "" {
			return run.ArtifactDriveID, nil
		}
		return "", fmt.Errorf("latest smoke SUCCEEDED but artifact_drive_id is empty (run_id=%s)", run.RunID)
	case store.SmokeStatusPending:
		return "", fmt.Errorf("latest smoke is still PENDING (run_id=%s, started_at=%s)", run.RunID, run.StartedAt.Format("2006-01-02T15:04:05Z"))
	case store.SmokeStatusFailed:
		return "", fmt.Errorf("latest smoke FAILED (run_id=%s, error=%s)", run.RunID, run.ErrorMessage)
	default:
		return "", fmt.Errorf("unknown smoke status %q (run_id=%s)", run.Status, run.RunID)
	}
}
