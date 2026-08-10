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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/store"
)

// FreshSmokeRunner adapts the real Level-D operation executor to the update
// pipeline. It always executes a new smoke operation before reading its
// durable result; an older smoke can never satisfy a post-update gate.
type FreshSmokeRunner struct {
	executor OperationExecutor
	runs     BackendSmokeRuns
	assetID  string
}

func NewFreshSmokeRunner(executor OperationExecutor, runs BackendSmokeRuns) *FreshSmokeRunner {
	return NewFreshSmokeRunnerWithAsset(executor, runs, "")
}

// NewFreshSmokeRunnerWithAsset binds the production smoke gate to an
// operator-configured READY asset. The legacy default remains only for
// isolated callers/tests; production wiring must provide VELOX_SMOKE_ASSET_ID.
func NewFreshSmokeRunnerWithAsset(executor OperationExecutor, runs BackendSmokeRuns, assetID string) *FreshSmokeRunner {
	return &FreshSmokeRunner{executor: executor, runs: runs, assetID: strings.TrimSpace(assetID)}
}

func (r *FreshSmokeRunner) RunLevelD(ctx context.Context, workerID string) (string, error) {
	if r == nil || r.executor == nil || r.runs == nil {
		return "", fmt.Errorf("fresh smoke runner: real executor not wired")
	}
	payload, err := json.Marshal(SmokePayload{AssetID: smokeAssetID(r.assetID), Reason: "worker update Level D smoke gate"})
	if err != nil {
		return "", fmt.Errorf("fresh smoke payload: %w", err)
	}
	op := &store.Operation{
		OperationID: fmt.Sprintf("update-smoke-%s-%d", workerID, time.Now().UnixNano()),
		WorkerID:    workerID, Op: OperationKindSmoke, RequestedBy: "update-executor",
		Reason: "worker update Level D smoke gate", Payload: payload,
		Status: store.OperationStatusRunning, QueuedAt: time.Now().UTC(),
	}
	if err := r.executor.Execute(ctx, op); err != nil {
		return "", err
	}
	return NewSmokeRunHealthChecker(r.runs).RunLevelD(ctx, workerID)
}

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
	return c.runLevelD(ctx, workerID, time.Time{})
}

// RunLevelDAfter is the resume-specific freshness gate. A successful
// smoke recorded before the resume operation was queued is stale and
// cannot make a worker eligible again.
func (c *SmokeRunHealthChecker) RunLevelDAfter(ctx context.Context, workerID string, notBefore time.Time) (string, error) {
	return c.runLevelD(ctx, workerID, notBefore)
}

func (c *SmokeRunHealthChecker) runLevelD(ctx context.Context, workerID string, notBefore time.Time) (string, error) {
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
	if !notBefore.IsZero() && !run.StartedAt.After(notBefore) {
		return "", fmt.Errorf("latest smoke predates resume operation (run_id=%s, started_at=%s, queued_at=%s)", run.RunID, run.StartedAt.Format(time.RFC3339), notBefore.Format(time.RFC3339))
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
