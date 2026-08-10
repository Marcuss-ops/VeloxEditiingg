package fleet

import (
	"context"
	"fmt"
	"log"
	"strings"

	"velox-server/internal/store"
	"velox-server/internal/workers"
)

func (e *UpdateExecutor) authenticatedRunningDigest(ctx context.Context, workerID string) (string, error) {
	if e.backend.Runtime == nil {
		return "", nil
	}
	snapshot, err := e.backend.Runtime.GetAuthenticatedRuntimeSnapshot(ctx, workerID)
	if err != nil {
		return "", fmt.Errorf("update: authenticated runtime snapshot: %w", err)
	}
	if snapshot == nil {
		return "", nil
	}
	return strings.TrimSpace(snapshot.DockerImageDigest), nil
}

func (e *UpdateExecutor) bootstrapLedger(ctx context.Context, op *store.Operation, targetDigest string, info *workers.Worker) error {
	runningDigest, err := e.authenticatedRunningDigest(ctx, op.WorkerID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBootstrapUnverifiable, err)
	}
	if runningDigest == "" {
		return fmt.Errorf("%w: authenticated runtime digest missing for worker %s", ErrBootstrapUnverifiable, op.WorkerID)
	}
	if normalizeDigest(runningDigest) != normalizeDigest(targetDigest) {
		return fmt.Errorf("%w: requested=%s running=%s", ErrBootstrapDigestMismatch, targetDigest, runningDigest)
	}
	if !workerConnected(info) {
		return fmt.Errorf("%w: worker %s is not CONNECTED", ErrBootstrapWorkerDisconnected, op.WorkerID)
	}
	if !workerHealthy(info) {
		return fmt.Errorf("%w: worker %s is not HEALTHY", ErrBootstrapWorkerUnhealthy, op.WorkerID)
	}

	baselineRepo, ok := e.backend.Deployments.(BackendDeploymentBaselineRepo)
	if !ok {
		return fmt.Errorf("%w: deployment baseline writer not wired", ErrBootstrapUnverifiable)
	}
	now := e.backend.Now()
	if err := baselineRepo.InsertBaselineDeploymentRecord(ctx, store.DeploymentRecord{
		DeploymentID:   fmt.Sprintf("bootstrap-%s-%d", op.WorkerID, now.UnixNano()),
		WorkerID:       op.WorkerID,
		PreviousDigest: "", // missing provenance is truthful; never invent it
		TargetDigest:   targetDigest,
		StartedAt:      now,
		FinishedAt:     &now,
		Status:         store.DeployStatusSucceeded,
		AppliedBy:      op.RequestedBy,
		IsRollback:     false,
	}); err != nil {
		return fmt.Errorf("%w: insert baseline: %v", ErrBootstrapUnverifiable, err)
	}
	log.Printf("[UPDATE] worker=%s target=%s BOOTSTRAPPED (authenticated runtime; no worker mutation)", op.WorkerID, targetDigest)
	return nil
}

func normalizeDigest(ref string) string {
	ref = strings.ToLower(strings.TrimSpace(ref))
	if at := strings.LastIndexByte(ref, '@'); at >= 0 {
		return ref[at+1:]
	}
	return ref
}

func workerConnected(info *workers.Worker) bool {
	if info == nil {
		return false
	}
	if info.ConnectionState != "" {
		return info.ConnectionState == workers.ConnectionConnected
	}
	return info.ConnectionStatus == workers.StatusConnected && info.SessionActive
}

func workerHealthy(info *workers.Worker) bool {
	if info == nil {
		return false
	}
	if info.HealthState != "" {
		return info.HealthState == workers.HealthHealthy
	}
	return info.Health == workers.WorkerHealthHealthy
}
