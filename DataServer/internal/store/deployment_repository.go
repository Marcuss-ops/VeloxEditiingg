package store

import (
	"context"
	"time"
)

// DeploymentRecordRepository adapts the deployment_records writer to the
// Fleet executor contract. It is intentionally distinct from SQLiteStore:
// SQLiteStore already owns fleet_operations.MarkSucceeded/MarkFailed, and a
// named adapter prevents those unrelated ledgers from sharing method names or
// accidentally being used interchangeably.
type DeploymentRecordRepository struct {
	store *SQLiteStore
}

func NewDeploymentRecordRepository(s *SQLiteStore) *DeploymentRecordRepository {
	if s == nil {
		return nil
	}
	return &DeploymentRecordRepository{store: s}
}

func (r *DeploymentRecordRepository) GetLatestDeploymentForWorker(ctx context.Context, workerID string) (*DeploymentRecord, error) {
	return r.store.GetLatestDeploymentForWorker(ctx, workerID)
}

func (r *DeploymentRecordRepository) InsertDeploymentRecord(ctx context.Context, record DeploymentRecord) error {
	return r.store.InsertDeploymentRecord(ctx, record)
}

// MarkVerifiedSucceeded is the forward-success writer: the store re-verifies
// observedDigest against the row's target digest inside the transition
// transaction, so last_successful_digest can only advance after a verified
// digest match (VERIFYING_DIGEST phase enforcement).
func (r *DeploymentRecordRepository) MarkVerifiedSucceeded(ctx context.Context, deploymentID, observedDigest string, finishedAt time.Time) error {
	return r.store.MarkVerifiedSucceeded(ctx, deploymentID, observedDigest, finishedAt)
}

func (r *DeploymentRecordRepository) MarkFailed(ctx context.Context, deploymentID string, finishedAt time.Time, errCode, errMsg string) error {
	return r.store.updateDeploymentTerminal(ctx, deploymentID, DeployStatusFailed, finishedAt, errCode, errMsg, false)
}

// RecordDeploymentPhase persists the in-flight rollout phase into the
// worker_deployment_state read model (migration 152) so the admin card can
// show WHERE a rollout is — or where it stopped.
func (r *DeploymentRecordRepository) RecordDeploymentPhase(ctx context.Context, workerID, phase string) error {
	return r.store.RecordDeploymentPhase(ctx, workerID, phase)
}

func (r *DeploymentRecordRepository) MarkDeploymentRolledBack(ctx context.Context, deploymentID string, finishedAt time.Time, rollbackOK bool, errCode string) error {
	status := DeployStatusRolledBack
	if !rollbackOK {
		status = DeployStatusFailed
	}
	return r.store.updateDeploymentTerminal(ctx, deploymentID, status, finishedAt, errCode, "", true)
}

func (r *DeploymentRecordRepository) InsertBaselineDeploymentRecord(ctx context.Context, record DeploymentRecord) error {
	return r.store.InsertBaselineDeploymentRecord(ctx, record)
}
