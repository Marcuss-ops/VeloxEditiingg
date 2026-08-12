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

func (r *DeploymentRecordRepository) MarkSucceeded(ctx context.Context, deploymentID string, finishedAt time.Time) error {
	return r.store.updateDeploymentTerminal(ctx, deploymentID, DeployStatusSucceeded, finishedAt, "", false)
}

func (r *DeploymentRecordRepository) MarkFailed(ctx context.Context, deploymentID string, finishedAt time.Time, errMsg string) error {
	return r.store.updateDeploymentTerminal(ctx, deploymentID, DeployStatusFailed, finishedAt, errMsg, false)
}

func (r *DeploymentRecordRepository) MarkDeploymentRolledBack(ctx context.Context, deploymentID string, finishedAt time.Time, rollbackOK bool) error {
	status := DeployStatusRolledBack
	if !rollbackOK {
		status = DeployStatusFailed
	}
	return r.store.updateDeploymentTerminal(ctx, deploymentID, status, finishedAt, "", true)
}

func (r *DeploymentRecordRepository) InsertBaselineDeploymentRecord(ctx context.Context, record DeploymentRecord) error {
	return r.store.InsertBaselineDeploymentRecord(ctx, record)
}
