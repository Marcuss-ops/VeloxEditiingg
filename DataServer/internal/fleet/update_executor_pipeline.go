package fleet

// update_executor_pipeline.go: runForward / runRollback /
// runRollbackSteps — the forward pipeline + rollback cascade
// of UpdateExecutor. Split out of update_executor.go; the
// struct + Execute live in update_executor.go, timeout definitions
// live in update_executor_defs.go, and per-step helpers live in
// update_executor_steps.go.

import (
	"context"
	"fmt"
	"log"

	"velox-server/internal/store"
)

// runForward executes the forward-only pipeline: cosign,
// docker pull, compose restart, container running, health
// check, master-connection check, smoke, Drive verify.
// Returns nil only on full forward success.
//
// If any step fails, the function returns the wrapped error;
// the caller (Execute) handles the rollback cascade.
func (e *UpdateExecutor) runForward(ctx context.Context, op *store.Operation, targetDigest string) error {
	// (a) Cosign verify the target_digest.
	if err := e.stepCosignVerify(ctx, targetDigest); err != nil {
		return fmt.Errorf("cosign: %w", err)
	}

	// (b) atomically activate the pinned image on the canonical worker
	// runtime. The helper validates, updates worker.env and restarts the
	// single velox-worker.service owner.
	if _, err := e.stepActivateImage(ctx, op.WorkerID, targetDigest); err != nil {
		return fmt.Errorf("activate image: %w", err)
	}

	// (d) ContainerRunning poll.
	if err := e.stepContainerRunning(ctx, op.WorkerID); err != nil {
		return fmt.Errorf("container_running: %w", err)
	}

	// (e) /health/ready poll on the worker.
	if err := e.stepHealthReady(ctx, op.WorkerID); err != nil {
		return fmt.Errorf("health_ready: %w", err)
	}

	// (f) Master-connection check (no SSH — pure registry).
	if err := e.stepMasterConnected(ctx, op.WorkerID); err != nil {
		return fmt.Errorf("master_connection: %w", err)
	}

	// (g) Level D smoke on the worker.
	smokeArtifactID, err := e.stepSmoke(ctx, op.WorkerID)
	if err != nil {
		return err
	}

	// (h) Drive verifier on the smoke artifact.
	if err := e.stepDriveVerify(ctx, smokeArtifactID); err != nil {
		return err
	}

	return nil
}

// runRollback executes the recovery cascade when forward
// failed. The earlier Execute code path has already UPDATed
// the FORWARD deployment_records row to FAILED; this function
// inserts a SECOND distinct row, executes the rollback, and
// transitions THAT row to ROLLED_BACK or FAILED depending on
// outcome.
//
// Returns either nil (forward never started — shouldn't
// happen since the rollback row insert succeeded) or a wrapped
// error encoding the rollback status via ErrRollbackSucceeded
// / ErrRollbackFailed sentinels so the caller (Execute) returns
// the right audit message.
//
// rollbackID is the deployment_id of the new rollback row;
// returned to the caller so ops logs can correlate.
func (e *UpdateExecutor) runRollback(ctx context.Context, op *store.Operation, previousDigest string, forwardErr error) error {
	now := e.backend.Now()
	deploymentID := fmt.Sprintf("rollback-%s-%d", op.WorkerID, now.UnixNano())
	log.Printf("[UPDATE] WORKER=%s forward=%v — cascading ROLLBACK to %s (deployment_id=%s)",
		op.WorkerID, forwardErr, previousDigest, deploymentID)

	// Insert the rollback PENDING row. is_rollback=true from
	// creation so Health() flips to ROLLBACK immediately.
	if err := e.backend.Deployments.InsertDeploymentRecord(ctx, store.DeploymentRecord{
		DeploymentID:   deploymentID,
		WorkerID:       op.WorkerID,
		PreviousDigest: previousDigest,
		TargetDigest:   previousDigest, // ROLLBACK restores previous_digest
		StartedAt:      now,
		Status:         store.DeployStatusPending,
		AppliedBy:      op.RequestedBy,
		IsRollback:     true,
	}); err != nil {
		return fmt.Errorf("rollback: insert PENDING: %w (forward_err=%v)", err, forwardErr)
	}

	// Execute the rollback steps. Each is non-fatal at the
	// audit-dashboard level — even partial rollback is
	// useful informatively for the operator.
	rbErr := e.runRollbackSteps(ctx, op.WorkerID, previousDigest)

	finishedAt := e.backend.Now()
	rollbackOK := rbErr == nil
	if err := e.backend.Deployments.MarkDeploymentRolledBack(ctx, deploymentID, finishedAt, rollbackOK); err != nil {
		log.Printf("[UPDATE] mark ROLLED_BACK for %s: %v (rollback err: %v)", deploymentID, err, rbErr)
		// Even if the DB write fails, surface the rollback
		// outcome to the caller so the audit row's
		// error_message reflects what actually happened.
	}

	if rollbackOK {
		return fmt.Errorf("%w: %v (rollback_ok to %s)", ErrRollbackSucceeded, forwardErr, previousDigest)
	}
	return fmt.Errorf("%w: forward=%v rollback=%v", ErrRollbackFailed, forwardErr, rbErr)
}

// runRollbackSteps executes the four rollback steps. Even
// partial rollback is reported so the operator sees a faithful
// audit message. Returns nil on full recovery.
func (e *UpdateExecutor) runRollbackSteps(ctx context.Context, workerID, previousDigest string) error {
	if _, err := e.stepActivateImage(ctx, workerID, previousDigest); err != nil {
		return fmt.Errorf("rollback activate previous_digest: %w", err)
	}
	if err := e.stepContainerRunning(ctx, workerID); err != nil {
		return fmt.Errorf("rollback container_running: %w", err)
	}
	// /health/ready on the rolled-back image (no re-smoke —
	// the previous digest already passed smoke historically).
	if err := e.stepHealthReady(ctx, workerID); err != nil {
		return fmt.Errorf("rollback health_ready: %w", err)
	}
	return nil
}
