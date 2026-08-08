package fleet

// update_executor_steps.go: per-step helpers for UpdateExecutor.
// Split out of update_executor.go; the struct + Execute live in
// update_executor.go and the forward/rollback orchestration in
// update_executor_pipeline.go.
//
// Each helper wraps context.WithTimeout + the BackendXxx
// surface call. Failures are wrapped with the step name so
// the audit row's error_message reads "<step>: <err>".

import (
	"context"
	"errors"
	"fmt"
)

func (e *UpdateExecutor) stepCosignVerify(parent context.Context, ref string) error {
	if e.backend.Cosign == nil {
		return errors.New("cosign verifier not wired")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutCosign)
	defer cancel()
	return e.backend.Cosign.Verify(ctx, ref)
}

func (e *UpdateExecutor) stepActivateImage(parent context.Context, workerID, image string) (string, error) {
	if e.backend.Docker == nil {
		return "", errors.New("docker client not wired")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutDockerPull)
	defer cancel()
	return e.backend.Docker.ActivateImage(ctx, workerID, image)
}

func (e *UpdateExecutor) stepContainerRunning(parent context.Context, workerID string) error {
	if e.backend.Docker == nil {
		return ErrContainerUnhealthy
	}
	ctx, cancel := context.WithTimeout(parent, timeoutContainerCheck)
	defer cancel()
	running, err := e.backend.Docker.ContainerRunning(ctx, workerID)
	if err != nil {
		return fmt.Errorf("%w: check failed: %v", ErrContainerUnhealthy, err)
	}
	if !running {
		return fmt.Errorf("%w: container not running", ErrContainerUnhealthy)
	}
	return nil
}

func (e *UpdateExecutor) stepHealthReady(parent context.Context, workerID string) error {
	// Production: `curl -fsS --max-time 5 http://127.0.0.1:8081/health/ready`
	// over the shared SSH client (loopback on the worker host).
	// Tests substitute a stub that returns a canned status. The
	// implementation is in the BackendDockerClient surface
	// (ActivateImage / ContainerRunning) — for
	// /health/ready we re-use the SSH surface since the health
	// endpoint is a loopback curl on the worker.
	if e.backend.SSHCmd == nil {
		return errors.New("ssh client not wired (cannot poll /health/ready)")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutHealthReady)
	defer cancel()
	// Production wires a real SSH client that curls the loopback
	// health endpoint. Stub Backends for tests will return
	// canned (=non-empty output + nil error = healthy).
	_, err := e.backend.SSHCmd.Run(ctx, workerID,
		"curl -fsS --max-time 5 http://127.0.0.1:8081/health/ready")
	return err
}

func (e *UpdateExecutor) stepMasterConnected(parent context.Context, workerID string) error {
	if e.backend.Registry == nil {
		return errors.New("registry gater not wired (cannot verify master connection)")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutMasterCheck)
	defer cancel()
	info, err := e.backend.Registry.GetWorker(ctx, workerID)
	if err != nil {
		return fmt.Errorf("registry lookup: %w", err)
	}
	if info == nil {
		return errors.New("worker missing from registry (master disconnected)")
	}
	if !info.SessionActive {
		return errors.New("session not active (master disconnected)")
	}
	if info.LastHB == "" {
		return errors.New("no heartbeat yet (master connection not established)")
	}
	return nil
}

func (e *UpdateExecutor) stepSmoke(parent context.Context, workerID string) (string, error) {
	if e.backend.Smoke == nil {
		return "", fmt.Errorf("%w: smoke runner not wired", ErrSmokeFailed)
	}
	ctx, cancel := context.WithTimeout(parent, timeoutSmokeRun)
	defer cancel()
	artifactID, err := e.backend.Smoke.RunLevelD(ctx, workerID)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSmokeFailed, err)
	}
	return artifactID, nil
}

func (e *UpdateExecutor) stepDriveVerify(parent context.Context, driveFileID string) error {
	if e.backend.Drive == nil {
		return errors.New("drive verifier not wired")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutDriveVerify)
	defer cancel()
	if err := e.backend.Drive.VerifyDelivery(ctx, driveFileID, 0 /*expectedBytes: caller may inject via deps upgrade*/); err != nil {
		// Map the dep's error to our canonical sentinels so the
		// audit dashboard's classification logic stays stable.
		if errors.Is(err, ErrDriveDeliverySize) || errors.Is(err, ErrDriveDeliveryMissing) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrDriveDeliveryMissing, err)
	}
	return nil
}
