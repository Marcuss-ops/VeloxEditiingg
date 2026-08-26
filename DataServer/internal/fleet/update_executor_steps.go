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
	"strings"
	"time"
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

// stepWaitNewSession implements the WAITING_READY phase gated on the NEW
// authenticated session. After the restart the rollout must NOT advance on a
// readiness signal from the pre-restart session: the authenticated runtime
// snapshot must be bound to a session DIFFERENT from the one observed before
// the restart (a fresh Hello proves the worker came back up), AND the
// registry must report the session active with a fresh heartbeat. A stale S1
// READY — cached or delayed — can never satisfy this gate.
func (e *UpdateExecutor) stepWaitNewSession(parent context.Context, workerID, preRestartSessionID string) error {
	if e.backend.Registry == nil {
		return errors.New("registry gater not wired (cannot verify new session)")
	}
	if e.backend.Runtime == nil {
		return errors.New("authenticated runtime reader not wired (cannot verify new session)")
	}
	budget := e.waitReadyTimeout
	if budget <= 0 {
		budget = timeoutWaitReady
	}
	deadline := time.Now().Add(budget)
	pollTicker := time.NewTicker(time.Second)
	defer pollTicker.Stop()
	for {
		if e.stepMasterConnected(parent, workerID) == nil {
			if snap, snapErr := e.backend.Runtime.GetAuthenticatedRuntimeSnapshot(parent, workerID); snapErr == nil &&
				snap != nil && snap.SessionID != "" && snap.SessionID != preRestartSessionID {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return errors.New("worker did not reconnect on a NEW authenticated session within budget")
		}
		select {
		case <-parent.Done():
			return fmt.Errorf("wait_for_new_session: ctx cancelled: %w", parent.Err())
		case <-pollTicker.C:
		}
	}
}

// stepVerifyDigest implements the VERIFYING_DIGEST phase: the digest the
// worker advertises through its authenticated session (the SAME snapshot the
// new-session gate just validated) must match the operation target. A
// mismatch fails the forward pipeline with ErrDigestMismatch — the caller
// marks the row FAILED with error_code `digest_mismatch` and runs the
// rollback cascade. running_digest is never written here: the control plane
// does not fabricate observed state.
func (e *UpdateExecutor) stepVerifyDigest(parent context.Context, workerID, targetDigest string) (string, error) {
	if e.backend.Runtime == nil {
		return "", errors.New("verify_digest: authenticated runtime reader not wired")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutDrainVerify)
	defer cancel()
	snapshot, err := e.backend.Runtime.GetAuthenticatedRuntimeSnapshot(ctx, workerID)
	if err != nil {
		return "", fmt.Errorf("verify_digest: authenticated snapshot: %w", err)
	}
	if snapshot == nil || strings.TrimSpace(snapshot.DockerImageDigest) == "" {
		return "", errors.New("verify_digest: no authenticated running digest observed")
	}
	observed := normalizeDigest(snapshot.DockerImageDigest)
	if observed != normalizeDigest(targetDigest) {
		return "", fmt.Errorf("%w: expected=%s observed=%s", ErrDigestMismatch, targetDigest, snapshot.DockerImageDigest)
	}
	return snapshot.DockerImageDigest, nil
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
