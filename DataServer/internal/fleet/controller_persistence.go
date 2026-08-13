package fleet

// controller_persistence.go: terminal-state persistence (retried, detached
// from the executor budget) + stale RUNNING reconciliation for the
// FleetController. Split out of controller.go.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/store"
)

const terminalPersistAttempts = 3

// persistSucceeded retries only the durable terminal transition. It never
// re-runs the executor, so an external side effect is performed once even
// when the ledger connection is temporarily unavailable.

func (c *FleetController) persistSucceeded(operationID string) {
	ctx, cancel := context.WithTimeout(context.Background(), terminalPersistTimeout)
	defer cancel()
	var err error
	for attempt := 0; attempt < terminalPersistAttempts; attempt++ {
		err = c.store.MarkSucceeded(ctx, operationID, time.Now().UTC())
		if err == nil {
			return
		}
		if !sleepTerminalRetry(ctx, attempt) {
			break
		}
	}
	log.Printf("[FLEET] terminal success persistence unresolved %s: %v", operationID, err)
}

func (c *FleetController) persistFailed(operationID, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), terminalPersistTimeout)
	defer cancel()
	var err error
	for attempt := 0; attempt < terminalPersistAttempts; attempt++ {
		err = c.store.MarkFailed(ctx, operationID, time.Now().UTC(), reason)
		if err == nil {
			return
		}
		if !sleepTerminalRetry(ctx, attempt) {
			break
		}
	}
	log.Printf("[FLEET] terminal failure persistence unresolved %s: %v", operationID, err)
}

func sleepTerminalRetry(ctx context.Context, attempt int) bool {
	d := time.Duration(attempt+1) * 25 * time.Millisecond
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// reconcileStaleRunning closes rows left RUNNING after a process/DB failure.
// Update operations route through resumeStaleUpdate, which consults the
// durable read model (worker_deployment_state) instead of blindly marking
// FAILED: when the read model proves the rollout landed on the target digest,
// the operation resumes to SUCCEEDED; otherwise it fails closed with a reason
// that preserves the drift. All other operation kinds keep the conservative
// unknown-outcome failure — replaying their side effects is unsafe.
func (c *FleetController) reconcileStaleRunning(ctx context.Context) {
	running, err := c.store.ListOperations(ctx, "", store.OperationStatusRunning, 0)
	if err != nil {
		log.Printf("[FLEET] list running operations for reconciliation: %v", err)
		return
	}
	cutoff := time.Now().UTC().Add(-c.opTimeout)
	for i := range running {
		op := &running[i]
		if op.StartedAt == nil || op.StartedAt.After(cutoff) {
			continue
		}
		if op.Op == OperationKindUpdate {
			c.resumeStaleUpdate(ctx, op)
			continue
		}
		c.persistFailed(op.OperationID, "stale RUNNING operation; executor outcome unknown")
	}
}

// resumeStaleUpdate recovers a RUNNING update operation whose executor
// outcome was lost in a Master crash/restart. It decides from the durable
// read model (worker_deployment_state) — never by replaying the executor:
//
//   - deployment already terminal SUCCEEDED → the executor had verified and
//     closed the rollout; only the operation's terminal write was lost →
//     SUCCEEDED.
//   - deployment already terminal FAILED/ROLLED_BACK → mirror the outcome.
//   - deployment PENDING at the digest gates (WAITING_READY / VERIFYING_DIGEST
//     — reachable only through the executor's NEW-session gate) AND the
//     authenticated heartbeat observed the target digest (running == desired)
//     → the new session landed the rollout → close via MarkVerifiedSucceeded
//     (the ONLY verified-success writer) → SUCCEEDED.
//   - anything else (no read model, interrupted before the gates, or digest
//     drift) → FAILED, leaving the PENDING row and the drift visible for the
//     operator. An interrupted deployment row is never auto-closed on an
//     unverified digest.
func (c *FleetController) resumeStaleUpdate(ctx context.Context, op *store.Operation) {
	state, err := c.store.GetWorkerDeploymentState(ctx, op.WorkerID)
	if err != nil {
		if errors.Is(err, store.ErrWorkerDeploymentStateNotFound) {
			// The crash hit before the PENDING insert: no deployment row and no
			// read model exist, so the rollout never started. Nothing to resume.
			c.persistFailed(op.OperationID, "stale RUNNING update: no deployment read model (rollout never started)")
			return
		}
		log.Printf("[FLEET] resume update %s: read model: %v", op.OperationID, err)
		return // transient read error: retry on the next tick
	}
	switch state.LastOperationStatus {
	case store.DeployStatusSucceeded:
		// The executor closed the deployment row but crashed before persisting
		// the operation terminal. Mirror the durable outcome — never a second
		// MarkVerifiedSucceeded on an already-terminal row.
		log.Printf("[FLEET] resume update %s: deployment %s already verified SUCCEEDED", op.OperationID, state.LastOperationID)
		c.persistSucceeded(op.OperationID)
		return
	case store.DeployStatusFailed, store.DeployStatusRolledBack:
		c.persistFailed(op.OperationID, fmt.Sprintf("stale RUNNING update: deployment %s already terminal %s", state.LastOperationID, state.LastOperationStatus))
		return
	}
	// In-flight (PENDING): the rollout was interrupted mid-pipeline.
	if !isResumableRolloutPhase(state.LastPhase) {
		c.persistFailed(op.OperationID, fmt.Sprintf("stale RUNNING update: interrupted at phase %q before digest verification", state.LastPhase))
		return
	}
	if !store.DigestRefsEqual(state.RunningDigest, state.DesiredDigest) {
		c.persistFailed(op.OperationID, fmt.Sprintf("stale RUNNING update: resume refused (running %s != desired %s); rollout did not land on the target", state.RunningDigest, state.DesiredDigest))
		return
	}
	// The new session's authenticated heartbeat observed the target digest:
	// the rollout landed. MarkVerifiedSucceeded re-verifies digest == target
	// inside the transition transaction and is the only path that advances
	// last_successful_digest.
	if err := c.store.MarkVerifiedSucceeded(ctx, state.LastOperationID, state.RunningDigest, time.Now().UTC()); err != nil {
		log.Printf("[FLEET] resume update %s: verified close %s: %v", op.OperationID, state.LastOperationID, err)
		c.persistFailed(op.OperationID, fmt.Sprintf("stale RUNNING update: verified close refused for %s: %v", state.LastOperationID, err))
		return
	}
	log.Printf("[FLEET] resume update %s: deployment %s closed SUCCEEDED (running digest == desired)", op.OperationID, state.LastOperationID)
	c.persistSucceeded(op.OperationID)
}

// isResumableRolloutPhase reports whether an interrupted rollout phase can be
// resumed from the read model. Only WAITING_READY and VERIFYING_DIGEST
// qualify: the UpdateExecutor records both only AFTER the container/health
// gates AND the new-session gate passed, so the read-model digest comparison
// carries the executor's session evidence. Earlier phases (DRAINING /
// DEPLOYING / RESTARTING) are refused — the worker may be torn, and a fresh
// operation is the correct retry.
func isResumableRolloutPhase(phase string) bool {
	switch phase {
	case RolloutPhaseWaitingReady, RolloutPhaseVerifyingDigest:
		return true
	}
	return false
}
