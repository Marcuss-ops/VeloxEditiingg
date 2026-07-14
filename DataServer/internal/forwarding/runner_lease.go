// Package forwarding provides the CreatorForwardingRunner.
package forwarding

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

// processLease handles a single claimed forwarding: polls the remote
// creator, manages lease renewal, and transitions to the appropriate
// next state. Returns an error classified by supervisor.ClassifyError
// so the tick aggregator + FailureTracker can route it through the
// ClassRestartable / ClassCritical restart policy.
//
// Verdetto P0 #1 (Blocco 2): the previous void-returning variant
// produced false-success paths — MarkCreatorForwardingFailed /
// MarkCreatorForwardingRetry failures were only logged, while
// metrics (Failed / Retried) were incremented BEFORE the CAS
// actually persisted. The new contract:
//   - metrics are incremented ONLY after the SQL CAS returns nil
//   - non-nil CAS results return supervisor.ErrElementScoped so
//     the tracker does not count them toward the consecutive-error
//     threshold (they are per-row failures already represented in
//     the row state machine)
//   - lease-lost (procCtx cancelled by the renewal loop) returns
//     supervisor.ErrLeaseLost so the runner does not touch the row
//     (the new lease holder owns it)
//
// Lease-loss propagation: a cancellable processing context (procCtx) is
// created for this lease. The renewal loop receives its cancel function;
// if the lease is lost (RenewCreatorForwardingLease returns
// ErrTransitionConflict), the renewal loop cancels procCtx, causing all
// in-flight operations (GetPipelineStatus, DB writes) to fail with a
// context error. The runner then exits without touching the row — the
// new lease holder owns it.
func (r *CreatorForwardingRunner) processLease(ctx context.Context, lease store.CreatorForwardingLease) error {
	// Create a processing context that the renewal loop can cancel
	// if the lease is lost.
	procCtx, procCancel := context.WithCancel(ctx)
	defer procCancel()

	// Start lease renewal in background.
	go r.renewLeaseLoop(procCtx, procCancel, lease)

	// Poll remote creator for status — uses procCtx so lease loss
	// cancels the in-flight request.
	resp, err := r.client.GetPipelineStatus(procCtx, lease.SourceJobID)
	if err != nil {
		log.Printf("[FORWARDING] poll failed forwarding=%s source_job=%s attempt=%d: %v",
			lease.ForwardingID, lease.SourceJobID, lease.AttemptCount, err)
		// Check if we lost the lease (procCtx was cancelled by renewal loop).
		if procCtx.Err() != nil {
			log.Printf("[FORWARDING] lease lost during poll forwarding=%s; abandoning", lease.ForwardingID)
			return errors.Join(supervisor.ErrLeaseLost, err)
		}
		// Poll error: the per-row retry path is run via handleRetry,
		// which returns an error if the MarkCreatorForwardingRetry
		// CAS failed. The metric increment is owned by handleRetry
		// (post-CAS).
		if retryErr := r.handleRetry(ctx, lease, "POLL_ERROR", err.Error()); retryErr != nil {
			return retryErr
		}
		return nil
	}

	// Defensive nil check: GetPipelineStatus should return (nil, error)
	// on failure, but some HTTP client edge cases (e.g. redirect to
	// empty body) can produce (nil, nil). Treat as a transient poll
	// error rather than panicking on resp.Status.
	if resp == nil {
		log.Printf("[FORWARDING] nil response forwarding=%s source_job=%s: GetPipelineStatus returned nil without error",
			lease.ForwardingID, lease.SourceJobID)
		if retryErr := r.handleRetry(ctx, lease, "NIL_RESPONSE",
			"GetPipelineStatus returned nil response without error"); retryErr != nil {
			return retryErr
		}
		return nil
	}

	// Classify the remote status.
	switch {
	case isTerminalSuccess(resp.Status):
		// Remote creator completed successfully.
		if r.enqueuer != nil {
			// Full atomic lifecycle: build Job+TaskSpec and enqueue+forward
			// in a single SQLite transaction. The metrics + classification
			// are owned by atomicEnqueueAndForward (post-CAS).
			return r.atomicEnqueueAndForward(ctx, lease, resp.Result)
		}
		// Fallback: store payload for a separate forwarding service.
		payloadJSON, payloadSHA256 := marshalPayload(resp.Result)
		if payloadJSON == "" && payloadSHA256 == "" {
			// Non-serializable payload — mark BLOCKED permanently.
			if err := r.dbStore.MarkCreatorForwardingBlocked(ctx,
				lease.ForwardingID, r.identity, lease.LeaseID,
				"PAYLOAD_MARSHAL_ERROR",
				"result payload is not JSON-serializable",
			); err != nil {
				return errors.Join(supervisor.ErrElementScoped,
					fmt.Errorf("mark blocked: %w", err))
			}
			log.Printf("[FORWARDING] payload marshal failed forwarding=%s; marked BLOCKED", lease.ForwardingID)
			r.metrics.Failed.Add(1)
			return nil
		}
		if err := r.dbStore.MarkCreatorForwardingReadyToForward(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			payloadJSON, payloadSHA256,
		); err != nil {
			// CAS failure: persist the retry on the row (if possible)
			// and report the element-scoped error so the tracker
			// does not count it.
			log.Printf("[FORWARDING] mark ready-to-forward failed forwarding=%s: %v", lease.ForwardingID, err)
			if retryErr := r.handleRetry(ctx, lease, "MARK_READY_ERROR", err.Error()); retryErr != nil {
				return retryErr
			}
			return nil
		}
		log.Printf("[FORWARDING] ready-to-forward forwarding=%s source_job=%s source_provider=%s",
			lease.ForwardingID, lease.SourceJobID, lease.SourceProvider)
		r.metrics.Forwarded.Add(1)
		return nil

	case isTerminalFailure(resp.Status):
		// Remote creator failed.
		errMsg := resp.Error
		if errMsg == "" {
			errMsg = fmt.Sprintf("remote status: %s", resp.Status)
		}
		if err := r.dbStore.MarkCreatorForwardingFailed(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"REMOTE_FAILED", errMsg,
		); err != nil {
			// CAS failure: keep row visible (a reaper can retry) but report
			// the failure so the supervisor knows the state didn't transition.
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark failed: %w", err))
		}
		log.Printf("[FORWARDING] failed forwarding=%s source_job=%s status=%s",
			lease.ForwardingID, lease.SourceJobID, resp.Status)
		r.metrics.Failed.Add(1)
		return nil

	default:
		// Still running / queued — release the claim immediately so another
		// runner (or the next tick) can pick it up. No backoff: the job is
		// still in progress, not errored.
		nextAttempt := time.Now().UTC() // immediate re-claim eligibility
		if err := r.dbStore.MarkCreatorForwardingRetry(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"NOT_FINISHED", fmt.Sprintf("remote status: %s", resp.Status),
			nextAttempt,
		); err != nil {
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark retry (still-running): %w", err))
		}
		r.metrics.Retried.Add(1)
		return nil
	}
}

// renewLeaseLoop extends the lease periodically while processLease is
// polling the remote creator. Stops when the context is cancelled (which
// happens when processLease returns or when the lease is lost).
//
// Lease-loss propagation: if RenewCreatorForwardingLease returns
// ErrTransitionConflict (another runner preempted the lease), the loop
// calls procCancel to cancel the processing context, causing processLease
// to abort and release the forwarding without further DB writes.
func (r *CreatorForwardingRunner) renewLeaseLoop(ctx context.Context, procCancel context.CancelFunc, lease store.CreatorForwardingLease) {
	interval := r.cfg.LeaseDuration / 3
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			newExpiry := time.Now().UTC().Add(r.cfg.LeaseDuration)
			if err := r.dbStore.RenewCreatorForwardingLease(
				ctx, // bound to procCtx; cancelled on lease loss
				lease.ForwardingID, r.identity, lease.LeaseID, newExpiry,
			); err != nil {
				log.Printf("[FORWARDING] renew lease failed forwarding=%s: %v", lease.ForwardingID, err)
				// If the lease was preempted by another runner, cancel
				// processLease so it abandons the forwarding without
				// further DB writes.
				procCancel()
				return
			}
		}
	}
}
