// Package forwarding provides the CreatorForwardingRunner.
package forwarding

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"velox-server/internal/remoteengine"
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
	var leaseWasLost atomic.Bool

	// Start lease renewal in background.
	go r.renewLeaseLoop(procCtx, procCancel, lease, &leaseWasLost)

	// Poll remote creator for status — uses procCtx so lease loss
	// cancels the in-flight request.
	resp, err := r.client.GetPipelineStatus(procCtx, lease.SourceJobID)

	// Record the poll attempt under the exact claim identity. A failed
	// lease-fenced CAS is authoritative: the runner must stop immediately
	// and must not retry or transition a row owned by a takeover runner.
	if leaseErr := leaseLostError(procCtx, &leaseWasLost); leaseErr != nil {
		return leaseErr
	}
	{
		remoteStatus := ""
		if resp != nil {
			remoteStatus = resp.Status
		}
		// next_poll_at is set to now so the next tick can reclaim the
		// row immediately. The actual scheduling is handled by the
		// runner's PollInterval + claim query.
		if recordErr := r.dbStore.RecordCreatorForwardingPoll(
			procCtx, lease.ForwardingID, lease.RunnerID, lease.LeaseID,
			remoteStatus, time.Now().UTC(),
		); recordErr != nil {
			if leaseErr := leaseLostError(procCtx, &leaseWasLost); leaseErr != nil {
				return leaseErr
			}
			if errors.Is(recordErr, store.ErrLeaseLost) {
				log.Printf("[FORWARDING] lease lost while recording poll forwarding=%s runner=%s lease=%s; abandoning",
					lease.ForwardingID, lease.RunnerID, lease.LeaseID)
				procCancel()
				return supervisor.ErrLeaseLost
			}
			return forwardingStateError(fmt.Sprintf("record poll forwarding=%s", lease.ForwardingID), recordErr)
		}
	}

	if err != nil {
		log.Printf("[FORWARDING] poll failed forwarding=%s source_job=%s attempt=%d: %v",
			lease.ForwardingID, lease.SourceJobID, lease.AttemptCount, err)
		// Check if we lost the lease (procCtx was cancelled by renewal loop).
		if leaseErr := leaseLostError(procCtx, &leaseWasLost); leaseErr != nil {
			log.Printf("[FORWARDING] lease lost during poll forwarding=%s; abandoning", lease.ForwardingID)
			return errors.Join(leaseErr, err)
		}
		// Poll error: the per-row retry path is run via handleRetry,
		// which returns an error if the MarkCreatorForwardingRetry
		// CAS failed. The metric increment is owned by handleRetry
		// (post-CAS).
		errorClass := ""
		if re, ok := err.(*remoteengine.RemoteError); ok {
			errorClass = string(re.Class)
		}
		if retryErr := r.handleRetry(procCtx, lease, "POLL_ERROR", err.Error(), errorClass); retryErr != nil {
			if leaseErr := leaseLostError(procCtx, &leaseWasLost); leaseErr != nil {
				return leaseErr
			}
			if errors.Is(retryErr, store.ErrTransitionConflict) || errors.Is(retryErr, store.ErrLeaseLost) {
				return supervisor.ErrLeaseLost
			}
			return retryErr
		}
		return nil
	}

	if leaseErr := leaseLostError(procCtx, &leaseWasLost); leaseErr != nil {
		return leaseErr
	}

	// Defensive nil check: GetPipelineStatus should return (nil, error)
	// on failure, but some HTTP client edge cases (e.g. redirect to
	// empty body) can produce (nil, nil). Treat as a transient poll
	// error rather than panicking on resp.Status.
	if resp == nil {
		log.Printf("[FORWARDING] nil response forwarding=%s source_job=%s: GetPipelineStatus returned nil without error",
			lease.ForwardingID, lease.SourceJobID)
		if retryErr := r.handleRetry(procCtx, lease, "NIL_RESPONSE",
			"GetPipelineStatus returned nil response without error", ""); retryErr != nil {
			if leaseErr := leaseLostError(procCtx, &leaseWasLost); leaseErr != nil {
				return leaseErr
			}
			if errors.Is(retryErr, store.ErrTransitionConflict) || errors.Is(retryErr, store.ErrLeaseLost) {
				return supervisor.ErrLeaseLost
			}
			return retryErr
		}
		return nil
	}

	// Classify the remote status.
	switch {
	case isTerminalSuccess(resp.Status):
		// Remote creator completed successfully. The runner delegates
		// the forward-completed path exclusively to the canonical
		// creatorflow.Resolver.Resolve via atomicEnqueueAndForward.
		if r.enqueuer != nil {
			err := r.atomicEnqueueAndForward(procCtx, lease, resp.Result)
			if leaseErr := leaseLostError(procCtx, &leaseWasLost); leaseErr != nil {
				return leaseErr
			}
			if errors.Is(err, store.ErrTransitionConflict) || errors.Is(err, store.ErrLeaseLost) {
				return supervisor.ErrLeaseLost
			}
			return err
		}
		// Fallback: store payload for a separate forwarding service.
		payloadJSON, payloadSHA256 := marshalPayload(resp.Result)
		if payloadJSON == "" && payloadSHA256 == "" {
			// Non-serializable payload — mark BLOCKED permanently.
			if err := r.dbStore.MarkCreatorForwardingBlocked(procCtx,
				lease.ForwardingID, lease.RunnerID, lease.LeaseID,
				"PAYLOAD_MARSHAL_ERROR",
				"result payload is not JSON-serializable",
			); err != nil {
				if errors.Is(err, store.ErrTransitionConflict) || leaseWasLost.Load() {
					return supervisor.ErrLeaseLost
				}
				return forwardingStateError("mark blocked", err)
			}
			log.Printf("[FORWARDING] payload marshal failed forwarding=%s; marked BLOCKED", lease.ForwardingID)
			r.recordFailed()
			return nil
		}
		if err := r.dbStore.MarkCreatorForwardingReadyToForward(procCtx,
			lease.ForwardingID, lease.RunnerID, lease.LeaseID,
			payloadJSON, payloadSHA256,
		); err != nil {
			// CAS failure: persist the retry on the row (if possible)
			// and report the element-scoped error so the tracker
			// does not count it.
			log.Printf("[FORWARDING] mark ready-to-forward failed forwarding=%s: %v", lease.ForwardingID, err)
			if errors.Is(err, store.ErrTransitionConflict) {
				return supervisor.ErrLeaseLost
			}
			if retryErr := r.handleRetry(procCtx, lease, "MARK_READY_ERROR", err.Error(), ""); retryErr != nil {
				if leaseErr := leaseLostError(procCtx, &leaseWasLost); leaseErr != nil {
					return leaseErr
				}
				return retryErr
			}
			return nil
		}
		log.Printf("[FORWARDING] ready-to-forward forwarding=%s source_job=%s source_provider=%s",
			lease.ForwardingID, lease.SourceJobID, lease.SourceProvider)
		r.recordForwarded()
		return nil

	case isTerminalFailure(resp.Status):
		// Remote creator failed.
		errMsg := resp.Error
		if errMsg == "" {
			errMsg = fmt.Sprintf("remote status: %s", resp.Status)
		}
		if err := r.dbStore.MarkCreatorForwardingFailed(procCtx,
			lease.ForwardingID, lease.RunnerID, lease.LeaseID,
			"REMOTE_FAILED", errMsg, "",
		); err != nil {
			// CAS failure: keep row visible (a reaper can retry) but report
			// the failure so the supervisor knows the state didn't transition.
			if errors.Is(err, store.ErrTransitionConflict) || leaseWasLost.Load() {
				return supervisor.ErrLeaseLost
			}
			return forwardingStateError("mark failed", err)
		}
		log.Printf("[FORWARDING] failed forwarding=%s source_job=%s status=%s",
			lease.ForwardingID, lease.SourceJobID, resp.Status)
		r.recordFailed()
		return nil

	default:
		// Still running / queued — release the claim immediately so another
		// runner (or the next tick) can pick it up. No backoff: the job is
		// still in progress, not errored.
		nextAttempt := time.Now().UTC() // immediate re-claim eligibility
		if err := r.dbStore.MarkCreatorForwardingRetry(procCtx,
			lease.ForwardingID, lease.RunnerID, lease.LeaseID,
			"NOT_FINISHED", fmt.Sprintf("remote status: %s", resp.Status), "",
			nextAttempt,
		); err != nil {
			if errors.Is(err, store.ErrTransitionConflict) || leaseWasLost.Load() {
				return supervisor.ErrLeaseLost
			}
			return forwardingStateError("mark retry (still-running)", err)
		}
		r.recordRetried()
		return nil
	}
}

// leaseLostError maps cancellation of the processing context to the
// supervisor's lease-loss sentinel. The processing context is owned by the
// current lease; once cancelled, no subsequent row mutation is safe.
func leaseLostError(ctx context.Context, leaseWasLost *atomic.Bool) error {
	if leaseWasLost != nil && leaseWasLost.Load() {
		return supervisor.ErrLeaseLost
	}
	if ctx == nil || ctx.Err() == nil {
		return nil
	}
	return ctx.Err()
}

// renewLeaseLoop extends the lease periodically while processLease is
// polling the remote creator. Stops when the context is cancelled (which
// happens when processLease returns or when the lease is lost).
//
// Lease-loss propagation: if RenewCreatorForwardingLease returns
// ErrTransitionConflict (another runner preempted the lease), the loop
// calls procCancel to cancel the processing context, causing processLease
// to abort and release the forwarding without further DB writes.
func (r *CreatorForwardingRunner) renewLeaseLoop(ctx context.Context, procCancel context.CancelFunc, lease store.CreatorForwardingLease, leaseWasLost *atomic.Bool) {
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
				lease.ForwardingID, lease.RunnerID, lease.LeaseID, newExpiry,
			); err != nil {
				log.Printf("[FORWARDING] renew lease failed forwarding=%s: %v", lease.ForwardingID, err)
				// Only a CAS conflict proves that another runner owns the
				// row. Parent cancellation and infrastructure failures must
				// not be mislabeled as takeover.
				if errors.Is(err, store.ErrTransitionConflict) || errors.Is(err, store.ErrLeaseLost) {
					if leaseWasLost != nil {
						leaseWasLost.Store(true)
					}
				}
				procCancel()
				return
			}
		}
	}
}
