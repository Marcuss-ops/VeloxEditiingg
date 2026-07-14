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

// handleEnqueueRetry transitions the forwarding to RETRY_WAIT with backoff
// when the enqueue phase fails (e.g. payload build error, atomic write
// conflict). Uses MarkCreatorForwardingEnqueueRetry which handles
// FORWARDING/READY_TO_FORWARD states. On max attempts or CAS failure,
// falls back to MarkCreatorForwardingFailed to prevent silent stuck rows.
//
// Returns an error classified by supervisor.ClassifyError. The
// Verdetto P0 #1 contract: metrics (Failed / Retried) are persisted
// only after the underlying SQL CAS returns nil.
func (r *CreatorForwardingRunner) handleEnqueueRetry(ctx context.Context, lease store.CreatorForwardingLease, code, msg string) error {
	maxAttempts := r.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 12
	}
	if lease.AttemptCount >= maxAttempts {
		if err := r.dbStore.MarkCreatorForwardingFailed(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"MAX_ENQUEUE_ATTEMPTS",
			fmt.Sprintf("exhausted %d attempts: %s", maxAttempts, msg),
		); err != nil {
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark failed (max enqueue attempts): %w", err))
		}
		log.Printf("[FORWARDING] max enqueue attempts exhausted forwarding=%s source_job=%s attempts=%d",
			lease.ForwardingID, lease.SourceJobID, lease.AttemptCount)
		r.metrics.Failed.Add(1)
		return nil
	}

	backoff := r.cfg.backoffForAttempt(lease.AttemptCount)
	nextAttempt := time.Now().UTC().Add(backoff)
	if err := r.dbStore.MarkCreatorForwardingEnqueueRetry(ctx,
		lease.ForwardingID, code, msg, nextAttempt,
	); err != nil {
		// CAS failure (race with another runner or already transitioned) —
		// fall back to terminal failure to prevent the row from being
		// silently stuck in FORWARDING/READY_TO_FORWARD forever.
		// With full lease-authority CAS, this is a best-effort safety
		// net: if another runner already claimed the row, the CAS will
		// also fail (which is correct — the new lease holder owns it).
		log.Printf("[FORWARDING] enqueue retry CAS failed forwarding=%s: %v; best-effort FAILED (may no-op if preempted)", lease.ForwardingID, err)
		if ferr := r.dbStore.MarkCreatorForwardingFailed(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"ENQUEUE_RETRY_CAS_FAILED",
			fmt.Sprintf("CAS failure on enqueue retry: %v", err),
		); ferr != nil {
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark failed (CAS fallback): %w (orig=%v)", ferr, err))
		}
		r.metrics.Failed.Add(1)
		return nil
	}
	r.metrics.Retried.Add(1)
	return nil
}

// handleRetry transitions the forwarding to RETRY_WAIT with the
// backoff schedule applied. If max attempts are exhausted, the
// forwarding is marked FAILED instead.
//
// Returns an error classified by supervisor.ClassifyError. The
// Verdetto P0 #1 contract: metrics (Failed / Retried) are persisted
// only after the underlying SQL CAS returns nil. The caller no
// longer adds the Retried metric — handleRetry owns it.
func (r *CreatorForwardingRunner) handleRetry(ctx context.Context, lease store.CreatorForwardingLease, code, msg string) error {
	maxAttempts := r.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 12
	}
	if lease.AttemptCount >= maxAttempts {
		if err := r.dbStore.MarkCreatorForwardingFailed(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"MAX_ATTEMPTS",
			fmt.Sprintf("exhausted %d attempts: %s", maxAttempts, msg),
		); err != nil {
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark failed (max attempts): %w", err))
		}
		log.Printf("[FORWARDING] max attempts exhausted forwarding=%s source_job=%s attempts=%d",
			lease.ForwardingID, lease.SourceJobID, lease.AttemptCount)
		r.metrics.Failed.Add(1)
		return nil
	}

	backoff := r.cfg.backoffForAttempt(lease.AttemptCount)
	nextAttempt := time.Now().UTC().Add(backoff)
	if err := r.dbStore.MarkCreatorForwardingRetry(ctx,
		lease.ForwardingID, r.identity, lease.LeaseID,
		code, msg, nextAttempt,
	); err != nil {
		return errors.Join(supervisor.ErrElementScoped,
			fmt.Errorf("mark retry: %w", err))
	}
	r.metrics.Retried.Add(1)
	return nil
}
