// Package forwarding provides the CreatorForwardingRunner.
package forwarding

import (
	"context"
	"errors"
	"fmt"
	"log"

	"velox-server/internal/creatorflow"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

// atomicEnqueueAndForward resolves the forwarding through the canonical
// Resolver. The runner:
//
//  1. Marshals the result map first — non-serializable payloads mark the
//     forwarding BLOCKED before any Resolver call (the Resolver would
//     return a hard marshal error anyway, but BLOCKED is the row state
//     the runner already understands).
//  2. Promotes POLLING → READY_TO_FORWARD via the lease-aware
//     MarkCreatorForwardingReadyToForward transition (the runner has
//     a legitimate lease; the sync handler does not).
//  3. Delegates to Resolver.Resolve with req.ForwardingID = the run's
//     lease id. The Resolver runs idempotency + payload normalization +
//     URL rewriting + atomic CAS in a single place, returning the
//     (job_id, forwarding_id) pair the runner logs.
//
// Returns an error classified by supervisor.ClassifyError. The
// Verdetto P0 #1 contract: metrics (Forwarded) and per-row state
// transitions are persisted only when the corresponding CAS returns
// nil. CAS failures bubble up as supervisor.ErrElementScoped so the
// consecutive-error counter does not include them.
//
// Blocco 5 of the Verdetto (P1 #11): this method is now the single
// path the runner uses for the FORWARDING transition. The legacy
// inline (BuildPayload + PrepareJobAndTask + AtomicForwardAndEnqueue)
// sequence lives only inside the Resolver.
func (r *CreatorForwardingRunner) atomicEnqueueAndForward(ctx context.Context, lease store.CreatorForwardingLease, result map[string]interface{}) error {
	if result == nil {
		result = map[string]interface{}{}
	}

	// 1. Marshal safety check (same semantics as the runner's
	//    pre-resolver code path — non-serializable payloads BLOCK).
	payloadJSON, payloadSHA256 := marshalPayload(result)
	if payloadJSON == "" && payloadSHA256 == "" {
		if err := r.dbStore.MarkCreatorForwardingBlocked(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"PAYLOAD_MARSHAL_ERROR",
			"enqueue payload is not JSON-serializable",
		); err != nil {
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark blocked: %w", err))
		}
		log.Printf("[FORWARDING] payload marshal failed forwarding=%s; marked BLOCKED", lease.ForwardingID)
		r.metrics.Failed.Add(1)
		return nil
	}

	// 2. POLLING → READY_TO_FORWARD. The runner has a legitimate
	//    lease so the leasable CAS guard applies.
	if err := r.dbStore.MarkCreatorForwardingReadyToForward(ctx,
		lease.ForwardingID, r.identity, lease.LeaseID,
		payloadJSON, payloadSHA256,
	); err != nil {
		log.Printf("[FORWARDING] mark ready-to-forward failed forwarding=%s: %v", lease.ForwardingID, err)
		if retryErr := r.handleRetry(ctx, lease, "MARK_READY_ERROR", err.Error(), ""); retryErr != nil {
			return retryErr
		}
		return nil
	}

	// 3. Delegate to the Resolver. The Resolver applies idempotency
	//    + payload normalization + atomic CAS.
	rs := r.lazyResolver()
	if rs == nil {
		// No enqueuer wired (forwarder-only runner); skip the
		// atomic step. The forwarding row is already READY_TO_FORWARD;
		// a separate forwarder can pick it up via ListReadyToForward.
		log.Printf("[FORWARDING] resolver unavailable for forwarding=%s; row left at READY_TO_FORWARD", lease.ForwardingID)
		return nil
	}
	out, err := rs.Resolve(ctx, creatorflow.ResolveRequest{
		ForwardingID:     lease.ForwardingID,
		SourceProvider:   lease.SourceProvider,
		SourceJobID:      lease.SourceJobID,
		TargetExecutorID: lease.TargetExecutorID,
		Payload:          result,
	})
	if err != nil {
		if errors.Is(err, creatorflow.ErrResolverNotComplete) {
			// Element-scoped: leave row at READY_TO_FORWARD for the
			// next tick to re-run the resolve.
			return errors.Join(supervisor.ErrElementScoped, err)
		}
		log.Printf("[FORWARDING] resolver.Resolve failed forwarding=%s: %v", lease.ForwardingID, err)
		if retryErr := r.handleEnqueueRetry(ctx, lease, "ENQUEUE_FAILED", err.Error(), ""); retryErr != nil {
			return retryErr
		}
		return nil
	}
	if out == nil {
		// Resolver returned ErrResolverNotComplete-equivalent
		// sentinel (nil output normally paired with error, but be
		// conservative). Leave the row in READY_TO_FORWARD so the
		// next tick re-runs the resolve.
		return nil
	}
	log.Printf("[FORWARDING] forwarded forwarding=%s → job=%s source=%s (via Resolver)",
		lease.ForwardingID, out.JobID, lease.SourceProvider)
	r.metrics.Forwarded.Add(1)
	return nil
}
