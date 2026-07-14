// Package forwarding provides the CreatorForwardingRunner.
package forwarding

import (
	"context"
	"fmt"
	"log"
	"sync"

	"velox-server/internal/store"
)

// tick performs one poll: claim up to ClaimBatch claimable forwardings,
// then process each one with bounded concurrency. Errors from the
// inner goroutines are aggregated under a mutex and the FIRST
// classified error is returned to Run so the existing
// supervisor.FailureTracker machinery can route it through the
// ClassRestartable / ClassCritical restart policy. Per-element errors
// are persisted on the row by processLease (so a single bad forwarding
// does not poison the consecutive-error counter); lease-lost cancels
// the in-flight context; infrastructure errors propagate.
func (r *CreatorForwardingRunner) tick(ctx context.Context) error {
	if r.client == nil || !r.client.IsConfigured() {
		return nil // remote creator not configured; no work to do
	}

	// P0-02: cap the claim batch at Concurrency so every claimed lease
	// can acquire the semaphore immediately without waiting. Leases
	// that sit behind the semaphore cannot be renewed (renewLeaseLoop
	// starts only after sem acquisition), so a ClaimBatch > Concurrency
	// creates a window where claimed leases expire before they start
	// processing. Capping at the source also avoids attempt_count
	// inflation from claim-then-release cycles.
	effectiveClaimBatch := r.cfg.ClaimBatch
	if effectiveClaimBatch > r.cfg.Concurrency {
		effectiveClaimBatch = r.cfg.Concurrency
	}
	leases, err := r.dbStore.ClaimCreatorForwardings(ctx, r.identity, "cf", r.cfg.LeaseDuration, effectiveClaimBatch)
	if err != nil {
		return fmt.Errorf("claim forwardings: %w", err)
	}
	if len(leases) == 0 {
		return nil
	}

	r.metrics.Claimed.Add(int64(len(leases)))
	log.Printf("[FORWARDING] claimed %d forwardings", len(leases))

	var (
		wg         sync.WaitGroup
		errMu      sync.Mutex
		aggregated error
	)
	for _, lease := range leases {
		wg.Add(1)
		go func(l store.CreatorForwardingLease) {
			defer wg.Done()
			// Acquire semaphore (bounded concurrency).
			select {
			case r.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-r.sem }()

			if leaseErr := r.processLease(ctx, l); leaseErr != nil {
				errMu.Lock()
				if aggregated == nil {
					aggregated = leaseErr
				}
				errMu.Unlock()
			}
		}(lease)
	}
	wg.Wait()
	return aggregated
}
