package deliveries

// runner_process_lease.go: lease-renewal loop + timeout classification for
// the DeliveryRunner. Split out of runner_process.go; processLease stays in
// that file and credential resolution lives in runner_process_credentials.go.

import (
	"context"
	"errors"
	"net"
	"time"

	"velox-server/internal/store"
)

func isDeliveryTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var timeoutErr net.Error
	return errors.As(err, &timeoutErr) && timeoutErr.Timeout()
}

// renewDeliveryLeaseLoop extends the lease periodically (every
// leaseDuration/3) while provider.Deliver is running. When the deliver
// context is cancelled, the goroutine exits. When a renewal fails (e.g.
// CAS conflict from another runner reclaiming the lease), the onFailure
// callback is invoked so the upload can be interrupted.
func (r *DeliveryRunner) renewDeliveryLeaseLoop(ctx context.Context, done chan<- struct{}, lease store.DeliveryLease, onFailure func(error)) {
	defer close(done)

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
			if err := r.dbStore.RenewDeliveryLease(
				context.Background(), // intentionally detached from request ctx
				lease.DeliveryID, lease.RunnerID, lease.LeaseID, newExpiry,
			); err != nil {
				onFailure(err)
				return
			}
		}
	}
}
