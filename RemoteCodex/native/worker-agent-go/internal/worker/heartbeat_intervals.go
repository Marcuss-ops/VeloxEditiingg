package worker

import (
	"time"
)

// Heartbeat interval policy based on worker status, plus the backoff
// parameters applied by heartbeat_loop.go after consecutive failures.
// All callers are inside the worker package.
const (
	heartbeatIntervalIdle      = 60 * time.Second       // Idle: less frequent
	heartbeatIntervalBusy      = 2 * time.Second        // Busy: live progress cadence
	heartbeatIntervalError     = 10 * time.Second       // Error: rapid recovery attempts
	heartbeatWakeMinInterval   = 250 * time.Millisecond // Burst floor for phase/segment edges
	heartbeatMaxBackoff        = 5 * time.Minute        // Maximum backoff interval
	heartbeatBackoffMultiplier = 2.0                    // Backoff multiplier

	// Lease renewal cadence + requested expiry. Mirrored by lease_renewal.go's
	// leaseRenewLoop (15s ticker, 30m requested expiry) so the cadence is
	// visible alongside the heartbeat constants for operators tuning the comms
	// schedule.
	leaseRenewalInterval = 15 * time.Second // Task-native lease renewal cadence
	leaseRenewalExpiry   = 30 * time.Minute // Requested lease expiry per renewal
)

// getHeartbeatInterval returns the appropriate heartbeat interval based on worker status.
func (w *Worker) getHeartbeatInterval() time.Duration {
	w.activeTasksMu.RLock()
	busy := len(w.activeTasks) > 0
	w.activeTasksMu.RUnlock()
	if busy {
		return heartbeatIntervalBusy
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	switch w.status {
	case StatusBusy:
		return heartbeatIntervalBusy
	case StatusError:
		return heartbeatIntervalError
	default:
		return heartbeatIntervalIdle
	}
}
