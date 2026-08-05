package taskgraph

// LeaseState is the canonical lifecycle state of a task lease. A lease is
// distinct from Task.Status: task execution may remain RUNNING while its
// lease is renewed, and an expired lease is reclaimed by the master reaper.
type LeaseState string

const (
	LeaseActive   LeaseState = "ACTIVE"
	LeaseReleased LeaseState = "RELEASED"
	LeaseExpired  LeaseState = "EXPIRED"
	LeaseRevoked  LeaseState = "REVOKED"
)

// IsTerminal reports whether the lease can no longer be renewed.
func (s LeaseState) IsTerminal() bool {
	return s == LeaseReleased || s == LeaseExpired || s == LeaseRevoked
}

// LeaseStateFromTaskStatus projects the persisted task row onto its lease
// dimension at compatibility boundaries. Empty worker/lease identity means
// there is no active lease; task status alone never invents lease identity.
func LeaseStateFromTaskStatus(status Status, workerID, leaseID string) LeaseState {
	if workerID == "" || leaseID == "" {
		return LeaseReleased
	}
	switch status {
	case StatusLeased, StatusRunning:
		return LeaseActive
	case StatusTimedOut:
		return LeaseExpired
	case StatusCancelled:
		return LeaseRevoked
	default:
		return LeaseReleased
	}
}
