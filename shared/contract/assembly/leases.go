package assembly

import "fmt"

type LeaseKind string

const (
	LeasePreparation LeaseKind = "preparation"
	LeaseExecution   LeaseKind = "execution"
)

// PreparationLease is a soft reservation. It may guide placement and
// prefetch, but it never consumes an execution slot.
type PreparationLease struct {
	LeaseID         string    `json:"lease_id"`
	JobID           string    `json:"job_id"`
	WorkerID        string    `json:"worker_id"`
	PreparationHash string    `json:"preparation_hash"`
	Kind            LeaseKind `json:"kind"`
	ExpiresAt       string    `json:"expires_at"`
}

// ExecutionLease is acquired only after the final manifest is ready and the
// scheduler has re-evaluated worker availability.
type ExecutionLease struct {
	LeaseID  string    `json:"lease_id"`
	JobID    string    `json:"job_id"`
	WorkerID string    `json:"worker_id"`
	Kind     LeaseKind `json:"kind"`
}

func (l PreparationLease) Validate() error {
	if l.Kind != LeasePreparation {
		return fmt.Errorf("assembly: invalid preparation lease kind %q", l.Kind)
	}
	if l.LeaseID == "" || l.JobID == "" || l.WorkerID == "" || l.PreparationHash == "" || l.ExpiresAt == "" {
		return fmt.Errorf("assembly: incomplete preparation lease")
	}
	return nil
}

func (l ExecutionLease) Validate() error {
	if l.Kind != LeaseExecution {
		return fmt.Errorf("assembly: invalid execution lease kind %q", l.Kind)
	}
	if l.LeaseID == "" || l.JobID == "" || l.WorkerID == "" {
		return fmt.Errorf("assembly: incomplete execution lease")
	}
	return nil
}
