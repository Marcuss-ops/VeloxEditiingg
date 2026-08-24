package assembly

import (
	"fmt"
	"strings"
	"time"
)

type LeaseKind string

const (
	LeasePreparation LeaseKind = "preparation"
	LeaseExecution   LeaseKind = "execution"
)

// PreparationState is the lifecycle of a non-blocking prefetch lease.
type PreparationState string

const (
	PreparationAssigned    PreparationState = "assigned_for_prefetch"
	PreparationPrefetching PreparationState = "prefetching"
	PreparationPrepared    PreparationState = "prepared"
	PreparationWaiting     PreparationState = "waiting_final_manifest"
	PreparationExpired     PreparationState = "expired"
	PreparationFailed      PreparationState = "failed"
	PreparationReleased    PreparationState = "released"
)

func (s PreparationState) valid() bool {
	switch s {
	case PreparationAssigned, PreparationPrefetching, PreparationPrepared,
		PreparationWaiting, PreparationExpired, PreparationFailed, PreparationReleased:
		return true
	default:
		return false
	}
}

func (s PreparationState) terminal() bool {
	return s == PreparationExpired || s == PreparationFailed || s == PreparationReleased
}

// ExecutionLeaseState is deliberately separate from PreparationState: an
// execution lease consumes a real task slot, while a preparation lease does
// not.
type ExecutionLeaseState string

const (
	ExecutionPending   ExecutionLeaseState = "pending"
	ExecutionActive    ExecutionLeaseState = "active"
	ExecutionCompleted ExecutionLeaseState = "completed"
	ExecutionFailed    ExecutionLeaseState = "failed"
	ExecutionExpired   ExecutionLeaseState = "expired"
	ExecutionReleased  ExecutionLeaseState = "released"
)

func (s ExecutionLeaseState) valid() bool {
	switch s {
	case ExecutionPending, ExecutionActive, ExecutionCompleted,
		ExecutionFailed, ExecutionExpired, ExecutionReleased:
		return true
	default:
		return false
	}
}

func (s ExecutionLeaseState) terminal() bool {
	return s == ExecutionCompleted || s == ExecutionFailed || s == ExecutionExpired || s == ExecutionReleased
}

// PreparationLease is a soft reservation. It may guide placement and
// prefetch, but it never consumes an execution slot.
type PreparationLease struct {
	LeaseID         string           `json:"lease_id"`
	JobID           string           `json:"job_id"`
	WorkerID        string           `json:"worker_id"`
	PreparationHash string           `json:"preparation_hash"`
	Kind            LeaseKind        `json:"kind"`
	State           PreparationState `json:"state,omitempty"`
	ExpiresAt       string           `json:"expires_at"`
	Revision        uint64           `json:"revision,omitempty"`
}

// ExecutionLease is acquired only after the final manifest is ready and the
// scheduler has re-evaluated worker availability. FallbackFromWorkerID is
// set when the preparation worker was no longer eligible and the scheduler
// selected another worker; WorkerID always remains the immutable mTLS ID of
// the selected worker.
type ExecutionLease struct {
	LeaseID              string              `json:"lease_id"`
	JobID                string              `json:"job_id"`
	WorkerID             string              `json:"worker_id"`
	Kind                 LeaseKind           `json:"kind"`
	State                ExecutionLeaseState `json:"state,omitempty"`
	ExpiresAt            string              `json:"expires_at,omitempty"`
	PreparationLeaseID   string              `json:"preparation_lease_id,omitempty"`
	FallbackFromWorkerID string              `json:"fallback_from_worker_id,omitempty"`
	Revision             uint64              `json:"revision,omitempty"`
}

func parseLeaseExpiry(value string) (time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, fmt.Errorf("assembly: lease expiry is required")
	}
	expiry, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("assembly: invalid lease expiry %q: %w", value, err)
	}
	return expiry.UTC(), nil
}

func normalizeLeaseNow(now time.Time) (time.Time, error) {
	if now.IsZero() {
		return time.Time{}, fmt.Errorf("assembly: lease clock is required")
	}
	return now.UTC(), nil
}

func validateLeaseTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("assembly: lease ttl must be positive")
	}
	return nil
}

func (l PreparationLease) Validate() error {
	if l.Kind != LeasePreparation {
		return fmt.Errorf("assembly: invalid preparation lease kind %q", l.Kind)
	}
	if l.LeaseID == "" || l.JobID == "" || l.WorkerID == "" || l.PreparationHash == "" || l.ExpiresAt == "" {
		return fmt.Errorf("assembly: incomplete preparation lease")
	}
	if _, err := parseLeaseExpiry(l.ExpiresAt); err != nil {
		return err
	}
	// State is optional on the legacy wire shape. New leases always set it.
	if l.State != "" && !l.State.valid() {
		return fmt.Errorf("assembly: invalid preparation lease state %q", l.State)
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
	if l.ExpiresAt != "" {
		if _, err := parseLeaseExpiry(l.ExpiresAt); err != nil {
			return err
		}
	}
	if l.State != "" && !l.State.valid() {
		return fmt.Errorf("assembly: invalid execution lease state %q", l.State)
	}
	if l.State != "" && l.ExpiresAt == "" {
		return fmt.Errorf("assembly: execution lease state requires expires_at")
	}
	if l.FallbackFromWorkerID == l.WorkerID && l.FallbackFromWorkerID != "" {
		return fmt.Errorf("assembly: execution lease fallback worker must differ from worker_id")
	}
	return nil
}

// NewPreparationLease creates an assigned, non-blocking preparation lease.
func NewPreparationLease(leaseID, jobID, workerID, preparationHash string, now time.Time, ttl time.Duration) (PreparationLease, error) {
	if leaseID == "" || jobID == "" || workerID == "" || preparationHash == "" {
		return PreparationLease{}, fmt.Errorf("assembly: incomplete preparation lease identity")
	}
	if err := validateLeaseTTL(ttl); err != nil {
		return PreparationLease{}, err
	}
	now, err := normalizeLeaseNow(now)
	if err != nil {
		return PreparationLease{}, err
	}
	lease := PreparationLease{
		LeaseID: leaseID, JobID: jobID, WorkerID: workerID,
		PreparationHash: preparationHash, Kind: LeasePreparation,
		State: PreparationAssigned, ExpiresAt: now.Add(ttl).Format(time.RFC3339Nano), Revision: 1,
	}
	return lease, lease.Validate()
}

// Transition applies one legal preparation lifecycle transition. The
// revision is incremented only after validation, allowing persistence layers
// to use it as an optimistic-concurrency token.
func (l PreparationLease) Transition(to PreparationState, now time.Time) (PreparationLease, error) {
	if err := l.Validate(); err != nil {
		return PreparationLease{}, err
	}
	if l.State == "" {
		return PreparationLease{}, fmt.Errorf("assembly: preparation lease state is required for transition")
	}
	if _, err := normalizeLeaseNow(now); err != nil {
		return PreparationLease{}, err
	}
	if !validPreparationTransition(l.State, to) {
		return PreparationLease{}, fmt.Errorf("assembly: invalid preparation lease transition %s -> %s", l.State, to)
	}
	l.State = to
	l.Revision++
	return l, nil
}

func validPreparationTransition(from, to PreparationState) bool {
	switch from {
	case PreparationAssigned:
		return to == PreparationPrefetching || to == PreparationExpired || to == PreparationReleased
	case PreparationPrefetching:
		return to == PreparationPrepared || to == PreparationFailed || to == PreparationExpired || to == PreparationReleased
	case PreparationPrepared:
		return to == PreparationWaiting || to == PreparationExpired || to == PreparationReleased
	case PreparationWaiting:
		return to == PreparationExpired || to == PreparationReleased
	default:
		return false
	}
}

// Renew extends the absolute deadline without consuming an execution slot.
// Renewal is rejected after expiry or any terminal state and always moves the
// revision forward.
func (l PreparationLease) Renew(now time.Time, ttl time.Duration) (PreparationLease, error) {
	if err := l.Validate(); err != nil {
		return PreparationLease{}, err
	}
	if l.State == "" || l.State.terminal() {
		return PreparationLease{}, fmt.Errorf("assembly: preparation lease %s is not renewable in state %q", l.LeaseID, l.State)
	}
	if err := validateLeaseTTL(ttl); err != nil {
		return PreparationLease{}, err
	}
	now, err := normalizeLeaseNow(now)
	if err != nil {
		return PreparationLease{}, err
	}
	expiry, err := parseLeaseExpiry(l.ExpiresAt)
	if err != nil {
		return PreparationLease{}, err
	}
	if !expiry.After(now) {
		return PreparationLease{}, fmt.Errorf("assembly: preparation lease %s has expired", l.LeaseID)
	}
	l.ExpiresAt = now.Add(ttl).Format(time.RFC3339Nano)
	l.Revision++
	return l, nil
}

// Expire moves an active preparation lease to expired only when its deadline
// has elapsed. Replaying expiry is an idempotent no-op.
func (l PreparationLease) Expire(now time.Time) (PreparationLease, error) {
	if err := l.Validate(); err != nil {
		return PreparationLease{}, err
	}
	if l.State == PreparationExpired {
		return l, nil
	}
	if l.State == "" || l.State.terminal() {
		return PreparationLease{}, fmt.Errorf("assembly: preparation lease %s cannot expire from state %q", l.LeaseID, l.State)
	}
	now, err := normalizeLeaseNow(now)
	if err != nil {
		return PreparationLease{}, err
	}
	expiry, err := parseLeaseExpiry(l.ExpiresAt)
	if err != nil {
		return PreparationLease{}, err
	}
	if expiry.After(now) {
		return PreparationLease{}, fmt.Errorf("assembly: preparation lease %s has not expired", l.LeaseID)
	}
	l.State = PreparationExpired
	l.Revision++
	return l, nil
}

// NewExecutionLease creates a pending execution lease for the selected
// worker. The caller must subsequently transition it to active once the task
// lease/CAS has committed.
func NewExecutionLease(leaseID, jobID, workerID, preparationLeaseID string, now time.Time, ttl time.Duration) (ExecutionLease, error) {
	if leaseID == "" || jobID == "" || workerID == "" {
		return ExecutionLease{}, fmt.Errorf("assembly: incomplete execution lease identity")
	}
	if err := validateLeaseTTL(ttl); err != nil {
		return ExecutionLease{}, err
	}
	now, err := normalizeLeaseNow(now)
	if err != nil {
		return ExecutionLease{}, err
	}
	lease := ExecutionLease{
		LeaseID: leaseID, JobID: jobID, WorkerID: workerID,
		Kind: LeaseExecution, State: ExecutionPending,
		ExpiresAt:          now.Add(ttl).Format(time.RFC3339Nano),
		PreparationLeaseID: preparationLeaseID, Revision: 1,
	}
	return lease, lease.Validate()
}

func validExecutionTransition(from, to ExecutionLeaseState) bool {
	switch from {
	case ExecutionPending:
		return to == ExecutionActive || to == ExecutionExpired || to == ExecutionReleased
	case ExecutionActive:
		return to == ExecutionCompleted || to == ExecutionFailed || to == ExecutionExpired || to == ExecutionReleased
	default:
		return false
	}
}

// Transition applies one legal execution lifecycle transition.
func (l ExecutionLease) Transition(to ExecutionLeaseState, now time.Time) (ExecutionLease, error) {
	if err := l.Validate(); err != nil {
		return ExecutionLease{}, err
	}
	if l.State == "" {
		return ExecutionLease{}, fmt.Errorf("assembly: execution lease state is required for transition")
	}
	if _, err := normalizeLeaseNow(now); err != nil {
		return ExecutionLease{}, err
	}
	if !validExecutionTransition(l.State, to) {
		return ExecutionLease{}, fmt.Errorf("assembly: invalid execution lease transition %s -> %s", l.State, to)
	}
	l.State = to
	l.Revision++
	return l, nil
}

// Renew extends an active execution lease. The task repository remains the
// authoritative execution CAS; this method only updates the shared contract
// projection after that CAS succeeds.
func (l ExecutionLease) Renew(now time.Time, ttl time.Duration) (ExecutionLease, error) {
	if err := l.Validate(); err != nil {
		return ExecutionLease{}, err
	}
	if l.State != ExecutionPending && l.State != ExecutionActive {
		return ExecutionLease{}, fmt.Errorf("assembly: execution lease %s is not renewable in state %q", l.LeaseID, l.State)
	}
	if err := validateLeaseTTL(ttl); err != nil {
		return ExecutionLease{}, err
	}
	now, err := normalizeLeaseNow(now)
	if err != nil {
		return ExecutionLease{}, err
	}
	expiry, err := parseLeaseExpiry(l.ExpiresAt)
	if err != nil {
		return ExecutionLease{}, err
	}
	if !expiry.After(now) {
		return ExecutionLease{}, fmt.Errorf("assembly: execution lease %s has expired", l.LeaseID)
	}
	l.ExpiresAt = now.Add(ttl).Format(time.RFC3339Nano)
	l.Revision++
	return l, nil
}

// Expire moves a pending/active execution lease to expired only after its
// deadline. Replaying expiry is idempotent.
func (l ExecutionLease) Expire(now time.Time) (ExecutionLease, error) {
	if err := l.Validate(); err != nil {
		return ExecutionLease{}, err
	}
	if l.State == ExecutionExpired {
		return l, nil
	}
	if l.State != ExecutionPending && l.State != ExecutionActive {
		return ExecutionLease{}, fmt.Errorf("assembly: execution lease %s cannot expire from state %q", l.LeaseID, l.State)
	}
	now, err := normalizeLeaseNow(now)
	if err != nil {
		return ExecutionLease{}, err
	}
	expiry, err := parseLeaseExpiry(l.ExpiresAt)
	if err != nil {
		return ExecutionLease{}, err
	}
	if expiry.After(now) {
		return ExecutionLease{}, fmt.Errorf("assembly: execution lease %s has not expired", l.LeaseID)
	}
	l.State = ExecutionExpired
	l.Revision++
	return l, nil
}

// PromotePreparationToExecution closes the soft-placement gap. The
// preparation worker is attempted first; if it is no longer eligible, the
// existing deterministic scorer selects an alternative eligible worker. The
// returned decision records the fallback, while worker_id remains untouched.
func PromotePreparationToExecution(
	preparation PreparationLease,
	executionLeaseID string,
	now time.Time,
	ttl time.Duration,
	workers []WorkerPlacementSnapshot,
	request PlacementRequest,
) (ExecutionLease, PlacementDecision, error) {
	if err := preparation.Validate(); err != nil {
		return ExecutionLease{}, PlacementDecision{}, err
	}
	if preparation.State == "" || preparation.State.terminal() {
		return ExecutionLease{}, PlacementDecision{}, fmt.Errorf("assembly: preparation lease %s is not executable in state %q", preparation.LeaseID, preparation.State)
	}
	now, err := normalizeLeaseNow(now)
	if err != nil {
		return ExecutionLease{}, PlacementDecision{}, err
	}
	expiry, err := parseLeaseExpiry(preparation.ExpiresAt)
	if err != nil {
		return ExecutionLease{}, PlacementDecision{}, err
	}
	if !expiry.After(now) {
		return ExecutionLease{}, PlacementDecision{}, fmt.Errorf("assembly: preparation lease %s has expired", preparation.LeaseID)
	}

	var decision PlacementDecision
	// Preserve the warm worker when it is still eligible. It is only a
	// preference: an unavailable/full/misconfigured worker must not block the
	// final execution lease.
	if preferred, preferredErr := SelectPreferredWorker(
		filterWorkerByID(workers, preparation.WorkerID), request,
	); preferredErr == nil {
		decision = preferred
	} else {
		decision, err = SelectPreferredWorker(workers, request)
		if err != nil {
			return ExecutionLease{}, PlacementDecision{}, fmt.Errorf("assembly: execution fallback: %w", err)
		}
	}
	lease, err := NewExecutionLease(executionLeaseID, preparation.JobID, decision.WorkerID, preparation.LeaseID, now, ttl)
	if err != nil {
		return ExecutionLease{}, PlacementDecision{}, err
	}
	if decision.WorkerID != preparation.WorkerID {
		lease.FallbackFromWorkerID = preparation.WorkerID
	}
	if err := lease.Validate(); err != nil {
		return ExecutionLease{}, PlacementDecision{}, err
	}
	return lease, decision, nil
}

func filterWorkerByID(workers []WorkerPlacementSnapshot, workerID string) []WorkerPlacementSnapshot {
	filtered := make([]WorkerPlacementSnapshot, 0, 1)
	for _, worker := range workers {
		if worker.WorkerID == workerID {
			filtered = append(filtered, worker)
		}
	}
	return filtered
}
