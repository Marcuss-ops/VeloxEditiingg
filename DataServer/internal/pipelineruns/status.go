package pipelineruns

// This file extends model.go with:
//
//  1. Valid()           — true when the Status is one of the known constants.
//  2. AllStatuses()     — the canonical slice of every aggregated status.
//  3. Stage()           — groups the 17 statuses into 6 lifecycle stages
//     (remote, forwarding, worker, artifact, delivery, terminal) so the
//     timeline endpoint can render phase headers without hard-coding
//     string comparisons.
//  4. DeriveStatus()    — the SINGLE projection function that maps the
//     internal states of creator_forwardings, jobs, artifacts, and
//     job_deliveries into the aggregated Status exposed to API clients.
//
// The projection is intentionally one-way: it READS internal states and
// WRITES the aggregated status. It never drives internal transitions —
// those remain owned by each domain's state machine.

// ── Validation ───────────────────────────────────────────────────────

// allStatuses is the canonical set, kept as a package-level slice so
// Valid() and AllStatuses() share one source of truth.
var allStatuses = []Status{
	StatusAccepted,
	StatusRemoteSubmitting,
	StatusRemoteQueued,
	StatusRemoteRunning,
	StatusRemoteCompleted,
	StatusForwarding,
	StatusWorkerQueued,
	StatusRendering,
	StatusArtifactProcessing,
	StatusArtifactReady,
	StatusDeliveryPending,
	StatusDelivering,
	StatusScheduled,
	StatusPublished,
	StatusCompleted,
	StatusFailed,
	StatusCancelled,
}

// AllStatuses returns every valid aggregated status for use in
// validation, API docs, and tests.
func AllStatuses() []Status {
	out := make([]Status, len(allStatuses))
	copy(out, allStatuses)
	return out
}

// statusSet is the O(1) membership view of allStatuses. Derived once at init
// so Valid() on the placement/costmodel/status-boundary hot paths never
// scans the 17-status slice (mirrors the telemetry originSet/scopeSet idiom).
var statusSet = func() map[Status]struct{} {
	m := make(map[Status]struct{}, len(allStatuses))
	for _, v := range allStatuses {
		m[v] = struct{}{}
	}
	return m
}()

// Valid returns true when s is one of the known aggregated statuses.
func (s Status) Valid() bool {
	_, ok := statusSet[s]
	return ok
}

// ── Stage grouping ───────────────────────────────────────────────────

// Stage is the high-level lifecycle phase a Status belongs to. It is
// used by the timeline endpoint to group events into phase headers.
type Stage string

const (
	StageRemote     Stage = "REMOTE"     // remote engine generation
	StageForwarding Stage = "FORWARDING" // creator → Velox handoff
	StageWorker     Stage = "WORKER"     // Velox job rendering
	StageArtifact   Stage = "ARTIFACT"   // artifact processing/verification
	StageDelivery   Stage = "DELIVERY"   // delivery to destinations
	StageTerminal   Stage = "TERMINAL"   // COMPLETED / FAILED / CANCELLED
)

// StageOf returns the lifecycle stage the status belongs to. Returns
// StageTerminal for unknown statuses (defensive — callers should
// validate with Valid() first).
func (s Status) StageOf() Stage {
	switch s {
	case StatusAccepted, StatusRemoteSubmitting, StatusRemoteQueued,
		StatusRemoteRunning, StatusRemoteCompleted:
		return StageRemote
	case StatusForwarding:
		return StageForwarding
	case StatusWorkerQueued, StatusRendering:
		return StageWorker
	case StatusArtifactProcessing, StatusArtifactReady:
		return StageArtifact
	case StatusDeliveryPending, StatusDelivering,
		StatusScheduled, StatusPublished:
		return StageDelivery
	case StatusCompleted, StatusFailed, StatusCancelled:
		return StageTerminal
	default:
		return StageTerminal
	}
}

// ── Projection from internal states ──────────────────────────────────

// InternalState is the read-only snapshot of the internal domain states
// that DeriveStatus consults. Every field is a plain string so the
// caller does not need to import the jobs / store / artifacts / deliveries
// packages — it just passes the raw status strings it already loaded.
//
// Fields are intentionally pointers (nil = "not applicable / no row yet")
// so DeriveStatus can distinguish "delivery is PENDING" from "there is
// no delivery row for this run yet".
type InternalState struct {
	// ForwardingStatus is the creator_forwardings.status string
	// (PENDING, POLLING, READY_TO_FORWARD, FORWARDING, FORWARDED,
	// RETRY_WAIT, FAILED, BLOCKED). Nil when no forwarding row exists.
	ForwardingStatus *string

	// RemoteJobStatus is the remote engine's job status string
	// (queued, running, completed, failed, cancelled). Nil when no
	// remote job has been submitted yet or the remote engine has not
	// returned a status.
	RemoteJobStatus *string

	// JobStatus is the Velox jobs.status string
	// (PENDING, LEASED, RUNNING, AWAITING_ARTIFACT, RETRY_WAIT,
	// SUCCEEDED, FAILED, CANCELLED). Nil when no Velox job has been
	// created yet.
	JobStatus *string

	// ArtifactStatus is the artifacts.status string
	// (STAGING, READY, QUARANTINED, DELETED, FAILED). Nil when no
	// artifact row exists yet.
	ArtifactStatus *string

	// DeliveryStatus is the job_deliveries.status string
	// (PENDING, RUNNING, RETRY_WAIT, SUCCEEDED, FAILED, BLOCKED_AUTH,
	// CANCELLED). Nil when no delivery row exists yet.
	DeliveryStatus *string

	// HasScheduledDelivery is true when the delivery has a publish_at
	// timestamp in the future (the video is scheduled but not yet
	// published). Only consulted when DeliveryStatus == "SUCCEEDED".
	HasScheduledDelivery bool

	// AllDeliveriesSucceeded is true when ALL delivery rows for the run
	// are in SUCCEEDED state and none are scheduled. When true,
	// DeriveStatus returns COMPLETED instead of PUBLISHED. Callers that
	// only inspect a single delivery row should leave this false.
	AllDeliveriesSucceeded bool
}

// strPtr is a convenience helper for tests and callers that have plain
// string values. Returns a pointer to the string, or nil when the
// string is empty (meaning "not applicable").
func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	v := s
	return &v
}

// DeriveStatus computes the aggregated pipeline_run Status from the
// internal domain states. It is the SINGLE projection function —
// callers should never hand-map internal states to aggregated statuses.
//
// Resolution order (first match wins):
//
//  1. If any internal domain is in a terminal FAILURE state → FAILED.
//  2. If any internal domain is CANCELLED → CANCELLED.
//  3. Delivery: SUCCEEDED + scheduled → SCHEDULED; SUCCEEDED + not
//     scheduled → PUBLISHED; all deliveries SUCCEEDED → COMPLETED.
//  4. Artifact: READY → ARTIFACT_READY; STAGING/other → ARTIFACT_PROCESSING.
//  5. Job: SUCCEEDED → ARTIFACT_PROCESSING (waiting for artifact);
//     RUNNING/LEASED → RENDERING; PENDING/RETRY_WAIT → WORKER_QUEUED.
//  6. Forwarding: FORWARDED → WORKER_QUEUED; FORWARDING → FORWARDING;
//     READY_TO_FORWARD → FORWARDING; PENDING/POLLING/RETRY_WAIT → REMOTE_QUEUED.
//  7. Remote: completed → REMOTE_COMPLETED; running → REMOTE_RUNNING;
//     queued → REMOTE_QUEUED.
//  8. Default → ACCEPTED.
//
// The function is pure: it does not modify InternalState or have side
// effects. Callers persist the returned Status via
// store.UpdatePipelineRunStatus.
func DeriveStatus(state InternalState) Status {
	// Preserve the documented precedence: terminal failure, cancellation,
	// then the most advanced internal phase available.
	if hasFailedState(state) {
		return StatusFailed
	}
	if hasCancelledState(state) {
		return StatusCancelled
	}
	if status, ok := deriveDeliveryStatus(state); ok {
		return status
	}
	if status, ok := deriveArtifactStatus(state); ok {
		return status
	}
	if status, ok := deriveJobStatus(state); ok {
		return status
	}
	if status, ok := deriveForwardingStatus(state); ok {
		return status
	}
	if status, ok := deriveRemoteStatus(state); ok {
		return status
	}
	return StatusAccepted
}

func hasFailedState(state InternalState) bool {
	return isFailedState(state.ForwardingStatus) ||
		isFailedState(state.JobStatus) ||
		isFailedState(state.ArtifactStatus) ||
		isFailedState(state.DeliveryStatus) ||
		isFailedRemoteState(state.RemoteJobStatus)
}

func hasCancelledState(state InternalState) bool {
	return isCancelledState(state.ForwardingStatus) ||
		isCancelledState(state.JobStatus) ||
		isCancelledState(state.DeliveryStatus) ||
		isCancelledRemoteState(state.RemoteJobStatus)
}

func deriveDeliveryStatus(state InternalState) (Status, bool) {
	if state.DeliveryStatus == nil {
		return "", false
	}
	switch *state.DeliveryStatus {
	case "SUCCEEDED":
		if state.HasScheduledDelivery {
			return StatusScheduled, true
		}
		if state.AllDeliveriesSucceeded {
			return StatusCompleted, true
		}
		return StatusPublished, true
	case "PENDING":
		// PENDING means the delivery runner has not picked up the row yet,
		// regardless of artifact readiness.
		return StatusDeliveryPending, true
	case "RUNNING", "RETRY_WAIT":
		// Active delivery requires a READY artifact. Otherwise it remains
		// pending from the aggregate pipeline's point of view.
		if state.ArtifactStatus != nil && *state.ArtifactStatus == "READY" {
			return StatusDelivering, true
		}
		return StatusDeliveryPending, true
	default:
		return "", false
	}
}

func deriveArtifactStatus(state InternalState) (Status, bool) {
	if state.ArtifactStatus == nil {
		return "", false
	}
	switch *state.ArtifactStatus {
	case "READY":
		// Artifact is ready but no delivery row exists yet.
		return StatusArtifactReady, true
	case "STAGING":
		return StatusArtifactProcessing, true
	default:
		// QUARANTINED / DELETED / other non-failed states remain in
		// artifact processing; failure precedence was handled above.
		return StatusArtifactProcessing, true
	}
}

func deriveJobStatus(state InternalState) (Status, bool) {
	if state.JobStatus == nil {
		return "", false
	}
	switch *state.JobStatus {
	case "SUCCEEDED", "AWAITING_ARTIFACT":
		// The job is done rendering but its artifact phase is not complete.
		return StatusArtifactProcessing, true
	case "RUNNING", "LEASED":
		return StatusRendering, true
	case "PENDING", "RETRY_WAIT":
		return StatusWorkerQueued, true
	default:
		return "", false
	}
}

func deriveForwardingStatus(state InternalState) (Status, bool) {
	if state.ForwardingStatus == nil {
		return "", false
	}
	switch *state.ForwardingStatus {
	case "FORWARDED":
		return StatusWorkerQueued, true
	case "FORWARDING", "READY_TO_FORWARD":
		return StatusForwarding, true
	case "PENDING", "POLLING", "RETRY_WAIT":
		return StatusRemoteQueued, true
	default:
		return "", false
	}
}

func deriveRemoteStatus(state InternalState) (Status, bool) {
	if state.RemoteJobStatus == nil {
		return "", false
	}
	switch *state.RemoteJobStatus {
	case "completed":
		return StatusRemoteCompleted, true
	case "running":
		return StatusRemoteRunning, true
	case "queued":
		return StatusRemoteQueued, true
	default:
		return "", false
	}
}

// ── Internal classification helpers ──────────────────────────────────

// isFailedState returns true when the string pointer is non-nil and
// contains a terminal failure status from the internal domain state
// machines (forwarding, job, artifact, delivery).
func isFailedState(s *string) bool {
	if s == nil {
		return false
	}
	switch *s {
	case "FAILED", "BLOCKED", "BLOCKED_AUTH", "QUARANTINED":
		return true
	default:
		return false
	}
}

// isFailedRemoteState returns true when the remote engine job status
// indicates a terminal failure. The remote engine uses lowercase status
// strings ("failed", "cancelled") while internal domains use uppercase.
func isFailedRemoteState(s *string) bool {
	if s == nil {
		return false
	}
	switch *s {
	case "failed", "FAILED":
		return true
	default:
		return false
	}
}

// isCancelledState returns true when the string pointer is non-nil and
// contains a cancelled status from the internal domain state machines.
func isCancelledState(s *string) bool {
	if s == nil {
		return false
	}
	switch *s {
	case "CANCELLED", "DELETED":
		return true
	default:
		return false
	}
}

// isCancelledRemoteState returns true when the remote engine job
// status indicates cancellation.
func isCancelledRemoteState(s *string) bool {
	if s == nil {
		return false
	}
	switch *s {
	case "cancelled", "CANCELLED":
		return true
	default:
		return false
	}
}
