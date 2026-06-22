// Package workflowevents — concrete outbox handlers. During the workflow→
// taskgraph cutover (PR-operation 01, Fase 4a) the step-resolution path is
// mediation-free: artifacts.FinalizeArtifactVerified writes the canonical
// job/artifact/delivery state, and the supervisor's zombie-reaper tick
// (jobs.LifecycleService.RequeueExpiredLeases + taskgraph DAG propagation)
// covers dependent-release.
//
// The four handlers here are kept as inert receivers so the outbox.Registry
// registers a binding for the legacy event types. Any residual row sitting
// in outbox_events from before/during the cutover gets ACK'd without side
// effects, draining the queue cleanly. New traffic does NOT emit
// WORKFLOW_STEP_READY (POST routes through creatorflow.CreateJobWithPlan,
// which uses store.AtomicJobTaskCreator + jobs.Writer directly) and does
// NOT route JOB_SUCCEEDED through workflow state (artifacts handles the
// canonical transition), so these handlers stay quiet in steady state.
// Schedule: deletion booked for Fase 8 once outbox_events is purged of
// all legacy rows (see docs/operations/01-workflow-taskgraph-cutover.md).
package workflowevents

import (
	"context"

	"velox-server/internal/outbox"
)

// StepReadyHandler — drain-only stub for WORKFLOW_STEP_READY. The event is
// no longer emitted by production writers because the legacy
// workflow.Repository.CreateRun path was decommissioned in the Fase 3
// cutover (POST /api/v1/orchestrator/jobs routes through the canonical
// creatorflow.CreateJobWithPlan + AtomicJobTaskCreator).
type StepReadyHandler struct{}

// EventType returns the canonical outbox topic.
func (StepReadyHandler) EventType() string { return "WORKFLOW_STEP_READY" }

// Handle acks the event without side-effects. ACK semantics: returning nil
// tells the dispatcher the row is consumed; a FAILED terminal row would
// pollute metrics, so the safe default during a cutover is to ACK.
func (StepReadyHandler) Handle(_ context.Context, _ outbox.Event) error {
	return nil
}

// JobSucceededHandler — drain-only stub for JOB_SUCCEEDED. The artifact
// finalization path already updates canonical job state via
// jobs.LifecycleService.Complete + jobs.Writer.Update; the step-level
// lookup that used to bridge into workflow.Repository (Wf.GetStepByJobID →
// Wf.CompleteStepAndReleaseDependents) is obsolete after Fase 4.
type JobSucceededHandler struct{}

// EventType returns the canonical outbox topic.
func (JobSucceededHandler) EventType() string { return "JOB_SUCCEEDED" }

// Handle acks the event without side-effects.
func (JobSucceededHandler) Handle(_ context.Context, _ outbox.Event) error {
	return nil
}

// ArtifactReadyHandler — drain-only stub for ARTIFACT_READY. Pre-cutover
// implementations logged + forwarded; today the canonical pipeline
// consumes artifact readiness inside the artifact finalization transaction.
type ArtifactReadyHandler struct{}

// EventType returns the canonical outbox topic.
func (ArtifactReadyHandler) EventType() string { return "ARTIFACT_READY" }

// Handle acks the event without side-effects.
func (ArtifactReadyHandler) Handle(_ context.Context, _ outbox.Event) error {
	return nil
}

// DeliveryCreatedHandler — drain-only stub for DELIVERY_CREATED. The
// delivery runner (deliveries.DeliveryRunner) polls PENDING
// job_deliveries directly; no fan-out needed.
type DeliveryCreatedHandler struct{}

// EventType returns the canonical outbox topic.
func (DeliveryCreatedHandler) EventType() string { return "DELIVERY_CREATED" }

// Handle acks the event without side-effects.
func (DeliveryCreatedHandler) Handle(_ context.Context, _ outbox.Event) error {
	return nil
}
