// Package handlers/outbox — concrete outbox handlers that translate
// domain events emitted inside the master into workflow.state mutations.
//
// All four handlers are registered with the outbox.Registry at startup
// (one central registry, no per-package switches). Per PR 8 spec:
//
//   registry.Register(jobSucceededHandler)
//   registry.Register(artifactReadyHandler)
//   registry.Register(workflowStepReadyHandler)
//   registry.Register(deliveryCreatedHandler)
//
// Handlers MUST be idempotent because the dispatcher may retry on
// transient errors. They run inside the dispatcher's tick so blocking I/O
// should be limited; the heavy lifting (artifact verification, delivery)
// happens elsewhere — these handlers only mutate workflow state.
//
// All other production handlers in the codebase follow the same shape:
//   - structured Payload field,
//   - aggregate_id understood via WorkflowOutboxEvent.AggregateID,
//   - best-effort: a non-fatal error becomes Transient so the dispatcher
//     retries with backoff up to MaxAttempts.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"velox-server/internal/outbox"
	"velox-server/internal/queue"
	"velox-server/internal/workflow"
)

// StepReadyHandler reacts to WORKFLOW_STEP_READY. It creates a FileQueue
// job with workflow_steps.input_json as the payload and writes the new
// job_id back into workflow_steps.job_id, then transitions the step from
// READY → RUNNING.
//
// Idempotency: if workflow_steps.job_id is already set OR the step is no
// longer in READY (was cancelled or already advanced), the handler is a
// no-op — re-dispatches are safe.
type StepReadyHandler struct {
	Wf workflow.Repository
	Q  *queue.FileQueue
}

func (StepReadyHandler) EventType() string { return "WORKFLOW_STEP_READY" }

func (h StepReadyHandler) Handle(ctx context.Context, e outbox.Event) error {
	var p struct {
		RunID   string `json:"run_id"`
		StepID  string `json:"step_id"`
		StepKey string `json:"step_key"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return outbox.Permanent(fmt.Errorf("workflowStepReady: payload: %w", err))
	}
	if p.RunID == "" || p.StepID == "" {
		return outbox.Permanent(fmt.Errorf("workflowStepReady: run_id/step_id missing"))
	}

	steps, err := h.Wf.ListSteps(ctx, p.RunID)
	if err != nil {
		return outbox.Transient(fmt.Errorf("workflowStepReady: list steps: %w", err))
	}
	var st *workflow.Step
	for i := range steps {
		if steps[i].StepID == p.StepID || (p.StepKey != "" && steps[i].StepKey == p.StepKey) {
			st = &steps[i]
			break
		}
	}
	if st == nil {
		return outbox.Permanent(fmt.Errorf("workflowStepReady: step not found"))
	}
	if st.JobID != nil && *st.JobID != "" {
		// Already dispatched (idempotency)
		return nil
	}
	if st.Status != workflow.StepStatusReady {
		// Step may have been cancelled or is no longer dispatchable.
		return nil
	}

	// Build the worker payload from step input + workflow context.
	payload := map[string]any{
		"step_key": st.StepKey,
		"step_id":  st.StepID,
		"run_id":   p.RunID,
	}
	for k, v := range st.Input {
		payload[k] = v
	}

	// job_id = "{run_id}-{step_key}" so the JOB_SUCCEEDED handler can
	// recover the run via the workflow_steps.job_id UNIQUE column.
	jobID := fmt.Sprintf("%s-%s", p.RunID, st.StepKey)
	if err := h.Q.SubmitJob(ctx, jobID, payload); err != nil {
		return outbox.Transient(fmt.Errorf("workflowStepReady: submit job: %w", err))
	}

	attempt := st.Attempt + 1
	if attempt < 1 {
		attempt = 1
	}
	if err := h.Wf.MarkStepRunning(ctx, workflow.StartStep{
		RunID:   p.RunID,
		StepID:  st.StepID,
		JobID:   jobID,
		Attempt: attempt,
	}); err != nil {
		return outbox.Transient(fmt.Errorf("workflowStepReady: mark running: %w", err))
	}
	return nil
}

// JobSucceededHandler reacts to JOB_SUCCEEDED events.
// The outbox row's aggregate_id is the job_id; the handler asks the
// Repository to recover the owning workflow step via job_id.
type JobSucceededHandler struct {
	Wf workflow.Repository
}

func (JobSucceededHandler) EventType() string { return "JOB_SUCCEEDED" }

func (h JobSucceededHandler) Handle(ctx context.Context, e outbox.Event) error {
	var p struct {
		JobID   string         `json:"job_id"`
		Output  map[string]any `json:"output,omitempty"`
		Attempt int            `json:"attempt,omitempty"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return outbox.Permanent(fmt.Errorf("jobSucceeded: payload: %w", err))
	}
	// aggregate_id is the canonical source; payload.JobID is a fallback.
	jobID := e.AggregateID
	if jobID == "" {
		jobID = p.JobID
	}
	if jobID == "" {
		return outbox.Permanent(fmt.Errorf("jobSucceeded: empty job_id"))
	}

	step, runID, err := h.Wf.GetStepByJobID(ctx, jobID)
	if err != nil {
		return outbox.Transient(fmt.Errorf("jobSucceeded: lookup: %w", err))
	}
	if step == nil {
		// Job isn't part of any workflow run — silently no-op.
		return nil
	}
	if step.Status == workflow.StepStatusSucceeded {
		return nil
	}

	_, err = h.Wf.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID:   runID,
		StepID:  step.StepID,
		Output:  p.Output,
		Attempt: p.Attempt,
	})
	if err != nil {
		return outbox.Transient(fmt.Errorf("jobSucceeded: complete step: %w", err))
	}
	return nil
}

// ArtifactReadyHandler reacts to ARTIFACT_READY. ARTIFACT_READY events
// are emitted by FinalizeArtifactVerified inside the store layer. They
// do not directly unblock workflow steps — JOB_SUCCEEDED does that —
// but downstream observability subsystems can subscribe to this signal.
// We keep the handler as a registry anchor so PR 9 §"handler registry
// unico" is satisfied; production extensions (e.g. webhook fan-out) can
// RegisterNoReplace it later.
type ArtifactReadyHandler struct {
	Wf workflow.Repository
}

func (ArtifactReadyHandler) EventType() string { return "ARTIFACT_READY" }

func (ArtifactReadyHandler) Handle(ctx context.Context, e outbox.Event) error {
	var p struct {
		JobID      string `json:"job_id"`
		ArtifactID string `json:"artifact_id"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return outbox.Permanent(fmt.Errorf("artifactReady: payload: %w", err))
	}
	return nil
}

// DeliveryCreatedHandler reacts to DELIVERY_CREATED. Currently a no-op
// (kept as a registry anchor for future delivery-side fan-out).
type DeliveryCreatedHandler struct {
	Wf workflow.Repository
}

func (DeliveryCreatedHandler) EventType() string { return "DELIVERY_CREATED" }

func (DeliveryCreatedHandler) Handle(ctx context.Context, e outbox.Event) error {
	return nil
}
