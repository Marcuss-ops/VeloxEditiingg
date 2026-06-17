package queue

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/store"
)

// ────────────────────────────────────────────────────────────────────────────
// Lifecycle
// ────────────────────────────────────────────────────────────────────────────

// Start begins the orchestrator's background polling loop.
// Polls both the outbox table (for events) and a ticker (for advancement).
// PR5: No channels — all events go through the outbox table.
func (o *Orchestrator) Start(ctx context.Context) {
	log.Printf("[ORCH] Starting orchestrator loop (outbox-based, SQLite-authoritative)")

	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[ORCH] Stopping (context done)")
			o.saveAll()
			return
		case <-o.stopCh:
			log.Printf("[ORCH] Stopping")
			o.saveAll()
			return
		case <-o.notifyCh:
			// Wakeup signal — there are new outbox entries to process
			o.pollOutbox(ctx)
		case <-ticker.C:
			o.pollOutbox(ctx)
			o.poll(ctx)
		}
	}
}

// Stop signals the orchestrator to shut down gracefully.
func (o *Orchestrator) Stop() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.stopped {
		o.stopped = true
		close(o.stopCh)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Callbacks
// ────────────────────────────────────────────────────────────────────────────

// SetStepReadyCallback sets a callback invoked when a step becomes ready.
func (o *Orchestrator) SetStepReadyCallback(cb func(step *JobStep) error) {
	o.onStepReady = cb
}

// SetJobCompleteCallback sets a callback invoked when all steps of a job complete.
func (o *Orchestrator) SetJobCompleteCallback(cb func(job *MultiStepJob)) {
	o.onJobComplete = cb
}

// SetJobFailCallback sets a callback invoked when a job fails (non-recoverable).
func (o *Orchestrator) SetJobFailCallback(cb func(job *MultiStepJob, reason string)) {
	o.onJobFail = cb
}

// ────────────────────────────────────────────────────────────────────────────
// Job Management (PR5: outbox-based, SQLite-authoritative)
// ────────────────────────────────────────────────────────────────────────────

// SubmitMultiStepJob registers a new multi-step pipeline for orchestration.
// Writes to SQLite + outbox in a single transaction.
func (o *Orchestrator) SubmitMultiStepJob(ctx context.Context, jobID string, steps []*JobStep, pipelineType string, metadata map[string]interface{}) error {
	o.mu.Lock()
	if _, exists := o.jobs[jobID]; exists {
		o.mu.Unlock()
		return fmt.Errorf("orchestrator job %s already exists", jobID)
	}

	now := time.Now().UTC().Format(time.RFC3339)

	msj := &MultiStepJob{
		JobID:        jobID,
		PipelineType: pipelineType,
		Status:       StepPending,
		TotalSteps:   len(steps),
		CurrentStep:  0,
		Metadata:     metadata,
		CreatedAt:    now,
		UpdatedAt:    now,
		Steps:        make([]*JobStep, len(steps)),
	}

	for i, step := range steps {
		step.Status = StepPending
		step.CreatedAt = now
		if step.MaxRetries <= 0 {
			step.MaxRetries = o.defaultMaxRetries
		}
		msj.Steps[i] = step
	}

	o.mu.Unlock()

	// Persist to SQLite first (PR5: SQLite is authoritative, cache follows).
	// resolveJob() handles cache misses by loading from SQLite.
	raw := marshalJob(msj)
	if err := o.dbStore.UpsertOrchestratorJobWithOutbox(
		msj.JobID, string(msj.Status), msj.PipelineType,
		msj.TotalSteps, msj.CurrentStep, raw,
		"", "", // startedAt/completedAt empty on initial submit
		[]store.OutboxEntry{{
			EventType: "step_ready",
			JobID:     jobID,
			Payload:   "{}",
		}},
	); err != nil {
		return fmt.Errorf("failed to persist orchestrator job: %w", err)
	}

	// Update cache AFTER successful SQLite write (no dirty cache window)
	o.mu.Lock()
	o.jobs[jobID] = msj
	o.mu.Unlock()

	o.notify()
	log.Printf("[ORCH] Submitted multi-step job %s (%d steps, type=%s)", jobID[:min(8, len(jobID))], msj.TotalSteps, pipelineType)
	return nil
}

// GetJob returns the current state of a multi-step job (cache, SQLite on miss).
func (o *Orchestrator) GetJob(jobID string) *MultiStepJob {
	return o.resolveJob(jobID)
}

// ListJobs returns all tracked multi-step jobs from cache.
func (o *Orchestrator) ListJobs() []*MultiStepJob {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]*MultiStepJob, 0, len(o.jobs))
	for _, j := range o.jobs {
		out = append(out, j)
	}
	return out
}

// ReportStepComplete notifies the orchestrator that a step has completed.
// PR5: Writes to outbox table instead of channel (events never lost).
func (o *Orchestrator) ReportStepComplete(jobID, stepID string, result map[string]interface{}, execErr error) {
	payload := map[string]interface{}{
		"result": result,
	}
	if execErr != nil {
		payload["error"] = execErr.Error()
		o.submitOutboxEntry("step_failed", jobID, stepID, payload)
	} else {
		o.submitOutboxEntry("step_complete", jobID, stepID, payload)
	}
	log.Printf("[ORCH] Reported step %s for job %s (outbox)", stepID, jobID[:min(8, len(jobID))])
}
