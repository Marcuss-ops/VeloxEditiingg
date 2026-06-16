package queue

import (
	"context"
	"fmt"
	"log"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Lifecycle
// ────────────────────────────────────────────────────────────────────────────

// Start begins the orchestrator's background polling loop.
func (o *Orchestrator) Start(ctx context.Context) {
	log.Printf("[ORCH] Starting orchestrator loop")

	ticker := time.NewTicker(DefaultOrchestratorConfig().PollInterval)
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
		case evt := <-o.dispatchCh:
			o.handleDispatch(ctx, evt)
		case evt := <-o.completeCh:
			o.handleComplete(ctx, evt)
		case <-ticker.C:
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
// The callback can, for example, enqueue the step in an external system.
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
// Job Management
// ────────────────────────────────────────────────────────────────────────────

// SubmitMultiStepJob registers a new multi-step pipeline for orchestration.
func (o *Orchestrator) SubmitMultiStepJob(ctx context.Context, jobID string, steps []*JobStep, pipelineType string, metadata map[string]interface{}) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.jobs[jobID]; exists {
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
			step.MaxRetries = DefaultOrchestratorConfig().DefaultMaxRetries
		}
		msj.Steps[i] = step
	}

	o.jobs[jobID] = msj
	o.persist(msj)

	log.Printf("[ORCH] Submitted multi-step job %s (%d steps, type=%s)", jobID[:8], msj.TotalSteps, pipelineType)
	return nil
}

// GetJob returns the current state of a multi-step job.
func (o *Orchestrator) GetJob(jobID string) *MultiStepJob {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.jobs[jobID]
}

// ListJobs returns all tracked multi-step jobs.
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
// Called externally (e.g. from a webhook or completion handler).
func (o *Orchestrator) ReportStepComplete(jobID, stepID string, result map[string]interface{}, execErr error) {
	evt := &stepCompleteEvent{
		JobID:  jobID,
		StepID: stepID,
		Result: result,
	}
	if execErr != nil {
		evt.Error = execErr.Error()
	}
	select {
	case o.completeCh <- evt:
	default:
		log.Printf("[ORCH] Warning: complete channel full, dropping event for %s/%s", jobID[:8], stepID)
	}
}
