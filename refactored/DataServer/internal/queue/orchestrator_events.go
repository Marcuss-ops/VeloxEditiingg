package queue

import (
	"context"
	"fmt"
	"log"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Event Handlers
// ────────────────────────────────────────────────────────────────────────────

func (o *Orchestrator) handleDispatch(ctx context.Context, evt *stepDispatchEvent) {
	o.mu.Lock()
	msj, ok := o.jobs[evt.JobID]
	o.mu.Unlock()
	if !ok {
		return
	}

	var step *JobStep
	for _, s := range msj.Steps {
		if s.StepID == evt.StepID {
			step = s
			break
		}
	}
	if step == nil {
		return
	}

	// Enqueue the step as a FileQueue job
	payload := make(map[string]interface{})
	for k, v := range step.Payload {
		payload[k] = v
	}
	payload["_orchestrator_job_id"] = evt.JobID
	payload["_orchestrator_step_id"] = evt.StepID

	queueJobID := fmt.Sprintf("%s-%s", evt.JobID, evt.StepID)
	if err := o.fileQ.SubmitJob(ctx, queueJobID, payload); err != nil {
		log.Printf("[ORCH] Failed to enqueue step %s for job %s: %v", evt.StepID, evt.JobID[:8], err)
		// Reset step status to Ready so it can be retried on the next poll cycle
		step.Status = StepReady
		o.persist(msj)
		return
	}

	step.Status = StepProcessing
	o.persist(msj)
	log.Printf("[ORCH] Dispatched step %s for job %s as job %s", step.StepName, msj.JobID[:8], queueJobID)
}

func (o *Orchestrator) handleComplete(ctx context.Context, evt *stepCompleteEvent) {
	o.mu.Lock()
	msj, ok := o.jobs[evt.JobID]
	o.mu.Unlock()
	if !ok {
		return
	}

	var step *JobStep
	for _, s := range msj.Steps {
		if s.StepID == evt.StepID {
			step = s
			break
		}
	}
	if step == nil {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if evt.Error != "" {
		step.Error = evt.Error
		step.RetryCount++
		if step.RetryCount < step.MaxRetries {
			step.Status = StepReady
			log.Printf("[ORCH] Step %s failed, retrying (%d/%d): %s", step.StepName, step.RetryCount, step.MaxRetries, evt.Error)
			o.dispatchStep(ctx, msj, step)
		} else {
			step.Status = StepFailed
			step.CompletedAt = &now
			o.failJob(msj, fmt.Sprintf("Step %s failed after %d retries: %s", step.StepName, step.RetryCount, evt.Error))
		}
	} else {
		step.Status = StepCompleted
		step.CompletedAt = &now
		step.Result = evt.Result
		log.Printf("[ORCH] Step %s completed for job %s", step.StepName, msj.JobID[:8])
	}

	o.persist(msj)

	// Advance to next steps
	if evt.Error == "" || step.Status == StepReady {
		o.advanceJob(ctx, msj)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Polling & Advancement
// ────────────────────────────────────────────────────────────────────────────

// poll checks active orchestrator jobs for ready steps and completed step jobs.
func (o *Orchestrator) poll(ctx context.Context) {
	o.mu.RLock()
	// Clone the relevant fields under the lock to avoid data races
	type jobSnapshot struct {
		msj    *MultiStepJob
		status StepStatus
	}
	snapshots := make([]jobSnapshot, 0, len(o.jobs))
	for _, j := range o.jobs {
		snapshots = append(snapshots, jobSnapshot{msj: j, status: j.Status})
	}
	o.mu.RUnlock()

	for _, s := range snapshots {
		if s.status == StepCompleted || s.status == StepFailed {
			continue
		}

		o.advanceJob(ctx, s.msj)
		o.checkJobTimeout(ctx, s.msj)
	}
}

// advanceJob finds the next ready step and dispatches it.
func (o *Orchestrator) advanceJob(ctx context.Context, msj *MultiStepJob) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if msj.Status == StepCompleted || msj.Status == StepFailed {
		return
	}

	// Mark job as processing if not already
	if msj.Status == StepPending {
		now := time.Now().UTC().Format(time.RFC3339)
		msj.Status = StepProcessing
		msj.StartedAt = &now
		msj.UpdatedAt = now
		o.persist(msj)
	}

	allDone := true
	for _, step := range msj.Steps {
		if step.Status == StepCompleted || step.Status == StepSkipped {
			continue
		}
		allDone = false

		// Check dependencies
		depsMet := true
		for _, depID := range step.Dependencies {
			depStep := findStep(msj, depID)
			if depStep == nil || depStep.Status != StepCompleted {
				depsMet = false
				break
			}
		}
		if !depsMet {
			continue
		}

		if step.Status == StepPending {
			step.Status = StepReady
			msj.CurrentStep = step.StepOrder
			msj.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			o.persist(msj)

			// Dispatch the ready step
			o.dispatchStep(ctx, msj, step)

			// Call external callback if set
			if o.onStepReady != nil {
				go func(s *JobStep) {
					if err := o.onStepReady(s); err != nil {
						log.Printf("[ORCH] Step ready callback error for %s: %v", s.StepID, err)
					}
				}(step)
			}
			return // Only dispatch one step at a time
		}
	}

	if allDone {
		o.completeJob(msj)
	}
}

// dispatchStep sends a step for execution as a FileQueue job.
func (o *Orchestrator) dispatchStep(ctx context.Context, msj *MultiStepJob, step *JobStep) {
	if step.Status != StepReady {
		return
	}
	step.Status = StepProcessing
	o.persist(msj)

	evt := &stepDispatchEvent{
		JobID:  msj.JobID,
		StepID: step.StepID,
	}
	select {
	case o.dispatchCh <- evt:
	default:
		// Channel full — reset step to Ready so poll cycle retries.
		// IMPORTANT: do NOT call handleDispatch here (would deadlock, already holding mu.Lock).
		log.Printf("[ORCH] Warning: dispatch channel full, step %s will retry next cycle", step.StepID)
		step.Status = StepReady
		o.persist(msj)
	}
}

// checkJobTimeout marks steps as failed if they've been PROCESSING too long.
func (o *Orchestrator) checkJobTimeout(ctx context.Context, msj *MultiStepJob) {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := time.Now()
	timeout := DefaultOrchestratorConfig().JobTimeout

	for _, step := range msj.Steps {
		if step.Status != StepProcessing {
			continue
		}
		created, err := time.Parse(time.RFC3339, step.CreatedAt)
		if err != nil {
			continue
		}
		if now.After(created.Add(timeout)) {
			log.Printf("[ORCH] Step %s timed out (exceeded %v)", step.StepName, timeout)
			step.Status = StepFailed
			step.Error = fmt.Sprintf("step timed out after %v", timeout)
			o.failJob(msj, fmt.Sprintf("Step %s timed out", step.StepName))
			o.persist(msj)
		}
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Job Completion / Failure
// ────────────────────────────────────────────────────────────────────────────

func (o *Orchestrator) completeJob(msj *MultiStepJob) {
	now := time.Now().UTC().Format(time.RFC3339)
	msj.Status = StepCompleted
	msj.CompletedAt = &now
	msj.UpdatedAt = now

	o.persist(msj)
	log.Printf("[ORCH] Multi-step job %s completed (%d steps)", msj.JobID[:8], msj.TotalSteps)

	if o.onJobComplete != nil {
		go o.onJobComplete(msj)
	}
}

func (o *Orchestrator) failJob(msj *MultiStepJob, reason string) {
	now := time.Now().UTC().Format(time.RFC3339)
	msj.Status = StepFailed
	msj.CompletedAt = &now
	msj.UpdatedAt = now

	o.persist(msj)
	log.Printf("[ORCH] Multi-step job %s failed: %s", msj.JobID[:8], reason)

	if o.onJobFail != nil {
		go o.onJobFail(msj, reason)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func findStep(job *MultiStepJob, stepID string) *JobStep {
	for _, s := range job.Steps {
		if s.StepID == stepID {
			return s
		}
	}
	return nil
}

// Stats returns diagnostic statistics about the orchestrator.
func (o *Orchestrator) Stats() map[string]interface{} {
	o.mu.RLock()
	defer o.mu.RUnlock()

	total := len(o.jobs)
	pending, running, completed, failed := 0, 0, 0, 0
	for _, j := range o.jobs {
		switch j.Status {
		case StepPending, StepReady:
			pending++
		case StepProcessing:
			running++
		case StepCompleted:
			completed++
		case StepFailed:
			failed++
		}
	}

	return map[string]interface{}{
		"total_jobs":       total,
		"pending_jobs":     pending,
		"running_jobs":     running,
		"completed_jobs":   completed,
		"failed_jobs":      failed,
		"dispatch_channel": len(o.dispatchCh),
		"complete_channel": len(o.completeCh),
	}
}
