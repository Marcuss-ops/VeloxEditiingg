package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"velox-server/internal/store"
)

// ────────────────────────────────────────────────────────────────────────────
// Outbox-based event processing (PR5: replaces channel-based dispatch)
// ────────────────────────────────────────────────────────────────────────────

// handleDispatch enqueues a step as a FileQueue job.
// Called from the outbox processor when a "step_dispatch" entry is processed.
// Idempotent: skips if the step is already Processing (outbox reprocessing).
func (o *Orchestrator) handleDispatch(ctx context.Context, msj *MultiStepJob, stepID string) {
	step := findStep(msj, stepID)
	if step == nil {
		return
	}
	// Idempotency guard: don't re-dispatch an already-processing step
	if step.Status == StepProcessing {
		return
	}

	// Build payload for the FileQueue job
	payload := make(map[string]interface{})
	for k, v := range step.Payload {
		payload[k] = v
	}
	payload["_orchestrator_job_id"] = msj.JobID
	payload["_orchestrator_step_id"] = stepID

	queueJobID := fmt.Sprintf("%s-%s", msj.JobID, stepID)
	if err := o.fileQ.SubmitJob(ctx, queueJobID, payload); err != nil {
		log.Printf("[ORCH] Failed to enqueue step %s for job %s: %v", stepID, msj.JobID[:min(8, len(msj.JobID))], err)
		// Reset step to Ready so it can be retried on the next poll cycle
		step.Status = StepReady
		o.persist(msj)
		return
	}

	step.Status = StepProcessing
	o.persist(msj)
	log.Printf("[ORCH] Dispatched step %s for job %s as job %s", step.StepName, msj.JobID[:min(8, len(msj.JobID))], queueJobID)
}

// handleComplete processes a completed step from the outbox.
func (o *Orchestrator) handleComplete(ctx context.Context, msj *MultiStepJob, stepID string, payloadRaw interface{}) {
	step := findStep(msj, stepID)
	if step == nil {
		return
	}
	// Idempotency guard: only process if step is still Processing
	if step.Status != StepProcessing {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// Parse payload for result data
	var result map[string]interface{}
	switch p := payloadRaw.(type) {
	case string:
		_ = json.Unmarshal([]byte(p), &result)
	case map[string]interface{}:
		if r, ok := p["result"].(map[string]interface{}); ok {
			result = r
		} else {
			result = p
		}
	}

	step.Status = StepCompleted
	step.CompletedAt = &now
	step.Result = result
	step.Error = ""
	log.Printf("[ORCH] Step %s completed for job %s", step.StepName, msj.JobID[:min(8, len(msj.JobID))])

	o.mu.Lock()
	o.persist(msj)
	o.mu.Unlock()

	// Advance to next steps
	o.advanceJob(ctx, msj)
}

// handleStepFailed processes a failed step from the outbox.
func (o *Orchestrator) handleStepFailed(ctx context.Context, msj *MultiStepJob, stepID string, payloadRaw interface{}) {
	step := findStep(msj, stepID)
	if step == nil {
		return
	}
	// Idempotency guard: only process if step is still Processing
	// (prevents double-incrementing RetryCount on outbox reprocessing)
	if step.Status != StepProcessing {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)

	var errMsg string
	switch p := payloadRaw.(type) {
	case string:
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(p), &m); err == nil {
			if e, ok := m["error"].(string); ok {
				errMsg = e
			}
		}
	case map[string]interface{}:
		if e, ok := p["error"].(string); ok {
			errMsg = e
		}
	}

	step.Error = errMsg
	step.RetryCount++

	if step.RetryCount < step.MaxRetries {
		step.Status = StepReady
		log.Printf("[ORCH] Step %s failed, retrying (%d/%d): %s", step.StepName, step.RetryCount, step.MaxRetries, errMsg)
		o.persist(msj)

		// Dispatch for retry
		o.dispatchStep(ctx, msj, step)
	} else {
		step.Status = StepFailed
		step.CompletedAt = &now
		o.failJob(msj, fmt.Sprintf("Step %s failed after %d retries: %s", step.StepName, step.RetryCount, errMsg))
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Polling & Advancement
// ────────────────────────────────────────────────────────────────────────────

// poll checks active orchestrator jobs for ready steps, job timeouts,
// and stale worker recovery (PR5b: heartbeat-based step reassignment).
func (o *Orchestrator) poll(ctx context.Context) {
	o.mu.RLock()
	snapshots := make([]*MultiStepJob, 0, len(o.jobs))
	for _, j := range o.jobs {
		if j.Status == StepCompleted || j.Status == StepFailed {
			continue
		}
		snapshots = append(snapshots, j)
	}
	o.mu.RUnlock()

	// PR5b: Sync worker assignments from FileQueue (read-only cross-check)
	o.syncWorkerAssignments(ctx)

	// PR5b: Recover steps assigned to stale/dead workers
	o.recoverStaleWorkerSteps(ctx)

	for _, msj := range snapshots {
		o.advanceJob(ctx, msj)
		o.checkJobTimeout(msj)
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

			// Dispatch the ready step via outbox
			o.dispatchStep(ctx, msj, step)

			// Call external callback if set
			if o.onStepReady != nil {
				go func(s *JobStep) {
					if err := o.onStepReady(s); err != nil {
						log.Printf("[ORCH] Step ready callback error for %s: %v", s.StepID, err)
					}
				}(step)
			}
			return
		}
	}

	if allDone {
		o.completeJob(msj)
	}
}

// dispatchStep writes a step_dispatch outbox entry and marks the step Ready→Processing.
func (o *Orchestrator) dispatchStep(ctx context.Context, msj *MultiStepJob, step *JobStep) {
	if step.Status != StepReady && step.Status != StepPending {
		return
	}

	// Write dispatch event to outbox (transactional with the step status change)
	step.Status = StepProcessing
	payload := "{}"
	if step.Payload != nil {
		if raw, err := json.Marshal(step.Payload); err == nil {
			payload = string(raw)
		}
	}

	o.persist(msj, store.OutboxEntry{
		EventType: "step_dispatch",
		JobID:     msj.JobID,
		StepID:    step.StepID,
		Payload:   payload,
	})
}

// checkJobTimeout marks steps as failed if they've been PROCESSING too long.
func (o *Orchestrator) checkJobTimeout(msj *MultiStepJob) {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := time.Now()

	for _, step := range msj.Steps {
		if step.Status != StepProcessing {
			continue
		}
		created, err := time.Parse(time.RFC3339, step.CreatedAt)
		if err != nil {
			continue
		}
		if now.After(created.Add(o.jobTimeout)) {
			log.Printf("[ORCH] Step %s timed out (exceeded %v)", step.StepName, o.jobTimeout)
			step.Status = StepFailed
			step.Error = fmt.Sprintf("step timed out after %v", o.jobTimeout)
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

	o.persist(msj, store.OutboxEntry{
		EventType: "job_complete",
		JobID:     msj.JobID,
		Payload:   `{}`,
	})
	log.Printf("[ORCH] Multi-step job %s completed (%d steps)", msj.JobID[:min(8, len(msj.JobID))], msj.TotalSteps)

	if o.onJobComplete != nil {
		go o.onJobComplete(msj)
	}
}

func (o *Orchestrator) failJob(msj *MultiStepJob, reason string) {
	now := time.Now().UTC().Format(time.RFC3339)
	msj.Status = StepFailed
	msj.CompletedAt = &now
	msj.UpdatedAt = now

	payloadMap := map[string]string{"reason": reason}
	payloadJSON, _ := json.Marshal(payloadMap)
	o.persist(msj, store.OutboxEntry{
		EventType: "job_fail",
		JobID:     msj.JobID,
		Payload:   string(payloadJSON),
	})
	log.Printf("[ORCH] Multi-step job %s failed: %s", msj.JobID[:min(8, len(msj.JobID))], reason)

	if o.onJobFail != nil {
		go o.onJobFail(msj, reason)
	}
}
