package queue

import (
	"context"
	"log"
	"time"
)

func (o *Orchestrator) SubmitMultiStepJob(ctx context.Context, jobID string, steps []*JobStep, pipelineType string, metadata map[string]interface{}) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := time.Now().UTC()

	msj := &MultiStepJob{
		JobID:        jobID,
		TotalSteps:   len(steps),
		CurrentStep:  0,
		Status:       StepStatusPending,
		Steps:        steps,
		CreatedAt:    now,
		UpdatedAt:    now,
		PipelineType: pipelineType,
		Metadata:     metadata,
	}

	for i, step := range msj.Steps {
		step.StepOrder = i
		if step.Status == "" {
			step.Status = StepStatusPending
		}
		if step.MaxRetries == 0 {
			step.MaxRetries = o.config.MaxRetryPerStep
		}
		if step.Timeout == 0 {
			step.Timeout = o.config.StepTimeout
		}
	}

	o.jobs[jobID] = msj

	if err := o.save(); err != nil {
		return err
	}

	select {
	case o.jobChan <- msj:
	default:
		log.Printf("[WARN] Orchestrator job channel full, job %s will be processed next cycle", jobID[:8])
	}

	log.Printf("[JOB] Multi-step job %s submitted with %d steps", jobID[:8], len(steps))
	return nil
}

func (o *Orchestrator) GetJob(jobID string) *MultiStepJob {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.jobs[jobID]
}

func (o *Orchestrator) ListJobs() []*MultiStepJob {
	o.mu.RLock()
	defer o.mu.RUnlock()

	jobs := make([]*MultiStepJob, 0, len(o.jobs))
	for _, job := range o.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

func (o *Orchestrator) GetReadySteps() []*JobStep {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var steps []*JobStep
	for _, job := range o.jobs {
		for _, step := range job.Steps {
			if step.Status == StepStatusReady {
				steps = append(steps, step)
			}
		}
	}
	return steps
}

func (o *Orchestrator) MarkStepProcessing(jobID, stepID, workerID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	job, ok := o.jobs[jobID]
	if !ok {
		return nil
	}

	for _, step := range job.Steps {
		if step.StepID == stepID {
			now := time.Now().UTC()
			step.Status = StepStatusProcessing
			step.StartedAt = &now
			step.WorkerID = workerID
			job.UpdatedAt = now
			break
		}
	}

	return o.save()
}

func (o *Orchestrator) ReportStepResult(result *StepResult) {
	select {
	case o.resultChan <- result:
	default:
		log.Printf("[WARN] Result channel full, result for step %s may be delayed", result.StepID)
	}
}

func (o *Orchestrator) Stats() map[string]interface{} {
	o.mu.RLock()
	defer o.mu.RUnlock()

	stats := map[string]interface{}{
		"total_jobs":      len(o.jobs),
		"pending":         0,
		"processing":      0,
		"completed":       0,
		"failed":          0,
		"total_steps":     0,
		"completed_steps": 0,
	}

	for _, job := range o.jobs {
		stats["total_steps"] = stats["total_steps"].(int) + len(job.Steps)

		switch job.Status {
		case StepStatusPending:
			stats["pending"] = stats["pending"].(int) + 1
		case StepStatusProcessing:
			stats["processing"] = stats["processing"].(int) + 1
		case StepStatusCompleted:
			stats["completed"] = stats["completed"].(int) + 1
		case StepStatusFailed:
			stats["failed"] = stats["failed"].(int) + 1
		}

		for _, step := range job.Steps {
			if step.Status == StepStatusCompleted {
				stats["completed_steps"] = stats["completed_steps"].(int) + 1
			}
		}
	}

	return stats
}

func (o *Orchestrator) checkPendingJobs(ctx context.Context) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, job := range o.jobs {
		if job.Status == StepStatusPending {
			select {
			case o.jobChan <- job:
			default:
			}
		}
	}
}

func (o *Orchestrator) checkTimeouts(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := time.Now().UTC()

	for _, job := range o.jobs {
		if job.Status != StepStatusProcessing {
			continue
		}

		for _, step := range job.Steps {
			if step.Status != StepStatusProcessing {
				continue
			}

			if step.StartedAt != nil && step.Timeout > 0 {
				if now.Sub(*step.StartedAt) > step.Timeout {
					log.Printf("[STEP] Step %s timed out after %v", step.StepName, step.Timeout)

					result := &StepResult{
						JobID:   job.JobID,
						StepID:  step.StepID,
						Success: false,
						Error:   "step timeout exceeded",
					}

					select {
					case o.resultChan <- result:
					default:
						o.handleStepResult(ctx, result)
					}
				}
			}
		}
	}
}

func (o *Orchestrator) retryStep(jobID, stepID string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	job, ok := o.jobs[jobID]
	if !ok {
		return
	}

	for _, step := range job.Steps {
		if step.StepID == stepID && step.Status == StepStatusFailed {
			step.Status = StepStatusReady
			step.Error = ""
			step.RetryCount = 0
			job.Status = StepStatusProcessing
			job.UpdatedAt = time.Now().UTC()

			select {
			case o.stepChan <- step:
			default:
			}

			log.Printf("[INFO] Step %s manually retried", step.StepName)
			break
		}
	}

	o.save()
}

func (o *Orchestrator) skipStep(jobID, stepID string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	job, ok := o.jobs[jobID]
	if !ok {
		return
	}

	for _, step := range job.Steps {
		if step.StepID == stepID {
			step.Status = StepStatusSkipped
			job.UpdatedAt = time.Now().UTC()
			log.Printf("[SKIP] Step %s skipped", step.StepName)
			break
		}
	}

	ctx := context.Background()
	o.advanceJob(ctx, job)
	o.save()
}

func (o *Orchestrator) cancelJob(jobID string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	job, ok := o.jobs[jobID]
	if !ok {
		return
	}

	job.Status = StepStatusFailed
	now := time.Now().UTC()
	job.CompletedAt = &now
	job.UpdatedAt = now

	for _, step := range job.Steps {
		if step.Status == StepStatusPending || step.Status == StepStatusProcessing || step.Status == StepStatusWaitingDeps {
			step.Status = StepStatusSkipped
		}
	}

	log.Printf("[JOB] Multi-step job %s cancelled", jobID[:8])
	o.save()
}
