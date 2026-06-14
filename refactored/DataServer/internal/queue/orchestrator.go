package queue

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	"velox-server/internal/store"
)

func NewOrchestrator(cfg *OrchestratorConfig, fq *FileQueue, dlq *DeadLetterQueue, dbStore *store.SQLiteStore) (*Orchestrator, error) {
	if cfg == nil {
		cfg = DefaultOrchestratorConfig("")
	}

	o := &Orchestrator{
		config:      cfg,
		jobs:        make(map[string]*MultiStepJob),
		filePath:    filepath.Join(cfg.DataDir, "jobs", "multi_step_jobs.json"),
		dbStore:     dbStore,
		jobChan:     make(chan *MultiStepJob, 100),
		stepChan:    make(chan *JobStep, 100),
		resultChan:  make(chan *StepResult, 100),
		commandChan: make(chan orchestratorCommand, 50),
		fileQueue:   fq,
		dlq:         dlq,
	}

	dir := filepath.Dir(o.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	if err := o.load(); err != nil {
		log.Printf("[WARN] Orchestrator load error (starting fresh): %v", err)
	}

	return o, nil
}

func (o *Orchestrator) load() error {
	// SQLite is the source of truth
	if o.dbStore != nil {
		jobs, err := o.dbStore.ListOrchestratorJobs()
		if err == nil && len(jobs) > 0 {
			for _, raw := range jobs {
				var job MultiStepJob
				b, _ := json.Marshal(raw)
				if err := json.Unmarshal(b, &job); err == nil {
					o.jobs[job.JobID] = &job
				}
			}
			log.Printf("[ORCH] Loaded %d jobs from SQLite", len(o.jobs))
			return nil
		}
	}

	// Fallback: legacy JSON file
	data, err := os.ReadFile(o.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if len(data) == 0 {
		return nil
	}

	if err := json.Unmarshal(data, &o.jobs); err != nil {
		return err
	}

	// Import into SQLite for next time
	if o.dbStore != nil && len(o.jobs) > 0 {
		for _, job := range o.jobs {
			o.persistJob(job)
		}
		log.Printf("[MIGRATE] Imported %d orchestrator jobs from JSON to SQLite", len(o.jobs))
	}

	return nil
}

func (o *Orchestrator) save() error {
	// SQLite is the source of truth
	if o.dbStore != nil {
		for _, job := range o.jobs {
			o.persistJob(job)
		}
	}

	// Backup: JSON file
	data, err := json.MarshalIndent(o.jobs, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := o.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, o.filePath)
}

// persistJob saves a single job to SQLite.
func (o *Orchestrator) persistJob(job *MultiStepJob) {
	raw, _ := json.Marshal(job)
	if err := o.dbStore.UpsertOrchestratorJob(
		job.JobID, string(job.Status), job.PipelineType,
		job.TotalSteps, job.CurrentStep, string(raw),
	); err != nil {
		log.Printf("[WARN] Failed to persist orchestrator job %s: %v", job.JobID[:8], err)
	}
	if err := o.dbStore.SetOrchestratorJobTimestamps(job.JobID, job.StartedAt, job.CompletedAt); err != nil {
		log.Printf("[WARN] Failed to persist orchestrator job timestamps %s: %v", job.JobID[:8], err)
	}
}

func (o *Orchestrator) SetStepReadyCallback(cb func(step *JobStep) error) {
	o.onStepReady = cb
}

func (o *Orchestrator) SetJobCompleteCallback(cb func(job *MultiStepJob)) {
	o.onJobComplete = cb
}

func (o *Orchestrator) SetJobFailCallback(cb func(job *MultiStepJob, reason string)) {
	o.onJobFail = cb
}

func (o *Orchestrator) Start(ctx context.Context) {
	log.Printf("[START] Orchestrator started")

	ticker := time.NewTicker(o.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[STOP] Orchestrator stopping...")
			o.save()
			return

		case job := <-o.jobChan:
			o.processJob(ctx, job)

		case result := <-o.resultChan:
			o.handleStepResult(ctx, result)

		case cmd := <-o.commandChan:
			o.handleCommand(ctx, cmd)

		case <-ticker.C:
			o.checkPendingJobs(ctx)
			o.checkTimeouts(ctx)
		}
	}
}

func (o *Orchestrator) processJob(ctx context.Context, job *MultiStepJob) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if job.Status == StepStatusProcessing {
		return
	}

	now := time.Now().UTC()
	job.Status = StepStatusProcessing
	job.StartedAt = &now
	job.UpdatedAt = now

	o.advanceJob(ctx, job)
}

func (o *Orchestrator) advanceJob(ctx context.Context, job *MultiStepJob) {
	for _, step := range job.Steps {
		if step.Status == StepStatusCompleted || step.Status == StepStatusSkipped {
			continue
		}

		depsMet := true
		for _, depID := range step.Dependencies {
			depStep := o.findStep(job, depID)
			if depStep == nil || depStep.Status != StepStatusCompleted {
				depsMet = false
				step.Status = StepStatusWaitingDeps
				break
			}
		}

		if !depsMet {
			continue
		}

		if step.Status == StepStatusPending || step.Status == StepStatusWaitingDeps {
			step.Status = StepStatusReady
			job.CurrentStep = step.StepOrder
			job.UpdatedAt = time.Now().UTC()

			if o.onStepReady != nil {
				go func(s *JobStep) {
					if err := o.onStepReady(s); err != nil {
						log.Printf("[WARN] Step ready callback error for %s: %v", s.StepID, err)
					}
				}(step)
			}

			select {
			case o.stepChan <- step:
			default:
				log.Printf("[WARN] Step channel full, step %s will be processed next cycle", step.StepID)
			}
		}

		return
	}

	allComplete := true
	for _, step := range job.Steps {
		if step.Status != StepStatusCompleted && step.Status != StepStatusSkipped {
			allComplete = false
			break
		}
	}

	if allComplete {
		o.completeJob(job)
	}
}

func (o *Orchestrator) findStep(job *MultiStepJob, stepID string) *JobStep {
	for _, step := range job.Steps {
		if step.StepID == stepID {
			return step
		}
	}
	return nil
}

func (o *Orchestrator) handleStepResult(ctx context.Context, result *StepResult) {
	o.mu.Lock()
	defer o.mu.Unlock()

	job, ok := o.jobs[result.JobID]
	if !ok {
		log.Printf("[WARN] Received result for unknown job %s", result.JobID[:8])
		return
	}

	var step *JobStep
	for _, s := range job.Steps {
		if s.StepID == result.StepID {
			step = s
			break
		}
	}

	if step == nil {
		log.Printf("[WARN] Received result for unknown step %s in job %s", result.StepID, result.JobID[:8])
		return
	}

	now := time.Now().UTC()

	if result.Success {
		step.Status = StepStatusCompleted
		step.CompletedAt = &now
		step.Result = result.Result
		job.UpdatedAt = now

		log.Printf("[OK] Step %s completed for job %s", step.StepName, job.JobID[:8])

		o.advanceJob(ctx, job)
	} else {
		step.Error = result.Error
		step.RetryCount++

		if o.config.EnableAutoRetry && step.RetryCount < step.MaxRetries {
			step.Status = StepStatusReady
			log.Printf("[INFO] Step %s failed, retrying (%d/%d): %s",
				step.StepName, step.RetryCount, step.MaxRetries, result.Error)

			select {
			case o.stepChan <- step:
			default:
			}
		} else {
			step.Status = StepStatusFailed
			o.failJob(job, result.Error)
		}
	}

	o.save()
}

func (o *Orchestrator) completeJob(job *MultiStepJob) {
	now := time.Now().UTC()
	job.Status = StepStatusCompleted
	job.CompletedAt = &now
	job.UpdatedAt = now

	log.Printf("[OK] Multi-step job %s completed (%d steps)", job.JobID[:8], job.TotalSteps)

	if o.onJobComplete != nil {
		go o.onJobComplete(job)
	}

	if o.fileQueue != nil {
		ctx := context.Background()
		if err := o.fileQueue.CompleteJob(ctx, job.JobID); err != nil {
			log.Printf("[WARN] Failed to mark main job %s as completed: %v", job.JobID[:8], err)
		}
	}

	o.save()
}

func (o *Orchestrator) failJob(job *MultiStepJob, reason string) {
	now := time.Now().UTC()
	job.Status = StepStatusFailed
	job.CompletedAt = &now
	job.UpdatedAt = now

	log.Printf("[ERROR] Multi-step job %s failed: %s", job.JobID[:8], reason)

	if o.dlq != nil && o.fileQueue != nil {
		ctx := context.Background()
		mainJob, err := o.fileQueue.GetJob(ctx, job.JobID)
		if err != nil {
			log.Printf("[WARN] Failed to retrieve main job %s for DLQ: %v", job.JobID[:8], err)
		} else if mainJob != nil {
			if err := o.dlq.AddJob(ctx, mainJob, "multi_step_failed", reason); err != nil {
				log.Printf("[WARN] Failed to add job %s to DLQ: %v", job.JobID[:8], err)
			} else {
				log.Printf("[ZOMBIE] Job %s persisted to DLQ", job.JobID[:8])
			}
		}
	} else {
		log.Printf("[WARN] DLQ not available, failed job %s will not be retried", job.JobID[:8])
	}

	o.save()

	if o.onJobFail != nil {
		go o.onJobFail(job, reason)
	}
}

func (o *Orchestrator) handleCommand(ctx context.Context, cmd orchestratorCommand) {
	switch cmd.command {
	case "retry_step":
		o.retryStep(cmd.jobID, cmd.stepID)
	case "skip_step":
		o.skipStep(cmd.jobID, cmd.stepID)
	case "cancel_job":
		o.cancelJob(cmd.jobID)
	}
}
