package queue

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// StepStatus represents the status of a job step
type StepStatus string

const (
	StepStatusPending     StepStatus = "PENDING"
	StepStatusWaitingDeps StepStatus = "WAITING_DEPS"
	StepStatusReady       StepStatus = "READY"
	StepStatusProcessing  StepStatus = "PROCESSING"
	StepStatusCompleted   StepStatus = "COMPLETED"
	StepStatusFailed      StepStatus = "FAILED"
	StepStatusSkipped     StepStatus = "SKIPPED"
)

// JobStep represents a single step in a multi-step job
type JobStep struct {
	StepID       string                 `json:"step_id"`
	JobID        string                 `json:"job_id"`
	StepName     string                 `json:"step_name"`
	StepOrder    int                    `json:"step_order"`
	Status       StepStatus             `json:"status"`
	Dependencies []string               `json:"dependencies,omitempty"` // Other step_ids this depends on
	Payload      map[string]interface{} `json:"payload,omitempty"`
	Result       map[string]interface{} `json:"result,omitempty"`
	Error        string                 `json:"error,omitempty"`
	StartedAt    *time.Time             `json:"started_at,omitempty"`
	CompletedAt  *time.Time             `json:"completed_at,omitempty"`
	Timeout      time.Duration          `json:"timeout,omitempty"`
	WorkerID     string                 `json:"worker_id,omitempty"`
	RetryCount   int                    `json:"retry_count,omitempty"`
	MaxRetries   int                    `json:"max_retries,omitempty"`
}

// MultiStepJob represents a job composed of multiple steps
type MultiStepJob struct {
	JobID        string                 `json:"job_id"`
	ParentJobID  string                 `json:"parent_job_id,omitempty"`
	TotalSteps   int                    `json:"total_steps"`
	CurrentStep  int                    `json:"current_step"`
	Status       StepStatus             `json:"status"`
	Steps        []*JobStep             `json:"steps"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	StartedAt    *time.Time             `json:"started_at,omitempty"`
	CompletedAt  *time.Time             `json:"completed_at,omitempty"`
	PipelineType string                 `json:"pipeline_type,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// OrchestratorConfig holds configuration for the orchestrator
type OrchestratorConfig struct {
	DataDir         string        `json:"data_dir"`
	MaxConcurrent   int           `json:"max_concurrent"`
	StepTimeout     time.Duration `json:"step_timeout"`
	CheckInterval   time.Duration `json:"check_interval"`
	MaxRetryPerStep int           `json:"max_retry_per_step"`
	EnableAutoRetry bool          `json:"enable_auto_retry"`
}

// DefaultOrchestratorConfig returns sensible defaults
func DefaultOrchestratorConfig(dataDir string) *OrchestratorConfig {
	return &OrchestratorConfig{
		DataDir:         dataDir,
		MaxConcurrent:   10,
		StepTimeout:     30 * time.Minute,
		CheckInterval:   5 * time.Second,
		MaxRetryPerStep: 3,
		EnableAutoRetry: true,
	}
}

// Orchestrator manages multi-step job execution
type Orchestrator struct {
	mu       sync.RWMutex
	config   *OrchestratorConfig
	jobs     map[string]*MultiStepJob
	filePath string

	// Channels for communication
	jobChan     chan *MultiStepJob
	stepChan    chan *JobStep
	resultChan  chan *StepResult
	commandChan chan orchestratorCommand

	// Callbacks
	onStepReady   func(step *JobStep) error
	onJobComplete func(job *MultiStepJob)
	onJobFail     func(job *MultiStepJob, reason string)

	// File queue reference
	fileQueue *FileQueue
	dlq       *DeadLetterQueue
}

// StepResult represents the result of a step execution
type StepResult struct {
	JobID    string                 `json:"job_id"`
	StepID   string                 `json:"step_id"`
	Success  bool                   `json:"success"`
	Error    string                 `json:"error,omitempty"`
	Result   map[string]interface{} `json:"result,omitempty"`
	Duration time.Duration          `json:"duration"`
}

// orchestratorCommand represents internal commands
type orchestratorCommand struct {
	command string
	jobID   string
	stepID  string
	data    interface{}
}

// NewOrchestrator creates a new orchestrator
func NewOrchestrator(cfg *OrchestratorConfig, fq *FileQueue, dlq *DeadLetterQueue) (*Orchestrator, error) {
	if cfg == nil {
		cfg = DefaultOrchestratorConfig("")
	}

	o := &Orchestrator{
		config:      cfg,
		jobs:        make(map[string]*MultiStepJob),
		filePath:    filepath.Join(cfg.DataDir, "jobs", "multi_step_jobs.json"),
		jobChan:     make(chan *MultiStepJob, 100),
		stepChan:    make(chan *JobStep, 100),
		resultChan:  make(chan *StepResult, 100),
		commandChan: make(chan orchestratorCommand, 50),
		fileQueue:   fq,
		dlq:         dlq,
	}

	// Ensure directory exists
	dir := filepath.Dir(o.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Load existing jobs
	if err := o.load(); err != nil {
		log.Printf("⚠️ Orchestrator load error (starting fresh): %v", err)
	}

	return o, nil
}

// load reads multi-step jobs from file
func (o *Orchestrator) load() error {
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

	return json.Unmarshal(data, &o.jobs)
}

// save writes multi-step jobs to file
func (o *Orchestrator) save() error {
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

// SetStepReadyCallback sets the callback for when a step is ready to execute
func (o *Orchestrator) SetStepReadyCallback(cb func(step *JobStep) error) {
	o.onStepReady = cb
}

// SetJobCompleteCallback sets the callback for when a job completes
func (o *Orchestrator) SetJobCompleteCallback(cb func(job *MultiStepJob)) {
	o.onJobComplete = cb
}

// SetJobFailCallback sets the callback for when a job fails
func (o *Orchestrator) SetJobFailCallback(cb func(job *MultiStepJob, reason string)) {
	o.onJobFail = cb
}

// SubmitMultiStepJob creates a new multi-step job
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

	// Initialize step statuses and set order
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

	// Queue for processing
	select {
	case o.jobChan <- msj:
	default:
		log.Printf("⚠️ Orchestrator job channel full, job %s will be processed next cycle", jobID[:8])
	}

	log.Printf("📋 Multi-step job %s submitted with %d steps", jobID[:8], len(steps))
	return nil
}

// Start begins the orchestrator main loop
func (o *Orchestrator) Start(ctx context.Context) {
	log.Printf("🚀 Orchestrator started")

	// Main loop using select for multiplexing
	ticker := time.NewTicker(o.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 Orchestrator stopping...")
			o.save() // Final save
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

// processJob processes a newly submitted job
func (o *Orchestrator) processJob(ctx context.Context, job *MultiStepJob) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check if job is already being processed
	if job.Status == StepStatusProcessing {
		return
	}

	// Update status
	now := time.Now().UTC()
	job.Status = StepStatusProcessing
	job.StartedAt = &now
	job.UpdatedAt = now

	// Find first step that can run
	o.advanceJob(ctx, job)
}

// advanceJob moves the job to the next available step
func (o *Orchestrator) advanceJob(ctx context.Context, job *MultiStepJob) {
	for _, step := range job.Steps {
		if step.Status == StepStatusCompleted || step.Status == StepStatusSkipped {
			continue
		}

		// Check dependencies
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

		// This step can run
		if step.Status == StepStatusPending || step.Status == StepStatusWaitingDeps {
			step.Status = StepStatusReady
			job.CurrentStep = step.StepOrder
			job.UpdatedAt = time.Now().UTC()

			// Trigger callback
			if o.onStepReady != nil {
				go func(s *JobStep) {
					if err := o.onStepReady(s); err != nil {
						log.Printf("⚠️ Step ready callback error for %s: %v", s.StepID, err)
					}
				}(step)
			}

			// Queue step for processing
			select {
			case o.stepChan <- step:
			default:
				log.Printf("⚠️ Step channel full, step %s will be processed next cycle", step.StepID)
			}
		}

		return // Only advance to one step at a time
	}

	// Check if all steps are complete
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

// findStep finds a step by ID within a job
func (o *Orchestrator) findStep(job *MultiStepJob, stepID string) *JobStep {
	for _, step := range job.Steps {
		if step.StepID == stepID {
			return step
		}
	}
	return nil
}

// handleStepResult processes a step execution result
func (o *Orchestrator) handleStepResult(ctx context.Context, result *StepResult) {
	o.mu.Lock()
	defer o.mu.Unlock()

	job, ok := o.jobs[result.JobID]
	if !ok {
		log.Printf("⚠️ Received result for unknown job %s", result.JobID[:8])
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
		log.Printf("⚠️ Received result for unknown step %s in job %s", result.StepID, result.JobID[:8])
		return
	}

	now := time.Now().UTC()

	if result.Success {
		step.Status = StepStatusCompleted
		step.CompletedAt = &now
		step.Result = result.Result
		job.UpdatedAt = now

		log.Printf("✅ Step %s completed for job %s", step.StepName, job.JobID[:8])

		// Advance to next step
		o.advanceJob(ctx, job)
	} else {
		step.Error = result.Error
		step.RetryCount++

		if o.config.EnableAutoRetry && step.RetryCount < step.MaxRetries {
			step.Status = StepStatusReady
			log.Printf("🔄 Step %s failed, retrying (%d/%d): %s",
				step.StepName, step.RetryCount, step.MaxRetries, result.Error)

			// Requeue step
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

// completeJob marks a job as completed
func (o *Orchestrator) completeJob(job *MultiStepJob) {
	now := time.Now().UTC()
	job.Status = StepStatusCompleted
	job.CompletedAt = &now
	job.UpdatedAt = now

	log.Printf("✅ Multi-step job %s completed (%d steps)", job.JobID[:8], job.TotalSteps)

	if o.onJobComplete != nil {
		go o.onJobComplete(job)
	}

	// Update main queue if file queue is available
	if o.fileQueue != nil {
		ctx := context.Background()
		if err := o.fileQueue.CompleteJob(ctx, job.JobID); err != nil {
			log.Printf("⚠️ Failed to mark main job %s as completed: %v", job.JobID[:8], err)
		}
	}

	o.save()
}

// failJob marks a job as failed
func (o *Orchestrator) failJob(job *MultiStepJob, reason string) {
	now := time.Now().UTC()
	job.Status = StepStatusFailed
	job.CompletedAt = &now
	job.UpdatedAt = now

	log.Printf("❌ Multi-step job %s failed: %s", job.JobID[:8], reason)

	// Persist to DLQ BEFORE firing callback to prevent race conditions
	if o.dlq != nil && o.fileQueue != nil {
		ctx := context.Background()
		mainJob, err := o.fileQueue.GetJob(ctx, job.JobID)
		if err != nil {
			log.Printf("⚠️ Failed to retrieve main job %s for DLQ: %v", job.JobID[:8], err)
		} else if mainJob != nil {
			if err := o.dlq.AddJob(ctx, mainJob, "multi_step_failed", reason); err != nil {
				log.Printf("⚠️ Failed to add job %s to DLQ: %v", job.JobID[:8], err)
			} else {
				log.Printf("📋 Job %s persisted to DLQ", job.JobID[:8])
			}
		}
	} else {
		log.Printf("⚠️ DLQ not available, failed job %s will not be retried", job.JobID[:8])
	}

	// Save state before firing async callback
	o.save()

	// Fire callback AFTER persistence
	if o.onJobFail != nil {
		go o.onJobFail(job, reason)
	}
}

// handleCommand processes internal orchestrator commands
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

// checkPendingJobs checks for jobs that need processing
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

// checkTimeouts checks for steps that have exceeded their timeout
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
					log.Printf("⏰ Step %s timed out after %v", step.StepName, step.Timeout)

					result := &StepResult{
						JobID:   job.JobID,
						StepID:  step.StepID,
						Success: false,
						Error:   "step timeout exceeded",
					}

					// Queue result
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

// retryStep retries a failed step
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

			log.Printf("🔄 Step %s manually retried", step.StepName)
			break
		}
	}

	o.save()
}

// skipStep skips a step
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
			log.Printf("⏭️ Step %s skipped", step.StepName)
			break
		}
	}

	// Check if we can advance
	ctx := context.Background()
	o.advanceJob(ctx, job)
	o.save()
}

// cancelJob cancels a multi-step job
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

	log.Printf("🚫 Multi-step job %s cancelled", jobID[:8])
	o.save()
}

// GetJob retrieves a multi-step job
func (o *Orchestrator) GetJob(jobID string) *MultiStepJob {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.jobs[jobID]
}

// ListJobs lists all multi-step jobs
func (o *Orchestrator) ListJobs() []*MultiStepJob {
	o.mu.RLock()
	defer o.mu.RUnlock()

	jobs := make([]*MultiStepJob, 0, len(o.jobs))
	for _, job := range o.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// GetReadySteps returns steps that are ready to execute
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

// MarkStepProcessing marks a step as processing (called by worker)
func (o *Orchestrator) MarkStepProcessing(jobID, stepID, workerID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	job, ok := o.jobs[jobID]
	if !ok {
		return nil // Not a multi-step job
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

// ReportStepResult reports a step execution result
func (o *Orchestrator) ReportStepResult(result *StepResult) {
	select {
	case o.resultChan <- result:
	default:
		log.Printf("⚠️ Result channel full, result for step %s may be delayed", result.StepID)
	}
}

// Stats returns orchestrator statistics
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

// Common pipeline templates

// PipelineAudioVideo creates a standard audio-first, then-video pipeline
func PipelineAudioVideo(jobID string, audioPayload, videoPayload map[string]interface{}) []*JobStep {
	return []*JobStep{
		{
			StepID:    jobID + "_audio",
			JobID:     jobID,
			StepName:  "generate_audio",
			StepOrder: 0,
			Status:    StepStatusPending,
			Payload:   audioPayload,
		},
		{
			StepID:       jobID + "_video",
			JobID:        jobID,
			StepName:     "generate_video",
			StepOrder:    1,
			Status:       StepStatusPending,
			Dependencies: []string{jobID + "_audio"},
			Payload:      videoPayload,
		},
	}
}

// PipelineScriptAudioVideo creates script -> audio -> video pipeline
func PipelineScriptAudioVideo(jobID string, scriptPayload, audioPayload, videoPayload map[string]interface{}) []*JobStep {
	return []*JobStep{
		{
			StepID:    jobID + "_script",
			JobID:     jobID,
			StepName:  "generate_script",
			StepOrder: 0,
			Status:    StepStatusPending,
			Payload:   scriptPayload,
		},
		{
			StepID:       jobID + "_audio",
			JobID:        jobID,
			StepName:     "generate_audio",
			StepOrder:    1,
			Status:       StepStatusPending,
			Dependencies: []string{jobID + "_script"},
			Payload:      audioPayload,
		},
		{
			StepID:       jobID + "_video",
			JobID:        jobID,
			StepName:     "generate_video",
			StepOrder:    2,
			Status:       StepStatusPending,
			Dependencies: []string{jobID + "_audio"},
			Payload:      videoPayload,
		},
	}
}
