// Package queue implements multi-step job orchestration.
//
// The Orchestrator manages jobs that consist of multiple sequential or
// dependency-ordered steps. It persists state via the orchestrator_jobs
// SQLite table and dispatches ready steps as individual FileQueue jobs.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"velox-server/internal/store"
)

// StepStatus represents the current state of a pipeline step.
type StepStatus string

const (
	StepPending    StepStatus = "PENDING"
	StepReady      StepStatus = "READY"
	StepProcessing StepStatus = "PROCESSING"
	StepCompleted  StepStatus = "COMPLETED"
	StepFailed     StepStatus = "FAILED"
	StepSkipped    StepStatus = "SKIPPED"
)

// JobStep is a single step in a multi-step pipeline.
type JobStep struct {
	StepID       string                 `json:"step_id"`
	StepName     string                 `json:"step_name"`
	StepOrder    int                    `json:"step_order"`
	Status       StepStatus             `json:"status"`
	JobType      string                 `json:"job_type"`          // e.g. "render", "process_video", "process_audio"
	Payload      map[string]interface{} `json:"payload,omitempty"` // parameters for this step's job
	Dependencies []string               `json:"dependencies,omitempty"`
	Result       map[string]interface{} `json:"result,omitempty"`
	Error        string                 `json:"error,omitempty"`
	RetryCount   int                    `json:"retry_count"`
	MaxRetries   int                    `json:"max_retries"`
	CreatedAt    string                 `json:"created_at"`
	CompletedAt  *string                `json:"completed_at,omitempty"`
}

// MultiStepJob represents a pipeline composed of multiple steps.
type MultiStepJob struct {
	JobID        string     `json:"job_id"`
	PipelineType string     `json:"pipeline_type"` // e.g. "video_generation", "audio_video_sync"
	Status       StepStatus `json:"status"`
	Steps        []*JobStep `json:"steps"`
	TotalSteps   int        `json:"total_steps"`
	CurrentStep  int        `json:"current_step"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt    string     `json:"created_at"`
	UpdatedAt    string     `json:"updated_at"`
	StartedAt    *string    `json:"started_at,omitempty"`
	CompletedAt  *string    `json:"completed_at,omitempty"`
}

// stepDispatchEvent is sent when a step should be dispatched as a FileQueue job.
type stepDispatchEvent struct {
	JobID  string
	StepID string
}

// stepCompleteEvent is sent when a FileQueue job completes (polled by the loop).
type stepCompleteEvent struct {
	JobID  string
	StepID string
	Result map[string]interface{}
	Error  string
}

// ────────────────────────────────────────────────────────────────────────────
// Orchestrator
// ────────────────────────────────────────────────────────────────────────────

// Orchestrator manages multi-step job pipelines.
type Orchestrator struct {
	mu       sync.RWMutex
	jobs     map[string]*MultiStepJob
	fileQ    *FileQueue
	dbStore  *store.SQLiteStore

	// Channels for internal event processing
	dispatchCh chan *stepDispatchEvent
	completeCh chan *stepCompleteEvent
	stopCh     chan struct{}
	stopped    bool

	// Callbacks (optional, for integration with external systems)
	onStepReady   func(step *JobStep) error
	onJobComplete func(job *MultiStepJob)
	onJobFail     func(job *MultiStepJob, reason string)
}

// OrchestratorConfig holds configuration for the orchestrator.
type OrchestratorConfig struct {
	PollInterval     time.Duration // how often to check for ready steps
	JobTimeout       time.Duration // max time a step can be processing before considered stuck
	DefaultMaxRetries int
}

// DefaultOrchestratorConfig returns sensible defaults.
func DefaultOrchestratorConfig() *OrchestratorConfig {
	return &OrchestratorConfig{
		PollInterval:      15 * time.Second,
		JobTimeout:        30 * time.Minute,
		DefaultMaxRetries: 2,
	}
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(cfg *OrchestratorConfig, fileQ *FileQueue, dbStore *store.SQLiteStore) (*Orchestrator, error) {
	if cfg == nil {
		cfg = DefaultOrchestratorConfig()
	}
	if fileQ == nil {
		return nil, fmt.Errorf("orchestrator: FileQueue is required")
	}
	if dbStore == nil {
		return nil, fmt.Errorf("orchestrator: SQLiteStore is required")
	}

	o := &Orchestrator{
		jobs:        make(map[string]*MultiStepJob),
		fileQ:       fileQ,
		dbStore:     dbStore,
		dispatchCh:  make(chan *stepDispatchEvent, 100),
		completeCh:  make(chan *stepCompleteEvent, 100),
		stopCh:      make(chan struct{}),
	}

	// Load existing orchestrator jobs from SQLite
	if err := o.load(); err != nil {
		log.Printf("[ORCH] Warning: could not load existing jobs: %v", err)
	}

	return o, nil
}

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

// ────────────────────────────────────────────────────────────────────────────
// Persistence (SQLite)
// ────────────────────────────────────────────────────────────────────────────

func (o *Orchestrator) load() error {
	rows, err := o.dbStore.ListOrchestratorJobs()
	if err != nil {
		return err
	}
	for _, raw := range rows {
		var job MultiStepJob
		data, _ := json.Marshal(raw)
		if err := json.Unmarshal(data, &job); err != nil {
			log.Printf("[ORCH] Warning: skipping malformed job: %v", err)
			continue
		}
		// Only load active jobs (not completed/failed)
		if job.Status == StepCompleted || job.Status == StepFailed {
			continue
		}
		o.jobs[job.JobID] = &job
	}
	if len(o.jobs) > 0 {
		log.Printf("[ORCH] Loaded %d active orchestrator jobs from SQLite", len(o.jobs))
	}
	return nil
}

func (o *Orchestrator) saveAll() {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, job := range o.jobs {
		o.persist(job)
	}
}

func (o *Orchestrator) persist(msj *MultiStepJob) {
	raw, err := json.Marshal(msj)
	if err != nil {
		log.Printf("[ORCH] Failed to marshal job %s: %v", msj.JobID[:8], err)
		return
	}
	started := ""
	if msj.StartedAt != nil {
		started = *msj.StartedAt
	}
	completed := ""
	if msj.CompletedAt != nil {
		completed = *msj.CompletedAt
	}
	if err := o.dbStore.UpsertOrchestratorJob(
		msj.JobID, string(msj.Status), msj.PipelineType,
		msj.TotalSteps, msj.CurrentStep, string(raw),
	); err != nil {
		log.Printf("[ORCH] Failed to persist job %s: %v", msj.JobID[:8], err)
	}
	if err := o.dbStore.SetOrchestratorJobTimestamps(msj.JobID, started, completed); err != nil {
		log.Printf("[ORCH] Failed to persist timestamps for %s: %v", msj.JobID[:8], err)
	}
}

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
		"total_jobs":        total,
		"pending_jobs":      pending,
		"running_jobs":      running,
		"completed_jobs":    completed,
		"failed_jobs":       failed,
		"dispatch_channel":  len(o.dispatchCh),
		"complete_channel":  len(o.completeCh),
	}
}
