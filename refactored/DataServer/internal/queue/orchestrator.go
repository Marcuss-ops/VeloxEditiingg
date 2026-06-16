// Package queue implements multi-step job orchestration.
//
// The Orchestrator manages jobs that consist of multiple sequential or
// dependency-ordered steps. It persists state via the orchestrator_jobs
// SQLite table and dispatches ready steps as individual FileQueue jobs.
package queue

import (
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
	JobID        string                 `json:"job_id"`
	PipelineType string                 `json:"pipeline_type"` // e.g. "video_generation", "audio_video_sync"
	Status       StepStatus             `json:"status"`
	Steps        []*JobStep             `json:"steps"`
	TotalSteps   int                    `json:"total_steps"`
	CurrentStep  int                    `json:"current_step"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt    string                 `json:"created_at"`
	UpdatedAt    string                 `json:"updated_at"`
	StartedAt    *string                `json:"started_at,omitempty"`
	CompletedAt  *string                `json:"completed_at,omitempty"`
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

// Orchestrator manages multi-step job pipelines.
type Orchestrator struct {
	mu      sync.RWMutex
	jobs    map[string]*MultiStepJob
	fileQ   *FileQueue
	dbStore *store.SQLiteStore

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
	PollInterval      time.Duration // how often to check for ready steps
	JobTimeout        time.Duration // max time a step can be processing before considered stuck
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
func NewOrchestrator(_ *OrchestratorConfig, fileQ *FileQueue, dbStore *store.SQLiteStore) (*Orchestrator, error) {
	if fileQ == nil {
		return nil, fmt.Errorf("orchestrator: FileQueue is required")
	}
	if dbStore == nil {
		return nil, fmt.Errorf("orchestrator: SQLiteStore is required")
	}

	o := &Orchestrator{
		jobs:       make(map[string]*MultiStepJob),
		fileQ:      fileQ,
		dbStore:    dbStore,
		dispatchCh: make(chan *stepDispatchEvent, 100),
		completeCh: make(chan *stepCompleteEvent, 100),
		stopCh:     make(chan struct{}),
	}

	// Load existing orchestrator jobs from SQLite
	if err := o.load(); err != nil {
		log.Printf("[ORCH] Warning: could not load existing jobs: %v", err)
	}

	return o, nil
}
