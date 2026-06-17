// Package queue implements multi-step job orchestration with SQLite-authoritative
// workflow state and transactional outbox (PR5: no events lost, SQLite is source of truth).
//
// The in-memory job map is a read-through cache only — SQLite is always authoritative.
// State changes and outbox entries are written in the same SQLite transaction.
package queue

import (
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
	JobType      string                 `json:"job_type"`
	Payload      map[string]interface{} `json:"payload,omitempty"`
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
	PipelineType string                 `json:"pipeline_type"`
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

// Orchestrator manages multi-step job pipelines.
// SQLite is the authoritative source; the in-memory map is a read-through cache.
type Orchestrator struct {
	mu   sync.RWMutex
	jobs map[string]*MultiStepJob // read-through cache (SQLite is authoritative)

	fileQ   *FileQueue
	dbStore *store.SQLiteStore

	// notifyCh wakes the poll loop when a new outbox entry is written
	// (avoids waiting for the next ticker cycle).
	notifyCh chan struct{}

	stopCh chan struct{}
	stopped bool

	// Callbacks
	onStepReady   func(step *JobStep) error
	onJobComplete func(job *MultiStepJob)
	onJobFail     func(job *MultiStepJob, reason string)

	// Config
	pollInterval      time.Duration
	jobTimeout        time.Duration
	defaultMaxRetries int
	outboxBatchSize   int
}

// OrchestratorConfig holds configuration for the orchestrator.
type OrchestratorConfig struct {
	PollInterval      time.Duration
	JobTimeout        time.Duration
	DefaultMaxRetries int
	OutboxBatchSize   int
}

// DefaultOrchestratorConfig returns sensible defaults.
func DefaultOrchestratorConfig() *OrchestratorConfig {
	return &OrchestratorConfig{
		PollInterval:      15 * time.Second,
		JobTimeout:        30 * time.Minute,
		DefaultMaxRetries: 2,
		OutboxBatchSize:   20,
	}
}

// NewOrchestrator creates a new orchestrator with SQLite-authoritative state.
func NewOrchestrator(cfg *OrchestratorConfig, fileQ *FileQueue, dbStore *store.SQLiteStore) (*Orchestrator, error) {
	if fileQ == nil {
		return nil, fmt.Errorf("orchestrator: FileQueue is required")
	}
	if dbStore == nil {
		return nil, fmt.Errorf("orchestrator: SQLiteStore is required")
	}
	if cfg == nil {
		cfg = DefaultOrchestratorConfig()
	}

	o := &Orchestrator{
		jobs:              make(map[string]*MultiStepJob),
		fileQ:             fileQ,
		dbStore:           dbStore,
		notifyCh:          make(chan struct{}, 5),
		stopCh:            make(chan struct{}),
		pollInterval:      cfg.PollInterval,
		jobTimeout:        cfg.JobTimeout,
		defaultMaxRetries: cfg.DefaultMaxRetries,
		outboxBatchSize:   cfg.OutboxBatchSize,
	}

	// Load existing jobs from SQLite into cache (PR5: SQLite is authoritative)
	if err := o.load(); err != nil {
		log.Printf("[ORCH] Warning: could not load existing jobs: %v", err)
	}

	return o, nil
}

// marshalJob serializes a MultiStepJob to JSON for SQLite storage.
func marshalJob(msj *MultiStepJob) string {
	raw, err := json.Marshal(msj)
	if err != nil {
		log.Printf("[ORCH] Failed to marshal job %s: %v", msj.JobID[:min(8, len(msj.JobID))], err)
		return "{}"
	}
	return string(raw)
}
