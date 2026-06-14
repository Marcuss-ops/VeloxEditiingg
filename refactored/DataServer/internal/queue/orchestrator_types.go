package queue

import (
	"sync"
	"time"

	"velox-server/internal/store"
)

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

type JobStep struct {
	StepID       string                 `json:"step_id"`
	JobID        string                 `json:"job_id"`
	StepName     string                 `json:"step_name"`
	StepOrder    int                    `json:"step_order"`
	Status       StepStatus             `json:"status"`
	Dependencies []string               `json:"dependencies,omitempty"`
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

type OrchestratorConfig struct {
	DataDir         string        `json:"data_dir"`
	MaxConcurrent   int           `json:"max_concurrent"`
	StepTimeout     time.Duration `json:"step_timeout"`
	CheckInterval   time.Duration `json:"check_interval"`
	MaxRetryPerStep int           `json:"max_retry_per_step"`
	EnableAutoRetry bool          `json:"enable_auto_retry"`
}

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

type Orchestrator struct {
	mu       sync.RWMutex
	config   *OrchestratorConfig
	jobs     map[string]*MultiStepJob
	filePath string
	dbStore  *store.SQLiteStore

	jobChan     chan *MultiStepJob
	stepChan    chan *JobStep
	resultChan  chan *StepResult
	commandChan chan orchestratorCommand

	onStepReady   func(step *JobStep) error
	onJobComplete func(job *MultiStepJob)
	onJobFail     func(job *MultiStepJob, reason string)

	fileQueue *FileQueue
	dlq       *DeadLetterQueue
}

type StepResult struct {
	JobID    string                 `json:"job_id"`
	StepID   string                 `json:"step_id"`
	Success  bool                   `json:"success"`
	Error    string                 `json:"error,omitempty"`
	Result   map[string]interface{} `json:"result,omitempty"`
	Duration time.Duration          `json:"duration"`
}

type orchestratorCommand struct {
	command string
	jobID   string
	stepID  string
	data    interface{}
}
