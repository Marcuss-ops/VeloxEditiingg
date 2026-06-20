// Package worker — type definitions extracted from worker_init.go.
package worker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/internal/worker/stageexec"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// Status represents the current status of a worker.
type Status string

const (
	StatusIdle    Status = "idle"
	StatusBusy    Status = "busy"
	StatusError   Status = "error"
	StatusStopped Status = "stopped"
)

// ConnectionState represents the worker's connection state to the master.
type ConnectionState string

const (
	ConnDisconnected   ConnectionState = "disconnected"
	ConnConnecting     ConnectionState = "connecting"
	ConnAuthenticating ConnectionState = "authenticating"
	ConnReady          ConnectionState = "ready"
	ConnDraining       ConnectionState = "draining"
)

// Registration backoff constants
const (
	registrationInitialBackoff = 5 * time.Second
	registrationMaxBackoff     = 5 * time.Minute
	registrationBackoffMult    = 2.0

	// connectionRetryBackoff is a short fixed delay used for connection-level
	// errors (reset, refused, transport unavailable). These typically happen
	// when the server is restarting and will recover in seconds. Exponential
	// backoff is reserved for application-level errors (credential mismatch,
	// protocol version, TLS).
	connectionRetryBackoff = 2 * time.Second
)

// ActiveJob represents a job currently being executed by the worker.
type ActiveJob struct {
	Job       *api.Job
	LeaseID   string
	StartedAt time.Time
	Cancel    context.CancelFunc
	Progress  JobProgress
}

// JobProgress tracks per-job execution progress.
type JobProgress struct {
	Percent     int32
	Scene       int32
	TotalScenes int32
	Stage       string
}

// backoffConfig configures exponential backoff for retry operations.
type backoffConfig struct {
	initialInterval time.Duration
	maxInterval     time.Duration
	multiplier      float64
}

// ExitFunc is the function type for worker exit (used for testing).
type ExitFunc func(int)

// Worker represents a Velox worker agent.
type Worker struct {
	config           *config.WorkerConfig
	apiClient        *api.Client                              // Retained for data-plane operations (upload, asset download)
	transport        controltransport.ControlTransport        // Current session's transport (recreated per connect)
	transportFactory func() controltransport.ControlTransport // Factory for new transport instances
	logger           *logger.Logger

	// Status management — error state only; busy/idle derived from activeJobs
	status Status
	mu     sync.RWMutex

	// Multi-job support: maps jobID -> ActiveJob for parallel execution
	activeJobs   map[string]*ActiveJob
	activeJobsMu sync.RWMutex

	// Connection state machine
	connState        ConnectionState
	connStateMu      sync.RWMutex
	connFailureCount int

	// Lifecycle management
	stopChan chan struct{}
	stopOnce sync.Once
	stopped  atomic.Bool
	wg       sync.WaitGroup

	// Backoff for heartbeat failures
	heartbeatBackoff *backoffConfig

	version string

	// Command management
	drainMode    atomic.Bool
	commandMu    sync.Mutex
	seenCommands map[string]time.Time

	// Job cancellation: maps jobID -> cancel function for active jobs
	jobCancelFuncs map[string]context.CancelFunc
	jobCancelMu    sync.Mutex

	// Pending lease jobs: accepted but waiting for JobLeaseGranted before execution
	pendingLeaseJobs map[string]*api.Job
	pendingLeaseMu   sync.Mutex

	// Job completion stats for heartbeat reporting
	jobsCompleted atomic.Int64
	jobsFailed    atomic.Int64

	recentLogs *recentLogBuffer

	// Concurrency limiter (Phase 1: worker policy)
	concurrencyLimiter *concurrency.ConcurrencyLimiter

	// Stage executor (Step 2: stage/chunk execution with retry)
	stageExecutor *stageexec.StageExecutor

	// Exit function (for testing, defaults to os.Exit)
	exitFunc ExitFunc
}

// recentLogBuffer is a thread-safe ring buffer for recent log lines.
type recentLogBuffer struct {
	mu      sync.Mutex
	lines   []string
	errors  []string
	partial string
	max     int
}

func newRecentLogBuffer(max int) *recentLogBuffer {
	if max <= 0 {
		max = 500
	}
	return &recentLogBuffer{max: max}
}

func (b *recentLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	chunk := b.partial + string(p)
	parts := strings.Split(chunk, "\n")
	if len(parts) == 0 {
		b.partial = ""
		return len(p), nil
	}

	for i := 0; i < len(parts)-1; i++ {
		b.appendLineLocked(parts[i])
	}
	b.partial = parts[len(parts)-1]
	return len(p), nil
}

func (b *recentLogBuffer) appendLineLocked(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	b.lines = append(b.lines, line)
	if len(b.lines) > b.max {
		b.lines = b.lines[len(b.lines)-b.max:]
	}

	ll := strings.ToLower(line)
	if strings.Contains(ll, "[error]") || strings.Contains(ll, " error ") || strings.HasSuffix(ll, " error") || strings.HasPrefix(ll, "error ") {
		b.errors = append(b.errors, line)
		if len(b.errors) > b.max {
			b.errors = b.errors[len(b.errors)-b.max:]
		}
	}
}

func (b *recentLogBuffer) Snapshot(maxLogs, maxErrors int) ([]string, []string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	outLogs := append([]string(nil), b.lines...)
	outErrs := append([]string(nil), b.errors...)

	if maxLogs > 0 && len(outLogs) > maxLogs {
		outLogs = outLogs[len(outLogs)-maxLogs:]
	}
	if maxErrors > 0 && len(outErrs) > maxErrors {
		outErrs = outErrs[len(outErrs)-maxErrors:]
	}
	return outLogs, outErrs
}
