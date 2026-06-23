// Package worker — type definitions extracted from worker_init.go.
package worker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/internal/worker/stageexec"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/blob"
	"velox-worker-agent/pkg/cache"
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

// ActiveTaskLease tracks a leased task-native entry for periodic
// MsgTaskLeaseRenewal dispatch (PR-2 / canonical-attempt-identity).
// Mirrors the canonical (task_id, attempt_id, lease_id) tuple from the
// master's TaskAttempt row at the moment of TaskLeaseGranted. leaseRenewLoop
// reads with an RLock snapshot + iterates outside the lock (transport.Send
// is network I/O and must NOT block write-side stop/drain paths).
type ActiveTaskLease struct {
	TaskID    string
	AttemptID string
	LeaseID   string
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

	// Pending tasks: accepted via TaskOffer, waiting for TaskLeaseGranted
	// before executeJob dispatch (PR-2 canonical-attempt-identity). The
	// map is keyed by task_id (NOT job_id, NOT attempt_id) because
	// (task_id, worker_id, lease_id) is the canonical worker-bound
	// identity on the master's side and there is exactly one outstanding
	// offer per task per session.
	pendingTasks   map[string]*api.Job
	pendingTasksMu sync.Mutex

	// Active task-native leases: keyed by task_id; the iteration source
	// for MsgTaskLeaseRenewal dispatch in leaseRenewLoop. Populated on
	// MsgTaskLeaseGranted (alongside pendingTasks → executeJob), drained
	// on Stop() / canonical terminal-state transition. Each entry carries
	// (task_id, attempt_id, lease_id) so the master's RenewLease CAS
	// predicate matches the canonical TaskAttempt row.
	activeTaskLeases   map[string]*ActiveTaskLease
	activeTaskLeasesMu sync.RWMutex

	// Job completion stats for heartbeat reporting
	jobsCompleted atomic.Int64
	jobsFailed    atomic.Int64

	recentLogs *recentLogBuffer

	// Concurrency limiter (Phase 1: worker policy)
	concurrencyLimiter *concurrency.ConcurrencyLimiter // Stage executor (Step 2: stage/chunk execution with retry)
	stageExecutor      *stageexec.StageExecutor

	// Executor registry (PR-3.5): single source of truth for hello/heartbeat
	// capabilities and (eventually) for the taskrouter dispatch table.
	// Never nil after Worker construction — defaults to an empty registry
	// when no WithRegistry option is supplied to New().
	executorRegistry *executor.Registry

	// PR-3.7: persistent local cache + blob store + the TaskRunner
	// built from them. cache + blobs are non-nil only when the
	// corresponding With* option is supplied; taskRunner is always
	// non-nil (built from cache/blobs/registry in New).
	cache      *cache.PersistedLocalCache
	blobs      *blob.BlobArtifacts
	taskRunner *taskrunner.TaskRunner

	// PR-3.6 / F4: worker-side resource sampler. Powers Heartbeat.resources
	// (cumulative typed counters → master F2 decodes + delta-converts) AND
	// api.HostInfo.{HasGPU,RAMBytes,DiskFreeBytes} (PR-3.6 future markers
	// at worker.go:177-183). Created in New(); goroutine launched in
	// runSession under sessionCtx so the loop terminates with the
	// session. nil-safe read paths in hostInfo / sendHeartbeat tolerate
	// a sampler that hasn't yet sampled.
	sampler *telemetry.Sampler

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
