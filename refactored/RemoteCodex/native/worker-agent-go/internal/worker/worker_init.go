// Package worker provides the core worker orchestration logic.
package worker

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

// Valid status transitions:
// idle -> busy (job started)
// busy -> idle (job completed successfully)
// busy -> error (job failed)
// error -> idle (recovered, ready for new job)
// * -> stopped (shutdown)

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
	config    *config.WorkerConfig
	apiClient *api.Client
	logger    *logger.Logger

	// Status management
	status     Status
	currentJob *api.Job
	mu         sync.RWMutex

	// Lifecycle management
	stopChan chan struct{}
	stopOnce sync.Once
	stopped  atomic.Bool
	wg       sync.WaitGroup

	// Backoff for heartbeat failures
	heartbeatBackoff *backoffConfig

	version string

	// Channels for coordinated shutdown
	jobDone chan struct{}

	// Command management
	commandChan  chan api.WorkerCommand
	drainMode    atomic.Bool
	commandMu    sync.Mutex
	seenCommands map[string]time.Time

	// Job completion stats for heartbeat reporting
	jobsCompleted atomic.Int64
	jobsFailed    atomic.Int64

	recentLogs *recentLogBuffer

	// Concurrency limiter (Phase 1: worker policy)
	concurrencyLimiter *ConcurrencyLimiter

	// Stage executor (Step 2: stage/chunk execution with retry)
	stageExecutor *StageExecutor

	// Exit function (for testing, defaults to os.Exit)
	exitFunc ExitFunc
}

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

// New creates a new Worker instance.
func New(cfg *config.WorkerConfig, version string) *Worker {
	logLevel := logger.ParseLevel(cfg.LogLevel)
	recentLogs := newRecentLogBuffer(600)
	logOut := io.MultiWriter(os.Stdout, recentLogs)
	log := logger.New(logLevel, logOut)
	log.SetPrefix(fmt.Sprintf("[%s]", cfg.WorkerID))

	apiClient := api.NewClient(cfg.MasterURL,
		api.WithWorkerID(cfg.WorkerID),
		api.WithTimeout(30*time.Second),
		api.WithRetry(3, 5*time.Second),
		api.WithCircuitBreaker(
			cfg.CircuitBreakerFailureThreshold,
			cfg.CircuitBreakerSuccessThreshold,
			time.Duration(cfg.CircuitBreakerTimeoutSecs)*time.Second,
		),
	)

	// Initialize stage executor for GOD workflow
	stageExecCfg := &StageExecutorConfig{
		MaxConcurrentChunks: cfg.MaxActiveJobs,
		ChunkTimeout:        5 * time.Minute,
		MaxChunkRetries:     2,
		ChunkRetryDelay:     2 * time.Second,
		StageTimeout:        15 * time.Minute,
	}
	stageExecutor := NewStageExecutor(stageExecCfg)

	return &Worker{
		config:    cfg,
		apiClient: apiClient,
		logger:    log,
		status:    StatusIdle,
		stopChan:  make(chan struct{}),
		heartbeatBackoff: &backoffConfig{
			initialInterval: 5 * time.Second,
			maxInterval:     60 * time.Second,
			multiplier:      2.0,
		},
		version:            version,
		jobDone:            make(chan struct{}, 1),
		commandChan:        make(chan api.WorkerCommand, 10),
		seenCommands:       make(map[string]time.Time),
		recentLogs:         recentLogs,
		concurrencyLimiter: NewConcurrencyLimiter(cfg.MaxActiveJobs),
		stageExecutor:      stageExecutor,
		exitFunc:           os.Exit, // Default to os.Exit
	}
}

const (
	seenCommandTTL        = 30 * time.Minute
	seenCommandMaxEntries = 10000 // Hard limit to prevent memory growth
)

func commandKey(cmd api.WorkerCommand) string {
	ts := strings.TrimSpace(cmd.Timestamp)
	if ts == "" {
		ts = "no-timestamp"
	}
	return fmt.Sprintf("%s|%s", strings.TrimSpace(cmd.Command), ts)
}

func (w *Worker) markCommandSeen(cmd api.WorkerCommand) bool {
	key := commandKey(cmd)
	now := time.Now().UTC()

	w.commandMu.Lock()
	defer w.commandMu.Unlock()

	// Opportunistic cleanup to keep the in-memory map bounded.
	for k, t := range w.seenCommands {
		if now.Sub(t) > seenCommandTTL {
			delete(w.seenCommands, k)
		}
	}

	// Enforce hard limit: evict oldest entries if we're at capacity
	if len(w.seenCommands) >= seenCommandMaxEntries {
		// Remove the oldest 10% of entries
		toRemove := seenCommandMaxEntries / 10
		// Since maps don't have order, just remove entries past the limit
		count := 0
		for k := range w.seenCommands {
			if count >= toRemove {
				break
			}
			delete(w.seenCommands, k)
			count++
		}
	}

	if firstSeenAt, ok := w.seenCommands[key]; ok && now.Sub(firstSeenAt) <= seenCommandTTL {
		return true
	}

	w.seenCommands[key] = now
	return false
}

// cleanupSeenCommands performs a full cleanup of expired command entries.
// Call this periodically (e.g., every 10 minutes) to bound map growth.
func (w *Worker) cleanupSeenCommands() {
	now := time.Now().UTC()

	w.commandMu.Lock()
	defer w.commandMu.Unlock()

	for k, t := range w.seenCommands {
		if now.Sub(t) > seenCommandTTL {
			delete(w.seenCommands, k)
		}
	}
}

// SetExitFunc sets a custom exit function for tests and controlled shutdowns.
func (w *Worker) SetExitFunc(fn ExitFunc) {
	w.exitFunc = fn
}

// Status returns the current worker status.
func (w *Worker) Status() Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

// setStatus updates the current worker status.
func (w *Worker) setStatus(status Status) {
	w.mu.Lock()
	defer w.mu.Unlock()
	oldStatus := w.status
	w.status = status
	w.logger.Debug("Status transition: %s -> %s", oldStatus, status)
}

// canTransitionTo checks if a status transition is valid.
func (w *Worker) canTransitionTo(newStatus Status) bool {
	w.mu.RLock()
	currentStatus := w.status
	w.mu.RUnlock()

	switch currentStatus {
	case StatusIdle:
		return newStatus == StatusBusy || newStatus == StatusStopped
	case StatusBusy:
		return newStatus == StatusIdle || newStatus == StatusError || newStatus == StatusStopped
	case StatusError:
		return newStatus == StatusIdle || newStatus == StatusStopped
	case StatusStopped:
		return false
	default:
		return false
	}
}

// IsStopped returns true if shutdown has been requested.
func (w *Worker) IsStopped() bool {
	return w.stopped.Load()
}

// IsDraining returns true if the worker is in drain mode.
func (w *Worker) IsDraining() bool {
	return w.drainMode.Load()
}
