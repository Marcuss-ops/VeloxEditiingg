// Package worker — initialization and lifecycle management.
package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/internal/worker/stageexec"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// New creates a new Worker instance.
// Returns an error if the initial transport setup fails (bad TLS config,
// missing control_grpc_url, insecure flag mismatch). Callers MUST check
// the error before calling Start().
func New(cfg *config.WorkerConfig, version string) (*Worker, error) {
	logLevel := logger.ParseLevel(cfg.LogLevel)
	recentLogs := newRecentLogBuffer(600)
	logOut := io.MultiWriter(os.Stdout, recentLogs)
	log := logger.New(logLevel, logOut)
	log.SetPrefix(fmt.Sprintf("[%s]", cfg.WorkerID))

	// Detect optimal concurrency from hardware
	detectedConcurrency := detectMaxParallelJobs()
	if cfg.MaxActiveJobs > 1 {
		detectedConcurrency = cfg.MaxActiveJobs
	}
	log.Info("[CONCURRENCY] Detected %d CPUs, using %d max parallel jobs", runtime.NumCPU(), detectedConcurrency)

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
	if token := strings.TrimSpace(os.Getenv("WORKER_TOKEN")); token != "" {
		apiClient.SetAuthToken(token)
		log.Info("[AUTH] Loaded worker token from WORKER_TOKEN")
	} else if token := strings.TrimSpace(os.Getenv("VELOX_WORKER_TOKEN")); token != "" {
		apiClient.SetAuthToken(token)
		log.Info("[AUTH] Loaded worker token from VELOX_WORKER_TOKEN")
	}

	// Initialize stage executor for GOD workflow
	stageExecCfg := &stageexec.StageExecutorConfig{
		MaxConcurrentChunks: cfg.MaxActiveJobs,
		ChunkTimeout:        5 * time.Minute,
		MaxChunkRetries:     2,
		ChunkRetryDelay:     2 * time.Second,
		StageTimeout:        15 * time.Minute,
	}
	stageExecutor := stageexec.NewStageExecutor(stageExecCfg)

	// Store a transport factory that creates fresh instances per session.
	// After Close(), transports are not reusable (channels + sync.Once),
	// so each reconnect loop iteration gets a brand-new transport.
	// Phase 2.1: the factory now returns (Transport, error); a non-nil error
	// surface here would mean config validation failed at startup time.
	transportFactory := func() controltransport.ControlTransport {
		t, err := newControlTransport(cfg, log)
		if err != nil {
			log.Error("[INIT] transport factory rejected config: %v", err)
			return nil
		}
		return t
	}

	initialTransport, err := newControlTransport(cfg, log)
	if err != nil {
		// Config problem on the very first attempt — fail the worker init
		// immediately so operators do not enter the reconnect loop with a
		// broken transport (previously this nil-panicked on first Connect).
		log.Error("[INIT] initial transport setup failed: %v", err)
		return nil, fmt.Errorf("transport factory: %w", err)
	}

	// ── Shadow mode (PR11): build gRPC transport factory for observation ───
	// When ShadowMode is true, the worker opens a second gRPC transport that
	// observes JobOffer messages but never sends JobAccepted.
	var shadowTransportFactory func() controltransport.ControlTransport
	if cfg.ShadowMode {
		shadowGRPCURL := cfg.ShadowGRPCURL
		if shadowGRPCURL == "" {
			shadowGRPCURL = cfg.ControlGRPCURL
			log.Debug("[SHADOW] ShadowGRPCURL not set — using control_grpc_url: %s", shadowGRPCURL)
		}
		shadowFactoryCfg := *cfg
		shadowFactoryCfg.ControlGRPCURL = shadowGRPCURL
		shadowFactoryCfg.JobDelivery = "push"
		shadowTransportFactory = func() controltransport.ControlTransport {
			t, err := newGRPCStreamTransport(&shadowFactoryCfg, log)
			if err != nil {
				log.Warn("[SHADOW] Shadow transport factory failed: %v", err)
				return nil
			}
			return t
		}
		log.Info("[SHADOW] Shadow mode enabled — gRPC shadow will observe at %s", shadowGRPCURL)
	}

	w := &Worker{
		config:           cfg,
		apiClient:        apiClient,
		transportFactory: transportFactory,
		transport:        initialTransport,
		logger:    log,
		status:    StatusIdle,
		stopChan:  make(chan struct{}),
		heartbeatBackoff: &backoffConfig{
			initialInterval: 5 * time.Second,
			maxInterval:     60 * time.Second,
			multiplier:      2.0,
		},
		version:            version,

		seenCommands:       make(map[string]time.Time),
		recentLogs:         recentLogs,
		jobCancelFuncs:     make(map[string]context.CancelFunc),
		activeJobs:         make(map[string]*ActiveJob),
		pendingLeaseJobs:   make(map[string]*api.Job),
		connState:          ConnDisconnected,
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(detectedConcurrency),
		stageExecutor:      stageExecutor,
		exitFunc:           os.Exit,

		// Shadow mode fields
		shadowTransportFactory: shadowTransportFactory,
		shadowSeen:             make(map[string]time.Time),
	}

	// Load persisted state from previous run (command dedup, job recovery info).
	w.loadLocalState()

	return w, nil
}

const (
	seenCommandTTL        = 30 * time.Minute
	seenCommandMaxEntries = 10000 // Hard limit to prevent memory growth
)

func commandKey(cmd api.WorkerCommand) string {
	// Gap #4 fix: use CommandID as primary dedup key when available;
	// fall back to command+timestamp for backward compatibility.
	cid := strings.TrimSpace(cmd.CommandID)
	if cid != "" {
		return "id:" + cid
	}
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

// Status returns the current worker status, derived from activeJobs count and error state.
// Busy = at least one active job. Error = last job failed (status field). Idle = no jobs, no error.
func (w *Worker) Status() Status {
	if w.stopped.Load() {
		return StatusStopped
	}
	w.activeJobsMu.RLock()
	activeCount := len(w.activeJobs)
	w.activeJobsMu.RUnlock()
	if activeCount > 0 {
		return StatusBusy
	}
	w.mu.RLock()
	s := w.status
	w.mu.RUnlock()
	if s == StatusError {
		return StatusError
	}
	return StatusIdle
}

// setStatus updates the persisted error/idle state.
// Busy is derived from activeJobs and should NOT be set via this method.
func (w *Worker) setStatus(s Status) {
	w.mu.Lock()
	defer w.mu.Unlock()
	oldStatus := w.status
	w.status = s
	w.logger.Debug("Status transition: %s -> %s", oldStatus, s)
}

// canTransitionTo checks if a status transition is valid.
// Current status is derived from activeJobs and error state.
func (w *Worker) canTransitionTo(newStatus Status) bool {
	currentStatus := w.Status()

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

// registerJobCancel stores a cancel function for a job.
// Called when a job starts executing, allowing cancel_job command to abort it.
func (w *Worker) registerJobCancel(jobID string, cancel context.CancelFunc) {
	w.jobCancelMu.Lock()
	defer w.jobCancelMu.Unlock()
	w.jobCancelFuncs[jobID] = cancel
	w.logger.Debug("[CANCEL] Registered cancel for job %s", jobID)
}

// unregisterJobCancel removes a stored cancel function for a job.
// Called when a job finishes execution (success or failure).
func (w *Worker) unregisterJobCancel(jobID string) {
	w.jobCancelMu.Lock()
	defer w.jobCancelMu.Unlock()
	delete(w.jobCancelFuncs, jobID)
	w.logger.Debug("[CANCEL] Unregistered cancel for job %s", jobID)
}

// cancelJob cancels a running job by calling its stored cancel function.
// Called by the cancel_job command handler.
func (w *Worker) cancelJob(jobID string) bool {
	w.jobCancelMu.Lock()
	defer w.jobCancelMu.Unlock()
	cancel, ok := w.jobCancelFuncs[jobID]
	if !ok {
		w.logger.Warn("[CANCEL] No cancel function found for job %s (not running here?)", jobID)
		return false
	}
	cancel()
	delete(w.jobCancelFuncs, jobID)
	w.logger.Info("[CANCEL] Cancel signal sent for job %s", jobID)
	return true

}
