package workers

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/queue"
	workersreg "velox-server/internal/workers"
)

// WorkerLifecycleManager handles proactive worker lifecycle management
type WorkerLifecycleManager struct {
	mu          sync.RWMutex
	registry    *workersreg.Registry
	fileQueue   *queue.FileQueue
	dlq         *queue.DeadLetterQueue
	workerQueue *queue.ZombieAwareFileQueue

	// Configuration
	config LifecycleConfig

	// State tracking
	pendingShutdown map[string]*ShutdownState
	healthStatus    map[string]*WorkerHealth

	// Channels
	alertChan chan WorkerAlert

	// Command manager - shared with WorkerLifecycle for HTTP pull endpoint
	cmdMgr *workersreg.CommandManager

	// Callbacks
	onWorkerOffline func(workerID string, activeJobs []string)
	onWorkerPreStop func(workerID string) error
	onJobReassigned func(jobID, oldWorker, newWorker string)
}

// LifecycleConfig holds configuration for lifecycle management
type LifecycleConfig struct {
	HealthCheckInterval    time.Duration `json:"health_check_interval"`
	HeartbeatTimeout       time.Duration `json:"heartbeat_timeout"`
	GracefulShutdownTime   time.Duration `json:"graceful_shutdown_time"`
	PreStopTimeout         time.Duration `json:"pre_stop_timeout"`
	MaxConcurrentShutdowns int           `json:"max_concurrent_shutdowns"`
	EnableAutoRecovery     bool          `json:"enable_auto_recovery"`
	EnableProactiveHealth  bool          `json:"enable_proactive_health"`
}

// DefaultLifecycleConfig returns sensible defaults
func DefaultLifecycleConfig() LifecycleConfig {
	return LifecycleConfig{
		HealthCheckInterval:    30 * time.Second,
		HeartbeatTimeout:       2 * time.Minute,
		GracefulShutdownTime:   5 * time.Minute,
		PreStopTimeout:         30 * time.Second,
		MaxConcurrentShutdowns: 5,
		EnableAutoRecovery:     true,
		EnableProactiveHealth:  true,
	}
}

// ShutdownState tracks the state of a graceful shutdown
type ShutdownState struct {
	WorkerID      string        `json:"worker_id"`
	InitiatedAt   time.Time     `json:"initiated_at"`
	Phase         string        `json:"phase"` // "requested", "draining", "stopping", "complete"
	ActiveJobs    []string      `json:"active_jobs"`
	Reason        string        `json:"reason"`
	LastHeartbeat time.Time     `json:"last_heartbeat"`
	Timeout       time.Duration `json:"timeout"`
	Completed     bool          `json:"completed"`
	Failed        bool          `json:"failed"`
	Error         string        `json:"error,omitempty"`
}

// WorkerHealth tracks worker health metrics
type WorkerHealth struct {
	WorkerID            string        `json:"worker_id"`
	LastHeartbeat       time.Time     `json:"last_heartbeat"`
	ConsecutiveFailures int           `json:"consecutive_failures"`
	HealthScore         float64       `json:"health_score"` // 0-1, 1 = healthy
	RecentErrors        []string      `json:"recent_errors,omitempty"`
	LastError           time.Time     `json:"last_error,omitempty"`
	JobsCompleted       int64         `json:"jobs_completed"`
	JobsFailed          int64         `json:"jobs_failed"`
	AvgJobDuration      time.Duration `json:"avg_job_duration"`
	Status              string        `json:"status"` // "healthy", "degraded", "unhealthy", "offline"
	LastChecked         time.Time     `json:"last_checked"`
}

// WorkerAlert represents an alert about a worker
type WorkerAlert struct {
	Type      string                 `json:"type"` // "health_degraded", "offline", "timeout", "error"
	WorkerID  string                 `json:"worker_id"`
	Severity  string                 `json:"severity"` // "info", "warning", "critical"
	Message   string                 `json:"message"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// WorkerCommand represents a command to send to a worker
type WorkerCommand struct {
	Command   string                 `json:"command"` // "drain", "stop", "restart", "ping"
	WorkerID  string                 `json:"worker_id"`
	Timestamp time.Time              `json:"timestamp"`
	Params    map[string]interface{} `json:"params,omitempty"`
}

// NewWorkerLifecycleManager creates a new lifecycle manager
func NewWorkerLifecycleManager(cfg LifecycleConfig, reg *workersreg.Registry, fq *queue.FileQueue, dlq *queue.DeadLetterQueue) *WorkerLifecycleManager {
	return &WorkerLifecycleManager{
		registry:        reg,
		fileQueue:       fq,
		dlq:             dlq,
		config:          cfg,
		pendingShutdown: make(map[string]*ShutdownState),
		healthStatus:    make(map[string]*WorkerHealth),
		alertChan:       make(chan WorkerAlert, 100),
		cmdMgr:          workersreg.NewCommandManager(),
	}
}

// SetCommandManager sets a shared command manager (for integration with WorkerLifecycle)
func (lm *WorkerLifecycleManager) SetCommandManager(cmdMgr *workersreg.CommandManager) {
	lm.cmdMgr = cmdMgr
}

// GetCommandManager returns the command manager
func (lm *WorkerLifecycleManager) GetCommandManager() *workersreg.CommandManager {
	return lm.cmdMgr
}

// SetWorkerQueue sets the zombie-aware queue for advanced handling
func (lm *WorkerLifecycleManager) SetWorkerQueue(wq *queue.ZombieAwareFileQueue) {
	lm.workerQueue = wq
}

// SetCallbacks sets lifecycle callbacks
func (lm *WorkerLifecycleManager) SetCallbacks(onOffline func(workerID string, activeJobs []string), onPreStop func(workerID string) error, onReassign func(jobID, oldWorker, newWorker string)) {
	lm.onWorkerOffline = onOffline
	lm.onWorkerPreStop = onPreStop
	lm.onJobReassigned = onReassign
}

// Start begins the lifecycle management loops
func (lm *WorkerLifecycleManager) Start(ctx context.Context) {
	log.Printf("🔄 Worker Lifecycle Manager started")

	// Health check loop
	go lm.healthCheckLoop(ctx)

	// Alert handling loop
	go lm.alertHandlerLoop(ctx)

	// Shutdown monitoring loop
	go lm.shutdownMonitorLoop(ctx)
}

// healthCheckLoop periodically checks worker health
func (lm *WorkerLifecycleManager) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(lm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lm.checkAllWorkersHealth(ctx)
		}
	}
}

// checkAllWorkersHealth evaluates health of all registered workers
func (lm *WorkerLifecycleManager) checkAllWorkersHealth(ctx context.Context) {
	workerList := lm.registry.List(ctx)
	now := time.Now()

	for _, w := range workerList {
		health := lm.evaluateWorkerHealth(ctx, w, now)

		// Update tracking
		lm.mu.Lock()
		prevHealth, exists := lm.healthStatus[w.WorkerID]
		lm.healthStatus[w.WorkerID] = health
		lm.mu.Unlock()

		// Check for status changes
		if exists && prevHealth.Status != health.Status {
			lm.handleHealthStatusChange(ctx, w.WorkerID, prevHealth.Status, health.Status)
		}

		// Generate alerts for degraded health
		if health.Status == "degraded" {
			lm.alertChan <- WorkerAlert{
				Type:      "health_degraded",
				WorkerID:  w.WorkerID,
				Severity:  "warning",
				Message:   "Worker health degraded",
				Timestamp: now,
				Data: map[string]interface{}{
					"health_score": health.HealthScore,
					"failures":     health.ConsecutiveFailures,
				},
			}
		}

		// Handle offline workers
		if health.Status == "offline" {
			lm.handleWorkerOffline(ctx, w.WorkerID)
		}
	}
}

// evaluateWorkerHealth calculates health score for a worker
func (lm *WorkerLifecycleManager) evaluateWorkerHealth(ctx context.Context, w workersreg.WorkerInfo, now time.Time) *WorkerHealth {
	health := &WorkerHealth{
		WorkerID:    w.WorkerID,
		LastChecked: now,
		Status:      "healthy",
		HealthScore: 1.0,
	}

	// Parse last heartbeat
	if w.LastHB != "" {
		t, err := time.Parse(time.RFC3339, w.LastHB)
		if err == nil {
			health.LastHeartbeat = t
		}
	}

	// Check heartbeat freshness
	timeSinceHB := now.Sub(health.LastHeartbeat)

	switch {
	case timeSinceHB > lm.config.HeartbeatTimeout*2:
		health.Status = "offline"
		health.HealthScore = 0
	case timeSinceHB > lm.config.HeartbeatTimeout:
		health.Status = "unhealthy"
		health.HealthScore = 0.3
	case timeSinceHB > lm.config.HeartbeatTimeout/2:
		health.Status = "degraded"
		health.HealthScore = 0.6
	}

	// Consider error rate
	if health.JobsCompleted > 0 {
		errorRate := float64(health.JobsFailed) / float64(health.JobsCompleted+health.JobsFailed)
		if errorRate > 0.3 {
			health.HealthScore *= 0.7
			if health.Status == "healthy" {
				health.Status = "degraded"
			}
		}
	}

	return health
}

// handleHealthStatusChange handles transitions in health status
func (lm *WorkerLifecycleManager) handleHealthStatusChange(ctx context.Context, workerID, oldStatus, newStatus string) {
	shortID := workerID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	log.Printf("📊 Worker %s health changed: %s -> %s", shortID, oldStatus, newStatus)

	if lm.alertChan != nil {
		lm.alertChan <- WorkerAlert{
			Type:      "status_change",
			WorkerID:  workerID,
			Severity:  "info",
			Message:   "Worker health status changed",
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"old_status": oldStatus,
				"new_status": newStatus,
			},
		}
	}
}

// handleWorkerOffline handles when a worker goes offline
func (lm *WorkerLifecycleManager) handleWorkerOffline(ctx context.Context, workerID string) {
	// Check if we're already managing a shutdown for this worker
	lm.mu.RLock()
	_, hasShutdown := lm.pendingShutdown[workerID]
	lm.mu.RUnlock()

	if hasShutdown {
		return // Already being handled
	}

	// Get worker's active jobs
	workerInfo := lm.registry.GetWorker(ctx, workerID)
	var activeJobs []string
	if workerInfo != nil && workerInfo.CurrentJob != "" {
		activeJobs = append(activeJobs, workerInfo.CurrentJob)
	}

	// Get jobs assigned to this worker from queue
	if lm.fileQueue != nil {
		processingJobs, _ := lm.fileQueue.GetProcessingJobs(ctx)
		for _, job := range processingJobs {
			if job.AssignedTo == workerID {
				activeJobs = append(activeJobs, job.JobID)
			}
		}
	}

	// Alert
	lm.alertChan <- WorkerAlert{
		Type:      "offline",
		WorkerID:  workerID,
		Severity:  "critical",
		Message:   "Worker went offline",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"active_jobs": activeJobs,
		},
	}

	// Callback
	if lm.onWorkerOffline != nil {
		go lm.onWorkerOffline(workerID, activeJobs)
	}

	// Auto-recovery
	if lm.config.EnableAutoRecovery && len(activeJobs) > 0 {
		lm.initiateJobRecovery(ctx, workerID, activeJobs)
	}
}

// initiateJobRecovery attempts to recover jobs from offline worker
func (lm *WorkerLifecycleManager) initiateJobRecovery(ctx context.Context, workerID string, jobIDs []string) {
	log.Printf("🔧 Initiating job recovery for worker %s (%d jobs)", workerID[:8], len(jobIDs))

	for _, jobID := range jobIDs {
		if lm.fileQueue != nil {
			// Requeue the job
			job, err := lm.fileQueue.GetJob(ctx, jobID)
			if err != nil {
				continue
			}

			// Reset job state
			job.Status = queue.StatusPending
			job.AssignedTo = ""
			job.AssignedAt = nil
			job.ClaimedBy = ""
			job.ClaimedAt = ""
			job.RetryCount++

			// Update via fail with requeue
			lm.fileQueue.FailJob(ctx, jobID, "Worker offline - job recovered", true)

			if lm.onJobReassigned != nil {
				lm.onJobReassigned(jobID, workerID, "")
			}
		}
	}
}

// RequestGracefulShutdown requests a graceful shutdown for a worker
func (lm *WorkerLifecycleManager) RequestGracefulShutdown(ctx context.Context, workerID, reason string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check if already shutting down
	if _, exists := lm.pendingShutdown[workerID]; exists {
		return nil
	}

	// Get active jobs
	var activeJobs []string
	workerInfo := lm.registry.GetWorker(ctx, workerID)
	if workerInfo != nil && workerInfo.CurrentJob != "" {
		activeJobs = append(activeJobs, workerInfo.CurrentJob)
	}

	// Create shutdown state
	state := &ShutdownState{
		WorkerID:    workerID,
		InitiatedAt: time.Now(),
		Phase:       "requested",
		ActiveJobs:  activeJobs,
		Reason:      reason,
		Timeout:     lm.config.GracefulShutdownTime,
	}

	lm.pendingShutdown[workerID] = state

	// Set worker to drain mode
	lm.registry.SetWorkerDrain(ctx, workerID, true)

	// Push drain command via CommandManager (worker pulls via /worker/command)
	if lm.cmdMgr != nil {
		lm.cmdMgr.PushCommand(workerID, "drain", map[string]interface{}{
			"reason":  reason,
			"timeout": lm.config.GracefulShutdownTime.String(),
		})
	}

	log.Printf("📤 Graceful shutdown requested for worker %s: %s", workerID[:8], reason)
	return nil
}

// shutdownMonitorLoop monitors pending shutdowns
func (lm *WorkerLifecycleManager) shutdownMonitorLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lm.checkPendingShutdowns(ctx)
		}
	}
}

// checkPendingShutdowns checks on ongoing shutdown processes
func (lm *WorkerLifecycleManager) checkPendingShutdowns(ctx context.Context) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()

	for workerID, state := range lm.pendingShutdown {
		elapsed := now.Sub(state.InitiatedAt)

		// Check if completed
		if state.Completed {
			delete(lm.pendingShutdown, workerID)
			continue
		}

		// Check for timeout
		if elapsed > state.Timeout {
			state.Failed = true
			state.Error = "shutdown timeout"
			lm.forceWorkerShutdown(ctx, workerID)
			delete(lm.pendingShutdown, workerID)
			continue
		}

		// Update phase based on active jobs
		if len(state.ActiveJobs) == 0 {
			if state.Phase != "stopping" {
				state.Phase = "stopping"
				// Push stop command via CommandManager
				if lm.cmdMgr != nil {
					lm.cmdMgr.PushCommand(workerID, "stop", nil)
				}
			}
		} else {
			// Still draining - update active jobs list
			if lm.fileQueue != nil {
				var remaining []string
				for _, jobID := range state.ActiveJobs {
					job, err := lm.fileQueue.GetJob(ctx, jobID)
					if err == nil && job.Status == queue.StatusProcessing {
						remaining = append(remaining, jobID)
					}
				}
				state.ActiveJobs = remaining
				state.Phase = "draining"
			}
		}
	}
}

// forceWorkerShutdown forcibly removes a worker
func (lm *WorkerLifecycleManager) forceWorkerShutdown(ctx context.Context, workerID string) {
	log.Printf("⚠️ Force shutdown for worker %s", workerID[:8])

	// Get remaining jobs first
	var jobsToRecover []string
	if lm.fileQueue != nil {
		processingJobs, _ := lm.fileQueue.GetProcessingJobs(ctx)
		for _, job := range processingJobs {
			if job.AssignedTo == workerID {
				jobsToRecover = append(jobsToRecover, job.JobID)
			}
		}
	}

	// Unregister worker
	lm.registry.UnregisterWorker(ctx, workerID)

	// Recover jobs
	if len(jobsToRecover) > 0 {
		lm.initiateJobRecovery(ctx, workerID, jobsToRecover)
	}
}

// alertHandlerLoop processes alerts
func (lm *WorkerLifecycleManager) alertHandlerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case alert := <-lm.alertChan:
			lm.processAlert(alert)
		}
	}
}

// processAlert handles an alert
func (lm *WorkerLifecycleManager) processAlert(alert WorkerAlert) {
	emoji := "ℹ️"
	switch alert.Severity {
	case "warning":
		emoji = "⚠️"
	case "critical":
		emoji = "🚨"
	}

	log.Printf("%s Worker %s: %s [%s] %s", emoji, alert.WorkerID[:8], alert.Type, alert.Severity, alert.Message)

	// Here you would integrate with external alerting systems
	// e.g., Slack, PagerDuty, email, etc.
}

// GetWorkerHealth returns health status for a worker
func (lm *WorkerLifecycleManager) GetWorkerHealth(workerID string) *WorkerHealth {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.healthStatus[workerID]
}

// GetAllHealth returns health status for all workers
func (lm *WorkerLifecycleManager) GetAllHealth() map[string]*WorkerHealth {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	result := make(map[string]*WorkerHealth)
	for k, v := range lm.healthStatus {
		result[k] = v
	}
	return result
}

// GetPendingShutdowns returns all pending shutdown states
func (lm *WorkerLifecycleManager) GetPendingShutdowns() map[string]*ShutdownState {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	result := make(map[string]*ShutdownState)
	for k, v := range lm.pendingShutdown {
		result[k] = v
	}
	return result
}

// GetAlertChannel returns the alert channel for external subscribers
func (lm *WorkerLifecycleManager) GetAlertChannel() <-chan WorkerAlert {
	return lm.alertChan
}

// Stats returns lifecycle manager statistics
func (lm *WorkerLifecycleManager) Stats() map[string]interface{} {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	stats := map[string]interface{}{
		"pending_shutdowns": len(lm.pendingShutdown),
		"tracked_workers":   len(lm.healthStatus),
		"health_summary": map[string]int{
			"healthy":   0,
			"degraded":  0,
			"unhealthy": 0,
			"offline":   0,
		},
	}

	for _, health := range lm.healthStatus {
		stats["health_summary"].(map[string]int)[health.Status]++
	}

	return stats
}

// RenameWorker handles POST /worker/rename
func RenameWorker(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			NewName  string `json:"new_name"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "worker_id required"})
			return
		}
		if body.NewName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "new_name required"})
			return
		}

		// Get current worker info
		workerInfo := reg.GetWorker(c.Request.Context(), body.WorkerID)
		if workerInfo == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "worker not found"})
			return
		}

		oldName := workerInfo.WorkerName
		// Update worker name via heartbeat
		_ = reg.Heartbeat(c.Request.Context(), body.WorkerID, body.NewName, workerInfo.Status, workerInfo.CurrentJob, nil)

		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"worker_id": body.WorkerID,
			"old_name":  oldName,
			"new_name":  body.NewName,
		})
	}
}

// SetWorkerGroup handles POST /worker/set_group
func SetWorkerGroup(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID    string `json:"worker_id"`
			WorkerGroup string `json:"worker_group"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "worker_id required"})
			return
		}

		// Get current worker info
		workerInfo := reg.GetWorker(c.Request.Context(), body.WorkerID)
		if workerInfo == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "worker not found"})
			return
		}

		// Update worker group via heartbeat with extra
		extra := map[string]interface{}{
			"worker_group": body.WorkerGroup,
		}
		_ = reg.Heartbeat(c.Request.Context(), body.WorkerID, workerInfo.WorkerName, workerInfo.Status, workerInfo.CurrentJob, extra)

		c.JSON(http.StatusOK, gin.H{
			"status":       "ok",
			"worker_id":    body.WorkerID,
			"worker_group": body.WorkerGroup,
		})
	}
}

// ReportWorkerError handles POST /worker/report_error
func ReportWorkerError(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID  string `json:"worker_id"`
			Error     string `json:"error"`
			JobID     string `json:"job_id"`
			Timestamp string `json:"timestamp"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "worker_id required"})
			return
		}

		// Log the error (in production, this would go to a logger or error tracking system)
		// For now, we just acknowledge receipt

		c.JSON(http.StatusOK, gin.H{
			"ok":        true,
			"worker_id": body.WorkerID,
			"error":     body.Error,
			"job_id":    body.JobID,
		})
	}
}
