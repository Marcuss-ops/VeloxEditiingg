package workers

import (
	"context"
	"log"
	"sync"
	"time"

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
	log.Printf("[LIFECYCLE] Worker Lifecycle Manager started")

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

	// Populate job counts from worker metrics
	if w.Metrics != nil {
		if v, ok := w.Metrics["jobs_completed"].(float64); ok {
			health.JobsCompleted = int64(v)
		} else if v, ok := w.Metrics["jobs_completed"].(int64); ok {
			health.JobsCompleted = v
		}
		if v, ok := w.Metrics["jobs_failed"].(float64); ok {
			health.JobsFailed = int64(v)
		} else if v, ok := w.Metrics["jobs_failed"].(int64); ok {
			health.JobsFailed = v
		}
	}

	// Consider error rate
	totalJobs := health.JobsCompleted + health.JobsFailed
	if totalJobs > 0 {
		errorRate := float64(health.JobsFailed) / float64(totalJobs)
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
	log.Printf("[HEALTH] Worker %s health changed: %s -> %s", shortID, oldStatus, newStatus)

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
	prefix := "[INFO]"
	switch alert.Severity {
	case "warning":
		prefix = "[WARN]"
	case "critical":
		prefix = "[CRIT]"
	}

	log.Printf("%s Worker %s: %s [%s] %s", prefix, alert.WorkerID[:8], alert.Type, alert.Severity, alert.Message)

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
