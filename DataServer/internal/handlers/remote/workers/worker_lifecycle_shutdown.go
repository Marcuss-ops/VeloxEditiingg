package workers

import (
	"context"
	"log"
	"time"

	"velox-server/internal/queue"
)

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

	log.Printf("[SHUTDOWN] Graceful shutdown requested for worker %s: %s", workerID[:8], reason)
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
	log.Printf("[SHUTDOWN] Force shutdown for worker %s", workerID[:8])

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
	log.Printf("[RECOVERY] Initiating job recovery for worker %s (%d jobs)", workerID[:8], len(jobIDs))

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
