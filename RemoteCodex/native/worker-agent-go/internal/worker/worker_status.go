// Package worker / worker_status.go
//
// Worker status derivation + job-cancellation lifecycle helpers,
// extracted from worker_init.go.
package worker

import "context"

// SetExitFunc sets a custom exit function for tests and controlled shutdowns.
func (w *Worker) SetExitFunc(fn ExitFunc) {
	w.exitFunc = fn
}

// Status returns the current worker status, derived from activeTasks count and error state.
// Busy = at least one active task. Error = last task failed (status field). Idle = none.
func (w *Worker) Status() Status {
	if w.stopped.Load() {
		return StatusStopped
	}
	w.activeTasksMu.RLock()
	activeCount := len(w.activeTasks)
	w.activeTasksMu.RUnlock()
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

// cancelJob cancels all running tasks for a job by looking up taskIDsByJob.
// Called by MsgCancelJob + MsgLeaseRevoked + recovery directive handlers.
// Snapshots cancel funcs under activeTasksMu, then calls them outside the
// lock to avoid blocking heartbeat/Status readers during cancellation.
func (w *Worker) cancelJob(jobID string) bool {
	w.activeTasksMu.Lock()
	taskIDs := w.taskIDsByJob[jobID]
	if len(taskIDs) == 0 {
		w.activeTasksMu.Unlock()
		w.logger.Warn("[CANCEL] No active tasks for job %s", jobID)
		return false
	}
	// Snapshot cancel funcs before unlocking to avoid holding the write
	// lock during context cancellation (which may trigger defers that
	// try to RLock activeTasksMu in heartbeat/Status paths).
	cancels := make([]context.CancelFunc, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		if at, ok := w.activeTasks[taskID]; ok && at.Cancel != nil {
			cancels = append(cancels, at.Cancel)
		}
	}
	w.activeTasksMu.Unlock()

	cancelled := 0
	for _, cancel := range cancels {
		cancel()
		cancelled++
	}
	w.logger.Info("[CANCEL] Cancelled %d/%d tasks for job %s", cancelled, len(taskIDs), jobID)
	return cancelled > 0
}
