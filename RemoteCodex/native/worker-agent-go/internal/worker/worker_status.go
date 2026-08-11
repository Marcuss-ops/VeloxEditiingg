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
		// Busy is derived from the active-task set. Accepting another
		// lease while a slot is already running must not be rejected as a
		// state transition; the limiter has already enforced the cap.
		return newStatus == StatusBusy || newStatus == StatusIdle || newStatus == StatusError || newStatus == StatusStopped
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

// cancelJob cancels all running tasks and removes all accepted-but-not-yet-
// leased offers for a job. A TaskOffer is counted against capacity as soon
// as it is accepted, so leaving a pending offer behind after cancellation
// permanently consumes a slot until the worker restarts.
// Called by MsgCancelJob + MsgLeaseRevoked + recovery directive handlers.
// Snapshots cancel funcs under activeTasksMu and removes pending offers under
// pendingTasksMu, then calls the funcs outside the locks to avoid blocking
// heartbeat/Status readers during cancellation.
func (w *Worker) cancelJob(jobID string) bool {
	w.activeTasksMu.Lock()
	taskIDs := w.taskIDsByJob[jobID]
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

	pendingCancelled := w.removePendingTasksForJob(jobID)
	if len(taskIDs) == 0 && pendingCancelled == 0 {
		w.logger.Warn("[CANCEL] No active or pending tasks for job %s", jobID)
		return false
	}

	cancelled := 0
	for _, cancel := range cancels {
		cancel()
		cancelled++
	}
	w.logger.Info("[CANCEL] Cancelled %d/%d active tasks and removed %d pending offers for job %s", cancelled, len(taskIDs), pendingCancelled, jobID)
	return cancelled > 0 || pendingCancelled > 0
}

// removePendingTasksForJob removes accepted TaskOffers that are waiting for
// TaskLeaseGranted. They have no cancellation function yet, but they still
// count toward MaxActiveJobs and must not survive a job cancellation.
func (w *Worker) removePendingTasksForJob(jobID string) int {
	if jobID == "" {
		return 0
	}
	w.pendingTasksMu.Lock()
	defer w.pendingTasksMu.Unlock()
	removed := 0
	for taskID, pending := range w.pendingTasks {
		if pending != nil && pending.JobID == jobID {
			delete(w.pendingTasks, taskID)
			removed++
		}
	}
	return removed
}
