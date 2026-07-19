package worker

// active_lease_registry.go owns the in-memory set of task-native leases
// in flight on this worker. leaseRenewLoop.go reads the snapshot and
// sends MsgTaskLeaseRenewal; message handlers in worker_receive.go
// call Add/Remove on MsgTaskLeaseGranted / canonical terminal-state
// transitions. The three operations are colocated so the locking
// contract (activeTaskLeasesMu) is enforced in a single file.

// AddActiveTaskLease registers a task-native lease so leaseRenewLoop will
// dispatch MsgTaskLeaseRenewal for it. Called by the MsgTaskLeaseGranted
// handler (PR #5 / canonical-attempt-identity) right after it pops the
// pending task from pendingTasks. Safe for concurrent callers; nil/empty
// taskID or leaseID is a no-op (caller must drop via canonical-error path).
//
// The map is unconditionally initialized in worker_init.go New(); this
// helper never performs lazy-init — if the map is nil here, the worker
// was constructed defectively and a panic is the correct loud-failure
// surface (cover-up via lazy init masks operator bugs).
func (w *Worker) AddActiveTaskLease(taskID, jobID, attemptID, leaseID string, attemptNumber, revision int) {
	if taskID == "" || jobID == "" || attemptID == "" || leaseID == "" || attemptNumber <= 0 {
		return
	}
	w.activeTaskLeasesMu.Lock()
	defer w.activeTaskLeasesMu.Unlock()
	w.activeTaskLeases[taskID] = &ActiveTaskLease{
		TaskID:        taskID,
		JobID:         jobID,
		AttemptID:     attemptID,
		LeaseID:       leaseID,
		AttemptNumber: attemptNumber,
		Revision:      revision,
	}
}

// RemoveActiveTaskLease deregisters a task-native lease so leaseRenewLoop
// stops dispatching MsgTaskLeaseRenewal for it. Called on MsgLeaseRevoked
// canonical terminal-state transition (executeTask returns SUCCEEDED /
// FAILED / CANCELLED / TIMED_OUT). Empty taskID is a no-op.
func (w *Worker) RemoveActiveTaskLease(taskID string) {
	if taskID == "" {
		return
	}
	w.activeTaskLeasesMu.Lock()
	defer w.activeTaskLeasesMu.Unlock()
	delete(w.activeTaskLeases, taskID)
}

// SnapshotActiveTaskLeases returns a defensive copy of the current
// task-native lease set. Iteration over the snapshot must occur WITHOUT
// holding activeTaskLeasesMu — transport.Send is network I/O and would
// otherwise block Remove-side writers (Stop, cancelJob, Revoked handlers).
func (w *Worker) SnapshotActiveTaskLeases() []*ActiveTaskLease {
	w.activeTaskLeasesMu.RLock()
	defer w.activeTaskLeasesMu.RUnlock()
	if len(w.activeTaskLeases) == 0 {
		return nil
	}
	out := make([]*ActiveTaskLease, 0, len(w.activeTaskLeases))
	for _, tl := range w.activeTaskLeases {
		out = append(out, tl)
	}
	return out
}
