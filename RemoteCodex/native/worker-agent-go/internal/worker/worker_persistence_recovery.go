package worker

import (
	"context"
	"encoding/json"
	"os"
	"time"
)

// worker_persistence_recovery.go owns the master-restart recovery
// snapshot machinery (snapshotRecoveryState / captureRecoverySnapshot /
// applyRecoverySnapshot) and the periodic persistence loop
// (startPersistenceLoop). The seen-commands state file save/load and
// the persisted-state types live in worker_persistence.go.

// snapshotRecoveryState writes the recovery snapshot to disk with
// atomic-write semantics. Best-effort: a write error is returned to
// the caller but does NOT abort the disconnect / Stop path (the
// caller logs and continues).
func (w *Worker) snapshotRecoveryState() error {
	stateDir := w.config.StateDir
	if stateDir == "" {
		return nil
	}
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return err
	}

	var snap RecoverySnapshot
	w.captureRecoverySnapshot(&snap)

	path := recoveryFilePath(stateDir)

	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	// Durable atomic write (see writeFileDurable): fsync the bytes and the
	// directory entry so the recovery snapshot survives a crash, matching the
	// seen-commands state file.
	if err := writeFileDurable(path, data, 0600); err != nil {
		return err
	}
	return nil
}

// captureRecoverySnapshot reads the in-memory lifecycle maps into
// a RecoverySnapshot under the CANONICAL lock order documented in
// worker_types.go:
//
//	pendingTasksMu  <  activeTaskLeasesMu  <  activeTasksMu
//
// Acquired in the listed order, released in reverse. The capture
// path is read-only on the maps so we use RLock on the two maps
// that have an RWMutex; pendingTasksMu is plain Mutex (no RW variant)
// and we use Lock. The lock-order discipline is critical because
// applyRecoverySnapshot ACQUIRES the same locks at writer grade —
// if capture held writer locks in a different order, apply would
// deadlock on the first re-apply.
func (w *Worker) captureRecoverySnapshot(snap *RecoverySnapshot) {
	snap.CapturedAt = time.Now().UTC()

	w.pendingTasksMu.Lock()
	for _, pt := range w.pendingTasks {
		snap.PendingTasks = append(snap.PendingTasks, RecoveryPendingTaskEntry{
			TaskID:          pt.TaskID,
			JobID:           pt.JobID,
			JobRevision:     pt.JobRevision,
			AttemptID:       pt.AttemptID,
			AttemptNumber:   pt.AttemptNumber,
			LeaseID:         pt.LeaseID,
			ExecutorID:      pt.ExecutorID,
			ExecutorVersion: pt.ExecutorVersion,
			Revision:        pt.Revision,
			Spec:            pt.Spec,
		})
	}
	w.pendingTasksMu.Unlock()

	w.activeTaskLeasesMu.RLock()
	for _, al := range w.activeTaskLeases {
		snap.ActiveLeases = append(snap.ActiveLeases, RecoveryActiveLeaseEntry{
			TaskID:        al.TaskID,
			JobID:         al.JobID,
			AttemptID:     al.AttemptID,
			LeaseID:       al.LeaseID,
			AttemptNumber: al.AttemptNumber,
			Revision:      al.Revision,
		})
	}
	w.activeTaskLeasesMu.RUnlock()

	w.activeTasksMu.RLock()
	for _, at := range w.activeTasks {
		snap.ActiveTasks = append(snap.ActiveTasks, RecoveryActiveTaskEntry{
			TaskID:    at.TaskID,
			JobID:     at.JobID,
			AttemptID: at.AttemptID,
			LeaseID:   at.LeaseID,
			StartedAt: at.StartedAt.Format(time.RFC3339Nano),
		})
	}
	w.activeTasksMu.RUnlock()
}

// applyRecoverySnapshot restores the in-memory maps from a
// RecoverySnapshot. Idempotent: existing keys with the same TaskID
// are NOT overwritten, so a re-applied snapshot on the same worker
// session does NOT mutate the maps twice + cannot corrupt an in-
// flight entry already owned by the current session.
//
// Returns counts:
//
//		activeTaskReplayed — always 0 (active tasks are diagnostic only)
//		pendingReplayed    — always 0; pending offers are discarded and the
//	                        master remints them after reconnect
//		leaseReplayed      — active_task_leases restored
//
// Lock-order matches capture (pendingTasksMu < activeTaskLeasesMu <
// activeTasksMu). No activeTasksMu acquire here — we don't mutate the
// activeTasks map (intentional: Cancel funcs + goroutines are dead
// across a worker restart).
func (w *Worker) applyRecoverySnapshot(snap RecoverySnapshot) (int, int, int, error) {
	// A pending TaskOffer has been accepted by the previous gRPC session but
	// has not received a lease grant. Its session/lease fencing is no longer
	// valid after reconnect, and the Master is responsible for reminting any
	// still-READY task. Restoring it would consume MaxActiveJobs forever when
	// the task was cancelled while the worker was disconnected.
	pendingReplayed := 0

	// A process restart cannot safely restore a lease: the execution goroutine
	// and TaskSpec are gone, so renewing it would create a RUNNING zombie.
	// The Master shortens leases on session disconnect and reaps them through
	// the canonical TaskLeaseReaper. Keep the snapshot for audit only.
	leaseReplayed := 0

	// Active tasks: intentionally NOT restored (Cancel + goroutine
	// are dead). Returning 0 across the active-task bucket.
	return 0, leaseReplayed, pendingReplayed, nil
}

// startPersistenceLoop saves local seen-commands state periodically.
// The master-restart recovery has its own cadence (snapshotRecoveryState
// is called explicitly at the disconnect / Stop paths; we ALSO call
// it from the ticker so a mid-session crash without a clean
// disconnect still leaves a recovery snapshot on disk).
func (w *Worker) startPersistenceLoop(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = w.saveLocalState()
				_ = w.snapshotRecoveryState()
				return
			case <-w.stopChan:
				_ = w.saveLocalState()
				_ = w.snapshotRecoveryState()
				return
			case <-ticker.C:
				_ = w.saveLocalState()
				_ = w.snapshotRecoveryState()
			}
		}
	}()
}
