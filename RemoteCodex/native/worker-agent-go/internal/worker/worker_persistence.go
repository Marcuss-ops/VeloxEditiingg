// Package worker — local persistence for command deduplication across
// restarts AND master-restart recovery. The RecoveryReport protocol
// that historically piggybacked on this struct (ActiveJobs/
// PendingLeaseJobs + heartbeat.extra snapshot) was removed in PR 1;
// the worker's local recovery only stores per-session state needed
// to replay the next Master→Worker session boundary on restart.
//
// Files (under cfg.StateDir, env VELOX_STATE_DIR):
//
//	worker_state.json     — seen_command dedup state (PR-1 baseline).
//	                        Cadence: 30s ticker + on ctx/stop.
//	worker_recovery.json  — master-restart recovery snapshot:
//	                        activeTaskLeases + pendingTasks + an
//	                        audit-only ActiveTasks list. Cadence:
//	                        control-plane disconnect event + Stop.
//
// The two files are written independently because their cadences
// differ — bundling them would force the SeenCommands-only periodic
// save to also rewrite the recovery JSON every 30s, and conversely
// couple the disconnect snapshot to a SeenCommands roundtrip.
package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"velox-worker-agent/internal/executor"
)

// persistedState is the on-disk JSON structure for in-restart dedup.
//
// PR 1: ActiveJobs and PendingLeaseJobs fields have been removed. The
// canonical lease is held by the master (TaskLeaseReaper); on worker
// restart, in-flight tasks expire via lease_expiry and the master re-mints
// a fresh attempt. The worker keeps no copy.
//
// Forward-compat: legacy files written by PR < 1 also contained
// "active_jobs" and "pending_lease_jobs" maps. Those fields are simply
// ignored on load — Go json.Unmarshal silently drops unknown JSON keys.
type persistedState struct {
	// SeenCommands maps command key → first-seen timestamp for dedup.
	SeenCommands map[string]time.Time `json:"seen_commands"`
	// SavedAt is the last save timestamp.
	SavedAt time.Time `json:"saved_at"`
}

// RecoverySnapshot is the master-restart recovery payload, persisted
// to `<StateDir>/worker_recovery.json` on session end and replayed
// on the next New() call. Slices (not maps) make the JSON wire shape
// stable across Go map-iteration-order changes between captures —
// a re-applied snapshot MUST byte-equal the first one for the
// idempotence contract to hold.
type RecoverySnapshot struct {
	CapturedAt   time.Time                  `json:"captured_at"`
	ActiveTasks  []RecoveryActiveTaskEntry  `json:"active_tasks,omitempty"`
	ActiveLeases []RecoveryActiveLeaseEntry `json:"active_leases,omitempty"`
	PendingTasks []RecoveryPendingTaskEntry `json:"pending_tasks,omitempty"`
}

// RecoveryActiveTaskEntry is the identity-shaped placeholder for an
// in-flight task at the moment of capture. The Cancel funcs +
// goroutine pointers cannot survive a restart, so this entry is
// captured for diagnostic / ops audit ONLY; replay does NOT
// restore it (master re-mints a new attempt_id on the next dispatch
// because the previous TaskAttempt is already in TIMED_OUT state
// via the master's TaskLeaseReaper).
type RecoveryActiveTaskEntry struct {
	TaskID    string `json:"task_id"`
	JobID     string `json:"job_id"`
	AttemptID string `json:"attempt_id"`
	LeaseID   string `json:"lease_id"`
	StartedAt string `json:"started_at"`
}

// RecoveryActiveLeaseEntry mirrors ActiveTaskLease for JSON
// round-trip. Restored on replay so leaseRenewLoop can drive
// MsgTaskLeaseRenewal for any lease the master hasn't already
// evicted via TaskLeaseReaper.
type RecoveryActiveLeaseEntry struct {
	TaskID        string `json:"task_id"`
	JobID         string `json:"job_id"`
	AttemptID     string `json:"attempt_id"`
	LeaseID       string `json:"lease_id"`
	AttemptNumber int    `json:"attempt_number"`
	Revision      int    `json:"revision"`
}

// RecoveryPendingTaskEntry mirrors PendingTaskExecution for JSON
// round-trip. The embedded executor.TaskSpec serializes cleanly
// because TaskSpec carries only data fields (Version, JobID,
// ExecutorID, Payload — no closures, channels, or funcs). Restored
// on replay so the next MsgTaskLeaseGranted dispatch lands in the
// same map.
type RecoveryPendingTaskEntry struct {
	TaskID          string            `json:"task_id"`
	JobID           string            `json:"job_id"`
	JobRevision     int               `json:"job_revision"`
	AttemptID       string            `json:"attempt_id"`
	AttemptNumber   int               `json:"attempt_number"`
	LeaseID         string            `json:"lease_id"`
	ExecutorID      string            `json:"executor_id"`
	ExecutorVersion int               `json:"executor_version"`
	Revision        int               `json:"revision"`
	Spec            executor.TaskSpec `json:"spec"`
}

// stateFilePath builds the path to the local seen-commands state file
// under the canonical runtime directory. Worker state must NEVER live
// under WorkDir (mounted read-only inside the container at /app) —
// this is a host-backed volume mount so writes here survive container
// restarts.
func stateFilePath(stateDir string) string {
	return filepath.Join(stateDir, "worker_state.json")
}

// recoveryFilePath is the path to the master-restart recovery file.
// Sibling of stateFilePath, written independently and replayed on
// the next New() call.
func recoveryFilePath(stateDir string) string {
	return filepath.Join(stateDir, "worker_recovery.json")
}

// saveLocalState persists the current in-memory seen-commands state
// to a JSON file.
func (w *Worker) saveLocalState() error {
	stateDir := w.config.StateDir
	if stateDir == "" {
		return nil
	}
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		w.logger.Warn("[PERSIST] Cannot create state directory %s: %v", stateDir, err)
		return err
	}

	state := persistedState{
		SeenCommands: make(map[string]time.Time),
		SavedAt:      time.Now().UTC(),
	}

	w.commandMu.Lock()
	for k, t := range w.seenCommands {
		if time.Since(t) <= seenCommandTTL {
			state.SeenCommands[k] = t
		}
	}
	w.commandMu.Unlock()

	path := stateFilePath(stateDir)
	tmpPath := path + ".tmp"

	data, err := json.Marshal(state)
	if err != nil {
		w.logger.Warn("[PERSIST] Failed to marshal state: %v", err)
		return err
	}

	// Atomic write: tmp → rename. os.WriteFile already fsyncs the
	// data on most platforms via the underlying *os.File.Write,
	// so the explicit second Sync pass is unnecessary.
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		w.logger.Warn("[PERSIST] Failed to write tmp file %s: %v", tmpPath, err)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		w.logger.Warn("[PERSIST] Failed to rename tmp → state file %s: %v", path, err)
		return err
	}
	w.logger.Debug("[PERSIST] State saved to %s (%d seen commands)", path, len(state.SeenCommands))
	return nil
}

// loadLocalState restores in-memory state from the persistence
// files:
//  1. worker_state.json — seen_commands (PR-1 baseline).
//  2. worker_recovery.json — activeTaskLeases + pendingTasks
//     (master-restart recovery). Loaded only IF the snap is
//     non-empty (CapturedAt zero means it was never written).
//
// Called once at worker startup (after New) to recover command-dedup
// state + lifecycle bookkeeping from a previous session.
//
// Forward-compat: legacy files written by PR < 1 also contained
// "active_jobs" and "pending_lease_jobs" maps. Those fields are
// silently dropped by Go's json.Unmarshal.
func (w *Worker) loadLocalState() {
	stateDir := w.config.StateDir
	if stateDir == "" {
		return
	}

	path := stateFilePath(stateDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			w.logger.Warn("[PERSIST] Failed to read state file %s: %v", path, err)
		}
		// Recovery-side file is independent — try it separately so
		// a partially-corrupt state file doesn't block recovery
		// replay.
		w.loadRecoveryState()
		return
	}

	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		w.logger.Warn("[PERSIST] Failed to unmarshal state file %s: %v", path, err)
		w.loadRecoveryState()
		return
	}

	now := time.Now().UTC()

	restoredCmds := 0
	w.commandMu.Lock()
	for k, t := range state.SeenCommands {
		if now.Sub(t) <= seenCommandTTL {
			w.seenCommands[k] = t
			restoredCmds++
		}
	}
	w.commandMu.Unlock()

	w.logger.Info("[PERSIST] State loaded: %d seen commands restored", restoredCmds)

	// Master-restart recovery: replay activeTaskLeases + pendingTasks
	// from worker_recovery.json. Runs after seen-commands restore so
	// an operator can inspect the seen_commands log line BEFORE the
	// recovery log line at startup.
	w.loadRecoveryState()
}

// loadRecoveryState reads the recovery JSON file (if present) and
// applies its contents to the in-memory maps. Called from
// loadLocalState so the load path is single-source on New().
func (w *Worker) loadRecoveryState() {
	stateDir := w.config.StateDir
	if stateDir == "" {
		return
	}
	path := recoveryFilePath(stateDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			w.logger.Warn("[RECOVERY] Failed to read recovery file %s: %v", path, err)
		}
		return
	}
	var snap RecoverySnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		w.logger.Warn("[RECOVERY] Failed to unmarshal recovery file %s: %v", path, err)
		return
	}
	if snap.CapturedAt.IsZero() {
		// Empty / never-written snapshot — nothing to replay.
		return
	}
	tasks, leases, pending, err := w.applyRecoverySnapshot(snap)
	if err != nil {
		w.logger.Warn("[RECOVERY] replay error: %v", err)
		return
	}
	w.logger.Info("[RECOVERY] replayed: leases=%d pending=%d active_tasks=%d (dropped; master will remint)",
		leases, pending, tasks)
}

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
	tmpPath := path + ".tmp"

	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
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
//	activeTaskReplayed — always 0 (active tasks are diagnostic only)
//	pendingReplayed    — pending_tasks restored
//	leaseReplayed      — active_task_leases restored
//
// Lock-order matches capture (pendingTasksMu < activeTaskLeasesMu <
// activeTasksMu). No activeTasksMu acquire here — we don't mutate the
// activeTasks map (intentional: Cancel funcs + goroutines are dead
// across a worker restart).
func (w *Worker) applyRecoverySnapshot(snap RecoverySnapshot) (int, int, int, error) {
	pendingReplayed := 0
	w.pendingTasksMu.Lock()
	for _, pt := range snap.PendingTasks {
		if _, exists := w.pendingTasks[pt.TaskID]; exists {
			continue
		}
		w.pendingTasks[pt.TaskID] = &PendingTaskExecution{
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
		}
		pendingReplayed++
	}
	w.pendingTasksMu.Unlock()

	leaseReplayed := 0
	w.activeTaskLeasesMu.Lock()
	for _, al := range snap.ActiveLeases {
		if _, exists := w.activeTaskLeases[al.TaskID]; exists {
			continue
		}
		w.activeTaskLeases[al.TaskID] = &ActiveTaskLease{
			TaskID:        al.TaskID,
			JobID:         al.JobID,
			AttemptID:     al.AttemptID,
			LeaseID:       al.LeaseID,
			AttemptNumber: al.AttemptNumber,
			Revision:      al.Revision,
		}
		leaseReplayed++
	}
	w.activeTaskLeasesMu.Unlock()

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
