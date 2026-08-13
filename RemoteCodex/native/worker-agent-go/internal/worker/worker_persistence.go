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
//
// The recovery snapshot capture/replay machinery (snapshotRecoveryState
// / captureRecoverySnapshot / applyRecoverySnapshot) and the
// persistence loop (startPersistenceLoop) live in the sibling file
// worker_persistence_recovery.go.
package worker

import (
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

	data, err := json.Marshal(state)
	if err != nil {
		w.logger.Warn("[PERSIST] Failed to marshal state: %v", err)
		return err
	}

	// Atomic durable write: tmp → fsync → close → rename → directory fsync.
	// The previous os.WriteFile did NOT fsync the bytes (it only writes and
	// closes) nor the directory entry after rename, so a crash could lose the
	// just-saved dedup state and make a replayed command look unseen.
	if err := writeFileDurable(path, data, 0600); err != nil {
		w.logger.Warn("[PERSIST] Failed to write state file %s: %v", path, err)
		return err
	}
	w.logger.Debug("[PERSIST] State saved to %s (%d seen commands)", path, len(state.SeenCommands))
	return nil
}

// writeFileDurable persists data to path with atomic-write durability
// guarantees: write to <path>.tmp → fsync → close → rename onto path →
// best-effort fsync of the parent directory. A plain os.WriteFile+rename
// leaves both the bytes and the new directory entry unflushed, so a crash
// between "state saved" and the OS writeback loses the just-persisted state.
func writeFileDurable(path string, data []byte, perm os.FileMode) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	cleanup := func() { _ = os.Remove(tmpPath) }
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		cleanup()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		cleanup()
		return err
	}
	if err := f.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return err
	}
	// fsync the parent directory so the new name survives a crash.
	if dir, derr := os.Open(filepath.Dir(path)); derr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
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
