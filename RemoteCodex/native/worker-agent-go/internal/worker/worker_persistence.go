// Package worker — local persistence for worker state recovery.
// Uses a JSON file on disk (no SQLite dependency) to persist:
//   - seenCommands for command deduplication across restarts
//   - activeTasks metadata for job recovery after restart
//   - pendingTasks metadata for lease recovery after restart
package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// persistedState is the on-disk JSON structure for local recovery.
type persistedState struct {
	// SeenCommands maps command key → first-seen timestamp for dedup.
	SeenCommands map[string]time.Time `json:"seen_commands"`
	// ActiveJobs saves metadata for in-flight jobs.
	ActiveJobs map[string]persistedJobInfo `json:"active_jobs"`
	// PendingLeaseJobs saves metadata for jobs waiting for JobLeaseGranted.
	PendingLeaseJobs map[string]persistedJobInfo `json:"pending_lease_jobs"`
	// SavedAt is the last save timestamp.
	SavedAt time.Time `json:"saved_at"`
}

// persistedJobInfo is the minimal metadata for a job.
type persistedJobInfo struct {
	JobID     string `json:"job_id"`
	JobRunID  string `json:"job_run_id"`
	JobType   string `json:"job_type"`
	LeaseID   string `json:"lease_id"`
	StartedAt string `json:"started_at,omitempty"`
}

// stateFilePath builds the path to the local state file.
func stateFilePath(workDir string) string {
	return filepath.Join(workDir, "worker_state.json")
}

// saveLocalState persists the current in-memory state to a JSON file.
func (w *Worker) saveLocalState() error {
	workDir := w.config.WorkDir
	if workDir == "" {
		return nil
	}
	if err := os.MkdirAll(workDir, 0755); err != nil {
		w.logger.Warn("[PERSIST] Cannot create work directory %s: %v", workDir, err)
		return err
	}

	state := persistedState{
		SeenCommands:     make(map[string]time.Time),
		ActiveJobs:       make(map[string]persistedJobInfo),
		PendingLeaseJobs: make(map[string]persistedJobInfo),
		SavedAt:          time.Now().UTC(),
	}

	// Copy seen commands
	w.commandMu.Lock()
	for k, t := range w.seenCommands {
		if time.Since(t) <= seenCommandTTL {
			state.SeenCommands[k] = t
		}
	}
	w.commandMu.Unlock()

	// Copy active tasks metadata
	w.activeTasksMu.RLock()
	for _, at := range w.activeTasks {
		if at == nil || at.Job == nil {
			continue
		}
		state.ActiveJobs[at.JobID] = persistedJobInfo{
			JobID:     at.JobID,
			JobRunID:  at.Job.JobRunID,
			JobType:   at.Job.JobType,
			LeaseID:   at.LeaseID,
			StartedAt: at.StartedAt.UTC().Format(time.RFC3339),
		}
	}
	w.activeTasksMu.RUnlock()

	// PendingLeaseJobs left empty — the legacy map was dead code,
	// never populated. The serialized field stays for schema compat.

	path := stateFilePath(workDir)
	tmpPath := path + ".tmp"

	data, err := json.Marshal(state)
	if err != nil {
		w.logger.Warn("[PERSIST] Failed to marshal state: %v", err)
		return err
	}

	// Atomic write: tmp → fsync → rename prevents JSON corruption on crash.
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		w.logger.Warn("[PERSIST] Failed to write tmp file %s: %v", tmpPath, err)
		return err
	}
	if f, err := os.OpenFile(tmpPath, os.O_RDWR, 0600); err == nil {
		if syncErr := f.Sync(); syncErr != nil {
			w.logger.Warn("[PERSIST] Sync failed for tmp file %s: %v", tmpPath, syncErr)
		}
		f.Close()
	}
	if err := os.Rename(tmpPath, path); err != nil {
		w.logger.Warn("[PERSIST] Failed to rename tmp → state file %s: %v", path, err)
		return err
	}
	w.logger.Debug("[PERSIST] State saved to %s (%d seen commands, %d active jobs, %d pending leases)",
		path, len(state.SeenCommands), len(state.ActiveJobs), len(state.PendingLeaseJobs))
	return nil
}

// loadLocalState restores in-memory state from the JSON state file.
// Called once at worker startup (after New) to recover command dedup
// state and basic job metadata from the previous run.
func (w *Worker) loadLocalState() {
	workDir := w.config.WorkDir
	if workDir == "" {
		return
	}

	path := stateFilePath(workDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			w.logger.Warn("[PERSIST] Failed to read state file %s: %v", path, err)
		}
		return
	}

	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		w.logger.Warn("[PERSIST] Failed to unmarshal state file %s: %v", path, err)
		return
	}

	now := time.Now().UTC()

	// Restore seen commands (expired entries are dropped).
	restoredCmds := 0
	w.commandMu.Lock()
	for k, t := range state.SeenCommands {
		if now.Sub(t) <= seenCommandTTL {
			w.seenCommands[k] = t
			restoredCmds++
		}
	}
	w.commandMu.Unlock()

	w.logger.Info("[PERSIST] State loaded: %d seen commands restored, %d active jobs from previous run, %d pending leases",
		restoredCmds, len(state.ActiveJobs), len(state.PendingLeaseJobs))

	// Log previous active jobs for operator visibility — the worker
	// does NOT auto-resume them (leases expire, master re-dispatches).
	if len(state.ActiveJobs) > 0 {
		w.logger.Warn("[PERSIST] Previous run had %d active jobs — master will re-dispatch via lease expiry", len(state.ActiveJobs))
	}
	if len(state.PendingLeaseJobs) > 0 {
		w.logger.Warn("[PERSIST] Previous run had %d pending lease jobs — master will re-dispatch via lease expiry", len(state.PendingLeaseJobs))
	}
}

// startPersistenceLoop saves local state periodically.
func (w *Worker) startPersistenceLoop(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = w.saveLocalState() // Final save before exit
				return
			case <-w.stopChan:
				_ = w.saveLocalState() // Final save before exit
				return
			case <-ticker.C:
				_ = w.saveLocalState()
			}
		}
	}()
}
