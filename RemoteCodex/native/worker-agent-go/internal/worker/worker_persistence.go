// Package worker — local persistence for command deduplication across
// restarts. The RecoveryReport protocol that historically piggybacked on
// this struct (ActiveJobs/PendingLeaseJobs + heartbeat.extra snapshot) was
// removed in PR 1: master-side lease expiry + TaskLeaseReaper are the
// canonical recovery mechanism. The worker only persists seen command IDs
// so duplicate MasterToWorkerEnvelope deliveries after a restart are deduped.
package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// persistedState is the on-disk JSON structure for in-restart dedup.
//
// PR 1: ActiveJobs and PendingLeaseJobs fields have been removed. The
// canonical lease is held by the master (TaskLeaseReaper); on worker
// restart, in-flight tasks expire via lease_expiry and the master re-mints
// a fresh attempt. The worker keeps no copy.
type persistedState struct {
	// SeenCommands maps command key → first-seen timestamp for dedup.
	SeenCommands map[string]time.Time `json:"seen_commands"`
	// SavedAt is the last save timestamp.
	SavedAt time.Time `json:"saved_at"`
}

// stateFilePath builds the path to the local state file under the
// canonical runtime directory (cfg.StateDir, env VELOX_STATE_DIR).
// Worker state must NEVER live under WorkDir, which points at the
// release checkout (/opt/velox by default) and is mounted read-only
// inside the container at /app. The runtime dir is a host-backed
// volume mount so writes here survive container restarts.
func stateFilePath(stateDir string) string {
	return filepath.Join(stateDir, "worker_state.json")
}

// saveLocalState persists the current in-memory state to a JSON file.
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

	// Copy seen commands.
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
	w.logger.Debug("[PERSIST] State saved to %s (%d seen commands)", path, len(state.SeenCommands))
	return nil
}

// loadLocalState restores in-memory state from the JSON state file.
// Called once at worker startup (after New) to recover command-dedup state.
//
// Forward-compat: legacy files written by PR < 1 also contained
// "active_jobs" and "pending_lease_jobs" maps. Those fields are simply
// ignored on load — Go json.Unmarshal silently drops unknown JSON keys.
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

	w.logger.Info("[PERSIST] State loaded: %d seen commands restored", restoredCmds)
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
