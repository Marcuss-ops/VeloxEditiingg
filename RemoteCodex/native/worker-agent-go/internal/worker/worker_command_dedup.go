// Package worker / worker_command_dedup.go
//
// Command dedup state for the worker: seenCommands key derivation,
// TTL + hard-cap enforcement, and periodic cleanup. Extracted from
// worker_init.go; processCommand (the actual dispatch) lives in
// worker_commands.go.
package worker

import (
	"fmt"
	"strings"
	"time"

	"velox-worker-agent/pkg/api"
)

const (
	seenCommandTTL        = 30 * time.Minute
	seenCommandMaxEntries = 10000 // Hard limit to prevent memory growth
)

func commandKey(cmd api.WorkerCommand) string {
	// Gap #4 fix: use CommandID as primary dedup key when available;
	// fall back to command+timestamp for backward compatibility.
	cid := strings.TrimSpace(cmd.CommandID)
	if cid != "" {
		return "id:" + cid
	}
	ts := strings.TrimSpace(cmd.Timestamp)
	if ts == "" {
		ts = "no-timestamp"
	}
	return fmt.Sprintf("%s|%s", strings.TrimSpace(cmd.Command), ts)
}

func (w *Worker) markCommandSeen(cmd api.WorkerCommand) bool {
	key := commandKey(cmd)
	now := time.Now().UTC()

	w.commandMu.Lock()
	defer w.commandMu.Unlock()

	// Opportunistic cleanup to keep the in-memory map bounded.
	for k, t := range w.seenCommands {
		if now.Sub(t) > seenCommandTTL {
			delete(w.seenCommands, k)
		}
	}

	// Enforce hard limit: evict oldest entries if we're at capacity
	if len(w.seenCommands) >= seenCommandMaxEntries {
		// Remove the oldest 10% of entries
		toRemove := seenCommandMaxEntries / 10
		// Since maps don't have order, just remove entries past the limit
		count := 0
		for k := range w.seenCommands {
			if count >= toRemove {
				break
			}
			delete(w.seenCommands, k)
			count++
		}
	}

	if firstSeenAt, ok := w.seenCommands[key]; ok && now.Sub(firstSeenAt) <= seenCommandTTL {
		return true
	}

	w.seenCommands[key] = now
	return false
}

// cleanupSeenCommands performs a full cleanup of expired command entries.
// Call this periodically (e.g., every 10 minutes) to bound map growth.
func (w *Worker) cleanupSeenCommands() {
	now := time.Now().UTC()

	w.commandMu.Lock()
	defer w.commandMu.Unlock()

	for k, t := range w.seenCommands {
		if now.Sub(t) > seenCommandTTL {
			delete(w.seenCommands, k)
		}
	}
}
