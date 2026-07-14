// Package grpcserver / handler_session.go
//
// workerSession helpers for placement, capabilities and executor tracking.
// Extracted from handler.go to keep the core types file focused.
package grpcserver

import (
	"time"

	"velox-server/internal/placement"
)

// placementSnapshot builds an immutable WorkerSnapshot from the in-memory
// session state. The snapshot is consistent at a single instant (executors
// and capabilities read under their respective RLock). The caller must
// NOT hold any session mutex when calling this method.
func (s *workerSession) placementSnapshot(workerID string) placement.WorkerSnapshot {
	s.executorsMu.RLock()
	executors := make(map[placement.ExecutorKey]struct{}, len(s.executors))
	for key := range s.executors {
		executors[key] = struct{}{}
	}
	s.executorsMu.RUnlock()

	s.capabilitiesMu.RLock()
	caps := make(map[string]bool, len(s.capabilities))
	for key, enabled := range s.capabilities {
		caps[key] = enabled
	}
	s.capabilitiesMu.RUnlock()

	return placement.WorkerSnapshot{
		WorkerID:           workerID,
		SessionID:          s.sessionID,
		Ready:              s.ready.Load(),
		Draining:           s.draining.Load(),
		SessionAlive:       true,
		MaxParallelJobs:    int(s.maxParallelJobs.Load()),
		ActiveJobs:         int(s.activeJobsCount.Load()),
		Executors:          executors,
		Capabilities:       caps,
		CapabilityRevision: s.capabilityRevision.Load(),
		LastHeartbeat: time.Unix(
			s.lastHeartbeatUnix.Load(),
			0,
		).UTC(),
	}
}

// replaceCapabilities atomically replaces the session's executor and
// capability maps with the parsed values from the Hello handshake.
// It bumps the capability revision so any pending claim that was
// built from a stale snapshot can be detected by the fencing check.
func (s *workerSession) replaceCapabilities(
	executors map[placement.ExecutorKey]struct{},
	capabilities map[string]bool,
) {
	s.executorsMu.Lock()
	s.executors = executors
	s.executorsMu.Unlock()

	s.capabilitiesMu.Lock()
	s.capabilities = capabilities
	s.capabilitiesMu.Unlock()

	s.capabilityRevision.Add(1)
}

func maxParallelJobsFromCapabilities(capsMap map[string]interface{}) int {
	if capsMap == nil {
		return 0
	}
	if mpj, ok := capsMap["max_parallel_jobs"]; ok {
		switch v := mpj.(type) {
		case float64:
			return int(v)
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		}
	}
	if host, ok := capsMap["host"].(map[string]interface{}); ok {
		if mpj, ok := host["max_parallel_jobs"]; ok {
			switch v := mpj.(type) {
			case float64:
				return int(v)
			case int:
				return v
			case int32:
				return int(v)
			case int64:
				return int(v)
			}
		}
	}
	return 0
}

// invalidateExecutor removes a single executor key from the session's
// executor map and bumps the capability revision. Called when the
// worker rejects a task with reason="unsupported_executor" — the
// placement snapshot said the worker supports this executor, but the
// worker disagrees. Invalidating prevents further offers of the same
// incompatible executor until the next Hello re-advertises it.
func (s *workerSession) invalidateExecutor(key placement.ExecutorKey) {
	s.executorsMu.Lock()
	delete(s.executors, key)
	s.executorsMu.Unlock()

	s.capabilityRevision.Add(1)
}
