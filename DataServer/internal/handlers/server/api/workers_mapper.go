// Package api — workers endpoint conversion / sanitization / parsing.
//
// workers_mapper.go owns the high-level mapping from the registry read
// model (workers.WorkerInfo) to the operator-facing WorkerResponse. The
// individual responsibilities are split into focused files:
//
//   - workers_sanitise.go — hostname redaction (RW-PROD-005 §3 A6).
//   - workers_metrics.go  — typed WorkerMetrics parsing from the raw
//     JSON-decoded metrics blob.
//   - workers_executors.go — executor extraction from capabilities.
//   - workers_filters.go   — GET param parsing and in-memory filter applier.
//   - workers_mapper.go    — top-level sanitizeWorker / heartbeatAgeSeconds /
//     canonicalReason orchestration.
package api

import (
	"time"

	workersreg "velox-server/internal/workers"
)

// canonicalReason maps the canonical state-derivation output to the
// 3-element Reason taxonomy. Pure function — no I/O. Callers supply
// the freshly-hydrated (sessionActive, drain, lastHB, now) values so
// the mapping is testable without DB plumbing.
//
// Precedence (spec §2.2):
//  1. drain=true                                         → "drain"
//  2. session_active == false                           → "detached_session"
//  3. lastHB empty/unparseable OR
//     session_active AND (now - lastHB) >= ConnectionStaleThreshold
//     → "heartbeat_stale"
//  4. fresh (session_active AND now - lastHB < 150s)      → ""
//
// Note on the third rule: spec text says "lastHB stale|empty" maps
// to heartbeat_stale. detached_session (rule 2) wins over rule 3
// because a closed stream also implies the heartbeat will stop;
// emitting heartbeat_stale would mislead operators who care about the
// auth-side root cause.
func canonicalReason(sessionActive bool, drain bool, lastHB string, now time.Time) string {
	if drain {
		return ReasonDrain
	}
	if !sessionActive {
		return ReasonDetachedSession
	}
	if lastHB == "" {
		return ReasonHeartbeatStale
	}
	t, err := time.Parse(time.RFC3339, lastHB)
	if err != nil {
		return ReasonHeartbeatStale
	}
	if now.Sub(t.UTC()) >= ConnectionStaleThreshold {
		return ReasonHeartbeatStale
	}
	return ""
}

// heartbeatAgeSeconds returns the number of seconds since last heartbeat,
// or 0 if the timestamp is unparseable.
func heartbeatAgeSeconds(lastHB string) int64 {
	if lastHB == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, lastHB)
	if err != nil {
		return 0
	}
	age := time.Since(t).Seconds()
	if age < 0 {
		return 0
	}
	return int64(age)
}

// sanitizeWorker converts a raw workers.WorkerInfo into the operator-facing
// WorkerResponse, stripping all sensitive fields.
//
// Connection status: trust the registry's `WorkerInfo.ConnectionStatus`
// (CONNECTED | STALE | DISCONNECTED | DRAINING) since it merges heartbeat
// freshness with the canonical `session_active` signal from `worker_sessions`.
// The canonical `workers.ConnectionStatus` always returns one of the four
// enum strings on every read path (registry_query.go guarantees this),
// so no legacy/heartbeat-only fallback is needed.
func sanitizeWorker(w workersreg.WorkerInfo) WorkerResponse {
	resp := WorkerResponse{
		WorkerID:            w.WorkerID,
		WorkerName:          w.WorkerName,
		SessionActive:       w.SessionActive,
		Status:              w.ConnectionStatus,
		Reason:              w.Reason,
		Hostname:            w.Host,
		WorkerClass:         w.Class,
		RolloutGroup:        w.RolloutGroup,
		ProtocolVersion:     w.ProtocolVersion,
		EngineVersion:       w.EngineVersion,
		BundleVersion:       w.BundleVersion,
		ConnectedAt:         w.FirstSeen,
		LastHeartbeatAt:     w.LastHB,
		HeartbeatAgeSeconds: heartbeatAgeSeconds(w.LastHB),
		CurrentTaskID:       w.CurrentJob,
		Executors:           extractExecutors(w.Capabilities),
	}

	// Resource counters: extracted from the typed metrics map produced
	// by the gRPC heartbeat handler (registry_heartbeat.go stores the
	// proto WorkerResourceCounters fields under the "metrics" key).
	metrics := ParseWorkerMetrics(w.Metrics)
	resp.ActiveTasks = metrics.ActiveTasks
	resp.TaskSlots = metrics.TaskSlots
	resp.CPUUtilizationRatio = metrics.CPUUtilizationRatio
	resp.MemoryUsedBytes = metrics.MemoryUsedBytes
	resp.DiskFreeBytes = metrics.DiskFreeBytes
	resp.JobsCompleted = metrics.JobsCompleted
	resp.JobsFailed = metrics.JobsFailed
	if len(metrics.ActiveJobs) > 0 {
		resp.ActiveTaskRuntime = make([]ActiveTaskRuntime, len(metrics.ActiveJobs))
		for i, job := range metrics.ActiveJobs {
			resp.ActiveTaskRuntime[i] = ActiveTaskRuntime{
				JobID:       job.JobID,
				TaskID:      job.TaskID,
				AttemptID:   job.AttemptID,
				Executor:    job.Executor,
				Stage:       job.Stage,
				Percent:     job.Percent,
				Scene:       job.Scene,
				TotalScenes: job.TotalScenes,
				LeaseID:     job.LeaseID,
				StartedAt:   job.StartedAt,
			}
		}
	}

	return resp
}
