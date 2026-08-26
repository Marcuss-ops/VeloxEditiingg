// Package api — workers endpoint conversion / sanitization / parsing.
//
// workers_mapper.go owns the high-level mapping from the registry read
// model (workers.Worker) to the operator-facing WorkerResponse. The
// individual responsibilities are split into focused files:
//
//   - workers_sanitise.go — hostname redaction (RW-PROD-005 §3 A6).
//   - workers_metrics.go  — typed WorkerMetrics parsing from the raw
//     JSON-decoded metrics blob.
//   - workers_executors.go — executor extraction from capabilities.
//   - workers_filters.go   — GET param parsing and in-memory filter applier.
//   - workers_mapper.go    — top-level sanitizeWorker orchestration.
package api

import (
	"time"

	workersreg "velox-server/internal/workers"
)

// sanitizeWorker converts a raw workers.Worker into the operator-facing
// WorkerResponse, stripping all sensitive fields.
//
// Connection status: trust the registry's `Worker.ConnectionStatus`
// (CONNECTED | STALE | DISCONNECTED | DRAINING) since it merges heartbeat
// freshness with the canonical `session_active` signal from `worker_sessions`.
// The canonical `workers.ConnectionStatus` always returns one of the four
// enum strings on every read path (registry_query.go guarantees this),
// so no legacy/heartbeat-only fallback is needed.
func sanitizeWorker(w workersreg.Worker) WorkerResponse {
	resp := WorkerResponse{
		WorkerID:            w.WorkerID.String(),
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
		HeartbeatAgeSeconds: workersreg.HeartbeatAgeSeconds(w.LastHB, time.Now().UTC()),
		CurrentTaskID:       w.CurrentJob,
		Executors:           extractExecutors(w.ExecutorRegistrySnapshot()),
	}

	// ReleaseIdentity certificate (registered at hello/heartbeat time).
	// Omitted when the worker has not advertised a certified release yet
	// so dashboards never infer evidence from sibling fields.
	if !w.ReleaseIdentity.IsEmpty() {
		resp.ReleaseIdentity = ReleaseIdentityResponse{
			ImageDigest:      w.ReleaseIdentity.ImageDigest,
			SourceCommit:     w.ReleaseIdentity.SourceCommit,
			SourceHash:       w.ReleaseIdentity.SourceHash,
			BundleHash:       w.ReleaseIdentity.BundleHash,
			EngineSHA256:     w.ReleaseIdentity.EngineSHA256,
			SoftwareVersion:  w.ReleaseIdentity.SoftwareVersion,
			ProtocolVersion:  w.ReleaseIdentity.ProtocolVersion,
			CapabilitySchema: w.ReleaseIdentity.CapabilitySchema,
		}
	}

	// Resource counters: extracted from the typed metrics map produced
	// by the gRPC heartbeat handler (registry_heartbeat.go stores the
	// proto WorkerResourceCounters fields under the "metrics" key).
	metrics := ParseWorkerMetrics(w.Metrics)
	resp.ActiveTasks = int32(w.Capacity.ActiveSlots)
	resp.TaskSlots = int32(w.Capacity.MaxSlots)
	resp.ActiveSlots = int32(w.Capacity.ActiveSlots)
	resp.MaxSlots = int32(w.Capacity.MaxSlots)
	resp.AvailableSlots = int32(w.Capacity.AvailableSlots)
	resp.CPUUtilizationRatio = metrics.CPUUtilizationRatio
	resp.MemoryUsedBytes = metrics.MemoryUsedBytes
	resp.DiskFreeBytes = metrics.DiskFreeBytes
	resp.JobsCompleted = metrics.JobsCompleted
	resp.JobsFailed = metrics.JobsFailed
	if len(metrics.ActiveJobs) > 0 {
		resp.ActiveTaskRuntime = make([]ActiveTaskRuntime, len(metrics.ActiveJobs))
		for i, job := range metrics.ActiveJobs {
			resp.ActiveTaskRuntime[i] = ActiveTaskRuntime{
				JobID:            job.JobID,
				TaskID:           job.TaskID,
				AttemptID:        job.AttemptID,
				Executor:         job.Executor,
				Stage:            job.Stage,
				Percent:          job.Percent,
				Scene:            job.Scene,
				TotalScenes:      job.TotalScenes,
				LeaseID:          job.LeaseID,
				StartedAt:        job.StartedAt,
				OperationalPhase: job.OperationalPhase,
				Segment:          job.Segment,
				TotalSegments:    job.TotalSegments,
				FramesDecoded:    job.FramesDecoded,
				FramesComposited: job.FramesComposited,
				FramesEncoded:    job.FramesEncoded,
				SpeedX:           job.SpeedX,
				ElapsedMS:        job.ElapsedMS,
				LastProgressAt:   job.LastProgressAt,
			}
		}
	}

	return resp
}
