// Package grpcserver / handler_workers.go
//
// Worker status lifecycle: heartbeat processing, readiness/draining
// decoding, and command ACK handling. Extracted from the original
// handler_workers.go (split per responsabilità, 2026-08):
//
//	handler_workers.go             — heartbeat + readiness + command ack
//	handler_placement.go           — push-mode placement pipeline (TaskOffer)
//	handler_payload_projection.go  — canonical vs legacy payload projection
//	handler_workers_metrics.go     — resource counters → Prometheus sink
//	session_capabilities.go        — capability parsing (extractSupportedJobTypes)
package grpcserver

import (
	"context"
	"fmt"
	"log"

	pb "velox-shared/controltransport/pb"
)

// handleHeartbeat processes a typed Heartbeat received via gRPC stream.
// Issue 7 fix: accepts sessionID and updates last_seen in worker_sessions table.
//
// Scorecard v1 / F2: also decodes heartbeat.resources into a typed
// ResourceSnapshot and forwards it to the WorkerResourceSink (promoted
// to Prometheus gauge/counter families via metrics.Collector). The
// cumulative→delta conversion lives in handler_workers_metrics.go so
// the handler stays purely structural.
func (h *Handler) handleHeartbeat(workerID, sessionID string, hb *pb.Heartbeat) {
	extra := make(map[string]interface{})
	// Populate extra from typed fields for backward compat with registry.
	// The free-form agent status string (Status/WorkerStatus) is NOT
	// propagated: worker state is derived master-side (worker_state.go).
	extra["worker_name"] = hb.GetWorkerName()
	extra["current_job"] = hb.GetCurrentJob()
	extra["code_version"] = hb.GetCodeVersion()
	extra["bundle_version"] = hb.GetBundleVersion()
	extra["bundle_hash"] = hb.GetBundleHash()
	extra["protocol_version"] = hb.GetProtocolVersion()
	extra["engine_version"] = hb.GetEngineVersion()
	extra["jobs_completed"] = hb.GetJobsCompleted()
	extra["jobs_failed"] = hb.GetJobsFailed()
	extra["active_jobs_count"] = hb.GetActiveJobsCount()
	// Keep the typed count aligned with the structured SQLite projection.
	// PersistWorkerHeartbeat consumes active_task_count for workers and
	// worker_metric_samples; without this bridge a busy worker was stored as
	// idle even though the heartbeat carried ActiveJobsCount correctly.
	extra["active_task_count"] = hb.GetActiveJobsCount()

	if hb.GetExtra() != nil {
		for k, v := range hb.GetExtra().AsMap() {
			extra[k] = v
		}
	}

	// F2: merge the typed resource counters into `extra` so the
	// persistent worker_registry row surfaces the same Prometheus-side
	// fields via the legacy HTTP /admin/workers path (channelised
	// worker debugging tools depend on this JSON view).
	if resExtra := ResourcesToExtra(hb.GetResources()); resExtra != nil {
		extra["resource_sample_present"] = true
		for k, v := range resExtra {
			extra[k] = v
		}
	}

	// Update capacity tracking on the session (for max_parallel_jobs check).
	sess := h.getSession(workerID)
	if sess != nil {
		sess.activeJobsCount.Store(int32(hb.GetActiveJobsCount()))
		if hb.GetExtra() != nil {
			extraMap := hb.GetExtra().AsMap()
			if caps, ok := extraMap["capabilities"].(map[string]interface{}); ok {
				sess.replaceAssetCacheKeys(extractAssetCacheKeys(caps))
			}
			if mpj, ok := extraMap["max_parallel_jobs"]; ok {
				switch v := mpj.(type) {
				case float64:
					sess.maxParallelJobs.Store(int32(v))
				case int64:
					sess.maxParallelJobs.Store(int32(v))
				}
			}
		}
	}

	// F2: defensive nil-check on h.registry so handler-level unit tests
	// can wire a Handler without standing up the persistent worker_registry
	// (preserves the existing pre-F2 contract that handleHeartbeat is
	// safe with no registry; production code always supplies one).
	if h.registry != nil {
		if err := h.registry.HeartbeatWithSession(context.Background(), sessionID, workerID, hb.GetWorkerName(), hb.GetCurrentJob(), extra); err != nil {
			log.Printf("[GRPC] Heartbeat failed for worker %s: %v", workerID, err)
		}
	}

	// Version correlation (Step 4 / Velox Metrics Center): store the
	// worker's software versions on the session so handleTaskResult can
	// stamp them on task_attempts at report time.
	//
	// git_sha → hb.GetCodeVersion() (the deployed commit hash)
	// engine_version → hb.GetEngineVersion()
	// worker_version / ffmpeg_version → hb.GetExtra() map (if present)
	if sess != nil {
		if v := hb.GetCodeVersion(); v != "" {
			sess.gitSHA.Store(v)
		}
		if v := hb.GetEngineVersion(); v != "" {
			sess.engineVersion.Store(v)
		}
		// worker_version and ffmpeg_version come from the extra map
		// (proto v3 does not carry top-level fields for these).
		if hb.GetExtra() != nil {
			extraMap := hb.GetExtra().AsMap()
			if v, ok := extraMap["worker_version"].(string); ok && v != "" {
				sess.workerVersion.Store(v)
			}
			if v, ok := extraMap["ffmpeg_version"].(string); ok && v != "" {
				sess.ffmpegVersion.Store(v)
			}
		}
	}

	// F2: forward typed resource counters onto the Prometheus registry
	// via the sink interface. NIL-tolerant — handlers running WITHOUT a
	// metrics surface keep the registry.Heartbeat() side active and
	// silently skip the projection (legacy mode).
	if h.resourceSink != nil {
		if snap := decodeWorkerResources(workerID, hb.GetResources()); snap != nil {
			h.resourceSink.RecordWorker(workerID, snap)
		}
	}

	if sess != nil && hb.GetExtra() != nil {
		if readiness, ok := hb.GetExtra().AsMap()["readiness"].(map[string]interface{}); ok {
			sess.ready.Store(readinessStatusOK(readiness))
			if detail, ok := readiness["detail"].(map[string]interface{}); ok {
				if draining, ok := detail["drain_mode"].(bool); ok {
					sess.draining.Store(draining)
				}
			}
		}
	}

	// Issue 7 fix / Phase 4.2 hardening: if the persisted session is gone
	// or revoked or expired, we MUST tear the active session down.
	if h.dbStore != nil && sessionID != "" {
		if dbSess, err := h.dbStore.ValidateSessionByID(sessionID); err != nil || dbSess == nil || dbSess.Revoked {
			log.Printf("[GRPC] Session %s for worker %s is invalid — tearing down (revoked=%v, err=%v)",
				sessionID, workerID, dbSess != nil && dbSess.Revoked, err)
			if activeSess := h.getSession(workerID); activeSess != nil && activeSess.sessionID == sessionID {
				select {
				case activeSess.writerErr <- fmt.Errorf("session revoked or expired"):
				default:
				}
				activeSess.cancel()
			}
			return
		}
		_ = h.dbStore.UpdateSessionLastSeen(sessionID)
	}
}

func readinessStatusOK(readiness map[string]interface{}) bool {
	status, ok := readiness["status"].(string)
	return ok && status == "ok"
}

// handleCommandAck processes typed CommandAck via gRPC stream.
// Only accepts ACK by command_id — the legacy type-based fallback is removed.
func (h *Handler) handleCommandAck(workerID string, ca *pb.CommandAck) {
	if ca.GetCommandId() != "" {
		if err := h.cmdMgr.AckCommandByID(workerID, ca.GetCommandId()); err != nil {
			log.Printf("[GRPC] Command ACK failed for %s (worker %s): %v", ca.GetCommandId(), workerID, err)
		}
	}
}
