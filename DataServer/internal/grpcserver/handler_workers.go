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
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/store"
	"velox-server/internal/workers"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		if hb.GetExtra() != nil {
			extraMap := hb.GetExtra().AsMap()
			if caps, ok := extraMap["capabilities"].(map[string]interface{}); ok {
				sess.replaceAssetCacheKeys(extractAssetCacheKeys(caps))
				if mpj := maxParallelJobsFromCapabilities(caps); mpj > 0 {
					sess.maxParallelJobs.Store(int32(mpj))
					sess.setCapacityAuthoritative(true)
				} else {
					sess.maxParallelJobs.Store(0)
					sess.setCapacityAuthoritative(false)
				}
				diskFreeBytes := snapshotHostInt64(caps, "disk_free_bytes")
				_, diskPresent := snapshotHostValue(caps, "disk_free_bytes")
				sess.updatePlacementResources(diskFreeBytes, diskPresent && diskFreeBytes >= 0, sess.placementSnapshot(workerID).EstimatedAvailableMS, sess.placementSnapshot(workerID).NetworkMbps, sess.placementSnapshot(workerID).LoadRatio)
				registry, err := parseExecutorCapabilities(caps)
				if err != nil {
					// A malformed re-advertisement must not leave stale
					// executor claims active. Fail closed until the worker
					// sends a valid canonical report again.
					sess.replaceExecutorRegistry(controltransport.EmptyExecutorRegistry())
				} else {
					sess.replaceExecutorRegistry(registry)
				}
			}
		}
	}

	// Compute CapacityScorecard from resource metrics and set per-phase slots
	// on the session so the placement matcher can use them.
	if sess != nil {
		if scorecard := computeScorecardFromHeartbeat(workerID, extra); scorecard != nil {
			sess.setPerPhaseSlots(scorecard.RenderSlots, scorecard.PrefetchSlots, scorecard.PublisherSlots)
			// Push per-phase slot limits to the worker so it enforces
			// phase-specific admission instead of the flat MaxActiveJobs.
			h.sendPerPhaseSlotsUpdate(context.Background(), workerID, sess, scorecard)
			// Persist the scorecard to the store so it survives Master
			// restarts and is available during registry hydration.
			if h.dbStore != nil {
				row := store.ScorecardRow{
					WorkerID:             workerID,
					RenderSlots:          scorecard.RenderSlots,
					PrefetchSlots:        scorecard.PrefetchSlots,
					PublisherSlots:       scorecard.PublisherSlots,
					RAMSlots:             scorecard.RAMSlots,
					CPUSlots:             scorecard.CPUSlots,
					DiskSlots:            scorecard.DiskSlots,
					NetworkSlots:         scorecard.NetworkSlots,
					LimitingResource:     scorecard.LimitingResource,
					TotalRAMBytes:        scorecard.TotalRAMBytes,
					AvailableRAMBytes:    scorecard.AvailableRAMBytes,
					EffectiveCPUCores:    scorecard.EffectiveCPUCores,
					DiskReadMbps:         scorecard.DiskReadMbps,
					DiskWriteMbps:        scorecard.DiskWriteMbps,
					DownloadMbps:         scorecard.DownloadMbps,
					UploadMbps:           scorecard.UploadMbps,
					RAMPerJobBytes:       scorecard.RAMPerJobBytes,
					CPUCoresPerJob:       scorecard.CPUCoresPerJob,
					DiskMBpsPerJob:       scorecard.DiskMBpsPerJob,
					NetworkMbpsPerJob:    scorecard.NetworkMbpsPerJob,
					RenderWallMsPerJob:   scorecard.RenderWallMsPerJob,
					PrefetchWallMsPerJob: scorecard.PrefetchWallMsPerJob,
					PublishWallMsPerJob:  scorecard.PublishWallMsPerJob,
				}
				if err := h.dbStore.UpsertScorecard(context.Background(), row); err != nil {
					logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCHeartbeatFailed, "[SCORECARD] failed to persist scorecard for worker %s: %v", workerID, err)
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
			logGRPCf(ctxForTaskSession(sess), logging.LevelError, logging.CodeGRPCHeartbeatFailed, "[GRPC] Heartbeat failed for worker %s: %v", workerID, err)
			if activeSess := h.getSession(workerID); activeSess != nil && activeSess.sessionID == sessionID {
				select {
				case activeSess.writerErr <- fmt.Errorf("heartbeat persistence failed: %w", err):
				default:
				}
				activeSess.cancel()
			}
			return
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

	// Canonical typed telemetry snapshot: parse the heartbeat Extra block,
	// gate it (sequence / staleness / worker identity / schema) and — on
	// acceptance — surface the typed fields as the state source. A rejected
	// snapshot is logged and dropped; it never reaches the sink or the
	// session. The accepted snapshot is stored on the session for admin-API
	// / placement projections.
	acceptedTelemetry := ingestTelemetrySnapshot(workerID, sess, extra)

	// Feed the latest accepted operational facts into the warm-placement
	// snapshot. Availability is deliberately an estimate (active work plus
	// queued downloads at a conservative one-second unit); it is a ranking
	// signal only, while capacity and disk remain authoritative gates.
	if sess != nil {
		current := sess.placementSnapshot(workerID)
		estimatedAvailableMS := current.EstimatedAvailableMS
		loadRatio := current.LoadRatio
		diskFreeBytes := int64(current.FreeDiskBytes)
		diskAuthoritative := current.DiskAuthoritative
		if acceptedTelemetry != nil {
			sess.setActiveExecutionSlots(acceptedTelemetry.ActiveLeases)
			estimatedAvailableMS = int64(acceptedTelemetry.ActiveLeases+acceptedTelemetry.DownloadQueue) * 1000
			if acceptedTelemetry.ActiveLeases > 0 && current.MaxParallelJobs > 0 {
				loadRatio = float64(acceptedTelemetry.ActiveLeases) / float64(current.MaxParallelJobs)
			}
			if acceptedTelemetry.DiskFreeBytes >= 0 {
				diskFreeBytes = acceptedTelemetry.DiskFreeBytes
				diskAuthoritative = true
			}
		}
		if resources := hb.GetResources(); resources != nil {
			if acceptedTelemetry == nil && resources.GetActiveTasks() >= 0 {
				sess.setActiveExecutionSlots(int(resources.GetActiveTasks()))
			}
			if resources.GetTaskSlots() > 0 {
				resourceLoad := float64(resources.GetActiveTasks()) / float64(resources.GetTaskSlots())
				if resourceLoad > loadRatio {
					loadRatio = resourceLoad
				}
			}
			if resources.GetActiveTasks() > 0 {
				estimatedAvailableMS = int64(resources.GetActiveTasks()) * 1000
			}
			if resources.GetDiskFreeBytes() >= 0 {
				diskFreeBytes = resources.GetDiskFreeBytes()
				diskAuthoritative = true
			}
		}
		sess.updatePlacementResources(diskFreeBytes, diskAuthoritative, estimatedAvailableMS, current.NetworkMbps, loadRatio)
	}

	// F2: forward typed resource counters onto the Prometheus registry
	// via the sink interface. NIL-tolerant — handlers running WITHOUT a
	// metrics surface keep the registry.Heartbeat() side active and
	// silently skip the projection (legacy mode). The accepted typed
	// snapshot overlays the fields it carries (cache bytes) onto the
	// decoded proto counters.
	if h.resourceSink != nil {
		if snap := decodeWorkerResources(workerID, hb.GetResources()); snap != nil {
			applyTelemetryToResourceSnapshot(snap, acceptedTelemetry)
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
			logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCSessionInvalid, "[GRPC] Session %s for worker %s is invalid — tearing down (revoked=%v, err=%v)", sessionID, workerID, dbSess != nil && dbSess.Revoked, err)
			if activeSess := h.getSession(workerID); activeSess != nil && activeSess.sessionID == sessionID {
				select {
				case activeSess.writerErr <- fmt.Errorf("session revoked or expired"):
				default:
				}
				activeSess.cancel()
			}
			return
		}
		if err := h.dbStore.UpdateSessionLastSeen(sessionID); err != nil {
			logGRPCf(ctxForTaskSession(sess), logging.LevelError, logging.CodeGRPCHeartbeatFailed, "[GRPC] Session %s last_seen persistence failed for worker %s: %v", sessionID, workerID, err)
			if activeSess := h.getSession(workerID); activeSess != nil && activeSess.sessionID == sessionID {
				select {
				case activeSess.writerErr <- fmt.Errorf("session last_seen persistence failed: %w", err):
				default:
				}
				activeSess.cancel()
			}
			return
		}
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
			logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCCommandFailed, "[GRPC] Command ACK failed for %s (worker %s): %v", ca.GetCommandId(), workerID, err)
		}
	}
}

// computeScorecardFromHeartbeat extracts resource metrics from the heartbeat
// extra map and computes a CapacityScorecard. Returns nil when insufficient
// data is available (caller should keep previous scorecard).
func computeScorecardFromHeartbeat(workerID string, extra map[string]interface{}) *workers.CapacityScorecard {
	if extra == nil {
		return nil
	}

	// Extract resource metrics from the extra map. The heartbeat merges
	// ResourcesToExtra output into the extra map.
	input := workers.ScorecardInput{WorkerID: workerID}

	if v, ok := extra["total_ram_bytes"].(float64); ok {
		input.TotalRAMBytes = int64(v)
	}
	if v, ok := extra["memory_available_bytes"].(float64); ok {
		input.AvailableRAMBytes = int64(v)
	}
	if v, ok := extra["effective_cpu_cores"].(float64); ok {
		input.EffectiveCPUCores = int32(v)
	}
	if v, ok := extra["disk_read_mbps"].(float64); ok {
		input.DiskReadMbps = v
	}
	if v, ok := extra["disk_write_mbps"].(float64); ok {
		input.DiskWriteMbps = v
	}
	if v, ok := extra["download_mbps"].(float64); ok {
		input.DownloadMbps = v
	}
	if v, ok := extra["upload_mbps"].(float64); ok {
		input.UploadMbps = v
	}

	// Use default per-job cost estimates when no historical data is available.
	// TODO: feed real per-job costs from task_attempt_metrics when available.
	if input.RAMPerJobBytes == 0 {
		input.RAMPerJobBytes = 512 * 1024 * 1024 // 512 MB default
	}
	if input.CPUCoresPerJob == 0 {
		input.CPUCoresPerJob = 1.0
	}
	if input.DiskMBpsPerJob == 0 {
		input.DiskMBpsPerJob = 100.0 // 100 Mbit/s
	}
	if input.NetworkMbpsPerJob == 0 {
		input.NetworkMbpsPerJob = 50.0 // 50 Mbit/s
	}

	// Only compute when we have at least RAM data
	if input.AvailableRAMBytes <= 0 {
		return nil
	}

	sc := workers.ComputeCapacityScorecard(input)
	return &sc
}

// sendPerPhaseSlotsUpdate sends a ConfigurationUpdate with per-phase slot
// limits to the worker. This is called after the CapacityScorecard is computed
// so the worker enforces phase-specific admission instead of the flat limit.
func (h *Handler) sendPerPhaseSlotsUpdate(ctx context.Context, workerID string, sess *workerSession, sc *workers.CapacityScorecard) {
	if sc == nil || sess == nil {
		return
	}
	// Only send when per-phase slots are actually different from zero.
	if sc.RenderSlots <= 0 && sc.PrefetchSlots <= 0 && sc.PublisherSlots <= 0 {
		return
	}

	cfgMap := map[string]interface{}{
		"max_parallel_jobs": float64(sc.RenderSlots),
		"render_slots":      float64(sc.RenderSlots),
		"prefetch_slots":    float64(sc.PrefetchSlots),
		"publisher_slots":   float64(sc.PublisherSlots),
	}
	cfgStruct, err := structpb.NewStruct(cfgMap)
	if err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacement, "[CAPACITY] failed to encode per-phase slots for worker %s: %v", workerID, err)
		return
	}

	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("config-phase-slots-%s-%d", workerID, time.Now().UnixNano()),
		WorkerId:        workerID,
		SessionId:       sess.sessionID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_ConfigurationUpdate{
			ConfigurationUpdate: &pb.ConfigurationUpdate{
				Configuration: cfgStruct,
			},
		},
	}

	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacement, "[CAPACITY] sendCh full for per-phase slots to worker %s", workerID)
	}
}
