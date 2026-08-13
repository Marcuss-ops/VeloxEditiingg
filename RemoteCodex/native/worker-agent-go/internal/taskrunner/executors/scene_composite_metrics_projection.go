package executors

import (
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/performance"
	"velox-worker-agent/pkg/video/pipeline"
)

// scene_composite_metrics_projection.go owns the pure projections that turn
// a pipeline.RunMetrics value into the executor's legacy dotted metric map,
// the segment/phase timing streams and the engine process telemetry events.
// SceneComposite.Execute only orchestrates: each of these helpers is a
// deterministic map of a single source of truth and never re-derives a
// ratio that performance already computes.

// projectRunMetrics materializes the pipeline/native/process/io/cpu/engine
// counters onto the legacy dotted metric map. The I/O and CPU derivations
// (performance.DeriveIO / DeriveCPU) are returned so the caller can share
// them with the raw metrics projection instead of recomputing them.
func projectRunMetrics(metrics *legacyMetricsProjection, pipelineID string, pipelineStart time.Time, run pipeline.RunMetrics, clipCount int) (performance.IOMetrics, performance.CPUMetrics) {
	metrics.Set("pipeline.total_ms", time.Since(pipelineStart).Milliseconds())
	metrics.Set("pipeline.id", pipelineID)
	metrics.Set("pipeline.resolve_ms", run.ResolveMs)
	metrics.Set("pipeline.validate_ms", run.ValidateMs)
	metrics.Set("pipeline.compile_ms", run.CompileMs)
	metrics.Set("pipeline.render_ms", run.RenderMs)
	metrics.Set("pipeline.timeline_items", int64(run.TimelineItems))
	metrics.Set("pipeline.audio_tracks", int64(run.AudioTracks))
	metrics.Set("native.total_ms", run.RenderMetrics.TotalMs)
	metrics.Set("native.plan_write_ms", run.RenderMetrics.PlanWriteMs)
	metrics.Set("native.process_wait_ms", run.RenderMetrics.ProcessWaitMs)
	metrics.Set("process.engine_spawn_count", run.RenderMetrics.EngineSpawnCount)
	metrics.Set("process.engine_spawn_ms", run.RenderMetrics.EngineSpawnMs)
	metrics.Set("process.external_count", run.RenderMetrics.ExternalProcessCount)
	metrics.Set("process.ffmpeg_exec_count", run.RenderMetrics.FfmpegExecCount)
	metrics.Set("process.ffprobe_exec_count", run.RenderMetrics.FfprobeExecCount)
	metrics.Set("process.shell_exec_count", run.RenderMetrics.ShellExecCount)
	metrics.Set("process.curl_exec_count", run.RenderMetrics.CurlExecCount)
	metrics.Set("process.child_wait_ms", run.RenderMetrics.ChildWaitMs)
	// I/O counters share ONE derivation with the PerformanceReceiptV1
	// (performance.DeriveIO): the executor telemetry and the receipt can
	// never disagree about what each io.* value means.
	derivedIO := performance.DeriveIO(run.RenderMetrics)
	metrics.Set("io.total_bytes_read", derivedIO.TotalBytesRead)
	metrics.Set("io.total_bytes_written", derivedIO.TotalBytesWritten)
	metrics.Set("io.asset_bytes_read", derivedIO.AssetBytesRead)
	metrics.Set("io.asset_bytes_copied", derivedIO.AssetBytesCopied)
	metrics.Set("io.temp_bytes_written", derivedIO.TempBytesWritten)
	metrics.Set("io.mux_bytes_read", derivedIO.MuxBytesRead)
	metrics.Set("io.mux_bytes_written", derivedIO.MuxBytesWritten)
	metrics.Set("io.final_bytes_written", derivedIO.FinalBytesWritten)
	metrics.Set("io.file_copy_count", derivedIO.FileCopyCount)
	metrics.Set("io.file_copy_bytes", derivedIO.FileCopyBytes)
	metrics.Set("io.input_open_count", derivedIO.InputOpenCount)
	metrics.Set("io.input_reopen_count", derivedIO.InputReopenCount)
	// CPU counters share ONE derivation with the PerformanceReceiptV1
	// (performance.DeriveCPU): cpu.wall_ms mirrors the pipeline wall
	// clock, cpu.total_ms = user + system, and cpu.wall_ratio tells us
	// whether the attempt was CPU-bound at all. The RSS counters come
	// straight from the /proc sampler (memory.* is not a derivation).
	derivedCPU := performance.DeriveCPU(run.RenderMetrics, run.TotalMs)
	metrics.Set("cpu.wall_ms", derivedCPU.WallMs)
	metrics.Set("cpu.user_ms", derivedCPU.CPUUserMs)
	metrics.Set("cpu.system_ms", derivedCPU.CPUSystemMs)
	metrics.Set("cpu.total_ms", derivedCPU.CPUTotalMs)
	metrics.Set("cpu.wall_ratio", derivedCPU.CPUWallRatio)
	metrics.Set("memory.peak_rss_bytes", run.RenderMetrics.PeakRSSBytes)
	metrics.Set("memory.current_rss_bytes", run.RenderMetrics.CurrentRSSBytes)
	// Derived KPIs share ONE derivation with the PerformanceReceiptV1
	// (performance.DerivedFromRenderMetrics → Derive): unaccounted_ms,
	// accounted_ratio, read/write amplification, processes_per_clip,
	// useful_work_ratio and cpu_wall_ratio are computed by the single
	// Deriver from the same raw facts the receipt uses — the executor
	// never recomputes a ratio. Amplifications start from the
	// engine-declared final size (receipt io.final_bytes_written) and
	// are re-projected with the verified artifact-manifest size on the
	// success path.
	// clip_count is owned by the compiled render plan. A legacy V1 payload
	// that has no compiled plan is explicitly "not measured"; timeline item
	// count is not a valid substitute because it can include non-clip work.
	derived := performance.DerivedFromRenderMetrics(run.RenderMetrics, run.TotalMs, clipCount, run.RenderMetrics.TotalSize)
	metrics.Set("derived.unaccounted_ms", derived.UnaccountedMS)
	metrics.Set("derived.accounted_ratio", derived.AccountedRatio)
	metrics.Set("derived.read_amplification", derived.ReadAmplification)
	metrics.Set("derived.write_amplification", derived.WriteAmplification)
	metrics.Set("derived.processes_per_clip", derived.ProcessesPerClip)
	metrics.Set("derived.useful_work_ratio", derived.UsefulWorkRatio)
	metrics.Set("derived.cpu_wall_ratio", derived.CPUWallRatio)
	// The Phase-1 accounted_ratio budget (>= 95%) is surfaced as a
	// single boolean so operators can alert on it without recomputing
	// the rule; "not measured" (ratio 0) is never a violation.
	metrics.Set("derived.accounted_ratio_budget_ok", len(performance.CheckDerivedBudgets(derived)) == 0)
	metrics.Set("engine.frames", run.RenderMetrics.Frames)
	metrics.Set("engine.frames_decoded", run.RenderMetrics.FramesDecoded)
	metrics.Set("engine.frames_composited", run.RenderMetrics.FramesComposited)
	metrics.Set("engine.fps", run.RenderMetrics.Fps)
	metrics.Set("engine.speed_x", run.RenderMetrics.SpeedX)
	metrics.Set("engine.encode_passes", run.RenderMetrics.EncodePasses)
	metrics.Set("engine.temp_bytes", run.RenderMetrics.TempBytes)
	metrics.Set("engine.output_durable", run.RenderMetrics.OutputDurable)
	metrics.Set("engine.duration_seconds", run.RenderMetrics.DurationSec)
	metrics.Set("engine.concat_mode", run.RenderMetrics.ConcatMode)
	metrics.Set("engine.bitrate", run.RenderMetrics.Bitrate)
	metrics.Set("engine.dup_frames", run.RenderMetrics.DupFrames)
	metrics.Set("engine.drop_frames", run.RenderMetrics.DropFrames)
	for k, v := range run.RenderMetrics.PhaseMS {
		metrics.Set("engine."+k, v)
	}
	for key, value := range run.RenderMetrics.Observability {
		flattenObservabilityMetric(metrics, key, value)
	}
	return derivedIO, derivedCPU
}

// emitEngineProcessTelemetry records the canonical engine subprocess facts:
// the PROCESS_STARTED spawn event (when the engine launched, failed renders
// included) and the engine-declared process counters projection.
func emitEngineProcessTelemetry(rec *telemetry.EventRecorder, run pipeline.RunMetrics) {
	// The ProcessRunner records the canonical spawn fact (PROCESS_STARTED)
	// when the engine subprocess actually launched — failed and cancelled
	// renders included, because the spawn cost is part of the attempt.
	// EngineSpawnCount is the explicit observed fact (cmd.Start() success),
	// never a derivation from ProcessStartMs; the worker.engine.spawn event
	// is owned by process_runner in the telemetry catalog.
	if rec != nil && run.RenderMetrics.EngineSpawnCount > 0 {
		rec.Emit(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeAttempt, Component: "worker.engine", Action: "spawn"}, telemetry.StatusOK, "", "")
	}

	// The engine-declared process facts (C++ sidecar process_counters
	// block: the engine's own external spawn ledger, its getrusage CPU,
	// context switches and page faults) ride one canonical
	// worker.engine.usage event, projected into the receipt's process /
	// cpu / scheduling sections by the ProcessCollector. Skipped when
	// the engine predates the block (all-zero facts — the single
	// all-zero check lives in EngineUsageMetadataJSON, so adding a
	// counter later cannot drift this gate).
	usage := telemetry.EngineUsageFacts{
		ExternalSpawnCount:         run.RenderMetrics.EngineExternalSpawnCount,
		FfmpegSpawnCount:           run.RenderMetrics.EngineFfmpegSpawnCount,
		FfprobeSpawnCount:          run.RenderMetrics.EngineFfprobeSpawnCount,
		ShellSpawnCount:            run.RenderMetrics.EngineShellSpawnCount,
		CurlSpawnCount:             run.RenderMetrics.EngineCurlSpawnCount,
		CPUUserMs:                  run.RenderMetrics.EngineCPUUserMs,
		CPUSystemMs:                run.RenderMetrics.EngineCPUSystemMs,
		VoluntaryContextSwitches:   run.RenderMetrics.EngineVoluntaryContextSwitches,
		InvoluntaryContextSwitches: run.RenderMetrics.EngineInvoluntaryContextSwitches,
		MinorPageFaults:            run.RenderMetrics.EngineMinorPageFaults,
		MajorPageFaults:            run.RenderMetrics.EngineMajorPageFaults,
	}
	if rec != nil && telemetry.EngineUsageMetadataJSON(usage) != "" {
		rec.Emit(telemetry.EventSpec{
			Origin:       telemetry.OriginWorker,
			Scope:        telemetry.ScopeAttempt,
			Component:    "worker.engine",
			Action:       "usage",
			MetadataJSON: telemetry.EngineUsageMetadataJSON(usage),
		}, telemetry.StatusOK, "", "")
	}
}

// projectSegments maps the engine's per-segment counters onto the executor
// segment timing stream. Capacity is preallocated from the source slice.
func projectSegments(rm pipeline.RenderMetrics) []executor.SegmentTiming {
	segments := make([]executor.SegmentTiming, 0, len(rm.Segments))
	for _, seg := range rm.Segments {
		segments = append(segments, executor.SegmentTiming{
			SegmentIndex:     seg.SegmentIndex,
			SceneWorkerIndex: seg.SceneWorkerIndex,
			SceneID:          seg.SceneID,
			SourceType:       seg.SourceType,
			DurationMS:       seg.DurationMS,
			AssetDownloadMS:  seg.AssetDownloadMS,
			FfmpegEncodeMS:   seg.FfmpegEncodeMS,
			SourceBytes:      seg.SourceBytes,
			OutputBytes:      seg.OutputBytes,
			FramesEncoded:    seg.FramesEncoded,
			FramesDecoded:    seg.FramesDecoded,
			FramesComposited: seg.FramesComposited,
			FfmpegSpeedX:     seg.FfmpegSpeedX,
			Codec:            seg.Codec,
			Preset:           seg.Preset,
			FfmpegThreads:    seg.FfmpegThreads,
			Status:           seg.Status,
			ErrorCode:        seg.ErrorCode,
			ErrorMessage:     seg.ErrorMessage,
			SourceURLHash:    seg.SourceURLHash,
			CacheKey:         seg.CacheKey,
			InputDurationMS:  seg.InputDurationMS,
			OutputDurationMS: seg.OutputDurationMS,
			MetadataJSON:     seg.MetadataJSON,
			StartedOffsetMS:  seg.StartedOffsetMS,
			FinishedOffsetMS: seg.FinishedOffsetMS,
			WorkerSlot:       seg.WorkerSlot,
			CPUThreads:       seg.CPUThreads,
			ParallelGroup:    seg.ParallelGroup,
		})
	}
	return segments
}

// projectDetailedPhases maps the engine's detailed phase ledger onto the
// executor phase timing stream and appends the observability category
// rollups. Capacity is preallocated from the source slice.
func projectDetailedPhases(rm pipeline.RenderMetrics) []executor.DetailedPhaseTiming {
	detailedPhases := make([]executor.DetailedPhaseTiming, 0, len(rm.DetailedPhases))
	for _, phase := range rm.DetailedPhases {
		detailedPhases = append(detailedPhases, executor.DetailedPhaseTiming{
			Origin:           phase.Origin,
			Scope:            phase.Scope,
			Component:        phase.Component,
			Action:           phase.Action,
			Phase:            phase.Phase,
			EventType:        phase.EventType,
			EventName:        phase.EventName,
			EventIndex:       phase.EventIndex,
			StartedAt:        phase.StartedAt,
			CompletedAt:      phase.CompletedAt,
			DurationMS:       phase.DurationMS,
			Status:           phase.Status,
			ErrorCode:        phase.ErrorCode,
			ErrorMessage:     phase.ErrorMessage,
			BytesIn:          phase.BytesIn,
			BytesOut:         phase.BytesOut,
			Frames:           phase.Frames,
			MetadataJSON:     phase.MetadataJSON,
			SegmentIndex:     phase.SegmentIndex,
			TrackKind:        phase.TrackKind,
			TrackIndex:       phase.TrackIndex,
			StartedOffsetMS:  phase.StartedOffsetMS,
			FinishedOffsetMS: phase.FinishedOffsetMS,
			CPUMS:            phase.CPUMS,
			QueueWaitMS:      phase.QueueWaitMS,
			FramesIn:         phase.FramesIn,
			FramesOut:        phase.FramesOut,
		})
	}
	appendObservabilitySummaryPhases(&detailedPhases, rm.Observability)
	return detailedPhases
}
