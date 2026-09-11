package executors

import (
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/pipeline"
)

// emitEngineProcessTelemetry records the canonical engine subprocess facts:
// the PROCESS_STARTED spawn event and the engine-declared process counters.
func emitEngineProcessTelemetry(rec *telemetry.EventRecorder, run pipeline.RunMetrics) {
	if rec != nil && run.RenderMetrics.EngineSpawnCount > 0 {
		rec.Emit(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeAttempt, Component: "worker.engine", Action: "spawn"}, telemetry.StatusOK, "", "")
	}
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
			Origin: telemetry.OriginWorker, Scope: telemetry.ScopeAttempt,
			Component: "worker.engine", Action: "usage",
			MetadataJSON: telemetry.EngineUsageMetadataJSON(usage),
		}, telemetry.StatusOK, "", "")
	}
}

// projectSegments maps the engine's per-segment counters onto the executor
// segment timing stream.
func projectSegments(rm pipeline.RenderMetrics) []executor.SegmentTiming {
	segments := make([]executor.SegmentTiming, 0, len(rm.Segments))
	for _, seg := range rm.Segments {
		segments = append(segments, executor.SegmentTiming{
			SegmentIndex: seg.SegmentIndex, SceneWorkerIndex: seg.SceneWorkerIndex,
			SceneID: seg.SceneID, SourceType: seg.SourceType,
			DurationMS: seg.DurationMS, AssetDownloadMS: seg.AssetDownloadMS,
			FfmpegEncodeMS: seg.FfmpegEncodeMS, SourceBytes: seg.SourceBytes,
			OutputBytes: seg.OutputBytes, FramesEncoded: seg.FramesEncoded,
			FramesDecoded: seg.FramesDecoded, FramesComposited: seg.FramesComposited,
			FfmpegSpeedX: seg.FfmpegSpeedX, Codec: seg.Codec, Preset: seg.Preset,
			FfmpegThreads: seg.FfmpegThreads, Status: seg.Status,
			ErrorCode: seg.ErrorCode, ErrorMessage: seg.ErrorMessage,
			SourceURLHash: seg.SourceURLHash, CacheKey: seg.CacheKey,
			InputDurationMS: seg.InputDurationMS, OutputDurationMS: seg.OutputDurationMS,
			MetadataJSON: seg.MetadataJSON, StartedOffsetMS: seg.StartedOffsetMS,
			FinishedOffsetMS: seg.FinishedOffsetMS, WorkerSlot: seg.WorkerSlot,
			CPUThreads: seg.CPUThreads, ParallelGroup: seg.ParallelGroup,
		})
	}
	return segments
}

// projectDetailedPhases maps the engine's detailed phase ledger onto the
// executor phase timing stream and appends observability category rollups.
func projectDetailedPhases(rm pipeline.RenderMetrics) []executor.DetailedPhaseTiming {
	detailedPhases := make([]executor.DetailedPhaseTiming, 0, len(rm.DetailedPhases))
	for _, phase := range rm.DetailedPhases {
		detailedPhases = append(detailedPhases, executor.DetailedPhaseTiming{
			Origin: phase.Origin, Scope: phase.Scope, Component: phase.Component,
			Action: phase.Action, Phase: phase.Phase, EventType: phase.EventType,
			EventName: phase.EventName, EventIndex: phase.EventIndex,
			StartedAt: phase.StartedAt, CompletedAt: phase.CompletedAt,
			DurationMS: phase.DurationMS, Status: phase.Status,
			ErrorCode: phase.ErrorCode, ErrorMessage: phase.ErrorMessage,
			BytesIn: phase.BytesIn, BytesOut: phase.BytesOut, Frames: phase.Frames,
			MetadataJSON: phase.MetadataJSON, SegmentIndex: phase.SegmentIndex,
			TrackKind: phase.TrackKind, TrackIndex: phase.TrackIndex,
			StartedOffsetMS: phase.StartedOffsetMS, FinishedOffsetMS: phase.FinishedOffsetMS,
			CPUMS: phase.CPUMS, QueueWaitMS: phase.QueueWaitMS,
			FramesIn: phase.FramesIn, FramesOut: phase.FramesOut,
		})
	}
	appendObservabilitySummaryPhases(&detailedPhases, rm.Observability)
	return detailedPhases
}
