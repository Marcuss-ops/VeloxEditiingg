package executors

import (
	"math"
	"sort"
	"strings"

	sharedtelemetry "velox-shared/telemetry"

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
	appendEnginePhaseMS(&detailedPhases, rm)
	appendObservabilitySummaryPhases(&detailedPhases, rm.Observability)
	return detailedPhases
}

// appendEnginePhaseMS projects the engine's own phase_ms ledger (the
// ScopedTimer timers written by RenderEngine::sidecarJson) into the detailed
// phase stream. The ledger is the only record of where time goes INSIDE the
// engine — for the copy-only path the packet mux is the dominant render cost
// — and it previously reached no operator-visible surface: it was mapped onto
// pipeline.RenderMetrics.PhaseMS and then dropped at the report boundary,
// so the Master showed only an aggregate engine render span.
//
// Only catalog-declared (component, action) pairs are projected: ImportCXX
// resolves every imported event through shared/telemetry and refuses unknown
// pairs, so an unregistered timer key must not be synthesized into an event.
// The engine writes one timer per key as "<action>_ms", and the catalog entry
// for that action owns origin/scope/phase, so the projected row is an
// ordinary catalog event rather than a new taxonomy.
func appendEnginePhaseMS(phases *[]executor.DetailedPhaseTiming, rm pipeline.RenderMetrics) {
	if len(rm.PhaseMS) == 0 {
		return
	}
	keys := make([]string, 0, len(rm.PhaseMS))
	for key := range rm.PhaseMS {
		keys = append(keys, key)
	}
	// Sorting keeps the projected EventIndex assignment deterministic across
	// runs (map iteration order is not).
	sort.Strings(keys)
	nextEventIndex := nextEngineEventIndex(*phases)
	for _, key := range keys {
		action := strings.TrimSuffix(key, "_ms")
		if action == "" || action == key {
			// The engine only declares "<action>_ms" timers; anything else is
			// not a phase duration and has no catalog entry to project.
			continue
		}
		spec, ok := sharedtelemetry.Catalog.Lookup("engine", action)
		if !ok {
			continue
		}
		durationMS := int64(math.Round(rm.PhaseMS[key]))
		if durationMS < 0 {
			durationMS = 0
		}
		*phases = append(*phases, executor.DetailedPhaseTiming{
			Origin:      spec.Origin,
			Scope:       spec.Scope,
			Component:   spec.Component,
			Action:      spec.Action,
			Phase:       spec.Phase,
			EventType:   spec.EventType,
			EventName:   spec.Action,
			EventIndex:  nextEventIndex,
			DurationMS:  durationMS,
			Status:      telemetry.StatusOK,
			CPUMS:       0,
			QueueWaitMS: 0,
		})
		nextEventIndex++
	}
}

// nextEngineEventIndex returns the first free engine-origin event index so a
// projected timer can never collide with a sidecar phase (ImportCXX treats a
// duplicate origin/index as a conflicting duplicate).
func nextEngineEventIndex(phases []executor.DetailedPhaseTiming) int64 {
	next := int64(0)
	for _, phase := range phases {
		if phase.Origin == telemetry.OriginEngine && phase.EventIndex >= next {
			next = phase.EventIndex + 1
		}
	}
	return next
}
