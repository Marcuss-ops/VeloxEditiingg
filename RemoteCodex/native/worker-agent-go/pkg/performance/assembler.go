package performance

// assembler.go defines the single PerformanceReceiptAssembler — the one
// resolver/aggregator that turns the existing telemetry sources
// (pipeline.RunMetrics, the C++ sidecar phase/segment timing, worker
// counters) into one canonical PerformanceReceiptV1.
//
// Contract: the assembler is the ONLY place that builds a receipt.
// Callers never hand-assemble sections; they only supply the context
// the telemetry itself cannot know (identity, workload profile, wall
// clock override).

import (
	"sort"
	"strings"

	"velox-worker-agent/pkg/video/pipeline"
)

// Assembler is the single resolver/aggregator for the render path. It
// is stateless and safe for concurrent use.
type Assembler struct{}

// NewAssembler returns an Assembler.
func NewAssembler() *Assembler { return &Assembler{} }

// AssemblyContext carries the caller-known context that the existing
// telemetry does not contain.
type AssemblyContext struct {
	// Identity identifies the attempt, worker and machine that produced
	// the run.
	Identity PerformanceIdentity

	// Workload describes what the run actually did. Fields the caller
	// leaves zero are filled with the closest telemetry equivalent
	// (clip_count ← timeline items); authoritative values win.
	Workload WorkloadProfile

	// WallMs overrides the receipt wall clock when the caller holds a
	// more authoritative clock than pipeline.RunMetrics.TotalMs (e.g.
	// the executor's total). Zero falls back to run.TotalMs.
	WallMs int64
}

// Assemble aggregates the given pipeline run into a canonical
// PerformanceReceiptV1.
//
// Every value that already exists in the telemetry is mapped. Sections
// whose counters are not collected yet (external process exec breakdown,
// real read/write I/O counters, memory, scheduling) stay zero so the
// receipt remains structurally valid — they land with their own
// collectors in later steps. Derived KPIs are computed by a later
// stage, not here.
func (a *Assembler) Assemble(run pipeline.RunMetrics, ctx AssemblyContext) *PerformanceReceiptV1 {
	receipt := NewPerformanceReceiptV1()
	receipt.Identity = ctx.Identity
	receipt.Workload = assembleWorkload(run, ctx.Workload)
	receipt.Timing = assembleTiming(run, ctx.WallMs)
	receipt.Process = assembleProcess(run.RenderMetrics)
	// IOMetrics stays zero until the real read/write/copy counters land;
	// only sidecar-derived media accounting is mapped today.
	receipt.Media = assembleMedia(run.RenderMetrics)
	// MemoryMetrics and SchedulingMetrics stay zero until the
	// corresponding collectors are added.
	receipt.Phases = assemblePhases(run.RenderMetrics)
	receipt.Segments = assembleSegments(run.RenderMetrics)
	return receipt
}

// assembleWorkload merges the caller-provided profile with the
// telemetry fallbacks. Caller values are never overwritten.
func assembleWorkload(run pipeline.RunMetrics, w WorkloadProfile) WorkloadProfile {
	if w.ClipCount <= 0 && run.TimelineItems > 0 {
		// Lower-bound estimate: in the copy-only path each clip is one
		// timeline item. Callers with authoritative counts override it.
		w.ClipCount = run.TimelineItems
	}
	return w
}

// assembleTiming maps the pipeline phase clocks and the native
// subprocess lifecycle clocks.
func assembleTiming(run pipeline.RunMetrics, wallOverride int64) TimingMetrics {
	t := TimingMetrics{
		WallMs:          run.TotalMs,
		ResolveMs:       run.ResolveMs,
		ValidateMs:      run.ValidateMs,
		CompileMs:       run.CompileMs,
		RenderMs:        run.RenderMs,
		PipelineTotalMs: run.TotalMs,
		PlanMarshalMs:   run.RenderMetrics.PlanMarshalMs,
		PlanWriteMs:     run.RenderMetrics.PlanWriteMs,
		ProcessStartMs:  run.RenderMetrics.ProcessStartMs,
		ProcessWaitMs:   run.RenderMetrics.ProcessWaitMs,
		NativeTotalMs:   run.RenderMetrics.TotalMs,
		// ExecutorTotalMs / ArtifactTotalMs are not carried by
		// RunMetrics; the executor owns those clocks.
	}
	if wallOverride > 0 {
		t.WallMs = wallOverride
	}
	return t
}

// assembleProcess maps the process lifecycle counters collected by the
// native client: the engine spawn (counted by the render client) and the
// external tool processes it spawned in its own process group (sampled
// from /proc while the engine ran).
func assembleProcess(rm pipeline.RenderMetrics) ProcessMetrics {
	return ProcessMetrics{
		EngineSpawnCount:     rm.EngineSpawnCount,
		EngineSpawnMs:        rm.EngineSpawnMs,
		ExternalProcessCount: rm.ExternalProcessCount,
		FfmpegExecCount:      rm.FfmpegExecCount,
		FfprobeExecCount:     rm.FfprobeExecCount,
		ShellExecCount:       rm.ShellExecCount,
		CurlExecCount:        rm.CurlExecCount,
		ChildWaitMs:          rm.ChildWaitMs,
	}
}

// assembleMedia mirrors the C++ engine sidecar counters already mapped
// into RenderMetrics by the worker.
func assembleMedia(rm pipeline.RenderMetrics) MediaMetrics {
	return MediaMetrics{
		Frames:           rm.Frames,
		FramesDecoded:    rm.FramesDecoded,
		FramesComposited: rm.FramesComposited,
		EncodePasses:     rm.EncodePasses,
		TempBytes:        rm.TempBytes,
		OutputBytes:      rm.TotalSize,
		DurationSec:      rm.DurationSec,
		ConcatMode:       rm.ConcatMode,
		FPS:              rm.Fps,
		SpeedX:           rm.SpeedX,
		Bitrate:          rm.Bitrate,
		DupFrames:        rm.DupFrames,
		DropFrames:       rm.DropFrames,
	}
}

// assemblePhases derives the canonical exclusive-measurement phase list.
// The sidecar phases[] stream (DetailedPhases) is the preferred source;
// legacy sidecars fall back to the flat PhaseMS map, sorted for
// deterministic output.
func assemblePhases(rm pipeline.RenderMetrics) []PhaseTiming {
	if len(rm.DetailedPhases) > 0 {
		phases := make([]PhaseTiming, 0, len(rm.DetailedPhases))
		for _, p := range rm.DetailedPhases {
			phases = append(phases, PhaseTiming{
				Name:        phaseName(p),
				DurationMS:  p.DurationMS,
				CPUMs:       int64(p.CPUMS),
				QueueWaitMS: int64(p.QueueWaitMS),
				BytesIn:     p.BytesIn,
				BytesOut:    p.BytesOut,
				FramesIn:    p.FramesIn,
				FramesOut:   p.FramesOut,
			})
		}
		return phases
	}
	if len(rm.PhaseMS) > 0 {
		keys := make([]string, 0, len(rm.PhaseMS))
		for key := range rm.PhaseMS {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		phases := make([]PhaseTiming, 0, len(keys))
		for _, key := range keys {
			phases = append(phases, PhaseTiming{Name: key, DurationMS: int64(rm.PhaseMS[key])})
		}
		return phases
	}
	return nil
}

// assembleSegments maps the per-segment C++ sidecar rows into the
// receipt. The order of the sidecar array is preserved: it already is
// the canonical timeline order.
func assembleSegments(rm pipeline.RenderMetrics) []SegmentTiming {
	if len(rm.Segments) == 0 {
		return nil
	}
	segments := make([]SegmentTiming, 0, len(rm.Segments))
	for _, seg := range rm.Segments {
		segments = append(segments, SegmentTiming{
			SegmentIndex:     seg.SegmentIndex,
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
			Status:           seg.Status,
			ErrorCode:        seg.ErrorCode,
			StartedOffsetMS:  seg.StartedOffsetMS,
			FinishedOffsetMS: seg.FinishedOffsetMS,
			WorkerSlot:       seg.WorkerSlot,
			CPUThreads:       seg.CPUThreads,
			ParallelGroup:    seg.ParallelGroup,
		})
	}
	return segments
}

// phaseName picks the most descriptive canonical name for a detailed
// phase event: the event name when present, else component.action, else
// the phase label, else the event type. Rows with no identifiable name
// are labeled "unknown" so accounting never sees an empty name.
func phaseName(p pipeline.DetailedPhaseTiming) string {
	if strings.TrimSpace(p.EventName) != "" {
		return p.EventName
	}
	if p.Component != "" && p.Action != "" {
		return p.Component + "." + p.Action
	}
	if p.Phase != "" {
		return p.Phase
	}
	if p.EventType != "" {
		return p.EventType
	}
	return "unknown"
}
