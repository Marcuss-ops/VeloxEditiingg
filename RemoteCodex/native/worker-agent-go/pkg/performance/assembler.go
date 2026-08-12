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

	"velox-shared/contract"
	"velox-shared/telemetry"
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

	// Workload describes what the run actually did. It MUST be built from
	// the CompiledRenderPlanV2 via WorkloadFromCompiledRenderPlan — the
	// render plan is the single owner of clip count and expected duration
	// (Fact Owner: render_plan). The assembler NEVER guesses workload from
	// telemetry (no timeline-items fallback): an absent value stays zero.
	Workload WorkloadProfile

	// WallMs overrides the receipt wall clock when the caller holds a
	// more authoritative clock than pipeline.RunMetrics.TotalMs (e.g.
	// the executor's total). Zero falls back to run.TotalMs.
	WallMs int64

	// CPUWallMS is the RAW accumulated-CPU fact observed by the attempt
	// telemetry session (cgroup v2, engine + children; Fact Owner:
	// attempt_telemetry). It feeds CPUWallRatio through the single
	// Deriver — the assembler never infers CPU from a wall clock. Zero
	// means the fact was not collected: the ratio stays zero.
	CPUWallMS int64

	// UsefulPipelineMS is the caller-known useful pipeline work time
	// (RAW input to UsefulWorkRatio). Zero means not measured.
	UsefulPipelineMS int64
}

// Assemble aggregates the given pipeline run into a canonical
// PerformanceReceiptV1.
//
// Every value that already exists in the telemetry is mapped. Sections
// whose counters are not collected yet (external process exec breakdown,
// real read/write I/O counters, memory, scheduling) stay zero so the
// receipt remains structurally valid — they land with their own
// collectors in later steps.
//
// The Derived section is populated by the SINGLE DerivedMetricsCalculator
// (Derive): the assembler never computes a ratio itself. It only gathers
// the raw facts (wall clock, exclusive phases, IO totals, process/workload
// counts, caller-supplied CPU/useful-work) and hands them to Derive.
func (a *Assembler) Assemble(run pipeline.RunMetrics, ctx AssemblyContext) *PerformanceReceiptV1 {
	receipt := NewPerformanceReceiptV1()
	receipt.Identity = ctx.Identity
	// The caller supplies the authoritative workload (built from the
	// CompiledRenderPlanV2); the assembler never reconstructs it.
	receipt.Workload = ctx.Workload // The wall clock is resolved once and shared by Timing and the CPU
	// section so the two can never disagree about wall_ms.
	wall := ctx.WallMs
	if wall <= 0 {
		wall = run.TotalMs
	}
	receipt.Timing = assembleTiming(run, wall)
	receipt.Process = assembleProcess(run.RenderMetrics)
	receipt.CPU = assembleCPU(run.RenderMetrics, wall)
	receipt.IO = DeriveIO(run.RenderMetrics)
	receipt.Media = assembleMedia(run.RenderMetrics)
	receipt.Memory = assembleMemory(run.RenderMetrics)
	receipt.Scheduling = assembleScheduling(run.RenderMetrics)
	receipt.Phases = assemblePhases(run.RenderMetrics)
	receipt.Segments = assembleSegments(run.RenderMetrics)
	// Derived KPIs: one call, one definition (the single
	// DerivedMetricsCalculator). The assembler only gathers the RAW facts
	// and hands them to Derive — it never computes a ratio itself.
	receipt.Derived = Derive(rawMetricsFrom(ctx, receipt))
	return receipt
}

// rawMetricsFrom gathers the RAW observed facts that Assemble hands to the
// single Deriver. It is the ONLY place that maps a receipt + caller context
// onto RawMetrics, and it is shared with the consistency test so the wiring
// pin can never drift from the production path.
func rawMetricsFrom(ctx AssemblyContext, receipt *PerformanceReceiptV1) RawMetrics {
	return RawMetrics{
		WallMs:               receipt.Timing.WallMs,
		Phases:               receipt.Phases,
		CPUWallMS:            ctx.CPUWallMS,
		TotalBytesRead:       receipt.IO.TotalBytesRead,
		TotalBytesWritten:    receipt.IO.TotalBytesWritten,
		OutputBytes:          receipt.IO.FinalBytesWritten,
		ExternalProcessCount: receipt.Process.ExternalProcessCount,
		ClipCount:            receipt.Workload.ClipCount,
		UsefulPipelineMS:     ctx.UsefulPipelineMS,
	}
}

// WorkloadFromCompiledRenderPlan builds the authoritative workload profile
// from the CompiledRenderPlanV2 — the single owner of clip count and
// expected duration (Fact Owner: render_plan). The assembler and any caller
// that builds a WorkloadProfile must use this constructor instead of
// reconstructing workload from pipeline telemetry (e.g. counting timeline
// items): the plan declares what the attempt is ABOUT, telemetry describes
// what HAPPENED. Fields the plan does not own (JobType, CopyOnly) stay zero
// for the caller to fill.
func WorkloadFromCompiledRenderPlan(plan *contract.CompiledRenderPlanV2) WorkloadProfile {
	if plan == nil {
		return WorkloadProfile{}
	}
	workload := WorkloadProfile{
		DurationUS:     plan.DurationUS,
		AssetCount:     len(plan.Assets),
		VideoCodec:     plan.Output.VideoCodec,
		AudioCodec:     plan.FinalAudio.Codec,
		Width:          plan.Output.Width,
		Height:         plan.Output.Height,
		FinalAudioCopy: plan.FinalAudio.Mode == contract.AudioModeFinalAudioCopy,
	}
	if plan.Output.FPSNum > 0 && plan.Output.FPSDen > 0 {
		workload.FPS = float64(plan.Output.FPSNum) / float64(plan.Output.FPSDen)
	}
	// clip_count = total ordered video segments across all video tracks.
	// The plan's segment list is the authoritative clip inventory.
	for _, track := range plan.VideoTracks {
		workload.ClipCount += len(track.Segments)
	}
	return workload
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
		// Engine-declared spawn ledger (sidecar process_counters block):
		// disjoint from the /proc sampler counts above.
		EngineExternalSpawnCount: rm.EngineExternalSpawnCount,
		EngineFfmpegSpawnCount:   rm.EngineFfmpegSpawnCount,
		EngineFfprobeSpawnCount:  rm.EngineFfprobeSpawnCount,
		EngineShellSpawnCount:    rm.EngineShellSpawnCount,
		EngineCurlSpawnCount:     rm.EngineCurlSpawnCount,
		ChildWaitMs:              rm.ChildWaitMs,
	}
}

// assembleScheduling maps the engine-declared scheduling/VM facts (the
// sidecar process_counters block: the engine process's own context
// switches and page faults) into the receipt. The QueueWaitMS/OffCPUMs
// fields stay zero until a deep-profile collector lands.
func assembleScheduling(rm pipeline.RenderMetrics) SchedulingMetrics {
	return SchedulingMetrics{
		VoluntaryContextSwitches:   rm.EngineVoluntaryContextSwitches,
		InvoluntaryContextSwitches: rm.EngineInvoluntaryContextSwitches,
		MinorPageFaults:            rm.EngineMinorPageFaults,
		MajorPageFaults:            rm.EngineMajorPageFaults,
	}
}

// assembleCPU maps the engine tree's CPU counters into the receipt.
// WallMs mirrors the effective wall clock (the same value Timing.WallMs
// resolves to) so the CPUWallRatio is self-contained.
func assembleCPU(rm pipeline.RenderMetrics, wallMs int64) CPUMetrics {
	total := rm.CPUUserMs + rm.CPUSystemMs
	ratio := 0.0
	if wallMs > 0 {
		ratio = float64(total) / float64(wallMs)
	}
	return CPUMetrics{
		WallMs:       wallMs,
		CPUUserMs:    rm.CPUUserMs,
		CPUSystemMs:  rm.CPUSystemMs,
		CPUTotalMs:   total,
		CPUWallRatio: ratio,
		// Engine-declared subset (sidecar process_counters block): the
		// engine process's own getrusage CPU.
		EngineCPUUserMs:   rm.EngineCPUUserMs,
		EngineCPUSystemMs: rm.EngineCPUSystemMs,
	}
}

// DeriveCPU maps the engine tree's CPU counters into CPUMetrics. It is
// exported so the executor can emit the SAME projection as cpu.* attempt
// metrics without duplicating the derivation — the receipt and the
// worker telemetry must never disagree about what cpu.user_ms means.
// wallMs is the same wall clock the receipt resolves for its Timing
// section.
func DeriveCPU(rm pipeline.RenderMetrics, wallMs int64) CPUMetrics {
	return assembleCPU(rm, wallMs)
}

// assembleMemory maps the engine tree's RSS counters (sampled from
// /proc while the engine ran) into the receipt: the peak resident set
// over the run and the RSS at the last sample.
func assembleMemory(rm pipeline.RenderMetrics) MemoryMetrics {
	return MemoryMetrics{
		PeakRSSBytes:    rm.PeakRSSBytes,
		CurrentRSSBytes: rm.CurrentRSSBytes,
	}
}

// DeriveIO maps the real I/O telemetry into IOMetrics. It is exported
// so the executor can emit the SAME per-kind projection as io.* attempt
// metrics without duplicating the derivation — the receipt and the
// worker telemetry must never disagree about what asset_bytes_read
// means. See assembleIO for the per-field provenance.
func DeriveIO(rm pipeline.RenderMetrics) IOMetrics {
	return assembleIO(rm)
}

// assembleIO maps the real I/O telemetry into the receipt:
//
//   - TotalBytesRead/TotalBytesWritten — measured from /proc/<pid>/io
//     over the whole engine process tree (engine + ffmpeg/ffprobe/shell
//     descendants) while it rendered. These are the amplification
//     denominators.
//   - AssetBytesRead — the engine-declared size of every asset it bound
//     or staged (sum of segments[].source_bytes). It is a file-size
//     proxy for bytes touched from assets, not a syscall counter.
//   - TempBytesWritten — the engine's own temp accounting (sidecar
//     temp_bytes). MediaMetrics.TempBytes mirrors the same value.
//   - MuxBytesRead/MuxBytesWritten — summed from the sidecar phases[]
//     events whose component/action is a mux or packet_mux operation
//     (e.g. the copy-only packet mux pass).
//   - FinalBytesWritten — the engine-declared final artifact size
//     (sidecar total_size), which the executor later re-verifies with
//     the artifact manifest.
//
// asset_bytes_copied, file_copy_count/bytes and input_open/reopen_count
// are engine-side counters reported in the sidecar io_counters block
// (file::copyFile/downloadAsset/avformat-open chokepoints). Zero on
// engines that predate the block; the Phase-1 copy-only target is
// exactly 0 copies and 0 external opens.
func assembleIO(rm pipeline.RenderMetrics) IOMetrics {
	muxRead, muxWritten := sumMuxPhaseBytes(rm.DetailedPhases)
	return IOMetrics{
		TotalBytesRead:    rm.TotalBytesRead,
		TotalBytesWritten: rm.TotalBytesWritten,
		AssetBytesRead:    sumSegmentSourceBytes(rm.Segments),
		AssetBytesCopied:  rm.AssetBytesCopied,
		TempBytesWritten:  rm.TempBytes,
		MuxBytesRead:      muxRead,
		MuxBytesWritten:   muxWritten,
		FinalBytesWritten: rm.TotalSize,
		FileCopyCount:     rm.FileCopyCount,
		FileCopyBytes:     rm.FileCopyBytes,
		InputOpenCount:    rm.InputOpenCount,
		InputReopenCount:  rm.InputReopenCount,
	}
}

// sumSegmentSourceBytes totals the engine-declared source asset sizes
// across all timeline segments.
func sumSegmentSourceBytes(segments []pipeline.SegmentTiming) int64 {
	var total int64
	for _, seg := range segments {
		total += seg.SourceBytes
	}
	return total
}

// sumMuxPhaseBytes totals the bytes_in/bytes_out of the sidecar phases[]
// events that describe a mux or packet_mux operation (copy-only uses a
// single packet mux pass). The predicate matches exact mux identities
// so that future "demux"/"remux" events are never misclassified as mux
// output.
func sumMuxPhaseBytes(phases []pipeline.DetailedPhaseTiming) (bytesIn, bytesOut int64) {
	for _, p := range phases {
		if !isMuxEvent(p) {
			continue
		}
		bytesIn += p.BytesIn
		bytesOut += p.BytesOut
	}
	return bytesIn, bytesOut
}

func isMuxEvent(p pipeline.DetailedPhaseTiming) bool {
	name := strings.ToLower(phaseName(p))
	return p.Component == "engine.mux" ||
		p.Action == "packet_mux" ||
		p.Action == "mux" ||
		name == "mux" ||
		strings.HasPrefix(name, "mux.") ||
		strings.HasSuffix(name, ".mux")
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

// assemblePhases derives the timed phase list of the attempt. The sidecar
// phases[] stream (DetailedPhases) is the preferred source; legacy sidecars
// fall back to the flat PhaseMS map, sorted for deterministic output.
//
// Every row is stamped with its canonical catalog timing role
// (classifyPhaseTiming). Derive then sums ONLY the TimingExclusive rows
// into accounted_ratio — span parents and span children (and unclassified
// rows) are carried for diagnostics but never summed, so parallel work can
// never double-count against the wall clock.
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
				TimingMode:  classifyPhaseTiming(p),
			})
		}
		return phases
	}
	if len(rm.PhaseMS) > 0 {
		// Legacy flat-map path: the keys are engine-side labels with no
		// catalog identity, so every row is unclassified (empty TimingMode)
		// and therefore QUARANTINED from accounted_ratio. A legacy-only
		// sidecar yields accounted_ratio=0 by design — fail-closed rather
		// than trusting an unverifiable label as exclusive.
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

// classifyPhaseTiming resolves the canonical catalog timing role of one
// sidecar phase event (the accounted_ratio classification):
//
//  1. shared-catalog event lookup by component.action — authoritative
//     (e.g. engine.render → exclusive; engine.video.decode → span_child);
//  2. phase-taxonomy fallback for attempt-scoped events that predate a
//     catalog key: an event stamped with the attempt scope and a canonical
//     phase inherits that phase's accounted role from the taxonomy;
//  3. anything else returns "" — the row is quarantined from
//     accounted_ratio (fail-closed: an unclassifiable event is never
//     treated as exclusive).
func classifyPhaseTiming(p pipeline.DetailedPhaseTiming) telemetry.TimingMode {
	return classifyTimingRole(p.Component, p.Action, p.Scope, p.Phase)
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
	return descriptiveName(p.EventName, p.Component, p.Action, p.Phase, p.EventType)
}
