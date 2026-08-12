package performance

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"velox-shared/contract"
	"velox-worker-agent/pkg/video/pipeline"
)

// sampleRun builds a fully-populated pipeline.RunMetrics that mirrors
// what scene.composite.v1 actually produces today.
func sampleRun() pipeline.RunMetrics {
	return pipeline.RunMetrics{
		ResolveMs:     120,
		ValidateMs:    40,
		CompileMs:     210,
		RenderMs:      18910,
		TotalMs:       19310,
		TimelineItems: 25,
		AudioTracks:   1,
		RenderMetrics: pipeline.RenderMetrics{
			Frames:               5000,
			FramesDecoded:        12,
			FramesComposited:     5000,
			Fps:                  30,
			SpeedX:               42.5,
			EncodePasses:         1,
			TempBytes:            900_000_000,
			OutputDurable:        true,
			DurationSec:          300,
			ConcatMode:           "segment_concat",
			TotalSize:            300_000_000,
			OutTimeMs:            300_000,
			Bitrate:              8_000_000,
			DupFrames:            3,
			DropFrames:           1,
			PlanMarshalMs:        55,
			PlanWriteMs:          12,
			ProcessStartMs:       85,
			ProcessWaitMs:        18700,
			TotalMs:              18800,
			EngineSpawnCount:     1,
			EngineSpawnMs:        85,
			ExternalProcessCount: 64,
			FfmpegExecCount:      32,
			FfprobeExecCount:     25,
			ShellExecCount:       6,
			CurlExecCount:        1,
			ChildWaitMs:          18700,
			TotalBytesRead:       1_800_000_000,
			TotalBytesWritten:    1_200_000_000,
			FileCopyCount:        3,
			FileCopyBytes:        4_194_304,
			AssetBytesCopied:     4_194_304,
			InputOpenCount:       5,
			InputReopenCount:     2,
			CPUUserMs:            1500,
			CPUSystemMs:          300,
			PeakRSSBytes:         320_000_000,
			CurrentRSSBytes:      300_000_000,
		},
	}
}

func TestAssembler_MapsPipelineTiming(t *testing.T) {
	receipt := NewAssembler().Assemble(sampleRun(), AssemblyContext{})

	require.Equal(t, PerformanceReceiptVersionV1, receipt.Version)
	require.Equal(t, int64(19310), receipt.Timing.WallMs)
	require.Equal(t, int64(120), receipt.Timing.ResolveMs)
	require.Equal(t, int64(40), receipt.Timing.ValidateMs)
	require.Equal(t, int64(210), receipt.Timing.CompileMs)
	require.Equal(t, int64(18910), receipt.Timing.RenderMs)
	require.Equal(t, int64(19310), receipt.Timing.PipelineTotalMs)
}

func TestAssembler_MapsCPU(t *testing.T) {
	receipt := NewAssembler().Assemble(sampleRun(), AssemblyContext{})

	cpu := receipt.CPU
	// wall_ms mirrors the resolved wall clock (Timing.WallMs).
	require.Equal(t, receipt.Timing.WallMs, cpu.WallMs)
	require.Equal(t, int64(19310), cpu.WallMs)
	require.Equal(t, int64(1500), cpu.CPUUserMs)
	require.Equal(t, int64(300), cpu.CPUSystemMs)
	require.Equal(t, int64(1800), cpu.CPUTotalMs)
	require.InDelta(t, 1800.0/19310.0, cpu.CPUWallRatio, 1e-9)
}

func TestAssembler_CPURatioZeroWithoutWall(t *testing.T) {
	run := sampleRun()
	run.TotalMs = 0
	receipt := NewAssembler().Assemble(run, AssemblyContext{})
	require.Zero(t, receipt.CPU.CPUWallRatio)
	require.Equal(t, int64(1800), receipt.CPU.CPUTotalMs, "total is still measured even without a wall clock")
}

func TestDeriveCPU_MatchesReceiptSection(t *testing.T) {
	// The executor emits cpu.* attempt metrics from performance.DeriveCPU;
	// it must never disagree with the receipt's CPU section built by the
	// assembler. Both go through the same derivation.
	run := sampleRun()
	receiptCPU := NewAssembler().Assemble(run, AssemblyContext{}).CPU
	require.Equal(t, receiptCPU, DeriveCPU(run.RenderMetrics, run.TotalMs))
}

func TestAssembler_MapsMemory(t *testing.T) {
	receipt := NewAssembler().Assemble(sampleRun(), AssemblyContext{})
	require.Equal(t, int64(320_000_000), receipt.Memory.PeakRSSBytes)
	require.Equal(t, int64(300_000_000), receipt.Memory.CurrentRSSBytes)
}

func TestAssembler_MapsNativeSidecar(t *testing.T) {
	receipt := NewAssembler().Assemble(sampleRun(), AssemblyContext{})

	// Subprocess lifecycle clocks.
	require.Equal(t, int64(55), receipt.Timing.PlanMarshalMs)
	require.Equal(t, int64(12), receipt.Timing.PlanWriteMs)
	require.Equal(t, int64(85), receipt.Timing.ProcessStartMs)
	require.Equal(t, int64(18700), receipt.Timing.ProcessWaitMs)
	require.Equal(t, int64(18800), receipt.Timing.NativeTotalMs)

	// Process lifecycle counters collected by the native client.
	require.Equal(t, int64(1), receipt.Process.EngineSpawnCount)
	require.Equal(t, int64(85), receipt.Process.EngineSpawnMs)
	require.Equal(t, int64(18700), receipt.Process.ChildWaitMs)
	require.Equal(t, int64(64), receipt.Process.ExternalProcessCount)
	require.Equal(t, int64(32), receipt.Process.FfmpegExecCount)
	require.Equal(t, int64(25), receipt.Process.FfprobeExecCount)
	require.Equal(t, int64(6), receipt.Process.ShellExecCount)
	require.Equal(t, int64(1), receipt.Process.CurlExecCount)

	// Sidecar media counters.
	media := receipt.Media
	require.Equal(t, int64(5000), media.Frames)
	require.Equal(t, int64(12), media.FramesDecoded)
	require.Equal(t, int64(5000), media.FramesComposited)
	require.Equal(t, int64(1), media.EncodePasses)
	require.Equal(t, int64(900_000_000), media.TempBytes)
	require.Equal(t, int64(300_000_000), media.OutputBytes)
	require.Equal(t, 300.0, media.DurationSec)
	require.Equal(t, "segment_concat", media.ConcatMode)
	require.Equal(t, 30.0, media.FPS)
	require.Equal(t, 42.5, media.SpeedX)
	require.Equal(t, 8_000_000.0, media.Bitrate)
	require.Equal(t, int64(3), media.DupFrames)
	require.Equal(t, int64(1), media.DropFrames)
}

func TestAssembler_MapsIO(t *testing.T) {
	run := sampleRun()
	run.RenderMetrics.TotalBytesRead = 1_800_000_000
	run.RenderMetrics.TotalBytesWritten = 1_200_000_000
	run.RenderMetrics.Segments = []pipeline.SegmentTiming{
		{SegmentIndex: 0, SourceBytes: 60_000_000},
		{SegmentIndex: 1, SourceBytes: 40_000_000},
	}
	run.RenderMetrics.DetailedPhases = []pipeline.DetailedPhaseTiming{
		{Component: "engine", Action: "packet_mux", BytesIn: 100_000_000, BytesOut: 300_000_000},
		{Component: "engine.mux", Action: "audio", BytesIn: 0, BytesOut: 2_000_000},
		{Component: "media", Action: "open", BytesIn: 50_000_000, BytesOut: 0}, // not a mux
	}

	io := NewAssembler().Assemble(run, AssemblyContext{}).IO
	require.Equal(t, int64(1_800_000_000), io.TotalBytesRead)
	require.Equal(t, int64(1_200_000_000), io.TotalBytesWritten)
	// asset_bytes_read = sum of segment source_bytes.
	require.Equal(t, int64(100_000_000), io.AssetBytesRead)
	// temp_bytes_written mirrors the sidecar temp accounting.
	require.Equal(t, int64(900_000_000), io.TempBytesWritten)
	// mux bytes summed only from mux/packet_mux phase events.
	require.Equal(t, int64(100_000_000), io.MuxBytesRead)
	require.Equal(t, int64(302_000_000), io.MuxBytesWritten)
	// final_bytes_written mirrors the engine-declared total size.
	require.Equal(t, int64(300_000_000), io.FinalBytesWritten)
	// Engine-side counters from the sidecar io_counters block.
	require.Equal(t, int64(4_194_304), io.AssetBytesCopied)
	require.Equal(t, int64(3), io.FileCopyCount)
	require.Equal(t, int64(4_194_304), io.FileCopyBytes)
	require.Equal(t, int64(5), io.InputOpenCount)
	require.Equal(t, int64(2), io.InputReopenCount)
}

func TestAssembler_EngineSidecarIOCountersAbsent(t *testing.T) {
	// Legacy engines (no io_counters block) leave the engine-side fields
	// zero — the receipt must not invent values.
	run := sampleRun()
	run.RenderMetrics.FileCopyCount = 0
	run.RenderMetrics.FileCopyBytes = 0
	run.RenderMetrics.AssetBytesCopied = 0
	run.RenderMetrics.InputOpenCount = 0
	run.RenderMetrics.InputReopenCount = 0

	io := NewAssembler().Assemble(run, AssemblyContext{}).IO
	require.Zero(t, io.AssetBytesCopied)
	require.Zero(t, io.FileCopyCount)
	require.Zero(t, io.InputOpenCount)
	require.Zero(t, io.InputReopenCount)
}

func TestDeriveIO_MatchesReceiptSection(t *testing.T) {
	// The executor emits io.* attempt metrics from performance.DeriveIO;
	// it must never disagree with the receipt's IO section built by the
	// assembler. Both go through the same derivation.
	run := sampleRun()
	run.RenderMetrics.Segments = []pipeline.SegmentTiming{{SourceBytes: 60_000_000}}
	run.RenderMetrics.DetailedPhases = []pipeline.DetailedPhaseTiming{
		{Component: "engine", Action: "packet_mux", BytesOut: 300_000_000},
	}

	receiptIO := NewAssembler().Assemble(run, AssemblyContext{}).IO
	require.Equal(t, receiptIO, DeriveIO(run.RenderMetrics))
}

func TestAssembler_NoEngineSpawnWhenProcessNeverStarted(t *testing.T) {
	run := sampleRun()
	run.RenderMetrics.ProcessStartMs = 0
	run.RenderMetrics.ProcessWaitMs = 0
	run.RenderMetrics.EngineSpawnCount = 0
	run.RenderMetrics.EngineSpawnMs = 0
	run.RenderMetrics.ChildWaitMs = 0

	receipt := NewAssembler().Assemble(run, AssemblyContext{})
	require.Zero(t, receipt.Process.EngineSpawnCount)
	require.Zero(t, receipt.Process.EngineSpawnMs)
	require.Zero(t, receipt.Process.ChildWaitMs)
}

func TestAssembler_PhasesFromDetailedPhases(t *testing.T) {
	run := sampleRun()
	run.RenderMetrics.DetailedPhases = []pipeline.DetailedPhaseTiming{
		{Component: "media", Action: "open", DurationMS: 400, CPUMS: 150, QueueWaitMS: 10, BytesIn: 1_800_000_000},
		{EventName: "mux.trailer", DurationMS: 30, CPUMS: 25, BytesOut: 300_000_000, FramesOut: 5000},
	}
	run.RenderMetrics.PhaseMS = map[string]float64{"engine.concat": 1200} // must be ignored

	phases := NewAssembler().Assemble(run, AssemblyContext{}).Phases
	require.Len(t, phases, 2)
	require.Equal(t, "media.open", phases[0].Name)
	require.Equal(t, int64(400), phases[0].DurationMS)
	require.Equal(t, int64(150), phases[0].CPUMs)
	require.Equal(t, int64(10), phases[0].QueueWaitMS)
	require.Equal(t, int64(1_800_000_000), phases[0].BytesIn)
	require.Equal(t, "mux.trailer", phases[1].Name)
	require.Equal(t, int64(30), phases[1].DurationMS)
	require.Equal(t, int64(300_000_000), phases[1].BytesOut)
	require.Equal(t, int64(5000), phases[1].FramesOut)
}

func TestAssembler_PhasesFallbackToPhaseMS_Sorted(t *testing.T) {
	run := sampleRun()
	run.RenderMetrics.PhaseMS = map[string]float64{
		"engine.concat":         1200,
		"engine.asset_download": 300,
	}

	phases := NewAssembler().Assemble(run, AssemblyContext{}).Phases
	require.Len(t, phases, 2)
	require.Equal(t, "engine.asset_download", phases[0].Name)
	require.Equal(t, int64(300), phases[0].DurationMS)
	require.Equal(t, "engine.concat", phases[1].Name)
	require.Equal(t, int64(1200), phases[1].DurationMS)
}

func TestAssembler_PhasesNilWhenNoSidecarTiming(t *testing.T) {
	receipt := NewAssembler().Assemble(sampleRun(), AssemblyContext{})
	require.Nil(t, receipt.Phases)
	require.Nil(t, receipt.Segments)
}

func TestAssembler_MapsSegments(t *testing.T) {
	run := sampleRun()
	run.RenderMetrics.Segments = []pipeline.SegmentTiming{
		{
			SegmentIndex:     0,
			SceneID:          "scene_1",
			SourceType:       "clip",
			DurationMS:       12000,
			AssetDownloadMS:  100,
			FfmpegEncodeMS:   8000,
			SourceBytes:      60_000_000,
			OutputBytes:      60_000_000,
			FramesEncoded:    2500,
			FramesDecoded:    2500,
			FramesComposited: 0,
			FfmpegSpeedX:     4.2,
			Codec:            "h264",
			Status:           "ok",
			StartedOffsetMS:  0,
			FinishedOffsetMS: 12000,
			WorkerSlot:       1,
			CPUThreads:       4,
			ParallelGroup:    "g0",
		},
	}

	segments := NewAssembler().Assemble(run, AssemblyContext{}).Segments
	require.Len(t, segments, 1)
	seg := segments[0]
	require.Equal(t, 0, seg.SegmentIndex)
	require.Equal(t, "scene_1", seg.SceneID)
	require.Equal(t, "clip", seg.SourceType)
	require.Equal(t, 12000.0, seg.DurationMS)
	require.Equal(t, 100.0, seg.AssetDownloadMS)
	require.Equal(t, 8000.0, seg.FfmpegEncodeMS)
	require.Equal(t, int64(60_000_000), seg.SourceBytes)
	require.Equal(t, int64(60_000_000), seg.OutputBytes)
	require.Equal(t, int64(2500), seg.FramesEncoded)
	require.Equal(t, int64(2500), seg.FramesDecoded)
	require.Equal(t, "h264", seg.Codec)
	require.Equal(t, "ok", seg.Status)
	require.Equal(t, 12000.0, seg.FinishedOffsetMS)
	require.Equal(t, 1, seg.WorkerSlot)
	require.Equal(t, 4, seg.CPUThreads)
	require.Equal(t, "g0", seg.ParallelGroup)
}

func TestAssembler_DetailedPhasesCarryFramesInOut(t *testing.T) {
	run := sampleRun()
	run.RenderMetrics.DetailedPhases = []pipeline.DetailedPhaseTiming{
		{Component: "packet", Action: "write", DurationMS: 100, FramesIn: 5000, FramesOut: 5000},
	}

	phases := NewAssembler().Assemble(run, AssemblyContext{}).Phases
	require.Len(t, phases, 1)
	require.Equal(t, int64(5000), phases[0].FramesIn)
	require.Equal(t, int64(5000), phases[0].FramesOut)
}

func TestAssembler_PhaseNameFallbacks(t *testing.T) {
	run := sampleRun()
	run.RenderMetrics.DetailedPhases = []pipeline.DetailedPhaseTiming{
		{Phase: "encode", DurationMS: 1},    // phase label only
		{EventType: "event", DurationMS: 2}, // event type only
		{},                                  // nothing → "unknown"
	}

	phases := NewAssembler().Assemble(run, AssemblyContext{}).Phases
	require.Len(t, phases, 3)
	require.Equal(t, "encode", phases[0].Name)
	require.Equal(t, "event", phases[1].Name)
	require.Equal(t, "unknown", phases[2].Name)
}

func TestAssembler_ExecutorClocksStayZero(t *testing.T) {
	// RunMetrics does not carry executor/artifact clocks; the assembler
	// must leave them zero instead of inventing values.
	receipt := NewAssembler().Assemble(sampleRun(), AssemblyContext{})
	require.Zero(t, receipt.Timing.ExecutorTotalMs)
	require.Zero(t, receipt.Timing.ArtifactTotalMs)
}

func TestAssembler_WallOverrideWins(t *testing.T) {
	receipt := NewAssembler().Assemble(sampleRun(), AssemblyContext{WallMs: 19340})
	require.Equal(t, int64(19340), receipt.Timing.WallMs)
	// The CPU section shares the same resolved wall clock.
	require.Equal(t, int64(19340), receipt.CPU.WallMs)
	require.InDelta(t, 1800.0/19340.0, receipt.CPU.CPUWallRatio, 1e-9)
}

func TestAssembler_WorkloadAndIdentity(t *testing.T) {
	ctx := AssemblyContext{
		Identity: PerformanceIdentity{
			JobID:     "job_1",
			AttemptID: "attempt_2",
			WorkerID:  "worker_3",
			GitCommit: "abc123",
		},
		Workload: WorkloadProfile{
			JobType:    "process_video",
			CopyOnly:   true,
			VideoCodec: "h264",
		},
	}

	receipt := NewAssembler().Assemble(sampleRun(), ctx)

	require.Equal(t, ctx.Identity, receipt.Identity)
	require.Equal(t, "process_video", receipt.Workload.JobType)
	require.True(t, receipt.Workload.CopyOnly)
}

// TestAssembler_DoesNotInferClipCount pins the no-guess rule: the assembler
// must NEVER reconstruct clip_count from telemetry (timeline_items=25 is
// present in sampleRun). The workload comes exclusively from the caller,
// who builds it from the CompiledRenderPlanV2.
func TestAssembler_DoesNotInferClipCountFromTimelineItems(t *testing.T) {
	if sampleRun().TimelineItems != 25 {
		t.Fatalf("sample fixture lost its timeline_items; this pin lost its point")
	}
	receipt := NewAssembler().Assemble(sampleRun(), AssemblyContext{})
	require.Zero(t, receipt.Workload.ClipCount, "clip_count must not be inferred from timeline items")
}

func TestAssembler_WorkloadClipCountOverrideWins(t *testing.T) {
	ctx := AssemblyContext{Workload: WorkloadProfile{ClipCount: 30}}
	receipt := NewAssembler().Assemble(sampleRun(), ctx)
	require.Equal(t, 30, receipt.Workload.ClipCount)
}

// TestAssembler_WorkloadFromCompiledRenderPlan verifies the authoritative
// workload constructor: clip count and expected duration come from the
// CompiledRenderPlanV2 (the render_plan owner), never from telemetry.
func TestAssembler_WorkloadFromCompiledRenderPlan(t *testing.T) {
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion: 2,
		DurationUS:  2_500_000,
		Output: contract.OutputContractV2{
			Container:  "mp4",
			VideoCodec: "h264",
			Width:      640,
			Height:     360,
			FPSNum:     30,
			FPSDen:     1,
		},
		FinalAudio: contract.FinalAudioV2{Mode: contract.AudioModeFinalAudioCopy, Codec: "aac"},
		VideoTracks: []contract.VideoTrackV2{
			{TrackID: "main", Segments: make([]contract.VideoSegmentV2, 2)},
			{TrackID: "overlay", Segments: make([]contract.VideoSegmentV2, 1)},
		},
		Assets: make([]contract.AssetRefV2, 4),
	}

	workload := WorkloadFromCompiledRenderPlan(plan)
	require.Equal(t, int64(2_500_000), workload.DurationUS)
	require.Equal(t, 3, workload.ClipCount)
	require.Equal(t, 4, workload.AssetCount)
	require.True(t, workload.FinalAudioCopy)
	require.Equal(t, "h264", workload.VideoCodec)
	require.Equal(t, "aac", workload.AudioCodec)
	require.Equal(t, 640, workload.Width)
	require.Equal(t, 360, workload.Height)
	require.Equal(t, 30.0, workload.FPS)

	// A nil plan yields an empty profile; the assembler maps it verbatim.
	require.Zero(t, WorkloadFromCompiledRenderPlan(nil).ClipCount)
	require.Zero(t, WorkloadFromCompiledRenderPlan(nil).DurationUS)
}

func TestAssembler_ReceiptMarshals(t *testing.T) {
	ctx := AssemblyContext{
		Identity: PerformanceIdentity{JobID: "job_1"},
		WallMs:   19340,
	}
	receipt := NewAssembler().Assemble(sampleRun(), ctx)

	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	require.True(t, json.Valid(data))
	require.Contains(t, string(data), `"render_ms":18910`)
	require.Contains(t, string(data), `"engine_spawn_count":1`)
	require.Contains(t, string(data), `"cpu_user_ms":1500`)
	require.Contains(t, string(data), `"peak_rss_bytes":320000000`)
}
