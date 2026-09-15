package executors

import (
	"testing"

	"velox-worker-agent/pkg/performance"
	"velox-worker-agent/pkg/video/pipeline"
)

func TestRawMetricsFromPipelineUsesTypedRawFacts(t *testing.T) {
	run := pipeline.RunMetrics{
		TotalMs: 2500,
		RenderMetrics: pipeline.RenderMetrics{
			Frames:           42,
			FramesDecoded:    40,
			FramesComposited: 41,
			SpeedX:           1.25,
			EncodePasses:     2,
			DurationSec:      2.5,
			TempBytes:        77,
			PeakRSSBytes:     8192,
		},
	}
	io := performance.IOMetrics{AssetBytesRead: 1000, TotalBytesRead: 2000, TotalBytesWritten: 3000, TempBytesWritten: 77, FinalBytesWritten: 4000}
	cpu := performance.CPUMetrics{CPUTotalMs: 900}

	got := rawMetricsFromPipeline(run, io, cpu)
	if got == nil {
		t.Fatal("raw metrics are nil")
	}
	if got.InputBytes != 1000 || got.OutputBytes != 4000 || got.CpuTimeMs != 900 {
		t.Fatalf("raw IO/CPU facts = %+v", *got)
	}
	if got.FramesEncoded != 42 || got.FramesDecoded != 40 || got.FramesComposited != 41 {
		t.Fatalf("raw frame facts = %+v", *got)
	}
	if got.FfmpegSpeedRatio != 1.25 || got.EncodePasses != 2 || got.WallClockSeconds != 2.5 {
		t.Fatalf("raw engine/timing facts = %+v", *got)
	}
}

func TestRawMetricsFromPipelineUsesAggregatePacketCopyCounters(t *testing.T) {
	run := pipeline.RunMetrics{RenderMetrics: pipeline.RenderMetrics{
		ConcatMode:        "mixed_packet",
		CopySegments:      51,
		TranscodeSegments: 0,
	}}

	got := rawMetricsFromPipeline(run, performance.IOMetrics{}, performance.CPUMetrics{})
	if got.SegmentsTotal != 51 || got.SegmentsPacketCopy != 51 ||
		got.SegmentsReencoded != 0 || got.PacketCopyRatio != 100 {
		t.Fatalf("aggregate packet-copy facts = %+v", *got)
	}
}

func TestRawMetricsFromPipelineUsesMixedPacketInvariant(t *testing.T) {
	run := pipeline.RunMetrics{RenderMetrics: pipeline.RenderMetrics{
		ConcatMode:   "mixed_packet",
		EncodePasses: 0,
	}}

	got := rawMetricsFromPipeline(run, performance.IOMetrics{}, performance.CPUMetrics{})
	if got.PacketCopyRatio != 100 {
		t.Fatalf("mixed packet invariant ratio = %v, want 100", got.PacketCopyRatio)
	}
}
