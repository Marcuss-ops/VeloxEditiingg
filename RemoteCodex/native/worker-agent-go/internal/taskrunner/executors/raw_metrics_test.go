package executors

import (
	"testing"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

func TestRawMetricsFromFFmpegResultProjectsObservedFacts(t *testing.T) {
	got := rawMetricsFromFFmpegResult(ffmpegrunner.FFmpegResult{
		UserCPUMs: 7, SystemCPUMs: 3, PeakRSSBytes: 100,
		ReadBytes: 200, WriteBytes: 300, ProcessWallMS: 340,
	})
	want := telemetry.RawExecutionMetrics{
		CpuTimeMs: 10, PeakRssBytes: 100, DiskReadBytes: 200,
		DiskWriteBytes: 300, WallClockSeconds: 0.34,
	}
	if *got != want {
		t.Fatalf("FFmpeg raw facts = %+v, want %+v", *got, want)
	}
}

func TestMergeRawFFmpegMetricsPreservesHighWaterMarkAndAddsCounters(t *testing.T) {
	dst := &telemetry.RawExecutionMetrics{
		OutputBytes:  999,
		PeakRssBytes: 500,
	}
	mergeRawFFmpegMetrics(dst, &telemetry.RawExecutionMetrics{
		CpuTimeMs: 10, PeakRssBytes: 1000, DiskReadBytes: 20,
		DiskWriteBytes: 30, WallClockSeconds: 0.25,
	})
	mergeRawFFmpegMetrics(dst, &telemetry.RawExecutionMetrics{
		CpuTimeMs: 5, PeakRssBytes: 700, DiskReadBytes: 4,
		DiskWriteBytes: 6, WallClockSeconds: 0.10,
	})
	if dst.CpuTimeMs != 15 || dst.DiskReadBytes != 24 || dst.DiskWriteBytes != 36 || dst.WallClockSeconds != 0.35 || dst.PeakRssBytes != 1000 {
		t.Fatalf("merged FFmpeg facts = %+v", *dst)
	}
	if dst.OutputBytes != 999 {
		t.Fatalf("merge overwrote non-FFmpeg owner OutputBytes = %d", dst.OutputBytes)
	}
}

func TestRawMetricsFromFFmpegResultClampsInvalidNegativeFacts(t *testing.T) {
	got := rawMetricsFromFFmpegResult(ffmpegrunner.FFmpegResult{
		UserCPUMs: -1, SystemCPUMs: 2, PeakRSSBytes: -3,
		ReadBytes: -4, WriteBytes: -5, ProcessWallMS: -6,
	})
	if got.CpuTimeMs != 1 || got.PeakRssBytes != 0 || got.DiskReadBytes != 0 || got.DiskWriteBytes != 0 || got.WallClockSeconds != 0 {
		t.Fatalf("negative FFmpeg facts were not normalized: %+v", *got)
	}
}
