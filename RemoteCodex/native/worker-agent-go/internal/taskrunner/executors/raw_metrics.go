package executors

import (
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

// rawMetricsFromFFmpegResult projects observed FFmpeg process facts into the
// canonical raw envelope. It carries CPU, memory, I/O, wall time, and process
// spawn timing facts owned by the process runner. Derived ratios, expected
// frame counts and operation KPIs remain projections and are not fabricated.
func rawMetricsFromFFmpegResult(result ffmpegrunner.FFmpegResult) *telemetry.RawExecutionMetrics {
	return &telemetry.RawExecutionMetrics{
		CpuTimeMs:        nonNegative(result.UserCPUMs + result.SystemCPUMs),
		PeakRssBytes:     nonNegative(result.PeakRSSBytes),
		DiskReadBytes:    nonNegative(result.ReadBytes),
		DiskWriteBytes:   nonNegative(result.WriteBytes),
		WallClockSeconds: float64(nonNegative(result.ProcessWallMS)) / 1000,
		// Per-process spawn metrics: each FFmpegResult = one ffmpeg process.
		FfmpegExecCount:   1,
		ProcessSpawnCount: 1,
		FfmpegProcessMs:   nonNegative(result.ProcessWallMS),
		ProcessStartupMs:  nonNegative(result.ProcessSpawnMS),
	}
}

// mergeRawFFmpegMetrics combines process-runner facts without overwriting
// facts from another producer. CPU, I/O, wall time, and process spawn counts
// are additive across processes; peak RSS is a high-water mark. The caller
// owns any artifact, media-quality or engine facts and those remain untouched.
func mergeRawFFmpegMetrics(dst, src *telemetry.RawExecutionMetrics) {
	if dst == nil || src == nil {
		return
	}
	dst.CpuTimeMs += src.CpuTimeMs
	dst.DiskReadBytes += src.DiskReadBytes
	dst.DiskWriteBytes += src.DiskWriteBytes
	dst.WallClockSeconds += src.WallClockSeconds
	dst.FfmpegExecCount += src.FfmpegExecCount
	dst.ProcessSpawnCount += src.ProcessSpawnCount
	dst.FfmpegProcessMs += src.FfmpegProcessMs
	dst.ProcessStartupMs += src.ProcessStartupMs
	if src.PeakRssBytes > dst.PeakRssBytes {
		dst.PeakRssBytes = src.PeakRssBytes
	}
}

func nonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
