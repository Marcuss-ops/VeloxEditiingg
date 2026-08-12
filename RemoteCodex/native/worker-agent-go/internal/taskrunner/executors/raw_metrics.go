package executors

import (
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

// rawMetricsFromFFmpegResult projects observed FFmpeg process facts into the
// canonical raw envelope. It deliberately carries only facts owned by the
// process runner: CPU, peak RSS, disk I/O and process wall time. Derived
// ratios, expected frame counts and operation KPIs remain projections and are
// not fabricated here.
func rawMetricsFromFFmpegResult(result ffmpegrunner.FFmpegResult) *telemetry.RawExecutionMetrics {
	return &telemetry.RawExecutionMetrics{
		CpuTimeMs:        nonNegative(result.UserCPUMs + result.SystemCPUMs),
		PeakRssBytes:     nonNegative(result.PeakRSSBytes),
		DiskReadBytes:    nonNegative(result.ReadBytes),
		DiskWriteBytes:   nonNegative(result.WriteBytes),
		WallClockSeconds: float64(nonNegative(result.ProcessWallMS)) / 1000,
	}
}

// mergeRawFFmpegMetrics combines process-runner facts without overwriting
// facts from another producer. CPU, I/O and wall time are additive across
// processes; peak RSS is a high-water mark. The caller owns any artifact,
// media-quality or engine facts and those fields remain untouched.
func mergeRawFFmpegMetrics(dst, src *telemetry.RawExecutionMetrics) {
	if dst == nil || src == nil {
		return
	}
	dst.CpuTimeMs += src.CpuTimeMs
	dst.DiskReadBytes += src.DiskReadBytes
	dst.DiskWriteBytes += src.DiskWriteBytes
	dst.WallClockSeconds += src.WallClockSeconds
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
