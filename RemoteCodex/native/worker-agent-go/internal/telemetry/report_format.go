package telemetry

import (
	"fmt"
	"sort"
	"strings"
)

func (r PerformanceReport) Format() string {
	var b strings.Builder

	b.WriteString("\nVELOX JOB PERFORMANCE REPORT\n")
	b.WriteString(reportSeparator + "\n\n")

	// Summary.
	b.WriteString(fmt.Sprintf("Video duration %12.3f s\n", r.MediaDurationSeconds))
	b.WriteString(fmt.Sprintf("Wall time      %12.3f s\n", r.WallClockSeconds))
	b.WriteString(fmt.Sprintf("Speed          %12.3fx\n\n", r.ThroughputX))

	// Input.
	b.WriteString("INPUT\n")
	b.WriteString(fmt.Sprintf("%d scenes\n", r.SceneCount))
	b.WriteString(fmt.Sprintf("%.3f s source\n", float64(r.SourceDurationMs)/1000))
	b.WriteString(fmt.Sprintf("%.3f s output\n", float64(r.OutputDurationMs)/1000))
	b.WriteString(fmt.Sprintf("%s downloaded\n\n", formatBytes(r.TotalInputBytes)))

	// Cache.
	if r.CacheHitCount+r.CacheMissCount > 0 {
		b.WriteString("CACHE\n")
		b.WriteString(fmt.Sprintf("hit %20d\n", r.CacheHitCount))
		b.WriteString(fmt.Sprintf("miss %19d\n", r.CacheMissCount))
		b.WriteString(fmt.Sprintf("hit ratio %14.1f%%\n", r.CacheHitRatio))
		if r.CacheHitBytes > 0 || r.CacheMissBytes > 0 {
			b.WriteString(fmt.Sprintf("hit bytes %13s\n", formatBytes(r.CacheHitBytes)))
			b.WriteString(fmt.Sprintf("miss bytes %12s\n", formatBytes(r.CacheMissBytes)))
		}
		if r.DriveDownloadMs > 0 {
			b.WriteString(fmt.Sprintf("drive dl %15.1f s\n", float64(r.DriveDownloadMs)/1000))
		}
		if r.BlobstoreDownloadMs > 0 {
			b.WriteString(fmt.Sprintf("blob dl %16.1f s\n", float64(r.BlobstoreDownloadMs)/1000))
		}
		if r.LocalCacheReadMs > 0 {
			b.WriteString(fmt.Sprintf("cache read %14.1f s\n", float64(r.LocalCacheReadMs)/1000))
		}
		if r.AssetDownloadWaitMs > 0 {
			b.WriteString(fmt.Sprintf("download wait %11.1f s\n", float64(r.AssetDownloadWaitMs)/1000))
		}
		b.WriteString("\n")
	}

	// Phases.
	b.WriteString("PHASES\n")
	for _, p := range r.Phases {
		if p.DurationMs == 0 && p.BytesIn == 0 && p.BytesOut == 0 {
			continue
		}
		label := p.Label
		if label == "" {
			label = p.Name
		}
		dur := fmt.Sprintf("%.1f s", float64(p.DurationMs)/1000)
		pct := fmt.Sprintf("%.1f%%", p.Percent)
		b.WriteString(fmt.Sprintf("%-25s %10s %8s\n", label, dur, pct))
	}
	b.WriteString("\n")

	// GPU.
	if r.GPUUtilAvgPct > 0 || r.GPUUtilPeakPct > 0 {
		b.WriteString("GPU\n")
		b.WriteString(fmt.Sprintf("GPU avg  %16.0f%%\n", r.GPUUtilAvgPct))
		b.WriteString(fmt.Sprintf("GPU peak %16.0f%%\n", r.GPUUtilPeakPct))
		b.WriteString(fmt.Sprintf("NVDEC avg %15.0f%%\n", r.NVDECUtilAvgPct))
		b.WriteString(fmt.Sprintf("NVENC avg %15.0f%%\n", r.NVENCUtilAvgPct))
		b.WriteString(fmt.Sprintf("Peak VRAM %14s\n\n", formatBytes(r.VRAMUsedPeakBytes)))
	}

	// Transfers.
	if r.FramesDownloadedGPU > 0 || r.FramesUploadedGPU > 0 {
		b.WriteString("TRANSFERS\n")
		b.WriteString(fmt.Sprintf("GPU→CPU frames %9d\n", r.FramesDownloadedGPU))
		b.WriteString(fmt.Sprintf("CPU→GPU frames %9d\n", r.FramesUploadedGPU))
		b.WriteString(fmt.Sprintf("GPU→CPU bytes %10s\n", formatBytes(r.GPUToCPUBytes)))
		b.WriteString(fmt.Sprintf("CPU→GPU bytes %10s\n\n", formatBytes(r.CPUToGPUBytes)))
	}

	// CPU.
	if r.CPUTotalMs > 0 {
		b.WriteString("CPU\n")
		b.WriteString(fmt.Sprintf("CPU avg  %16.0f%%\n", r.CPUPercentAvg))
		b.WriteString(fmt.Sprintf("CPU peak %16.0f%%\n", r.CPUPercentPeak))
		b.WriteString(fmt.Sprintf("Peak RSS %14s\n\n", formatBytes(r.PeakRSSBytes)))
	}

	// Segments.
	if r.SegmentsTotal > 0 {
		b.WriteString("SEGMENTS\n")
		b.WriteString(fmt.Sprintf("total        %11d\n", r.SegmentsTotal))
		b.WriteString(fmt.Sprintf("packet copy  %11d\n", r.SegmentsPacketCopy))
		b.WriteString(fmt.Sprintf("re-encoded   %11d\n", r.SegmentsReencoded))
		b.WriteString(fmt.Sprintf("composited   %11d\n", r.SegmentsComposited))
		b.WriteString(fmt.Sprintf("pc ratio     %11.1f%%\n\n", r.PacketCopyRatio))
	}

	// Output.
	b.WriteString("OUTPUT\n")
	b.WriteString(fmt.Sprintf("%s\n", formatBytes(r.OutputBytes)))
	if r.OutputSHA256 != "" {
		if len(r.OutputSHA256) > 16 {
			b.WriteString(fmt.Sprintf("SHA256      %s...\n", r.OutputSHA256[:16]))
		} else {
			b.WriteString(fmt.Sprintf("SHA256      %s\n", r.OutputSHA256))
		}
	}
	b.WriteString(fmt.Sprintf("ffprobe valid %8s\n\n", boolToYes(r.FfprobeValid)))

	// Audio.
	if r.AudioPacketsCopied > 0 || r.AudioPacketsEncoded > 0 || r.AudioCopyMs > 0 || r.AudioEncodeMs > 0 {
		b.WriteString("AUDIO\n")
		if r.AudioPacketsCopied > 0 {
			b.WriteString(fmt.Sprintf("packet copy  %11d\n", r.AudioPacketsCopied))
		}
		if r.AudioPacketsEncoded > 0 {
			b.WriteString(fmt.Sprintf("re-encoded  %11d\n", r.AudioPacketsEncoded))
		}
		if r.AudioCopyMs > 0 {
			b.WriteString(fmt.Sprintf("copy time   %11.1f s\n", float64(r.AudioCopyMs)/1000))
		}
		if r.AudioEncodeMs > 0 {
			b.WriteString(fmt.Sprintf("encode time %11.1f s\n", float64(r.AudioEncodeMs)/1000))
		}
		if r.AudioInputBytes > 0 || r.AudioOutputBytes > 0 {
			b.WriteString(fmt.Sprintf("input       %12s\n", formatBytes(r.AudioInputBytes)))
			b.WriteString(fmt.Sprintf("output      %12s\n", formatBytes(r.AudioOutputBytes)))
		}
		b.WriteString("\n")
	}

	// Disk / bandwidth.
	if r.OutputWriteMs > 0 || r.FinalReadMs > 0 || r.DownloadMbpsAvg > 0 {
		b.WriteString("DISK / BANDWIDTH\n")
		if r.OutputWriteMs > 0 {
			b.WriteString(fmt.Sprintf("output write %11.1f s\n", float64(r.OutputWriteMs)/1000))
		}
		if r.TempWriteMs > 0 {
			b.WriteString(fmt.Sprintf("temp write  %12.1f s\n", float64(r.TempWriteMs)/1000))
		}
		if r.FinalReadMs > 0 {
			b.WriteString(fmt.Sprintf("final read  %12.1f s\n", float64(r.FinalReadMs)/1000))
		}
		if r.DownloadMbpsAvg > 0 {
			b.WriteString(fmt.Sprintf("download  %13.0f Mbps\n", r.DownloadMbpsAvg))
		}
		if r.UploadMbpsAvg > 0 {
			b.WriteString(fmt.Sprintf("upload    %13.0f Mbps\n", r.UploadMbpsAvg))
		}
		if r.DriveUploadMbps > 0 {
			b.WriteString(fmt.Sprintf("drive up  %13.0f Mbps\n", r.DriveUploadMbps))
		}
		if r.ArtifactDownloadMbps > 0 {
			b.WriteString(fmt.Sprintf("artifact dl %11.0f Mbps\n", r.ArtifactDownloadMbps))
		}
		b.WriteString("\n")
	}

	// Process spawn.
	if r.FFmpegExecCount > 0 || r.FFprobeExecCount > 0 {
		b.WriteString("PROCESS SPAWN\n")
		if r.FFmpegExecCount > 0 {
			b.WriteString(fmt.Sprintf("ffmpeg execs  %10d\n", r.FFmpegExecCount))
		}
		if r.FFprobeExecCount > 0 {
			b.WriteString(fmt.Sprintf("ffprobe execs %9d\n", r.FFprobeExecCount))
		}
		if r.ProcessSpawnCount > 0 {
			b.WriteString(fmt.Sprintf("total spawns %10d\n", r.ProcessSpawnCount))
		}
		if r.ProcessStartupMs > 0 {
			b.WriteString(fmt.Sprintf("startup     %11.1f s\n", float64(r.ProcessStartupMs)/1000))
		}
		if r.FFmpegProcessMs > 0 {
			b.WriteString(fmt.Sprintf("ffmpeg time %10.1f s\n", float64(r.FFmpegProcessMs)/1000))
		}
		if r.FFprobeProcessMs > 0 {
			b.WriteString(fmt.Sprintf("ffprobe time %9.1f s\n", float64(r.FFprobeProcessMs)/1000))
		}
		b.WriteString("\n")
	}

	// Top scenes.
	if len(r.TopSlowestScenes) > 0 {
		b.WriteString("TOP SLOWEST SCENES\n")
		limit := min(10, len(r.TopSlowestScenes))
		sorted := make([]SceneReport, len(r.TopSlowestScenes))
		copy(sorted, r.TopSlowestScenes)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].TotalMs > sorted[j].TotalMs
		})
		for i := 0; i < limit; i++ {
			s := sorted[i]
			b.WriteString(fmt.Sprintf("%-12s %8.1f s  speed %4.1fx\n", s.SceneID, float64(s.TotalMs)/1000, s.RenderSpeed))
		}
		b.WriteString("\n")
	}

	// Bottleneck (critical path).
	if r.CriticalPathMs > 0 {
		// Handle multi-phase critical paths (e.g. "video.subtitle + video.encode").
		labels := strings.Split(r.CriticalPathComponent, " + ")
		displayLabels := make([]string, 0, len(labels))
		for _, name := range labels {
			name = strings.TrimSpace(name)
			if dl := PhaseDisplayNames[name]; dl != "" {
				displayLabels = append(displayLabels, dl)
			} else {
				displayLabels = append(displayLabels, name)
			}
		}
		b.WriteString("BOTTLENECK\n")
		b.WriteString(fmt.Sprintf("%s\n", strings.Join(displayLabels, " + ")))
		b.WriteString(fmt.Sprintf("%.1f s\n", float64(r.CriticalPathMs)/1000))
		b.WriteString(fmt.Sprintf("%.1f%% of job\n", r.CriticalPathPercent))
	}

	return b.String()
}

// ── Helpers ────────────────────────────────────────────────────────────────

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func boolToYes(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
