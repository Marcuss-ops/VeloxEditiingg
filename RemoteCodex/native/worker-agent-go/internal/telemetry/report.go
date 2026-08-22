// report.go — aggregated job performance report.
//
// PerformanceReport consumes a JobPhaseTimer and optional GPUStats to produce
// a comprehensive, human-readable performance breakdown. It computes derived
// metrics (percentages, real-time factor, critical path) and formats a
// multi-section report suitable for post-job logging and dashboarding.
//
// Usage:
//
//	timer := NewJobPhaseTimer()
//	// ... run job, calling timer.Begin/End around each phase ...
//	gpuStats := gpuSampler.Stats()
//	report := BuildPerformanceReport(timer, rawMetrics, gpuStats)
//	fmt.Println(report.Format())
package telemetry

import (
	"fmt"
	"sort"
	"strings"
)

// ── PerformanceReport ──────────────────────────────────────────────────────

// PerformanceReport is the aggregated result of a single job execution.
// It includes wall clock timings, phase breakdowns, GPU/CPU stats, and
// transfer metrics. All durations are in milliseconds.
type PerformanceReport struct {
	// ── Summary ────────────────────────────────────────────────────
	MediaDurationSeconds float64
	WallClockSeconds     float64
	RealTimeFactor       float64 // wall / media (lower is better)
	ThroughputX          float64 // media / wall (higher is better)

	// ── Input ──────────────────────────────────────────────────────
	SceneCount       int
	SourceDurationMs int64
	OutputDurationMs int64
	TotalInputBytes  int64

	// ── Cache ──────────────────────────────────────────────────────
	CacheHitCount  int64
	CacheMissCount int64
	CacheHitRatio  float64
	CacheHitBytes  int64
	CacheMissBytes int64

	// ── Phase breakdown (ordered, non-zero phases only) ────────────
	Phases []PhaseBreakdown

	// ── Segment stats ──────────────────────────────────────────────
	SegmentsTotal     int32
	SegmentsPacketCopy int32
	SegmentsReencoded  int32
	SegmentsComposited int32
	PacketCopyBytes    int64
	ReencodedBytes     int64
	PacketCopyRatio    float64

	// ── GPU ────────────────────────────────────────────────────────
	GPUUtilAvgPct    float64
	GPUUtilPeakPct   float64
	NVDECUtilAvgPct   float64
	NVDECUtilPeakPct  float64
	NVENCUtilAvgPct   float64
	NVENCUtilPeakPct  float64
	VRAMUsedAvgBytes  int64
	VRAMUsedPeakBytes int64
	GPUIdleMs         int64

	// ── GPU transfers ──────────────────────────────────────────────
	FramesDownloadedGPU int64
	FramesUploadedGPU   int64
	GPUToCPUBytes       int64
	CPUToGPUBytes       int64

	// ── CPU ────────────────────────────────────────────────────────
	CPUPercentAvg  float64
	CPUPercentPeak float64
	CPUTotalMs     int64
	PeakRSSBytes   int64

	// ── Output ─────────────────────────────────────────────────────
	OutputBytes    int64
	OutputSHA256   string
	FfprobeValid   bool
	DurationDiffSec float64

	// ── Audio ──────────────────────────────────────────────────────
	AudioCopyMs   int64
	AudioEncodeMs int64
	AudioPacketsCopied  int64
	AudioPacketsEncoded int64
	AudioInputBytes     int64
	AudioOutputBytes    int64

	// ── Per-scene breakdown ────────────────────────────────────────
	TopSlowestScenes []SceneReport

	// ── Critical path ──────────────────────────────────────────────
	CriticalPathComponent string
	CriticalPathMs        int64
	CriticalPathPercent   float64

	// ── Process spawn ──────────────────────────────────────────────
	FFmpegExecCount   int64
	FFprobeExecCount  int64
	ProcessSpawnCount int64
	FFmpegProcessMs   int64
	FFprobeProcessMs  int64
	ProcessStartupMs  int64

	// ── Download / Disk ────────────────────────────────────────────
	DriveDownloadMs      int64
	BlobstoreDownloadMs  int64
	LocalCacheReadMs     int64
	AssetDownloadWaitMs  int64
	DownloadMbpsAvg      float64
	UploadMbpsAvg        float64
	DriveUploadMbps      float64
	ArtifactDownloadMbps float64
	DiskReadMs           int64
	DiskWriteMs          int64
	OutputWriteMs        int64
	TempWriteMs          int64
	FinalReadMs          int64
}

// PhaseBreakdown is one phase's contribution to the job total.
type PhaseBreakdown struct {
	Name       string
	Label      string
	DurationMs int64
	Percent    float64
	Count      int64
	BytesIn    int64
	BytesOut   int64
	FramesIn   int64
	FramesOut  int64
}

// SceneReport is one scene's aggregated metrics.
type SceneReport struct {
	SceneID          string
	TotalMs          int64
	SourceDurationMs int64
	OutputDurationMs int64
	FramesDecoded    int64
	FramesEncoded    int64
	RenderSpeed      float64
	InputBytes       int64
	OutputBytes      int64
}

// ── Builder ────────────────────────────────────────────────────────────────

// BuildPerformanceReport constructs a report from the job timer, raw metrics,
// and optional GPU stats.
func BuildPerformanceReport(
	timer *JobPhaseTimer,
	raw *RawExecutionMetrics,
	gpu GPUStats,
) PerformanceReport {
	r := PerformanceReport{}

	if raw != nil {
		r.MediaDurationSeconds = raw.MediaDurationSeconds
		r.WallClockSeconds = raw.WallClockSeconds
		r.TotalInputBytes = raw.InputBytes
		r.OutputBytes = raw.OutputBytes
		r.OutputSHA256 = raw.OutputSha256
		r.FfprobeValid = raw.FfprobeValid == 1
		r.DurationDiffSec = raw.DurationDiffSec
		r.CPUPercentPeak = raw.CpuPercentPeak
		r.CPUTotalMs = raw.CpuTimeMs
		r.PeakRSSBytes = raw.PeakRssBytes
		r.CacheHitCount = raw.AssetCacheHitCount
		r.CacheMissCount = raw.AssetCacheMissCount
		r.DriveDownloadMs = raw.DriveDownloadMs
		r.BlobstoreDownloadMs = raw.BlobstoreDownloadMs
		r.LocalCacheReadMs = raw.LocalCacheReadMs
		r.AssetDownloadWaitMs = raw.AssetDownloadWaitMs
		r.CacheHitBytes = raw.CacheHitBytes
		r.CacheMissBytes = raw.CacheMissBytes
		r.DownloadMbpsAvg = raw.DownloadMbpsAvg
		r.UploadMbpsAvg = raw.UploadMbpsAvg
		r.DriveUploadMbps = raw.DriveUploadMbps
		r.ArtifactDownloadMbps = raw.ArtifactDownloadMbps
		r.OutputWriteMs = raw.OutputWriteMs
		r.TempWriteMs = raw.TempWriteMs
		r.DiskReadMs = raw.DiskReadMs
		r.DiskWriteMs = raw.DiskWriteMs
		r.FinalReadMs = raw.FinalReadMs
		r.FFmpegExecCount = raw.FfmpegExecCount
		r.FFprobeExecCount = raw.FfprobeExecCount
		r.ProcessSpawnCount = raw.ProcessSpawnCount
		r.FFmpegProcessMs = raw.FfmpegProcessMs
		r.FFprobeProcessMs = raw.FfprobeProcessMs
		r.ProcessStartupMs = raw.ProcessStartupMs
		r.AudioCopyMs = raw.AudioCopyMs
		r.AudioEncodeMs = raw.AudioEncodeMs
		r.AudioPacketsCopied = raw.AudioPacketCopy
		r.AudioPacketsEncoded = raw.AudioReencoded
		r.AudioInputBytes = raw.AudioInputBytes
		r.AudioOutputBytes = raw.AudioOutputBytes
	}

	// Compute RTF and throughput.
	// Prefer precomputed values from RawExecutionMetrics when available.
	if raw != nil && raw.RealTimeFactor != 0 {
		r.RealTimeFactor = raw.RealTimeFactor
		r.ThroughputX = raw.ThroughputX
	} else if r.MediaDurationSeconds > 0 && r.WallClockSeconds > 0 {
		r.RealTimeFactor = r.WallClockSeconds / r.MediaDurationSeconds
		r.ThroughputX = r.MediaDurationSeconds / r.WallClockSeconds
	}

	// Cache hit ratio.
	totalCache := r.CacheHitCount + r.CacheMissCount
	if totalCache > 0 {
		r.CacheHitRatio = float64(r.CacheHitCount) / float64(totalCache) * 100
	}

	// Phase breakdown.
	if timer != nil {
		phases := timer.PhaseTimings()
		totalJobMs := int64(0)
		// First pass: compute job total from non-zero phases for percentages.
		for _, p := range phases {
			if p.Timing.Duration > 0 {
				totalJobMs += p.Timing.DurationMs()
			}
		}
		for _, p := range phases {
			if p.Timing.Duration == 0 && p.Timing.BytesIn == 0 && p.Timing.BytesOut == 0 {
				continue // Skip truly empty phases for readability.
			}
			pct := float64(0)
			if totalJobMs > 0 {
				pct = float64(p.Timing.DurationMs()) / float64(totalJobMs) * 100
			}
			r.Phases = append(r.Phases, PhaseBreakdown{
				Name:       p.Name,
				Label:      PhaseDisplayNames[p.Name],
				DurationMs: p.Timing.DurationMs(),
				Percent:    round2(pct),
				Count:      p.Timing.Count,
				BytesIn:    p.Timing.BytesIn,
				BytesOut:   p.Timing.BytesOut,
				FramesIn:   p.Timing.FramesIn,
				FramesOut:  p.Timing.FramesOut,
			})
		}

		// Scene breakdown.
		scenes := timer.SceneTimings()
		r.SceneCount = len(scenes)
		for _, s := range scenes {
			r.TopSlowestScenes = append(r.TopSlowestScenes, SceneReport{
				SceneID:          s.SceneID,
				TotalMs:          s.Timing.TotalMs(),
				SourceDurationMs: s.Timing.SourceDurationMs,
				OutputDurationMs: s.Timing.OutputDurationMs,
				FramesDecoded:    s.Timing.FramesDecoded,
				FramesEncoded:    s.Timing.FramesEncoded,
				RenderSpeed:      round2(s.Timing.RenderSpeed()),
				InputBytes:       s.Timing.InputBytes,
				OutputBytes:      s.Timing.OutputBytes,
			})
		}

		// Source/Output durations from scenes if not set by raw metrics.
		if r.SourceDurationMs == 0 {
			for _, s := range scenes {
				r.SourceDurationMs += s.Timing.SourceDurationMs
			}
		}
		if r.OutputDurationMs == 0 {
			for _, s := range scenes {
				r.OutputDurationMs += s.Timing.OutputDurationMs
			}
		}

		// Critical path: prefer precomputed value from RawExecutionMetrics.
		if raw != nil && raw.CriticalPathMs > 0 {
			r.CriticalPathComponent = raw.CriticalPathComponent
			r.CriticalPathMs = raw.CriticalPathMs
			r.CriticalPathPercent = raw.CriticalPathPercent
		} else {
			// Fallback: find the phase with the highest duration.
			for _, p := range r.Phases {
				if p.DurationMs > r.CriticalPathMs {
					r.CriticalPathMs = p.DurationMs
					r.CriticalPathComponent = p.Name
					r.CriticalPathPercent = p.Percent
				}
			}
		}
	}

	// GPU.
	if gpu.SampleCount > 0 {
		r.GPUUtilAvgPct = gpu.GPUUtilAvgPct
		r.GPUUtilPeakPct = gpu.GPUUtilPeakPct
		r.NVDECUtilAvgPct = gpu.NVDECUtilAvgPct
		r.NVDECUtilPeakPct = gpu.NVDECUtilPeakPct
		r.NVENCUtilAvgPct = gpu.NVENCUtilAvgPct
		r.NVENCUtilPeakPct = gpu.NVENCUtilPeakPct
		r.VRAMUsedAvgBytes = gpu.VRAMUsedAvgBytes
		r.VRAMUsedPeakBytes = gpu.VRAMUsedPeakBytes
		r.GPUIdleMs = gpu.GPUIdleDuringRenderMs
	}

	return r
}

// SetGPUTransferMetrics attaches VRAM ↔ RAM transfer data.
func (r *PerformanceReport) SetGPUTransferMetrics(g GPUTransferMetrics) {
	r.FramesDownloadedGPU = g.FramesDownloadedGPU
	r.FramesUploadedGPU = g.FramesUploadedGPU
	r.GPUToCPUBytes = g.GPUToCPUBytes
	r.CPUToGPUBytes = g.CPUToGPUBytes
}

// SetSegmentStats records packet-copy vs re-encode breakdown.
func (r *PerformanceReport) SetSegmentStats(total, packetCopy, reencoded, composited int32, packetCopyBytes, reencodedBytes int64, packetCopyMs, reencodeMs int64) {
	r.SegmentsTotal = total
	r.SegmentsPacketCopy = packetCopy
	r.SegmentsReencoded = reencoded
	r.SegmentsComposited = composited
	r.PacketCopyBytes = packetCopyBytes
	r.ReencodedBytes = reencodedBytes
	if total > 0 {
		r.PacketCopyRatio = float64(packetCopy) / float64(total) * 100
	}
}

// SetAudioStats records audio encode/copy breakdown.
func (r *PerformanceReport) SetAudioStats(audioCopyMs, audioEncodeMs int64, packetsCopied, packetsEncoded int64, inputBytes, outputBytes int64) {
	r.AudioCopyMs = audioCopyMs
	r.AudioEncodeMs = audioEncodeMs
	r.AudioPacketsCopied = packetsCopied
	r.AudioPacketsEncoded = packetsEncoded
	r.AudioInputBytes = inputBytes
	r.AudioOutputBytes = outputBytes
}

// SetProcessStats records ffmpeg/ffprobe process spawn metrics.
func (r *PerformanceReport) SetProcessStats(ffmpegExecs, ffprobeExecs, spawns int64, ffmpegMs, ffprobeMs, startupMs int64) {
	r.FFmpegExecCount = ffmpegExecs
	r.FFprobeExecCount = ffprobeExecs
	r.ProcessSpawnCount = spawns
	r.FFmpegProcessMs = ffmpegMs
	r.FFprobeProcessMs = ffprobeMs
	r.ProcessStartupMs = startupMs
}

// SetDiskStats records disk I/O timing.
func (r *PerformanceReport) SetDiskStats(diskReadMs, diskWriteMs, outputWriteMs, tempWriteMs int64) {
	r.DiskReadMs = diskReadMs
	r.DiskWriteMs = diskWriteMs
	r.OutputWriteMs = outputWriteMs
	r.TempWriteMs = tempWriteMs
}

// SetBandwidth records download/upload throughput in MB/s.
func (r *PerformanceReport) SetBandwidth(downloadMBps, uploadMBps float64) {
	r.DownloadMbpsAvg = round2(downloadMBps)
	r.UploadMbpsAvg = round2(uploadMBps)
}

// ── Formatting ─────────────────────────────────────────────────────────────

const reportSeparator = "────────────────────────────────────"

// Format produces the human-readable performance report.
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