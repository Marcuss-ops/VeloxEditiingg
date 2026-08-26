package telemetry
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

