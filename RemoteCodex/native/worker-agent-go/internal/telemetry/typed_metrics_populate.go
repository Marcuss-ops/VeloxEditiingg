package telemetry

// CoverageMap decodes the optional report coverage block. Invalid or empty
// JSON is intentionally reported as absent rather than as all-false data.
// PopulateFromJobPhaseTimer fills all fine-grained phase timing fields from
// a JobPhaseTimer that was used to instrument the job execution. This is the
// canonical bridge between the runtime timer and the typed metrics envelope.
func (t *RawExecutionMetrics) PopulateFromJobPhaseTimer(timer *JobPhaseTimer) {
	if t == nil || timer == nil {
		return
	}
	phases := timer.PhaseTimings()
	for _, p := range phases {
		ms := p.Timing.DurationMs()
		if ms == 0 {
			continue
		}
		switch p.Name {
		case PhaseQueueWait:
			t.QueueWaitMs = ms
		case PhaseJobSetup:
			t.JobSetupMs = ms
		case PhaseAssetResolve:
			t.AssetResolveMs = ms
		case PhaseAssetDownload:
			t.AssetDownloadMs = ms
		case PhaseAssetVerify:
			t.AssetVerifyMs = ms
		case PhaseAssetMaterialize:
			t.AssetMaterializeMs = ms
		case PhaseAudioPrepare:
			t.AudioPrepareMs = ms
		case PhaseAudioTimelineBuild:
			t.AudioTimelineBuildMs = ms
		case PhaseRenderPlanBuild:
			t.RenderPlanBuildMs = ms
		case PhaseVideoDecode:
			t.VideoDecodeMs = ms
		case PhaseVideoSubtitle:
			t.VideoSubtitleMs = ms
		case PhaseVideoSubtitleRaster:
			t.VideoSubtitleRasterMs = ms
		case PhaseVideoSubtitleComposite:
			t.VideoSubtitleCompositeMs = ms
		case PhaseVideoWatermark:
			t.VideoWatermarkMs = ms
		case PhaseVideoWatermarkUpload:
			t.VideoWatermarkUploadMs = ms
		case PhaseVideoWatermarkComposite:
			t.VideoWatermarkCompositeMs = ms
		case PhaseVideoBlur:
			t.VideoBlurMs = ms
		case PhaseVideoFilter:
			t.VideoFilterMs = ms
		case PhaseVideoComposite:
			t.VideoCompositeMs = ms
		case PhaseVideoEncode:
			t.VideoEncodeMs = ms
		case PhaseVideoConcat:
			t.VideoConcatMs = ms
		case PhaseAudioMux:
			t.AudioMuxMs = ms
		case PhaseOutputFinalize:
			t.OutputFinalizeMs = ms
		case PhaseArtifactHash:
			t.Sha256Ms = ms
		case PhaseArtifactProbe:
			t.FfprobeMs = ms
		case PhaseArtifactVerify:
			t.ArtifactVerifyMs = ms
		case PhaseDriveUpload:
			t.DriveUploadMs = ms
		case PhaseDriveVerify:
			t.DriveVerifyMs = ms
		case PhaseDriveDownload:
			t.DriveDownloadMs = ms
		case PhaseBlobstoreDownload:
			t.BlobstoreDownloadMs = ms
		case PhaseLocalCacheRead:
			t.LocalCacheReadMs = ms
		case PhaseAssetDownloadWait:
			t.AssetDownloadWaitMs = ms
		case PhaseOutputWrite:
			t.OutputWriteMs = ms
		case PhaseTempWrite:
			t.TempWriteMs = ms
		case PhaseFinalRead:
			t.FinalReadMs = ms
		case PhaseCleanup:
			t.CleanupMs = ms
		}
	}
	// Total = sum of all phases (wall-clock may differ due to parallelism).
	t.JobTotalMs = timer.TotalDuration().Milliseconds()

	// ── Aggregated wall times per resource category (migration 160) ──
	// Render: plan build + video decode/subtitle/watermark/blur/filter/
	//         composite/encode/concat + audio mux + output finalize + hash/probe.
	t.JobRenderWallMs = t.RenderPlanBuildMs + t.VideoDecodeMs +
		t.VideoSubtitleMs + t.VideoSubtitleRasterMs + t.VideoSubtitleCompositeMs +
		t.VideoWatermarkMs + t.VideoWatermarkUploadMs + t.VideoWatermarkCompositeMs +
		t.VideoBlurMs + t.VideoFilterMs + t.VideoCompositeMs +
		t.VideoEncodeMs + t.VideoConcatMs +
		t.AudioMuxMs + t.OutputFinalizeMs +
		t.Sha256Ms + t.FfprobeMs + t.ArtifactVerifyMs
	// Asset: resolve + download + verify + materialize + audio prepare/build.
	t.JobAssetWallMs = t.AssetResolveMs + t.AssetDownloadMs +
		t.AssetVerifyMs + t.AssetMaterializeMs +
		t.AudioPrepareMs + t.AudioTimelineBuildMs +
		t.DriveDownloadMs + t.BlobstoreDownloadMs +
		t.LocalCacheReadMs + t.AssetDownloadWaitMs
	// Publish: drive upload + verify + cleanup.
	t.JobPublishWallMs = t.DriveUploadMs + t.DriveVerifyMs + t.CleanupMs

	// CPU core-seconds from accumulated CPU milliseconds.
	if t.CpuTimeMs > 0 {
		t.JobCpuCoreSeconds = float64(t.CpuTimeMs) / 1000.0
	}

	// ── Download / cache byte attribution ────────────────────────────
	// Accumulate per-source byte counters from phase-level data.
	timer.cacheMut.Lock()
	t.CacheHitBytes = timer.cacheHitBytes
	t.CacheMissBytes = timer.cacheMissBytes
	timer.cacheMut.Unlock()

	// ── Per-phase CPU time attribution ────────────────────────────────
	// Map phase-level accumulated CPU milliseconds to the canonical per-phase
	// CPU fields. These are additive: multiple invocations of the same phase
	// (e.g. across segments) are summed by the timer before reaching here.
	for _, p := range phases {
		cpuMs := p.Timing.CPUMs
		if cpuMs == 0 {
			continue
		}
		// Integer truncation from float64: CPUMS is measured in milliseconds
		// and typical values fit int64 without loss.
		cpu := int64(cpuMs)
		switch p.Name {
		case PhaseVideoSubtitle, PhaseVideoSubtitleRaster, PhaseVideoSubtitleComposite:
			t.SubtitleCpuMs += cpu
		case PhaseVideoBlur:
			t.BlurCpuMs += cpu
		case PhaseVideoComposite:
			t.CompositeCpuMs += cpu
		case PhaseVideoEncode:
			t.EncodeCpuMs += cpu
		case PhaseArtifactHash:
			t.HashCpuMs += cpu
		}
	}
}
func (t *RawExecutionMetrics) PopulateCentralMetrics(
	timer *JobPhaseTimer,
	transfers GPUTransferMetrics,
	gpuSamplerStats GPUStats,
	wallClockSeconds float64,
) {
	if t == nil {
		return
	}
	t.PopulateFromJobPhaseTimer(timer)
	t.PopulateFromGPUTransfers(transfers)
	t.PopulateFromGPUStats(gpuSamplerStats)
	t.WallClockSeconds = wallClockSeconds
	t.ComputeDerivedMetrics()
	t.ComputeDerivedBandwidth()
	t.ComputeCriticalPath()
}

func (t *RawExecutionMetrics) PopulateFromGPUStats(stats GPUStats) {
	if t == nil || stats.SampleCount == 0 {
		return
	}
	t.GpuUUID = stats.GPUUUID
	t.GpuUtilAvgPct = stats.GPUUtilAvgPct
	t.GpuUtilPeakPct = stats.GPUUtilPeakPct
	t.NvdecUtilAvgPct = stats.NVDECUtilAvgPct
	t.NvdecUtilPeakPct = stats.NVDECUtilPeakPct
	t.NvencUtilAvgPct = stats.NVENCUtilAvgPct
	t.NvencUtilPeakPct = stats.NVENCUtilPeakPct
	t.VramUsedAvgBytes = stats.VRAMUsedAvgBytes
	t.GpuIdleMs = stats.GPUIdleDuringRenderMs
}

func (t *RawExecutionMetrics) PopulateCPUStall(avgSteal, peakSteal, avgIOWait, peakIOWait, runQAvg float64, runQPeak int32) {
	if t == nil {
		return
	}
	t.CpuStealAvgPct = avgSteal
	t.CpuStealPeakPct = peakSteal
	t.CpuIOWaitAvgPct = avgIOWait
	t.CpuIOWaitPeakPct = peakIOWait
	t.RunQueueAvg = runQAvg
	t.RunQueuePeak = runQPeak
}

func (t *RawExecutionMetrics) SetFaststart(enabled bool, ms, bytesRewritten int64) {
	if t == nil {
		return
	}
	t.FaststartEnabled = enabled
	t.FaststartMs = ms
	t.FaststartBytesRewritten = bytesRewritten
}

func (t *RawExecutionMetrics) PopulateHTTPMetrics(reqs, reused, newConns, dnsMs, tcpMs, tlsMs, ttfbMs, http2 int64) {
	if t == nil {
		return
	}
	t.HttpRequests = reqs
	t.HttpConnectionReused = reused
	t.HttpNewConnections = newConns
	t.DnsMs = dnsMs
	t.TcpConnectMs = tcpMs
	t.TlsHandshakeMs = tlsMs
	t.TtfbMs = ttfbMs
	t.Http2Requests = http2
}

// PopulateFromGPUTransfers fills VRAM ↔ RAM transfer metrics.
func (t *RawExecutionMetrics) PopulateFromGPUTransfers(g GPUTransferMetrics) {
	if t == nil {
		return
	}
	t.FramesDownloadedFromGPU = g.FramesDownloadedGPU
	t.FramesUploadedToGPU = g.FramesUploadedGPU
	t.GpuToCpuTransferMs = g.GPUToCPUMs
	t.CpuToGpuTransferMs = g.CPUToGPUMs
	t.GpuToCpuBytes = g.GPUToCPUBytes
	t.CpuToGpuBytes = g.CPUToGPUBytes
}

// ResourceSnapshot is a point-in-time resource counter snapshot captured
// at attempt start. The delta between start and end snapshots provides
// per-attempt resource attribution.
type ResourceSnapshot struct {
	RSSPeakBytes   int64
	CPUClockMS     int64
	DiskReadBytes  int64
	DiskWriteBytes int64
	NetworkRxBytes int64
	NetworkTxBytes int64
	IOWaitMS       int64
	OpenFDs        int64
	PageFaults     int64
	PrefetchBytes  int64
	PublishBytes   int64
}

// PopulateJobResourceAttribution fills the per-job resource attribution
// fields from a start snapshot and end metrics. The start snapshot was
// captured before the attempt began; the end metrics come from the
// resource sampler or the attempt telemetry session at completion.
//
// For cumulative counters (disk, network, page faults), the attribution
// is end - start. For peak counters (RSS, FDs), the attribution is the
// end peak value. For cumulative CPU, the attribution is end clock - start clock.
func (t *RawExecutionMetrics) PopulateJobResourceAttribution(start ResourceSnapshot) {
	if t == nil {
		return
	}
	// Peak RSS delta: current peak minus pre-job baseline.
	t.JobPeakRssDeltaBytes = t.PeakRssBytes - start.RSSPeakBytes
	if t.JobPeakRssDeltaBytes < 0 {
		t.JobPeakRssDeltaBytes = 0
	}

	// CPU core-seconds: accumulated CPU time normalized to single-core seconds.
	cpuDeltaMS := t.CpuTimeMs - start.CPUClockMS
	if cpuDeltaMS > 0 {
		t.JobCpuCoreSeconds = float64(cpuDeltaMS) / 1000.0
	}

	// Disk I/O attribution.
	diskReadDelta := t.DiskReadBytes - start.DiskReadBytes
	if diskReadDelta > 0 {
		t.DiskReadBytes = diskReadDelta
	}
	diskWriteDelta := t.DiskWriteBytes - start.DiskWriteBytes
	if diskWriteDelta > 0 {
		t.DiskWriteBytes = diskWriteDelta
	}

	// Network I/O attribution.
	networkRxDelta := t.NetworkRxBytes - start.NetworkRxBytes
	if networkRxDelta > 0 {
		t.NetworkRxBytes = networkRxDelta
	}
	networkTxDelta := t.NetworkTxBytes - start.NetworkTxBytes
	if networkTxDelta > 0 {
		t.NetworkTxBytes = networkTxDelta
	}

	// I/O wait attribution.
	iowaitDelta := t.IowaitMs - start.IOWaitMS
	if iowaitDelta > 0 {
		t.IowaitMs = iowaitDelta
	}

	// Open FDs: peak during attempt.
	// t.OpenFdsPeak is already set by the sampler; no delta needed.

	// Major page faults attribution.
	faultsDelta := t.JobPageFaults - start.PageFaults
	if faultsDelta > 0 {
		t.JobPageFaults = faultsDelta
	}

	// Prefetch and publish bytes are set externally by the caller.
	// t.JobPrefetchBytes and t.JobPublishBytes are already populated
	// by the cache resolution sink and upload path respectively.
}
