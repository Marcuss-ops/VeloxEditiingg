package telemetry

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
