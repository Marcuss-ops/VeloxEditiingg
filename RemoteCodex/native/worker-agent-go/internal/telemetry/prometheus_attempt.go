package telemetry

// prometheus_attempt.go — typed per-attempt raw fact projection.
//
// The Prometheus sink calls RecordAttemptRawMetrics once for each completed
// AttemptSnapshot. The input is the CompleteRawEnvelope, not a legacy map or
// a producer-local report. Counters aggregate observations across attempts;
// the RSS gauge exposes the latest completed attempt because RSS is a
// point-in-time high-water mark. All labels are fixed semantic values.

// RecordAttemptRawMetrics projects typed CPU, RSS, I/O, frame, and process
// facts from one completed attempt. It deliberately does not accept an
// attempt ID, job ID, asset ID, or process ID as a label: those values would
// create unbounded Prometheus cardinality.
func (m *PrometheusMetrics) RecordAttemptRawMetrics(raw CompleteRawEnvelope) {
	m.attemptCPUTimeMs.add("total", nonNegativeMetric(raw.Resources.CpuTimeMs))
	m.attemptPeakRSSBytes.set("total", float64(nonNegativeMetric(raw.Resources.PeakRssBytes)))

	m.attemptIOBytes.add("disk_read", nonNegativeMetric(raw.Resources.DiskReadBytes))
	m.attemptIOBytes.add("disk_write", nonNegativeMetric(raw.Resources.DiskWriteBytes))
	m.attemptIOBytes.add("network_rx", nonNegativeMetric(raw.Resources.NetworkRxBytes))
	m.attemptIOBytes.add("network_tx", nonNegativeMetric(raw.Resources.NetworkTxBytes))

	m.attemptFrames.add("decoded", nonNegativeMetric(raw.Resources.FramesDecoded))
	m.attemptFrames.add("composited", nonNegativeMetric(raw.Resources.FramesComposited))
	m.attemptFrames.add("encoded", nonNegativeMetric(raw.Resources.FramesEncoded))
	m.attemptFrames.add("media_in", nonNegativeMetric(raw.Media.FramesIn))
	m.attemptFrames.add("media_out", nonNegativeMetric(raw.Media.FramesOut))

	m.attemptProcesses.add("engine_spawn", nonNegativeMetric(raw.Process.EngineSpawnCount))
	m.attemptProcesses.add("engine_external_spawn", nonNegativeMetric(raw.Process.EngineExternalSpawnCount))
	m.attemptProcesses.add("ffmpeg_spawn", nonNegativeMetric(raw.Process.EngineFfmpegSpawnCount))
	m.attemptProcesses.add("ffprobe_spawn", nonNegativeMetric(raw.Process.EngineFfprobeSpawnCount))
	m.attemptProcesses.add("shell_spawn", nonNegativeMetric(raw.Process.EngineShellSpawnCount))
	m.attemptProcesses.add("curl_spawn", nonNegativeMetric(raw.Process.EngineCurlSpawnCount))
}

// RecordJobResourceAttribution projects per-job resource attribution
// counters from one completed attempt. These are the per-job cost basis
// for capacity planning (MaxActiveJobs sweet spot).
func (m *PrometheusMetrics) RecordJobResourceAttribution(raw *RawExecutionMetrics) {
	if raw == nil {
		return
	}
	m.jobPeakRssDeltaBytes.set("total", float64(nonNegativeMetric(raw.JobPeakRssDeltaBytes)))
	m.jobCpuCoreSeconds.add("total", raw.JobCpuCoreSeconds)
	m.jobPrefetchBytes.add("total", nonNegativeMetric(raw.JobPrefetchBytes))
	m.jobPublishBytes.add("total", nonNegativeMetric(raw.JobPublishBytes))
	m.jobOpenFdsPeak.set("total", float64(nonNegativeMetric(raw.OpenFdsPeak)))
	m.jobPageFaults.add("total", nonNegativeMetric(raw.JobPageFaults))
	m.jobIoWaitMs.add("total", nonNegativeMetric(raw.IowaitMs))
}

func nonNegativeMetric(value int64) float64 {
	if value < 0 {
		return 0
	}
	return float64(value)
}
