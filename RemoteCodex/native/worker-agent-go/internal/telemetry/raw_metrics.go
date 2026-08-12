package telemetry

// MergeAttemptResourceFactsInto applies only fields owned by the attempt
// telemetry collector. Coverage gates prevent unavailable samplers from
// overwriting facts produced by the engine, resolver, muxer, or publisher.
func MergeAttemptResourceFactsInto(dst *RawExecutionMetrics, attempt RawExecutionMetrics) {
	if dst == nil {
		return
	}
	coverage := attempt.CoverageMap()
	if coverage == nil {
		// RawExecutionMetrics produced by older tests/callers predates the
		// coverage JSON. Preserve the historical behavior for that explicit
		// compatibility shape; current sessions always provide coverage.
		coverage = map[string]bool{"cpu": true, "memory": true, "disk": true, "network": true}
	}
	if coverage["cpu"] {
		dst.CpuTimeMs = attempt.CpuTimeMs
		dst.CpuPercentPeak = attempt.CpuPercentPeak
	}
	if coverage["memory"] {
		dst.PeakRssBytes = attempt.PeakRssBytes
		dst.OpenFdsPeak = attempt.OpenFdsPeak
	}
	if coverage["disk"] {
		dst.DiskReadBytes = attempt.DiskReadBytes
		dst.DiskWriteBytes = attempt.DiskWriteBytes
		dst.TempBytesWritten = attempt.TempBytesWritten
	}
	if coverage["network"] {
		dst.NetworkRxBytes = attempt.NetworkRxBytes
		dst.NetworkTxBytes = attempt.NetworkTxBytes
	}
	// These are session-level facts and remain useful even when one resource
	// family is unavailable; the values are accompanied by explicit coverage.
	dst.WallClockSeconds = attempt.WallClockSeconds
	dst.LogicalCpuCount = attempt.LogicalCpuCount
	dst.CpuQuota = attempt.CpuQuota
	dst.EffectiveCpuCount = attempt.EffectiveCpuCount
	dst.TelemetryCoverageJSON = attempt.TelemetryCoverageJSON
	dst.TelemetryComplete = attempt.TelemetryComplete
	dst.TelemetryCPUSource = attempt.TelemetryCPUSource
}
