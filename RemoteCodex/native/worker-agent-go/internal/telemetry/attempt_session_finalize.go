package telemetry

// attempt_session_finalize.go extracts the resource-delta computation and
// coverage-map construction from AttemptTelemetrySession.Stop().  The session
// remains the single authority; this file isolates the deterministic math so
// the lifecycle method stays readable.

import (
	"encoding/json"
	"runtime"
	"time"
)

// finalizeMetrics computes the terminal TypedExecutionMetrics, coverage map,
// and wall-clock seconds from the session's start/end snapshots.  It is a
// pure function of the session state — no side effects, no locks.
func (s *AttemptTelemetrySession) finalizeMetrics(
	end time.Time,
	resources *SampledResources,
	cgroup cgroupUsage,
	process processCPUUsage,
) (TypedExecutionMetrics, map[string]bool, float64) {
	wall := end.Sub(s.startedAt).Seconds()
	if wall < 0 {
		wall = 0
	}

	// ── CPU ──────────────────────────────────────────────────────────────
	cpuUsec := cgroup.CPUUsec - s.startCgroup.CPUUsec
	if s.cgroupRoot == "" {
		cpuUsec = process.totalUsec() - s.startProcess.totalUsec()
	}

	// ── Disk (cgroup v2 authoritative, fallback to /proc/diskstats) ─────
	diskReadBytes := positiveDelta(cgroup.DiskReadBytes - s.startCgroup.DiskReadBytes)
	diskWriteBytes := positiveDelta(cgroup.DiskWriteBytes - s.startCgroup.DiskWriteBytes)
	if !cgroupIOAvailable(s.cgroupRoot) {
		diskReadBytes = positiveDelta(
			resourceCounter(resources, func(r *SampledResources) int64 { return r.DiskReadBytesTotal }) -
				resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.DiskReadBytesTotal }))
		diskWriteBytes = positiveDelta(
			resourceCounter(resources, func(r *SampledResources) int64 { return r.DiskWriteBytesTotal }) -
				resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.DiskWriteBytesTotal }))
	}

	metrics := TypedExecutionMetrics{
		CpuTimeMs:        positiveDelta(cpuUsec) / 1000,
		PeakRssBytes:     s.peakRSSBytes,
		CpuPercentPeak:   s.peakCPUPercent,
		WallClockSeconds: wall,
		DiskReadBytes:    diskReadBytes,
		DiskWriteBytes:   diskWriteBytes,
		NetworkRxBytes: positiveDelta(
			resourceCounter(resources, func(r *SampledResources) int64 { return r.NetworkReceiveBytesTotal }) -
				resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.NetworkReceiveBytesTotal })),
		NetworkTxBytes: positiveDelta(
			resourceCounter(resources, func(r *SampledResources) int64 { return r.NetworkTransmitBytesTotal }) -
				resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.NetworkTransmitBytesTotal })),
		TempBytesWritten: positiveDelta(
			resourceCounter(resources, func(r *SampledResources) int64 { return r.TempBytesWritten }) -
				resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.TempBytesWritten })),
		IowaitMs:        int64(wall * 1000 * safeRatio(resourcesRatio(resources, func(r *SampledResources) float64 { return r.CPUIOWaitRatio }))),
		OpenFdsPeak:     s.peakOpenFDs,
		LogicalCpuCount: int32(runtime.NumCPU()),
	}

	cp := DetectCPUCapacity()
	metrics.CpuQuota = cp.CPUQuota
	metrics.EffectiveCpuCount = int32(cp.EffectiveCPUCount)

	// ── CPU source attribution ───────────────────────────────────────────
	if s.cgroupRoot != "" {
		metrics.TelemetryCPUSource = "cgroup_v2"
	} else if len(s.samplers) > 0 && s.samplers[0] != nil {
		metrics.TelemetryCPUSource = "proc"
	} else {
		metrics.TelemetryCPUSource = "missing"
	}

	// ── Coverage map ─────────────────────────────────────────────────────
	coverage := map[string]bool{
		"cpu": (s.cgroupRoot != "" && cgroup.CPUUsec >= s.startCgroup.CPUUsec) ||
			(s.cgroupRoot == "" && s.startProcess.valid && process.valid && process.totalUsec() >= s.startProcess.totalUsec()),
		"memory":       s.startCgroup.MemoryCurrent > 0 || cgroup.MemoryCurrent > 0 || s.peakRSSBytes > 0,
		"disk":         (s.cgroupRoot != "" && (cgroup.DiskReadBytes >= s.startCgroup.DiskReadBytes || cgroup.DiskWriteBytes >= s.startCgroup.DiskWriteBytes)) || (resources != nil && s.startResources != nil),
		"network":      resources != nil && s.startResources != nil,
		"cgroup":       s.cgroupRoot != "",
		"process_tree": s.cgroupRoot != "",
	}

	coverageJSON, _ := json.Marshal(coverage)
	metrics.TelemetryCoverageJSON = string(coverageJSON)
	metrics.TelemetryComplete = coverage["cpu"] && coverage["memory"] && coverage["disk"] && coverage["network"]

	return metrics, coverage, wall
}
