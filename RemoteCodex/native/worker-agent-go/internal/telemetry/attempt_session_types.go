package telemetry

// attempt_session_types.go holds the type definitions, constants, and pure
// helpers shared by the attempt session lifecycle, sampling, and finalization
// files.  AttemptTelemetrySession remains the single authority; these types
// are its supporting vocabulary.

import (
	"encoding/json"
	"os"
	"time"
)

const AttemptTelemetrySchemaVersion int32 = 2

type cgroupUsage struct {
	CPUUsec        int64
	MemoryCurrent  int64
	MemoryPeak     int64
	DiskReadBytes  int64
	DiskWriteBytes int64
}

type processCPUUsage struct {
	userUsec   int64
	systemUsec int64
	valid      bool
}

func (u processCPUUsage) totalUsec() int64 { return u.userUsec + u.systemUsec }

// PhaseResourceDelta is the resource portion available to a detailed phase.
// CPU is carried in the typed phase field; the remaining values are included
// in phase metadata until the phase storage schema grows typed columns.
type PhaseResourceDelta struct {
	CPUTimeMs      int64
	PeakRSSBytes   int64
	DiskReadBytes  int64
	DiskWriteBytes int64
	NetworkRxBytes int64
	NetworkTxBytes int64
}

type AttemptTelemetry struct {
	Metrics          RawExecutionMetrics
	Coverage         map[string]bool
	Complete         bool
	WallClockSeconds float64
	StartedAt        time.Time
	CompletedAt      time.Time
}

type PhaseResourceSnapshot struct {
	resources *SampledResources
	cgroup    cgroupUsage
	process   processCPUUsage
	at        time.Time
}

// ── Pure helpers ──────────────────────────────────────────────────────────

func positiveDelta(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func resourceCounter(r *SampledResources, f func(*SampledResources) int64) int64 {
	if r == nil {
		return 0
	}
	return f(r)
}

func resourcesRatio(r *SampledResources, f func(*SampledResources) float64) float64 {
	if r == nil {
		return 0
	}
	return f(r)
}

func safeRatio(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func countOpenFDs() int64 {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return int64(len(entries))
}

func PhaseResourceMetadataJSON(d PhaseResourceDelta) string {
	data, _ := json.Marshal(map[string]interface{}{
		"telemetry_schema_version": AttemptTelemetrySchemaVersion,
		"peak_rss_bytes":           d.PeakRSSBytes,
		"disk_read_bytes":          d.DiskReadBytes,
		"disk_write_bytes":         d.DiskWriteBytes,
		"network_rx_bytes":         d.NetworkRxBytes,
		"network_tx_bytes":         d.NetworkTxBytes,
	})
	return string(data)
}
