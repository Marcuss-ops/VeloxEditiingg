// Package telemetry provides metrics collection for the worker agent.
package telemetry

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// WorkerMetrics tracks operational metrics for the worker agent.
type WorkerMetrics struct {
	mu sync.RWMutex

	// Job metrics
	JobsReceived   int64
	JobsSucceeded  int64
	JobsFailed     int64
	JobsTimeout    int64
	TotalJobTimeMs int64

	// API metrics
	HeartbeatsSent   int64
	HeartbeatsFailed int64
	APIRequests      int64
	APIRetries       int64
	APIErrors        int64

	// Disk metrics
	DiskChecks    int64
	DiskWarnings  int64
	DiskCriticals int64
	GCCleanupRuns int64
	GCFilesPurged int64

	// Timing
	StartTime     time.Time
	LastHeartbeat time.Time
	LastJobTime   time.Time

	// Output
	output io.Writer
}

// Global metrics instance.
var globalMetrics = NewWorkerMetrics()

// NewWorkerMetrics creates a new metrics collector.
func NewWorkerMetrics() *WorkerMetrics {
	return &WorkerMetrics{
		StartTime: time.Now(),
		output:    os.Stdout,
	}
}

// Reset resets all metrics to zero.
func (m *WorkerMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.JobsReceived = 0
	m.JobsSucceeded = 0
	m.JobsFailed = 0
	m.JobsTimeout = 0
	m.TotalJobTimeMs = 0
	m.HeartbeatsSent = 0
	m.HeartbeatsFailed = 0
	m.APIRequests = 0
	m.APIRetries = 0
	m.APIErrors = 0
	m.DiskChecks = 0
	m.DiskWarnings = 0
	m.DiskCriticals = 0
	m.GCCleanupRuns = 0
	m.GCFilesPurged = 0
	m.StartTime = time.Now()
	m.LastHeartbeat = time.Time{}
	m.LastJobTime = time.Time{}
}

// RecordJobReceived increments the jobs received counter.
func (m *WorkerMetrics) RecordJobReceived() {
	m.mu.Lock()
	m.JobsReceived++
	m.LastJobTime = time.Now()
	m.mu.Unlock()
}

// RecordJobSuccess increments the jobs succeeded counter and adds duration.
func (m *WorkerMetrics) RecordJobSuccess(durationMs int64) {
	m.mu.Lock()
	m.JobsSucceeded++
	m.TotalJobTimeMs += durationMs
	m.LastJobTime = time.Now()
	m.mu.Unlock()
}

// RecordJobFailure increments the jobs failed counter.
func (m *WorkerMetrics) RecordJobFailure(durationMs int64) {
	m.mu.Lock()
	m.JobsFailed++
	m.TotalJobTimeMs += durationMs
	m.LastJobTime = time.Now()
	m.mu.Unlock()
}

// RecordJobTimeout increments the jobs timeout counter.
func (m *WorkerMetrics) RecordJobTimeout() {
	m.mu.Lock()
	m.JobsTimeout++
	m.LastJobTime = time.Now()
	m.mu.Unlock()
}

// RecordHeartbeat increments the heartbeats sent counter.
func (m *WorkerMetrics) RecordHeartbeat() {
	m.mu.Lock()
	m.HeartbeatsSent++
	m.LastHeartbeat = time.Now()
	m.mu.Unlock()
}

// RecordHeartbeatFailure increments the heartbeats failed counter.
func (m *WorkerMetrics) RecordHeartbeatFailure() {
	m.mu.Lock()
	m.HeartbeatsFailed++
	m.mu.Unlock()
}

// RecordAPIRequest increments the API requests counter.
func (m *WorkerMetrics) RecordAPIRequest() {
	m.mu.Lock()
	m.APIRequests++
	m.mu.Unlock()
}

// RecordAPIRetry increments the API retries counter.
func (m *WorkerMetrics) RecordAPIRetry() {
	m.mu.Lock()
	m.APIRetries++
	m.mu.Unlock()
}

// RecordAPIError increments the API errors counter.
func (m *WorkerMetrics) RecordAPIError() {
	m.mu.Lock()
	m.APIErrors++
	m.mu.Unlock()
}

// RecordDiskCheck increments the disk checks counter.
func (m *WorkerMetrics) RecordDiskCheck() {
	m.mu.Lock()
	m.DiskChecks++
	m.mu.Unlock()
}

// RecordDiskWarning increments the disk warnings counter.
func (m *WorkerMetrics) RecordDiskWarning() {
	m.mu.Lock()
	m.DiskWarnings++
	m.mu.Unlock()
}

// RecordDiskCritical increments the disk critical counter.
func (m *WorkerMetrics) RecordDiskCritical() {
	m.mu.Lock()
	m.DiskCriticals++
	m.mu.Unlock()
}

// RecordGCCleanup increments the GC cleanup counter.
func (m *WorkerMetrics) RecordGCCleanup(filesPurged int64) {
	m.mu.Lock()
	m.GCCleanupRuns++
	m.GCFilesPurged += filesPurged
	m.mu.Unlock()
}

// Snapshot returns a copy of the current metrics.
func (m *WorkerMetrics) Snapshot() WorkerMetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return WorkerMetricsSnapshot{
		JobsReceived:     m.JobsReceived,
		JobsSucceeded:    m.JobsSucceeded,
		JobsFailed:       m.JobsFailed,
		JobsTimeout:      m.JobsTimeout,
		TotalJobTimeMs:   m.TotalJobTimeMs,
		HeartbeatsSent:   m.HeartbeatsSent,
		HeartbeatsFailed: m.HeartbeatsFailed,
		APIRequests:      m.APIRequests,
		APIRetries:       m.APIRetries,
		APIErrors:        m.APIErrors,
		DiskChecks:       m.DiskChecks,
		DiskWarnings:     m.DiskWarnings,
		DiskCriticals:    m.DiskCriticals,
		GCCleanupRuns:    m.GCCleanupRuns,
		GCFilesPurged:    m.GCFilesPurged,
		UptimeMs:         time.Since(m.StartTime).Milliseconds(),
		AvgJobTimeMs:     m.calculateAvgJobTime(),
		SuccessRate:      m.calculateSuccessRate(),
	}
}

func (m *WorkerMetrics) calculateAvgJobTime() float64 {
	total := m.JobsSucceeded + m.JobsFailed + m.JobsTimeout
	if total == 0 {
		return 0
	}
	return float64(m.TotalJobTimeMs) / float64(total)
}

func (m *WorkerMetrics) calculateSuccessRate() float64 {
	total := m.JobsSucceeded + m.JobsFailed + m.JobsTimeout
	if total == 0 {
		return 0
	}
	return float64(m.JobsSucceeded) / float64(total) * 100
}

// WorkerMetricsSnapshot is a point-in-time copy of worker metrics.
type WorkerMetricsSnapshot struct {
	JobsReceived     int64   `json:"jobs_received"`
	JobsSucceeded    int64   `json:"jobs_succeeded"`
	JobsFailed       int64   `json:"jobs_failed"`
	JobsTimeout      int64   `json:"jobs_timeout"`
	TotalJobTimeMs   int64   `json:"total_job_time_ms"`
	HeartbeatsSent   int64   `json:"heartbeats_sent"`
	HeartbeatsFailed int64   `json:"heartbeats_failed"`
	APIRequests      int64   `json:"api_requests"`
	APIRetries       int64   `json:"api_retries"`
	APIErrors        int64   `json:"api_errors"`
	DiskChecks       int64   `json:"disk_checks"`
	DiskWarnings     int64   `json:"disk_warnings"`
	DiskCriticals    int64   `json:"disk_criticals"`
	GCCleanupRuns    int64   `json:"gc_cleanup_runs"`
	GCFilesPurged    int64   `json:"gc_files_purged"`
	UptimeMs         int64   `json:"uptime_ms"`
	AvgJobTimeMs     float64 `json:"avg_job_time_ms"`
	SuccessRate      float64 `json:"success_rate_pct"`
}

// Format formats the metrics snapshot as a human-readable string.
func (s WorkerMetricsSnapshot) String() string {
	uptime := time.Duration(s.UptimeMs) * time.Millisecond
	return fmt.Sprintf(
		"[METRICS] uptime=%s | jobs: received=%d success=%d failed=%d timeout=%d (%.1f%% success) | "+
			"avg_job_time=%.1fms | heartbeats: sent=%d failed=%d | api: reqs=%d retries=%d errors=%d | "+
			"disk: checks=%d warnings=%d critical=%d | gc: runs=%d files_purged=%d",
		uptime.Round(time.Second),
		s.JobsReceived, s.JobsSucceeded, s.JobsFailed, s.JobsTimeout, s.SuccessRate,
		s.AvgJobTimeMs,
		s.HeartbeatsSent, s.HeartbeatsFailed,
		s.APIRequests, s.APIRetries, s.APIErrors,
		s.DiskChecks, s.DiskWarnings, s.DiskCriticals,
		s.GCCleanupRuns, s.GCFilesPurged,
	)
}

// Global functions for convenience.

// GetMetrics returns the global metrics instance.
func GetMetrics() *WorkerMetrics {
	return globalMetrics
}

// RecordJobReceived records a job received on the global metrics.
func RecordJobReceived() {
	globalMetrics.RecordJobReceived()
}

// RecordJobSuccess records a successful job on the global metrics.
func RecordJobSuccess(durationMs int64) {
	globalMetrics.RecordJobSuccess(durationMs)
}

// RecordJobFailure records a failed job on the global metrics.
func RecordJobFailure(durationMs int64) {
	globalMetrics.RecordJobFailure(durationMs)
}

// RecordHeartbeat records a heartbeat on the global metrics.
func RecordHeartbeat() {
	globalMetrics.RecordHeartbeat()
}

// RecordHeartbeatFailure records a failed heartbeat on the global metrics.
func RecordHeartbeatFailure() {
	globalMetrics.RecordHeartbeatFailure()
}

// GetMetricsSnapshot returns a snapshot of the global metrics.
func GetMetricsSnapshot() WorkerMetricsSnapshot {
	return globalMetrics.Snapshot()
}
