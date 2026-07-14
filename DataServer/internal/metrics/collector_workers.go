// Package metrics / collector_workers.go
//
// Worker resource gauges + the typed payload (ResourceSnapshot) that
// flows from the gRPC handler to the Prometheus registry, sliced
// out of collector.go so the Collector struct definition stays
// focused on registration.
//
// The WorkerResourceSink interface pattern: defined in metrics (the
// package the consumer ginally depends on) but consumed by the
// handler package via implicit interface satisfaction. Same contract
// style as PlacementRejectionSink in collector_sinks.go.
//
// ResourceSnapshot stays a Go struct (not a protobuf message) so the
// metrics package has no cross-module dependency on shared/control-
// transport.
package metrics

import "time"

// WorkerResourceSink is the contract the gRPC handler depends on for
// forwarding worker resource counters (CPU/iowait/steal/RAM/disk/net/
// load/scheduler) onto the Prometheus registry. Defined here, in the
// CONSUMED-by-handler direction, so that:
//
//   - the gRPC handler package owns the conversion + delta-tracking glue
//     (handler_workers_metrics.go), keeping pb types out of metrics.go;
//   - the metrics package registers a default impl (Collector.RecordWorker)
//     so callers can wire it via interface without manual casting; and
//   - tests inject a stub sink that records calls without spinning the
//     full Prometheus registry.
//
// PR-2 / F2 / Scorecard v1: the in-band flow is processHeartbeat →
// handlerWorkers.decodeResources → sink.RecordWorker(workerID, snapshot).
type WorkerResourceSink interface {
	RecordWorker(workerID string, snapshot *ResourceSnapshot)
}

// Compile-time guard: *Collector implements WorkerResourceSink by default.
// Tests skipping this assertion would break RecordWorker wire-up silently.
var _ WorkerResourceSink = (*Collector)(nil)

// ResourceSnapshot is the typed payload RecordWorker expects; this
// matches pb.WorkerResourceCounters but stays decoupled from proto
// symbols (so internal/metrics has no cross-module dep).
type ResourceSnapshot struct {
	CPUUtilRatio          float64
	CPUIOWaitRatio        float64
	CPUStealRatio         float64
	ProcessRSSBytes       int64
	ProcessRSSPeakBytes   int64
	MemoryUsedBytes       int64
	DiskFreeBytes         int64
	TempBytesWritten      int64
	ActiveTasks           int32
	TaskSlots             int32
	Load1                 float64
	RunQueue              int32
	NetworkRxBytesDelta   uint64
	NetworkTxBytesDelta   uint64
	CacheEntries          int
	CacheBytesUsed        int64
	CacheEvictionsDelta   uint64
	CacheCorruptionsDelta uint64
	SampledAt             time.Time
}

// RecordWorker stamps a worker's resource counters onto the per-worker
// gauge set. The heartbeat period drives how often this is called from
// watchdogs (default 15s).
func (c *Collector) RecordWorker(workerID string, rs *ResourceSnapshot) {
	if rs == nil {
		return
	}
	wl := []string{workerID}
	c.workerCPUUtil.GaugeSet(wl, int64(rs.CPUUtilRatio*1000000))
	c.workerIOWait.GaugeSet(wl, int64(rs.CPUIOWaitRatio*1000000))
	c.workerSteal.GaugeSet(wl, int64(rs.CPUStealRatio*1000000))
	c.workerRSSBytes.GaugeSet(wl, rs.ProcessRSSBytes)
	c.workerRSSPeak.GaugeSet(wl, rs.ProcessRSSPeakBytes)
	c.workerMemoryUsed.GaugeSet(wl, rs.MemoryUsedBytes)
	c.workerDiskFree.GaugeSet(wl, rs.DiskFreeBytes)
	c.workerTempBytes.GaugeSet(wl, rs.TempBytesWritten)
	c.workerActiveTasks.GaugeSet(wl, int64(rs.ActiveTasks))
	c.workerTaskSlots.GaugeSet(wl, int64(rs.TaskSlots))
	c.workerLoad1.GaugeSet(wl, int64(rs.Load1*1000))
	c.workerRunQueue.GaugeSet(wl, int64(rs.RunQueue))

	// Counter diffs (network cumulatives).
	c.workerNetRxBytes.Inc(wl, rs.NetworkRxBytesDelta)
	c.workerNetTxBytes.Inc(wl, rs.NetworkTxBytesDelta)
	c.cacheEntries.GaugeSet(wl, int64(rs.CacheEntries))
	c.cacheSizeBytes.GaugeSet(wl, rs.CacheBytesUsed)
	c.cacheEvictions.Inc(wl, rs.CacheEvictionsDelta)
	c.cacheCorruptions.Inc(wl, rs.CacheCorruptionsDelta)

	// Heartbeat timestamp.
	c.stateMu.Lock()
	c.lastSeen[workerID] = rs.SampledAt
	c.stateMu.Unlock()
}
