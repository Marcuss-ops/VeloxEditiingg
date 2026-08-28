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
	CPUUserRatio          float64
	CPUSystemRatio        float64
	EffectiveCpuCores     int32
	ProcessRSSBytes       int64
	ProcessRSSPeakBytes   int64
	MemoryUsedBytes       int64
	MemoryAvailableBytes  int64
	PageCacheBytes        int64
	DiskFreeBytes         int64
	TempBytesWritten      int64
	ScratchCurrentBytes   int64
	ScratchPeakBytes      int64
	DiskReadMbps          float64
	DiskWriteMbps         float64
	DiskIoWaitMs          int64
	ActiveTasks           int32
	TaskSlots             int32
	RenderJobsActive      int32
	PrefetchJobsActive    int32
	PublisherJobsActive   int32
	Load1                 float64
	RunQueue              int32
	NetworkRxBytesDelta   uint64
	NetworkTxBytesDelta   uint64
	DownloadMbps          float64
	UploadMbps            float64
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
	c.workerCPUUser.GaugeSet(wl, int64(rs.CPUUserRatio*1000000))
	c.workerCPUSystem.GaugeSet(wl, int64(rs.CPUSystemRatio*1000000))
	c.workerEffectiveCpu.GaugeSet(wl, int64(rs.EffectiveCpuCores))
	c.workerRSSBytes.GaugeSet(wl, rs.ProcessRSSBytes)
	c.workerRSSPeak.GaugeSet(wl, rs.ProcessRSSPeakBytes)
	c.workerMemoryUsed.GaugeSet(wl, rs.MemoryUsedBytes)
	c.workerMemoryAvail.GaugeSet(wl, rs.MemoryAvailableBytes)
	c.workerPageCache.GaugeSet(wl, rs.PageCacheBytes)
	c.workerDiskFree.GaugeSet(wl, rs.DiskFreeBytes)
	c.workerTempBytes.GaugeSet(wl, rs.TempBytesWritten)
	c.workerScratchCurrent.GaugeSet(wl, rs.ScratchCurrentBytes)
	c.workerScratchPeak.GaugeSet(wl, rs.ScratchPeakBytes)
	c.workerDiskReadMbps.GaugeSet(wl, int64(rs.DiskReadMbps*1000))
	c.workerDiskWriteMbps.GaugeSet(wl, int64(rs.DiskWriteMbps*1000))
	c.workerDiskIoWaitMs.GaugeSet(wl, rs.DiskIoWaitMs)
	c.workerActiveTasks.GaugeSet(wl, int64(rs.ActiveTasks))
	c.workerTaskSlots.GaugeSet(wl, int64(rs.TaskSlots))
	c.workerRenderActive.GaugeSet(wl, int64(rs.RenderJobsActive))
	c.workerPrefetchActive.GaugeSet(wl, int64(rs.PrefetchJobsActive))
	c.workerPublisherActive.GaugeSet(wl, int64(rs.PublisherJobsActive))
	c.workerLoad1.GaugeSet(wl, int64(rs.Load1*1000))
	c.workerRunQueue.GaugeSet(wl, int64(rs.RunQueue))

	// Counter diffs (network cumulatives).
	c.workerNetRxBytes.Inc(wl, rs.NetworkRxBytesDelta)
	c.workerNetTxBytes.Inc(wl, rs.NetworkTxBytesDelta)
	c.workerDownloadMbps.GaugeSet(wl, int64(rs.DownloadMbps*1000))
	c.workerUploadMbps.GaugeSet(wl, int64(rs.UploadMbps*1000))
	c.cacheEntries.GaugeSet(wl, int64(rs.CacheEntries))
	c.cacheSizeBytes.GaugeSet(wl, rs.CacheBytesUsed)
	c.cacheEvictions.Inc(wl, rs.CacheEvictionsDelta)
	c.cacheCorruptions.Inc(wl, rs.CacheCorruptionsDelta)

	// Heartbeat timestamp.
	c.stateMu.Lock()
	c.lastSeen[workerID] = rs.SampledAt
	c.stateMu.Unlock()
}

// initWorkerFamilies creates the per-worker resource counter/gauge set
// recorded by RecordWorker. Called once from NewCollector at boot.
func (c *Collector) initWorkerFamilies() {
	c.workerCPUUtil = NewGaugeFamily("velox_worker_cpu_utilization_ratio",
		"Worker CPU utilization (0-1)", []string{"worker_id"})
	c.workerIOWait = NewGaugeFamily("velox_worker_cpu_iowait_ratio",
		"Worker iowait ratio", []string{"worker_id"})
	c.workerSteal = NewGaugeFamily("velox_worker_cpu_steal_ratio",
		"Worker steal time ratio", []string{"worker_id"})
	c.workerCPUUser = NewGaugeFamily("velox_worker_cpu_user_ratio",
		"Worker CPU user-space ratio (0-1)", []string{"worker_id"})
	c.workerCPUSystem = NewGaugeFamily("velox_worker_cpu_system_ratio",
		"Worker CPU kernel-space ratio (0-1)", []string{"worker_id"})
	c.workerEffectiveCpu = NewGaugeFamily("velox_worker_effective_cpu_cores",
		"Worker effective CPU cores (min logical, cgroup quota)", []string{"worker_id"})
	c.workerRSSBytes = NewGaugeFamily("velox_worker_process_rss_bytes",
		"Worker process RSS", []string{"worker_id"})
	c.workerRSSPeak = NewGaugeFamily("velox_worker_process_rss_peak_bytes",
		"Worker peak RSS", []string{"worker_id"})
	c.workerMemoryUsed = NewGaugeFamily("velox_worker_memory_used_bytes",
		"Worker system memory used", []string{"worker_id"})
	c.workerMemoryAvail = NewGaugeFamily("velox_worker_memory_available_bytes",
		"Worker system memory available", []string{"worker_id"})
	c.workerPageCache = NewGaugeFamily("velox_worker_page_cache_bytes",
		"Worker kernel page cache (Buffers+Cached)", []string{"worker_id"})
	c.workerDiskFree = NewGaugeFamily("velox_worker_disk_free_bytes",
		"Worker disk free bytes", []string{"worker_id"})
	c.workerTempBytes = NewGaugeFamily("velox_worker_temp_bytes",
		"Worker temp bytes (gauge at heartbeat time)", []string{"worker_id"})
	c.workerScratchCurrent = NewGaugeFamily("velox_worker_scratch_current_bytes",
		"Worker scratch directory current occupancy", []string{"worker_id"})
	c.workerScratchPeak = NewGaugeFamily("velox_worker_scratch_peak_bytes",
		"Worker scratch directory peak occupancy", []string{"worker_id"})
	c.workerDiskReadMbps = NewGaugeFamily("velox_worker_disk_read_mbps",
		"Worker disk read throughput (MB/s)", []string{"worker_id"})
	c.workerDiskWriteMbps = NewGaugeFamily("velox_worker_disk_write_mbps",
		"Worker disk write throughput (MB/s)", []string{"worker_id"})
	c.workerDiskIoWaitMs = NewGaugeFamily("velox_worker_disk_io_wait_ms",
		"Worker disk I/O wait (cumulative ms)", []string{"worker_id"})
	c.workerActiveTasks = NewGaugeFamily("velox_worker_active_tasks",
		"Active tasks on worker", []string{"worker_id"})
	c.workerTaskSlots = NewGaugeFamily("velox_worker_task_slots",
		"Worker task slots", []string{"worker_id"})
	c.workerRenderActive = NewGaugeFamily("velox_worker_render_jobs_active",
		"Worker active render jobs", []string{"worker_id"})
	c.workerPrefetchActive = NewGaugeFamily("velox_worker_prefetch_jobs_active",
		"Worker active prefetch jobs", []string{"worker_id"})
	c.workerPublisherActive = NewGaugeFamily("velox_worker_publisher_jobs_active",
		"Worker active publisher jobs", []string{"worker_id"})
	c.workerLoad1 = NewGaugeFamily("velox_worker_load1",
		"Worker 1-min loadavg", []string{"worker_id"})
	c.workerRunQueue = NewGaugeFamily("velox_worker_run_queue",
		"Worker run queue depth", []string{"worker_id"})
	c.workerNetRxBytes = NewCounterFamily("velox_worker_network_receive_bytes_total",
		"Worker net rx total", []string{"worker_id"})
	c.workerNetTxBytes = NewCounterFamily("velox_worker_network_transmit_bytes_total",
		"Worker net tx total", []string{"worker_id"})
	c.workerDownloadMbps = NewGaugeFamily("velox_worker_download_mbps",
		"Worker network download throughput (Mbit/s)", []string{"worker_id"})
	c.workerUploadMbps = NewGaugeFamily("velox_worker_upload_mbps",
		"Worker network upload throughput (Mbit/s)", []string{"worker_id"})
}

// workerFamilies returns the worker subset registered by NewCollector
// via allFamilies.
func (c *Collector) workerFamilies() []*Family {
	return []*Family{
		c.workerCPUUtil, c.workerIOWait, c.workerSteal,
		c.workerCPUUser, c.workerCPUSystem, c.workerEffectiveCpu,
		c.workerRSSBytes, c.workerRSSPeak, c.workerMemoryUsed, c.workerMemoryAvail, c.workerPageCache,
		c.workerDiskFree,		c.workerTempBytes, c.workerScratchCurrent, c.workerScratchPeak, c.workerDiskReadMbps, c.workerDiskWriteMbps, c.workerDiskIoWaitMs,
		c.workerActiveTasks, c.workerTaskSlots,
		c.workerRenderActive, c.workerPrefetchActive, c.workerPublisherActive,
		c.workerLoad1, c.workerRunQueue,
		c.workerNetRxBytes, c.workerNetTxBytes,
		c.workerDownloadMbps, c.workerUploadMbps,
	}
}
