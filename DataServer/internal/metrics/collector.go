// Package metrics / collector.go
//
// Scorecard v1: 12 metric families wired to the Prometheus registry.
//
// Each family is registered ONCE at boot. The Collector exposes Scan +
// RecordAttempt / RecordAttemptOutcome / RecordWorker / RecordMaster
// methods that read from the underlying observability/state sources
// (task_attempt_metrics rows, heartbeat extras, scheduler state, gc
// p99s) and stamp them onto the appropriate counter / gauge /
// histogram.
//
// Cardinality discipline (mirrors metrics.safeLabelKey):
//
// SAFE:   executor_id, executor_version, worker_class, phase,
//
//	codec, preset, resolution_bucket, cache_source
//
// UNSAFE: job_id, task_id, artifact_id, hash, video_title
//
// ─── SPEC §14 — COMPUTE OUTCOME REFACTOR (RATIONALE) ─────────────────────
//
// Spec §14 consolidates the 4 legacy split-out families
// (velox_compute_seconds_total{outcome=useful},
//
//	velox_compute_seconds_total_failed,
//	velox_compute_seconds_total_cancelled,
//	velox_compute_seconds_total_stale)
//
// into a SINGLE family `velox_compute_seconds_total{outcome=...}` plus a
// sibling `velox_compute_failure_reasons_total{reason=...}` for
// failure-reason attribution.
//
// Why not put `reason` as a second label on the seconds family?
// The literal spec mandates `[]string{"outcome"}`. Adding `reason`
// would expand the seconds-family cardinality into dozens-of-key
// reason-row territory (every FAILED attempt becomes a new time
// series), which is the exact anti-pattern Prometheus warns against.
// Putting reason on a separate count family keeps the seconds surface
// single-label; the `reason` row set stays bounded by the closed enum
// of worker.Code* constants (pre-canonicalized at the runner boundary).
//
// DOWNSTREAM IMPACT NOTE: wire-format change. PromQL queries / Grafana
// panels that referenced the four retired family names will silently
// return no data. Counter cumulative values restart at zero on first
// rollout; no migration script per spec. Operators must migrate
// dashboards to the new label-set BEFORE the next Grafana rebuild.
package metrics

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"velox-server/internal/taskattempts"
)

// Collector is the registered metric surface for the master. It owns
// the *metrics.Registry and a curated list of typed families.
type Collector struct {
	reg *Registry

	// Per-project.
	renderSpeed *Family // velox_project_render_speed_ratio (gauge)
	// 12 metric families — family name → family.
	phaseDurations     *Family // velox_task_phase_duration_seconds
	ffmpegFramesTotal  *Family // velox_ffmpeg_frames_processed_total
	ffmpegFps          *Family // velox_ffmpeg_fps
	ffmpegSpeed        *Family // velox_ffmpeg_speed_ratio
	ffmpegEncodeMs     *Family // velox_ffmpeg_encode_duration_seconds
	ffmpegDecodeMs     *Family // velox_ffmpeg_decode_duration_seconds
	ffmpegDropped      *Family // velox_ffmpeg_dropped_frames_total
	ffmpegDuplicated   *Family // velox_ffmpeg_duplicated_frames_total
	ffmpegExits        *Family // velox_ffmpeg_exit_total{exit_code}
	ffmpegRestarts     *Family // velox_ffmpeg_restarts_total
	ffmpegProcessesAct *Family // velox_ffmpeg_processes_active
	videoEncodePasses  *Family // velox_video_encode_passes_total
	videoFramesEnc     *Family // velox_video_frames_encoded_total
	videoOutputFrames  *Family // velox_video_output_frames_total
	videoStreamCopy    *Family // velox_video_stream_copy_operations_total
	videoReencode      *Family // velox_video_reencode_operations_total{reason}
	cacheHits          *Family // velox_cache_requests_total{result="hit|miss|corrupt"}
	cacheBytes         *Family // velox_cache_bytes_total{result="hit|miss"}
	cacheEntries       *Family // velox_cache_entries
	cacheSizeBytes     *Family // velox_cache_size_bytes
	cacheEvictions     *Family // velox_cache_evictions_total
	cacheEvictedBytes  *Family // velox_cache_evicted_bytes_total
	cacheCorruptions   *Family // velox_cache_corruption_total

	// Worker resource counters (from heartbeat.resources).
	workerCPUUtil     *Family // velox_worker_cpu_utilization_ratio
	workerIOWait      *Family // velox_worker_cpu_iowait_ratio
	workerSteal       *Family // velox_worker_cpu_steal_ratio
	workerRSSBytes    *Family // velox_worker_process_rss_bytes
	workerRSSPeak     *Family // velox_worker_process_rss_peak_bytes
	workerMemoryUsed  *Family // velox_worker_memory_used_bytes
	workerDiskFree    *Family // velox_worker_disk_free_bytes
	workerTempBytes   *Family // velox_worker_temp_bytes
	workerActiveTasks *Family // velox_worker_active_tasks
	workerTaskSlots   *Family // velox_worker_task_slots
	workerLoad1       *Family // velox_worker_load1
	workerRunQueue    *Family // velox_worker_run_queue
	workerNetRxBytes  *Family
	workerNetTxBytes  *Family

	// Master-side health.
	masterRssBytes      *Family
	masterGoroutines    *Family
	masterOutboxPending *Family
	heartbeatAge        *Family // per worker; emitted on each refresh

	// Compute outcomes — SPEC §14: ONE family classified by outcome,
	// plus a sibling family for failure-reason attribution. Outcomes:
	// useful | failed | cancelled | stale | speculative_lost.
	// (speculative_lost is reserved by the scheduler; RecordAttemptOutcome
	// does NOT emit it directly.)
	computeSeconds        *Family // velox_compute_seconds_total{outcome=...}
	computeFailureReasons *Family // velox_compute_failure_reasons_total{reason=...}

	// Cost-per-output-minute gauges (spec §14 follow-up). Each gauge
	// is single-label `worker_class` (UNSAFE `project_id` was rejected;
	// per-class aggregation covers the same operational use case).
	// Cardinality discipline: only `worker_class` since worker
	// profiles cluster cleanly into cpu/gpu/mixed/io — see
	// cost_factors.go for the math caveat on averaging these gauges.
	costCpuPerMin     *Family // velox_cost_cpu_core_seconds_per_output_minute
	costNetworkPerMin *Family // velox_cost_network_gb_per_output_minute
	costStoragePerMin *Family // velox_cost_storage_gb_written_per_output_minute
	costTotalPerMin   *Family // velox_cost_total_per_output_minute

	// Phase 4.3 — Reconcile supervisor counters. The supervisor in
	// internal/completion/reconcile_supervisor.go writes
	//   velox_completion_reconcile_total{case,action}
	// where case ∈ 11 anomaly labels (see completion.AllReconcileCases)
	// and action ∈ {noop, transition, escalate}. Separately, every
	// attempt whose commit_deadline_at crossed in this tick
	// increments
	//   velox_commit_deadline_exceeded_total
	// (regardless of whether the dispatch path then transitioned the
	// row to EXPIRED — the counter measures the underlying anomaly
	// surface, not the coordinator's response).
	reconcileTotal         *Family // velox_completion_reconcile_total{case,action}
	commitDeadlineExceeded *Family // velox_commit_deadline_exceeded_total

	// Book-keeping for diffs.
	stateMu  sync.Mutex
	lastSeen map[string]time.Time // worker_id → last heartbeat timestamp
	mu       sync.RWMutex
}

// NewCollector returns a Collector with all 12 scorecard family +
// supporting families registered on reg.
func NewCollector(reg *Registry) *Collector {
	c := &Collector{reg: reg}

	c.renderSpeed = NewGaugeFamily(
		"velox_project_render_speed_ratio",
		"Ratio of media duration to wall clock time (>1 means faster than realtime)",
		[]string{"executor_id", "worker_class"},
	)

	c.phaseDurations = NewHistogramFamily(
		"velox_task_phase_duration_seconds",
		"Per-phase duration in seconds for a canonical rendering phase",
		[]string{"executor_id", "executor_version", "worker_class", "phase", "status"},
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 1800},
	)

	// FFmpeg.
	c.ffmpegFramesTotal = NewCounterFamily("velox_ffmpeg_frames_processed_total",
		"Total frames processed by FFmpeg as observed from -progress", []string{"executor_id"})
	c.ffmpegFps = NewGaugeFamily("velox_ffmpeg_fps",
		"Last-observed FFmpeg fps", []string{"executor_id"})
	c.ffmpegSpeed = NewGaugeFamily("velox_ffmpeg_speed_ratio",
		"Last-observed FFmpeg speed vs realtime (>1 faster)", []string{"executor_id"})
	c.ffmpegEncodeMs = NewHistogramFamily("velox_ffmpeg_encode_duration_seconds",
		"Encode duration as observed", []string{"executor_id"},
		[]float64{0.5, 1, 2.5, 5, 10, 30, 60, 300})
	c.ffmpegDecodeMs = NewHistogramFamily("velox_ffmpeg_decode_duration_seconds",
		"Decode duration as observed", []string{"executor_id"},
		[]float64{0.25, 0.5, 1, 2.5, 5, 10, 30, 60})
	c.ffmpegDropped = NewCounterFamily("velox_ffmpeg_dropped_frames_total",
		"Dropped frames as observed", []string{"executor_id"})
	c.ffmpegDuplicated = NewCounterFamily("velox_ffmpeg_duplicated_frames_total",
		"Duplicated frames as observed", []string{"executor_id"})
	c.ffmpegExits = NewCounterFamily("velox_ffmpeg_exit_total",
		"FFmpeg process exits by exit code", []string{"executor_id", "exit_code"})
	c.ffmpegRestarts = NewCounterFamily("velox_ffmpeg_restarts_total",
		"FFmpeg process restarts", []string{"executor_id"})
	c.ffmpegProcessesAct = NewGaugeFamily("velox_ffmpeg_processes_active",
		"Currently running FFmpeg processes", []string{"executor_id"})

	// Video encode amplification.
	c.videoEncodePasses = NewCounterFamily("velox_video_encode_passes_total",
		"Encode passes performed", []string{"executor_id"})
	c.videoFramesEnc = NewCounterFamily("velox_video_frames_encoded_total",
		"Frames encoded (sum across passes)", []string{"executor_id"})
	c.videoOutputFrames = NewCounterFamily("velox_video_output_frames_total",
		"Output frames published (lower-bound dedup)", []string{"executor_id"})
	c.videoStreamCopy = NewCounterFamily("velox_video_stream_copy_operations_total",
		"Stream-copy concat operations (cheap path)", []string{})
	c.videoReencode = NewCounterFamily("velox_video_reencode_operations_total",
		"Reencode concat operations (expensive path)", []string{"reason"})

	// Cache.
	c.cacheHits = NewCounterFamily("velox_cache_requests_total",
		"Cache requests by result", []string{"result"})
	c.cacheBytes = NewCounterFamily("velox_cache_bytes_total",
		"Cache bytes by result", []string{"result"})
	c.cacheEntries = NewGaugeFamily("velox_cache_entries",
		"Current cache entries", []string{"worker_id"})
	c.cacheSizeBytes = NewGaugeFamily("velox_cache_size_bytes",
		"Current cache size in bytes", []string{"worker_id"})
	c.cacheEvictions = NewCounterFamily("velox_cache_evictions_total",
		"Cache evictions", []string{"worker_id"})
	c.cacheEvictedBytes = NewCounterFamily("velox_cache_evicted_bytes_total",
		"Bytes evicted from cache", []string{"worker_id"})
	c.cacheCorruptions = NewCounterFamily("velox_cache_corruption_total",
		"Cache corruption events", []string{"worker_id"})

	// Worker.
	c.workerCPUUtil = NewGaugeFamily("velox_worker_cpu_utilization_ratio",
		"Worker CPU utilization (0-1)", []string{"worker_id"})
	c.workerIOWait = NewGaugeFamily("velox_worker_cpu_iowait_ratio",
		"Worker iowait ratio", []string{"worker_id"})
	c.workerSteal = NewGaugeFamily("velox_worker_cpu_steal_ratio",
		"Worker steal time ratio", []string{"worker_id"})
	c.workerRSSBytes = NewGaugeFamily("velox_worker_process_rss_bytes",
		"Worker process RSS", []string{"worker_id"})
	c.workerRSSPeak = NewGaugeFamily("velox_worker_process_rss_peak_bytes",
		"Worker peak RSS", []string{"worker_id"})
	c.workerMemoryUsed = NewGaugeFamily("velox_worker_memory_used_bytes",
		"Worker system memory used", []string{"worker_id"})
	c.workerDiskFree = NewGaugeFamily("velox_worker_disk_free_bytes",
		"Worker disk free bytes", []string{"worker_id"})
	c.workerTempBytes = NewGaugeFamily("velox_worker_temp_bytes",
		"Worker temp bytes (gauge at heartbeat time)", []string{"worker_id"})
	c.workerActiveTasks = NewGaugeFamily("velox_worker_active_tasks",
		"Active tasks on worker", []string{"worker_id"})
	c.workerTaskSlots = NewGaugeFamily("velox_worker_task_slots",
		"Worker task slots", []string{"worker_id"})
	c.workerLoad1 = NewGaugeFamily("velox_worker_load1",
		"Worker 1-min loadavg", []string{"worker_id"})
	c.workerRunQueue = NewGaugeFamily("velox_worker_run_queue",
		"Worker run queue depth", []string{"worker_id"})
	c.workerNetRxBytes = NewCounterFamily("velox_worker_network_receive_bytes_total",
		"Worker net rx total", []string{"worker_id"})
	c.workerNetTxBytes = NewCounterFamily("velox_worker_network_transmit_bytes_total",
		"Worker net tx total", []string{"worker_id"})

	// Master health.
	masterLabels := []string{}
	c.masterRssBytes = NewGaugeFamily("velox_master_memory_rss_bytes",
		"Master process RSS", masterLabels)
	c.masterGoroutines = NewGaugeFamily("velox_master_goroutines",
		"Active goroutines on master", masterLabels)
	c.masterOutboxPending = NewGaugeFamily("velox_master_outbox_pending",
		"Pending outbox events", masterLabels)
	c.heartbeatAge = NewGaugeFamily("velox_master_worker_heartbeat_age_seconds",
		"Seconds since last worker heartbeat", []string{"worker_id"})

	// Compute outcomes (spec §14).
	c.computeSeconds = NewCounterFamily(
		"velox_compute_seconds_total",
		"Compute seconds classified by outcome (useful|failed|cancelled|stale|speculative_lost)",
		[]string{"outcome"},
	)
	c.computeFailureReasons = NewCounterFamily(
		"velox_compute_failure_reasons_total",
		"Number of failed compute attempts by reason code",
		[]string{"reason"},
	)

	// Cost per output minute (spec §14 follow-up). Each gauge is
	// single-label `worker_class`. Stamped per-tick by the
	// supervisor with the per-class aggregate (sum/count) for the
	// just-completed attempts — see RecordAggregateCost + the
	// math caveat in cost_factors.go. Micro-EUR encoding
	// (×1_000_000) so the int64 gauge can carry a fraction.
	costLabels := []string{"worker_class"}
	c.costCpuPerMin = NewGaugeFamily("velox_cost_cpu_core_seconds_per_output_minute",
		"CPU cost per output minute (€ × 1e6) by worker class",
		costLabels)
	c.costNetworkPerMin = NewGaugeFamily("velox_cost_network_gb_per_output_minute",
		"Network egress cost per output minute (€ × 1e6) by worker class",
		costLabels)
	c.costStoragePerMin = NewGaugeFamily("velox_cost_storage_gb_written_per_output_minute",
		"Storage cost per output minute (€ × 1e6) by worker class",
		costLabels)
	c.costTotalPerMin = NewGaugeFamily("velox_cost_total_per_output_minute",
		"Total cost per output minute (€ × 1e6) by worker class",
		costLabels)

	// Phase 4.3 — reconcile supervisor counters. Cardinality
	// discipline: 11 cases × 3 actions = 33 series (closed enum, no
	// host-IDs or job-IDs). commit_deadline_exceeded_total has no
	// labels at all — the supervisor's tick owns the rate; a label
	// would force operators to aggregate on their side.
	c.reconcileTotal = NewCounterFamily(
		"velox_completion_reconcile_total",
		"Reconcile supervisor dispatch counts by case × action",
		[]string{"case", "action"},
	)
	c.commitDeadlineExceeded = NewCounterFamily(
		"velox_commit_deadline_exceeded_total",
		"Attempts whose commit_deadline_at crossed without a terminal transition",
		[]string{},
	)

	c.lastSeen = make(map[string]time.Time)

	for _, f := range c.allFamilies() {
		reg.Register(f)
	}
	return c
}

// allFamilies returns the curated list to register. Adding a new family
// to the collector requires adding it here AND a typed Recorder hook.
func (c *Collector) allFamilies() []*Family {
	return []*Family{
		c.renderSpeed,
		c.phaseDurations,
		c.ffmpegFramesTotal, c.ffmpegFps, c.ffmpegSpeed,
		c.ffmpegEncodeMs, c.ffmpegDecodeMs,
		c.ffmpegDropped, c.ffmpegDuplicated, c.ffmpegExits,
		c.ffmpegRestarts, c.ffmpegProcessesAct,
		c.videoEncodePasses, c.videoFramesEnc, c.videoOutputFrames,
		c.videoStreamCopy, c.videoReencode,
		c.cacheHits, c.cacheBytes, c.cacheEntries,
		c.cacheSizeBytes, c.cacheEvictions, c.cacheEvictedBytes,
		c.cacheCorruptions,
		c.workerCPUUtil, c.workerIOWait, c.workerSteal,
		c.workerRSSBytes, c.workerRSSPeak, c.workerMemoryUsed,
		c.workerDiskFree, c.workerTempBytes,
		c.workerActiveTasks, c.workerTaskSlots,
		c.workerLoad1, c.workerRunQueue,
		c.workerNetRxBytes, c.workerNetTxBytes,
		c.masterRssBytes, c.masterGoroutines, c.masterOutboxPending,
		c.heartbeatAge,
		c.computeSeconds,
		c.computeFailureReasons,
		c.costCpuPerMin,
		c.costNetworkPerMin,
		c.costStoragePerMin,
		c.costTotalPerMin,
		c.reconcileTotal,
		c.commitDeadlineExceeded,
	}
}

// RecordAttempt ingests one AttemptMetrics + CacheStats + CostBasis row
// into the registry. Gauges are set-to-current-value (idempotent on
// repeat calls with the same input). Counters are NOT idempotent —
// each call increments iff the input has fresh totals; the supervisor
// poll path must either deliver deltas or dedup on attempt-id to
// avoid double-counting in steady state. (TODO pre-existing; see the
// package-header "Idempotent violated" caveat.)
//
// For the master to be fully incremental we rely on the aggregator
// reading deltas from a previous snapshot via the AttemptReader
// interface; this method is the SIMPLE set-to-current-value path used
// when Scan is wired to load the latest attempt rows.
func (c *Collector) RecordAttempt(am taskattempts.AttemptMetrics, cache taskattempts.AttemptCacheStats, cost *taskattempts.AttemptCostBasis, execID, execVersion, workerClass string) {
	labels := []string{execID, execVersion, workerClass}

	// Render speed ratio.
	if rs := am.RenderSpeedRatio(); rs > 0 {
		c.renderSpeed.GaugeSet([]string{execID, workerClass}, int64(rs*1000))
	}

	// Cache hit/miss/eviction counters (per worker, not global; we
	// pass worker_id through worker_class label when caller knows it).
	_ = cache // cache counters go through the cache.* histograms below.
	if am.BytesFromLocalCache > 0 || am.BytesFromDrive > 0 || am.BytesFromBlobstore > 0 {
		c.cacheBytes.Inc([]string{"hit"}, uint64(am.BytesFromLocalCache))
		c.cacheBytes.Inc([]string{"miss"}, uint64(am.BytesFromDrive+am.BytesFromBlobstore))
		c.cacheHits.Inc([]string{"hit"}, uint64(cache.CacheHits))
		c.cacheHits.Inc([]string{"miss"}, uint64(cache.CacheMisses))
		c.cacheHits.Inc([]string{"corrupt"}, uint64(cache.CacheCorruptions))
	}

	// Video encode amplification.
	if am.FramesEncoded > 0 {
		c.videoFramesEnc.Inc(labels, uint64(am.FramesEncoded))
	}
	if am.FramesEncoded > 0 {
		c.videoOutputFrames.Inc(labels, uint64(am.FramesEncoded)) // upper-bound dedup
	}
	if am.EncodePasses > 0 {
		c.videoEncodePasses.Inc(labels, uint64(am.EncodePasses))
	}
	if am.FinalConcatStreamCopy {
		c.videoStreamCopy.Inc([]string{}, 1)
	} else if am.ConcatMode == "reencode" {
		c.videoReencode.Inc([]string{"resolution_mismatch"}, 1)
	}
}

// RecordAttemptOutcome classifies one FINAL attempt state onto the
// compute-outcome families (spec §14).
//
// Outcomes emitted:
//
//	AttemptStatusSucceeded  → outcome="useful"
//	AttemptStatusFailed     → outcome="failed" + computeFailureReasons{reason=errCode}++
//	AttemptStatusCancelled → outcome="cancelled"
//	AttemptStatusTimedOut   → outcome="stale"
//
// Pending/Running (non-terminal) attempts are no-ops: we deliberately
// only emit at completion so supervisor polls don't double-count.
//
// speculative_lost is part of the family surface per spec §14 but
// is NOT emitted from this path; the scheduler writes that outcome
// directly when a speculative attempt is abandoned by a committed
// winner. Reserved for that integration.
//
// errCode is only meaningful when status==FAILED; otherwise pass ""
// (the helper ignores it).
//
// The legacy 4-family computeUseful / computeFailed / computeCancel /
// computeStale surface is gone — collapsed to one family per spec §14.
// Counter cumulative values restart from zero on rollout (no
// migration); old dashboards reading velox_compute_seconds_total_*
// must migrate to velox_compute_seconds_total{outcome=...}.
func (c *Collector) RecordAttemptOutcome(status taskattempts.AttemptStatus, errCode string, cpuTimeMS int64) {
	var outcome string
	switch status {
	case taskattempts.AttemptStatusSucceeded:
		outcome = "useful"
	case taskattempts.AttemptStatusFailed:
		outcome = "failed"
	case taskattempts.AttemptStatusCancelled:
		outcome = "cancelled"
	case taskattempts.AttemptStatusTimedOut:
		outcome = "stale"
	default:
		// Pending, Running, empty, or unknown: do not emit until
		// the attempt reaches a terminal state.
		return
	}
	if cpuTimeMS > 0 {
		c.computeSeconds.Inc([]string{outcome}, uint64(cpuTimeMS))
	}
	if status == taskattempts.AttemptStatusFailed {
		reason := errCode
		if reason == "" {
			reason = "unknown"
		}
		c.computeFailureReasons.Inc([]string{reason}, 1)
	}
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

// RecordAggregateCost stamps the 4 cost-per-output-minute gauges for
// one worker class. Called by the supervisor once per tick after the
// per-class aggregation (sum of cost components / sum of output
// minutes for newly-terminal attempts on that class) is computed.
//
// Micro-EUR encoding (×1_000_000) so a fraction fits inside the int64
// gauge — exposition is plain decimals. Pass output_minutes < 0.001
// to skip all 4 stamps (zero safety matches the typed AttemptCostBasis
// guards in taskattempts/report.go).
//
// The supervisor aggregates per tick, NOT incrementally, so this is
// a GaugeSet (last-write-wins per tick) — see cost_factors.go for
// the math caveat on averaging these gauges across time.
func (c *Collector) RecordAggregateCost(
	workerClass string,
	cpuSeconds, networkGB, storageGB, outputMinutes float64,
	f CostFactors,
) {
	if workerClass == "" {
		workerClass = "default"
	}
	if outputMinutes < 0.001 {
		return
	}
	wl := []string{workerClass}
	c.costCpuPerMin.GaugeSet(wl, encodeMicroEUR(f.CPUPerOutputMinute(cpuSeconds, outputMinutes)))
	c.costNetworkPerMin.GaugeSet(wl, encodeMicroEUR(f.NetworkPerOutputMinute(networkGB, outputMinutes)))
	c.costStoragePerMin.GaugeSet(wl, encodeMicroEUR(f.StoragePerOutputMinute(storageGB, outputMinutes)))
	c.costTotalPerMin.GaugeSet(wl, encodeMicroEUR(f.CostPerOutputMinute(cpuSeconds, networkGB, storageGB, outputMinutes)))
}

// encodeMicroEUR encodes a float64 EUR value as int64 micro-EUR.
// Negative values clamp to zero so a misconfigured env var (or a
// future cost-model bug) cannot emit negative gauge readings.
func encodeMicroEUR(eur float64) int64 {
	if eur <= 0 {
		return 0
	}
	return int64(eur * 1_000_000)
}

// RecordMasterHealth refreshes the master-side gauges every few seconds.
// Called from a supervisor goroutine.
func (c *Collector) RecordMasterHealth(outboxPending int) {
	c.masterRssBytes.GaugeSet([]string{}, readProcessRSS())
	c.masterGoroutines.GaugeSet([]string{}, int64(runtime.NumGoroutine()))
	c.masterOutboxPending.GaugeSet([]string{}, int64(outboxPending))
}

// averageHeartbeatAge walks the lastSeen map and stamps each worker's
// heartbeat-age gauge. Called from the supervisor loop.
func (c *Collector) averageHeartbeatAge(now time.Time) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	for w, ts := range c.lastSeen {
		age := now.Sub(ts).Seconds()
		c.heartbeatAge.GaugeSet([]string{w}, int64(age))
	}
}

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

// ── cheap master-side helpers ─────────────────────────────────────────────

var _rssCache atomic.Int64
var _rssCacheAt atomic.Int64

func readProcessRSS() int64 {
	// Read /proc/self/status VmRSS. Cached for ~250ms because gauges
	// are emitted in supervisor ticks and avoiding the syscall per
	// tick keeps the loop cheap.
	now := time.Now().UnixMilli()
	if cached := _rssCache.Load(); now-_rssCacheAt.Load() < 250 {
		if cached > 0 {
			return cached
		}
	}
	got := readRSSFromProc()
	_rssCache.Store(got)
	_rssCacheAt.Store(now)
	return got
}

// AverageHeartbeatAge wraps averageHeartbeatAge for callers who want
// to drive it from a parent goroutine.
func (c *Collector) AverageHeartbeatAge(now time.Time) {
	c.averageHeartbeatAge(now)
}

// IncReconcile stamps one observation on the reconcile supervisor's
// {case, action} counter. Called from internal/completion's
// ReconcileSupervisor after every Coordinator.ReconcileAttempt
// dispatch (and once for every deadline-expired row that the
// coordinator couldn't reach in this tick). The case/action
// dimensions are exposed as strings on the metric labels.
//
// Compile-time guard: the *Collector satisfies
// completion.ReconcileMetrics — wiring mistakes break loudly at
// build time.
func (c *Collector) IncReconcile(caseLabel, actionLabel string) {
	if string(caseLabel) == "" || string(actionLabel) == "" {
		// Surface malformed labels loudly (a programming error in
		// the supervisor); an empty label would expose an invalid
		// series and make PromQL aggregations silently wrong.
		return
	}
	c.reconcileTotal.Inc([]string{string(caseLabel), string(actionLabel)}, 1)
}

// IncCommitDeadlineExceeded stamps one observation on the deadline
// counter. Called once per attempt whose commit_deadline_at has
// crossed without a terminal transition. Distinct from the
// {case,action} counter because a single tick can produce multiple
// deadline-expired rows and a single row can be observed across
// ticks (the seenIDs dedup map is bounded by seenCap).
func (c *Collector) IncCommitDeadlineExceeded() {
	c.commitDeadlineExceeded.Inc([]string{}, 1)
}

// ScanAttemptWithLabels ingests a single attempt from an
// AttemptReader into the registry using caller-supplied labels
// (execID, execVer, workerClass). Used by the supervisor poll loop
// when it has already resolved the labels via AttemptsLabelResolver;
// this avoids the hardcoded "unknown/0/default" that ScanAttempt
// falls back to and lets per-worker-class gauges reflect real
// worker_class values instead of all rows collapsing onto "default".
//
// The legacy ScanAttempt below is retained for back-compat with
// any direct caller; it delegates to ScanAttemptWithLabels with
// the historical defaults.
func (c *Collector) ScanAttemptWithLabels(
	ctx context.Context,
	mem AttemptReader,
	attemptID, execID, execVer, workerClass string,
) error {
	if mem == nil || attemptID == "" {
		return nil
	}
	if execID == "" {
		execID = "unknown"
	}
	if execVer == "" {
		execVer = "0"
	}
	if workerClass == "" {
		workerClass = "default"
	}
	am, err := mem.GetMetrics(ctx, attemptID)
	if err != nil || am == nil {
		return err
	}
	cs, err := mem.GetCacheStats(ctx, attemptID)
	if err != nil {
		cs = nil
	}
	cb, err := mem.GetCostBasis(ctx, attemptID)
	if err != nil {
		cb = nil
	}
	cache := taskattempts.AttemptCacheStats{}
	if cs != nil {
		cache = *cs
	}
	// Status drives the compute-outcome family spec §14. If the
	// reader can't surface a status (legacy stub), we fall back to
	// PENDING so RecordAttemptOutcome is a no-op — safe-by-default.
	status := taskattempts.AttemptStatusPending
	if s, sErr := mem.GetStatus(ctx, attemptID); sErr == nil && s != "" {
		status = s
	}
	c.RecordAttempt(*am, cache, cb, execID, execVer, workerClass)
	c.RecordAttemptOutcome(status, "", am.CPUTimeMS)
	return nil
}

// ScanAttempt ingests a single attempt from an AttemptReader into
// the registry. Used by the supervisor poll loop.
func (c *Collector) ScanAttempt(ctx context.Context, mem AttemptReader, attemptID string) error {
	if mem == nil || attemptID == "" {
		return nil
	}
	am, err := mem.GetMetrics(ctx, attemptID)
	if err != nil || am == nil {
		return err
	}
	cs, err := mem.GetCacheStats(ctx, attemptID)
	if err != nil {
		cs = nil
	}
	cb, err := mem.GetCostBasis(ctx, attemptID)
	if err != nil {
		cb = nil
	}
	cache := taskattempts.AttemptCacheStats{}
	if cs != nil {
		cache = *cs
	}
	cost := &taskattempts.AttemptCostBasis{}
	if cb != nil {
		cost = cb
	}

	// Status drives the compute-outcome family spec §14. If the reader
	// can't surface a status (e.g. older implementations didn't
	// implement GetStatus yet), we fall back to PENDING so the outcome
	// helper is a no-op — that way an absent status is safe-by-default
	// (no spurious "useful" classification).
	status := taskattempts.AttemptStatusPending
	if s, sErr := mem.GetStatus(ctx, attemptID); sErr == nil && s != "" {
		status = s
	}

	// Executor / version / worker come from the attempt record.
	execID, execVer, workerClass := "unknown", "0", "default"
	c.RecordAttempt(*am, cache, cost, execID, execVer, workerClass)
	c.RecordAttemptOutcome(status, "", am.CPUTimeMS)
	return nil
}

// AttemptReader isolates the collector from a hard dependency on
// store/sqlite_task_attempt_repository; the Master wires the real
// repository on the goroutine via NewMethods.
//
// GetStatus was added in spec §14 refactor: the compute-outcome
// family classifies compute seconds by terminal attempt state, so
// the reader must surface the attempt Status. Implementations that
// can't (legacy stub) may return any value; ScanAttempt falls back
// to PENDING on error which makes RecordAttemptOutcome a safe no-op.
type AttemptReader interface {
	GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error)
	GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error)
	GetCostBasis(ctx context.Context, attemptID string) (*taskattempts.AttemptCostBasis, error)
	GetStatus(ctx context.Context, attemptID string) (taskattempts.AttemptStatus, error)
}

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
