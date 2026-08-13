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
	"sync"
	"time"
)

// Collector is the registered metric surface for the master. It owns
// the *metrics.Registry and a curated list of typed families.
//
// The family fields are grouped by domain. Each domain's families are
// created by the matching init<Domain>Families method and returned by
// the matching <domain>Families list method, both living in the
// collector_<domain>.go file next to that domain's recorders.
type Collector struct {
	reg         *Registry
	operational *OperationalTelemetry
	forwarding  *ForwardingTelemetry

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

	// HTTP control-plane route usage (Phase 6 API-surface unification).
	// Counter with {surface, route} labels; surface is one of
	// agent|admin|fleet|legacy|other, route is the gin route TEMPLATE
	// (bounded by the route table). See collector_http.go.
	httpRouteRequests *Family // velox_master_http_route_requests_total{surface,route}

	// Single counter family with labels {error_code, component, phase}
	// for failure-reason attribution. error_code is the canonical
	// closed-enum code (CanonicalErrorCode); component/phase are
	// low-cardinality enums (canonicalErrorComponents / canonicalErrorPhases).
	errorClassification       *Family // velox_error_classification_total
	opsalertsWorkerEvalErrors *Family // velox_opsalerts_worker_evaluation_errors_total{category}
	alertEvaluationErrors     *Family // velox_alert_evaluation_errors_total{engine,category}

	// Waste/cost metrics (Scorecard v2 / Step 17).
	// Single counter family with label {waste_type} for aggregate
	// waste tracking. waste_type ∈ {retry_count, wasted_cpu_ms,
	// wasted_download_bytes, wasted_cost_estimate}.
	wasteTotal *Family // velox_waste_total

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

	// Derived scorecard gauges (Scorecard v2 / Step 18). These are
	// pure derivations from task_attempt_metrics + cache_stats. They
	// are stamped per-attempt so dashboards can aggregate percentiles
	// directly without computing in PromQL.
	renderFactor            *Family // velox_render_factor
	encodeMsPerOutputMinute *Family // velox_encode_ms_per_output_minute
	cpuMsPerOutputMinute    *Family // velox_cpu_ms_per_output_minute
	tempWriteAmplification  *Family // velox_temp_write_amplification
	cacheHitRatio           *Family // velox_cache_hit_ratio
	downloadThroughput      *Family // velox_download_throughput_bytes_per_second

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

	// Placement rejection counter. Incremented every time the
	// placement matcher rejects a candidate (velox_placement_rejections_total)
	// with a single label `reason` carrying the stable RejectionCode
	// (e.g. capacity_full, unsupported_executor, missing_capability).
	placementRejections          *Family // velox_placement_rejections_total{reason}
	compatibilityAliasReads      *Family // velox_compatibility_alias_reads_total{alias,canonical}
	compatibilityAliasRejections *Family // velox_compatibility_rejections_total{alias,canonical}

	// Engine phase timing histograms (Scorecard v2 / Step 7).
	// Two histogram families capture per-phase and per-segment
	// durations from the C++ engine sidecar and Go pipeline.
	// Labels: executor_id, worker_id, phase, status (phase histogram)
	//         executor_id, worker_id, source_type, status (segment histogram)
	// NO job_id/task_id/attempt_id for cardinality reasons.
	enginePhaseDurations   *Family // velox_engine_phase_duration_seconds
	engineSegmentDurations *Family // velox_engine_segment_duration_seconds

	// Parallelism telemetry (migration 098). Gauges stamped per-attempt.
	parallelSerialWork   *Family // velox_taskrunner_serial_work_ms
	parallelRenderWindow *Family // velox_taskrunner_render_window_ms
	parallelUnionBusy    *Family // velox_taskrunner_union_busy_ms
	parallelOverlap      *Family // velox_taskrunner_overlap_ms
	parallelIdleGap      *Family // velox_taskrunner_idle_gap_ms
	parallelPeak         *Family // velox_taskrunner_parallel_peak
	parallelAverage      *Family // velox_taskrunner_parallel_average
	parallelEfficiency   *Family // velox_taskrunner_parallel_efficiency_ratio
	parallelSpeedup      *Family // velox_taskrunner_speedup_vs_serial
	parallelOversub      *Family // velox_resource_cpu_oversubscription_ratio

	// ConflictBudget (spec §14 Blocco 5) instrumentation. Three
	// counters + one histogram capture the consecutive-err
	// conflict path on the canonical attempt_commits CAS surface
	// (UpdateReadyCountExhaustive, SetExpired, MarkCommitted,
	// SetExpiredByID). Cardinality is bounded — no labels — so
	// the families stay single-series. The histogram observes
	// the SHAPE of the streak at every conflict increment, both
	// before and at the threshold boundary. Buckets [1,2,3,5,10]
	// cover the canonical default threshold=3 plus headroom for
	// future policy bumps.
	conflictStreakReset  *Family // velox_conflict_streak_reset_total
	conflictEscalations  *Family // velox_conflict_escalations_total
	conflictStayedUnder  *Family // velox_conflict_stayed_under_threshold_total
	conflictStreakLength *Family // velox_conflict_streak_length

	// Book-keeping for diffs.
	stateMu  sync.Mutex
	lastSeen map[string]time.Time // worker_id → last heartbeat timestamp
}

// NewCollector returns a Collector with all 12 scorecard family +
// supporting families registered on reg. Each domain's families are
// created by the init<Domain>Families methods defined in the matching
// collector_<domain>.go files.
func NewCollector(reg *Registry) *Collector {
	c := &Collector{reg: reg}
	c.operational = NewOperationalTelemetry(reg)
	c.forwarding = NewForwardingTelemetry(reg)

	c.initRenderFamilies()
	c.initFFmpegFamilies()
	c.initVideoFamilies()
	c.initCacheFamilies()
	c.initWorkerFamilies()
	c.initMasterFamilies()
	c.initComputeFamilies()
	c.initDerivedFamilies()
	c.initCostFamilies()
	c.initSinkFamilies()
	c.initEngineFamilies()
	c.initHTTPFamilies()

	c.lastSeen = make(map[string]time.Time)

	for _, f := range c.allFamilies() {
		reg.Register(f)
	}
	return c
}

// OperationalTelemetry returns the delivery/database/cache sink registered
// alongside the scorecard families.
func (c *Collector) OperationalTelemetry() *OperationalTelemetry {
	if c == nil {
		return nil
	}
	return c.operational
}

// ForwardingTelemetry returns the forwarding-runner sink registered on the
// same registry (velox_forwarding_* families).
func (c *Collector) ForwardingTelemetry() *ForwardingTelemetry {
	if c == nil {
		return nil
	}
	return c.forwarding
}

// allFamilies returns the curated list to register. Adding a new family
// to the collector requires adding it here AND a typed Recorder hook.
// The per-domain subsets are composed from the <domain>Families list
// methods defined in the collector_<domain>.go files.
func (c *Collector) allFamilies() []*Family {
	var families []*Family
	families = append(families, c.renderFamilies()...)
	families = append(families, c.ffmpegFamilies()...)
	families = append(families, c.videoFamilies()...)
	families = append(families, c.cacheFamilies()...)
	families = append(families, c.workerFamilies()...)
	families = append(families, c.masterFamilies()...)
	families = append(families, c.computeFamilies()...)
	families = append(families, c.derivedFamilies()...)
	families = append(families, c.costFamilies()...)
	families = append(families, c.sinkFamilies()...)
	families = append(families, c.engineFamilies()...)
	families = append(families, c.httpFamilies()...)
	return families
}
