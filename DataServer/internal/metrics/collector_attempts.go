// Package metrics / collector_attempts.go
//
// Per-attempt metric aggregation loop sliced out of collector.go so
// the Collector struct definition stays focused on registration.
//
// ScanAttemptWithLabels is the supervisor's hot path: every newly-terminal
// attempt per tick is fed through it to stamp both the attempt-metrics families
// and the
// compute-outcome family (spec §14).
//
// AttemptReader is the interface the supervisor depends on,
// defined in metrics so the supervisor can build against it without
// importing internal/store directly.
package metrics

import (
	"context"
	"fmt"

	"velox-server/internal/taskattempts"
)

// AttemptReader isolates the collector from a hard dependency on
// store/sqlite_task_attempt_repository; the Master wires the real
// repository on the goroutine via NewMethods.
//
// GetStatus was added in spec §14 refactor: the compute-outcome
// family classifies compute seconds by terminal attempt state, so
// the reader must surface the attempt Status. Implementations that
// can't (legacy stub) may return any value; the labeled scan falls back
// to PENDING on error which makes RecordAttemptOutcome a safe no-op.
type AttemptReader interface {
	GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error)
	GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error)
	GetCostBasis(ctx context.Context, attemptID string) (*taskattempts.AttemptCostBasis, error)
	GetStatus(ctx context.Context, attemptID string) (taskattempts.AttemptStatus, error)
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
	if c.operational != nil {
		lookups := cache.CacheLookups
		if lookups == 0 {
			lookups = cache.CacheHits + cache.CacheMisses
		}
		c.operational.RecordCacheSnapshot(cache.UniqueAssetsRequested, lookups, cache.CacheHits, cache.CacheMisses)
		c.operational.RecordCacheDownloads(cache.CacheDownloadCount, cache.CacheDownloadBytes)
	}
	// execVersion is intentionally unused here: the video counter
	// families are documented (docs/metrics-catalog.md) with the
	// single executor_id label only, so exec_version is not part of
	// any label tuple stamped by this method.

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

	// Video encode amplification. The video counter families are
	// registered (and documented in docs/metrics-catalog.md) with the
	// single executor_id label only — NOT the 3-label {executor_id,
	// exec_version, worker_class} tuple used by the attempt gauges.
	// Passing the wider tuple here used to panic the metrics supervisor
	// (label len mismatch), blocking the taskrunner/parallelism gauges
	// from being stamped.
	videoLabels := []string{execID}
	if am.FramesEncoded > 0 {
		c.videoFramesEnc.Inc(videoLabels, uint64(am.FramesEncoded))
	}
	if am.FramesEncoded > 0 {
		c.videoOutputFrames.Inc(videoLabels, uint64(am.FramesEncoded)) // upper-bound dedup
	}
	if am.EncodePasses > 0 {
		c.videoEncodePasses.Inc(videoLabels, uint64(am.EncodePasses))
	}
	if am.FinalConcatStreamCopy {
		c.videoStreamCopy.Inc([]string{}, 1)
	} else if am.ConcatMode == "reencode" {
		c.videoReencode.Inc([]string{"resolution_mismatch"}, 1)
	}

	// Derived scorecard gauges (Scorecard v2 / Step 18). Values are
	// dimensionless ratios or normalized per-output-minute rates so
	// operators can compare attempts directly.
	workerClassLabel := []string{workerClass}
	if rf := am.RenderFactor(); rf > 0 {
		c.renderFactor.GaugeSet(workerClassLabel, int64(rf*1_000_000))
	}
	if em := am.EncodeMsPerOutputMinute(); em > 0 {
		c.encodeMsPerOutputMinute.GaugeSet(workerClassLabel, int64(em))
	}
	if cm := am.CpuMsPerOutputMinute(); cm > 0 {
		c.cpuMsPerOutputMinute.GaugeSet(workerClassLabel, int64(cm))
	}
	if ta := am.TempStorageAmplification(); ta > 0 {
		c.tempWriteAmplification.GaugeSet(workerClassLabel, int64(ta*1_000_000))
	}
	if chr := cache.CacheHitRatio(); chr > 0 {
		c.cacheHitRatio.GaugeSet(workerClassLabel, int64(chr*1_000_000))
	}
	if dt := am.DownloadThroughputBytesPerSec(); dt > 0 {
		c.downloadThroughput.GaugeSet(workerClassLabel, int64(dt))
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

// ScanAttemptWithLabels ingests a single attempt from an
// AttemptReader into the registry using caller-supplied labels
// (execID, execVer, workerClass). Used by the supervisor poll loop
// when it has already resolved the labels via AttemptsLabelResolver;
// this avoids collapsing all rows onto hardcoded "unknown/0/default"
// labels and lets per-worker-class gauges reflect real worker_class values.
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
	if err != nil {
		return err
	}
	if am == nil {
		return fmt.Errorf("metrics: attempt %q returned nil metrics", attemptID)
	}
	cs, err := mem.GetCacheStats(ctx, attemptID)
	if err != nil {
		return fmt.Errorf("metrics: attempt %q cache stats: %w", attemptID, err)
	}
	cb, err := mem.GetCostBasis(ctx, attemptID)
	if err != nil {
		return fmt.Errorf("metrics: attempt %q cost basis: %w", attemptID, err)
	}
	cache := taskattempts.AttemptCacheStats{}
	if cs != nil {
		cache = *cs
	}
	// Status drives the compute-outcome family spec §14. If the
	// reader can't surface a status, we fall back to
	// PENDING so RecordAttemptOutcome is a no-op — safe-by-default.
	status := taskattempts.AttemptStatusPending
	s, sErr := mem.GetStatus(ctx, attemptID)
	if sErr != nil {
		return fmt.Errorf("metrics: attempt %q status: %w", attemptID, sErr)
	}
	if s != "" {
		status = s
	}
	c.RecordAttempt(*am, cache, cb, execID, execVer, workerClass)
	c.RecordAttemptOutcome(status, "", am.CPUTimeMS)
	// Scorecard v2 / Step 13: classify errors when the attempt
	// failed and the worker populated error classification fields.
	if status == taskattempts.AttemptStatusFailed && am.ErrorComponent != "" {
		c.RecordErrorClassification("", am.ErrorComponent, am.ErrorPhase)
	}
	// Scorecard v2 / Step 17: accumulate waste counters when the
	// worker reported waste fields. Only emit non-zero values to
	// avoid spamming the counter with zero-increment noise.
	if am.RetryCount > 0 {
		c.RecordWaste("retry_count", uint64(am.RetryCount))
	}
	if am.WastedCPUMS > 0 {
		c.RecordWaste("wasted_cpu_ms", uint64(am.WastedCPUMS))
	}
	if am.WastedDownloadBytes > 0 {
		c.RecordWaste("wasted_download_bytes", uint64(am.WastedDownloadBytes))
	}
	if am.WastedCostEstimate > 0 {
		c.RecordWaste("wasted_cost_estimate", uint64(am.WastedCostEstimate*1_000_000))
	}
	// Scorecard v2: engine-aggregate phase columns are stamped by
	// the supervisor tick (tickOnce) which prefers detailed phase
	// rows and falls back to the aggregate columns only when no
	// detailed rows exist for the attempt.  This avoids double-
	// counting on the velox_engine_phase_duration_seconds histogram.
	return nil
}

// initComputeFamilies creates the spec §14 compute-outcome counter
// families recorded by RecordAttemptOutcome. Called once from
// NewCollector at boot.
func (c *Collector) initComputeFamilies() {
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
}

// computeFamilies returns the compute-outcome subset registered by
// NewCollector via allFamilies.
func (c *Collector) computeFamilies() []*Family {
	return []*Family{
		c.computeSeconds,
		c.computeFailureReasons,
	}
}

// initDerivedFamilies creates the Scorecard v2 / Step 18 derived
// gauges recorded by RecordAttempt. Called once from NewCollector at
// boot.
func (c *Collector) initDerivedFamilies() {
	costLabels := []string{"worker_class"}
	c.renderFactor = NewGaugeFamily("velox_render_factor",
		"Wall clock seconds per output second (lower is faster)",
		costLabels)
	c.encodeMsPerOutputMinute = NewGaugeFamily("velox_encode_ms_per_output_minute",
		"Engine segment build milliseconds per output minute",
		costLabels)
	c.cpuMsPerOutputMinute = NewGaugeFamily("velox_cpu_ms_per_output_minute",
		"CPU milliseconds per output minute",
		costLabels)
	c.tempWriteAmplification = NewGaugeFamily("velox_temp_write_amplification",
		"Temp bytes written per output byte",
		costLabels)
	c.cacheHitRatio = NewGaugeFamily("velox_cache_hit_ratio",
		"Cache hit ratio (0-1)",
		costLabels)
	c.downloadThroughput = NewGaugeFamily("velox_download_throughput_bytes_per_second",
		"Download throughput in bytes per second",
		costLabels)
}

// derivedFamilies returns the derived-scorecard subset registered by
// NewCollector via allFamilies.
func (c *Collector) derivedFamilies() []*Family {
	return []*Family{
		c.renderFactor, c.encodeMsPerOutputMinute, c.cpuMsPerOutputMinute,
		c.tempWriteAmplification, c.cacheHitRatio, c.downloadThroughput,
	}
}
