package taskrunner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/oteltrace"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/logger"
)

// PR-3.7: mergeStatsInto reads cache.CacheStats / blob.BlobStats
// values through the CacheStatsProvider / BlobStatsProvider interfaces
// declared in context.go. The interfaces themselves (and the explicit
// cache+blob imports) live in context.go, so runner.go does not need
// to import cache/ blob directly here. Field accesses like cs.Hits and
// bs.Publish resolve through the interface return types.

// TaskRunner is the generic per-task lifecycle orchestrator. One
// TaskRunner is safe to share across goroutines (Run is concurrency-safe);
// each Run call gets its own derived ExecutionContext, report, and panic
// recovery.
//
// PR-3.3 invariants:
//   - One Run call yields exactly one TaskExecutionReport.
//   - All 5 canonical phases are attempted; skip is implicit (e.g. cache
//     lookup is a noop when LocalCache.Get returns not-found today).
//   - Free-form errors from Executor.Execute are mapped onto the closed
//     Code* enum before being written to TaskExecutionReport.ErrorCode.
//   - A panic in Executor.Execute is contained: never propagates to the
//     caller; the report surfaces CodeExecutorPanicContained.
type TaskRunner struct {
	registry  *executor.Registry
	artifacts executor.ArtifactAccess
	cache     executor.LocalCache
	telemetry executor.Telemetry
	resources executor.ResourceLimits
	clock     executor.Clock

	// PR-3.7: stats providers for surfacing cache + blob counters into
	// TaskExecutionReport.Metrics as dotted-key entries.
	cacheStats CacheStatsProvider
	blobStats  BlobStatsProvider

	callerLog *logger.Logger
	version   int // spec-version default to attempt when master omits
}

// NewTaskRunner returns a TaskRunner wired to the given registry. The
// remaining dependencies (artifacts, cache, telemetry, resources, clock)
// have safe defaults; pass real implementations as the worker matures.
//
// Panics if reg is nil. The runner cannot function without a registry;
// letting that surface as a runtime panic at worker bootstrap is louder
// than silent failure.
func NewTaskRunner(reg *executor.Registry, callerLog *logger.Logger) *TaskRunner {
	if reg == nil {
		panic("taskrunner: NewTaskRunner requires a non-nil executor.Registry")
	}
	if callerLog == nil {
		callerLog = logger.New(logger.InfoLevel, io.Discard)
		if callerLog == nil {
			callerLog = logger.New(logger.InfoLevel, os.Stderr)
		}
	}
	return &TaskRunner{
		registry:  reg,
		artifacts: nil,
		cache:     nil,
		telemetry: nil,
		resources: nil,
		clock:     nil,
		callerLog: callerLog,
		version:   1,
	}
}

// WithArtifacts replaces the artifact backend. Returns r for chaining.
func (r *TaskRunner) WithArtifacts(a executor.ArtifactAccess) *TaskRunner {
	r.artifacts = a
	return r
}

// WithCache replaces the local cache backend. Returns r for chaining.
func (r *TaskRunner) WithCache(c executor.LocalCache) *TaskRunner {
	r.cache = c
	return r
}

// WithCacheStats installs a PR-3.7 stats provider. After each Run, the
// provider's Stats() snapshot is merged into the report metrics as
// dotted-key entries (cache.hits, cache.misses, cache.evictions,
// cache.corruptions, cache.entries, cache.bytes, cache.pinned).
func (r *TaskRunner) WithCacheStats(p CacheStatsProvider) *TaskRunner {
	r.cacheStats = p
	return r
}

// WithBlobStats installs a PR-3.7 blob stats provider. After each Run,
// the provider's Stats() snapshot is merged into the report metrics as
// dotted-key entries (blob.publish, blob.publish_failed, blob.fetch,
// blob.fetch_miss, blob.fetch_corruption, blob.entries, blob.bytes).
func (r *TaskRunner) WithBlobStats(p BlobStatsProvider) *TaskRunner {
	r.blobStats = p
	return r
}

// WithTelemetry replaces the telemetry sink. Returns r for chaining.
func (r *TaskRunner) WithTelemetry(t executor.Telemetry) *TaskRunner {
	r.telemetry = t
	return r
}

// WithResources replaces the resource limits snapshot. Returns r.
func (r *TaskRunner) WithResources(l executor.ResourceLimits) *TaskRunner {
	r.resources = l
	return r
}

// WithClock replaces the clock. Returns r.
func (r *TaskRunner) WithClock(c executor.Clock) *TaskRunner {
	r.clock = c
	return r
}

// Run drives the canonical 5-phase lifecycle for one task. The second
// return value is non-nil only when the runner itself faulted before
// it could compute a report (e.g. programmer error like a nil registry);
// in normal operation TaskExecutionReport carries the full outcome and
// the second return is nil.
//
// Spec.Validate is run BEFORE any executor lookup; Executor.Validate
// runs AFTER resolve but BEFORE resource acquisition; Executor.Execute
// runs UNDER panic containment.
func (r *TaskRunner) Run(parent context.Context, spec executor.TaskSpec) (TaskExecutionReport, error) {
	overallStart := r.now()
	report := &TaskExecutionReport{
		JobID:        spec.JobID,
		ExecutorID:   spec.ExecutorID,
		Attempts:     1,
		StartedAt:    overallStart,
		PhaseMarkers: make([]PhaseMarker, 0, 5),
	}
	// appendPhase writes directly to report.PhaseMarkers so the
	// returned TaskExecutionReport always carries the recorded phases.
	// Run is single-goroutine; no mutex needed.
	appendPhase := func(m PhaseMarker) {
		report.PhaseMarkers = append(report.PhaseMarkers, m)
	}

	// Defensive: nil registry would brick Run. Externally we already
	// panic in NewTaskRunner; this catches post-construction mutation.
	if r.registry == nil {
		return r.completeError(report, appendPhase, CodeInternalRunnerFault, "nil registry at Run time"), nil
	}

	// Phase: spec.Validate runs FIRST. The PR-3 doc invariant: validate
	// task before resource acquisition. We expand to "validate before
	// resolve"; corrupt spec is the cheapest failure to return.
	// Scorecard v2 / Step 15: starts a "validate" span for distributed tracing.
	_, validateSpan := oteltrace.StartSpan(parent, "validate",
		oteltrace.AttrJobID(spec.JobID),
		oteltrace.AttrExecutorID(spec.ExecutorID),
	)
	if err := spec.Validate(); err != nil {
		validateSpan.End()
		return r.completeError(report, appendPhase, CodeValidationFailed,
			fmt.Sprintf("spec validation: %v", err)), nil
	}
	validateSpan.End()
	appendPhase(r.runPhase(PhaseCacheLookup, func() error { return nil }, overallStart))

	// Phase: resolve executor from the registry.
	version := r.specVersion(spec)
	exec, lookupErr := r.registry.Resolve(spec.ExecutorID, version)
	if lookupErr != nil {
		return r.completeError(report, appendPhase, CodeUnsupportedExecutor,
			fmt.Sprintf("resolve %s@%d: %v", spec.ExecutorID, version, lookupErr)), nil
	}
	desc := exec.Descriptor()
	report.ExecutorKey = desc.Key()

	// Build per-task ExecutionContext.
	execLog := &workerExecLogger{
		inner: r.callerLog,
		fields: map[string]interface{}{
			"executor_id": desc.ID,
			"job_id":      spec.JobID,
		},
	}
	rc, err := newRunnerContext(ContextOptions{
		Spec:       spec,
		ParentCtx:  parent,
		Logger:     execLog,
		Clock:      r.clock,
		Telemetry:  r.telemetry,
		Resources:  r.resources,
		LocalCache: r.cache,
		Artifacts:  r.artifacts,
		CacheStats: r.cacheStats,
		BlobStats:  r.blobStats,
	})
	if err != nil {
		return r.completeError(report, appendPhase, CodeInternalRunnerFault,
			fmt.Sprintf("build ExecutionContext: %v", err)), nil
	}

	// Phase: Executor.Validate BEFORE Execute. PR-3 invariant.
	if err := exec.Validate(spec); err != nil {
		return r.completeError(report, appendPhase, CodeValidationFailed,
			fmt.Sprintf("executor.Validate: %v", err)), nil
	}
	appendPhase(r.runPhase(PhasePrefetch, func() error { return nil }, overallStart))

	// Phase: Execute with panic containment + cancellation mapping.
	// Scorecard v2 / Step 15: starts a "render" span for distributed tracing.
	_, renderSpan := oteltrace.StartSpan(rc.ctx, "render",
		oteltrace.AttrJobID(spec.JobID),
		oteltrace.AttrExecutorID(spec.ExecutorID),
	)
	result, execErr := r.runExecute(rc, exec, spec, appendPhase)
	renderSpan.End()

	// Map internal err into a stable Code for the report.
	switch {
	case execErr == nil && (result.Status == "" || result.Status == "succeeded"):
		// success path
	case execErr == nil && isPanicContained(result):
		code := CodeExecutorPanicContained
		return r.completeError(report, appendPhase, code, result.ErrorDetail), nil
	case errors.Is(execErr, context.DeadlineExceeded):
		return r.completeError(report, appendPhase, CodeContextDeadlineExceeded, execErr.Error()), nil
	case errors.Is(execErr, context.Canceled):
		// PR-3.5 will split lease-loss vs operator-cancel. Today both
		// map to CodeCanceled.
		return r.completeError(report, appendPhase, CodeCanceled, execErr.Error()), nil
	case execErr != nil:
		// The Executor returned an error or panicked; classify.
		if isPanicErr(execErr) {
			return r.completeError(report, appendPhase, CodeExecutorPanicContained, execErr.Error()), nil
		}
		return r.completeError(report, appendPhase, CodeExecuteFailed, execErr.Error()), nil
	default:
		// Executor returned a non-"succeeded" status string.
		return r.completeError(report, appendPhase, CodeExecuteFailed,
			fmt.Sprintf("executor returned non-success status %q (code=%q detail=%q)",
				result.Status, result.ErrorCode, result.ErrorDetail)), nil
	}

	// Phase: upload (skipped if no outputs).
	uploadErr := r.runUpload(rc, result, appendPhase)
	if uploadErr != nil {
		return r.completeError(report, appendPhase, CodeUploadFailed, uploadErr.Error()), nil
	}

	// Phase: report - already built; mark final.
	appendPhase(r.runPhase(PhaseReport, func() error { return nil }, overallStart))
	report.Status = "succeeded"
	report.Outputs = result.Outputs
	report.Metrics = result.Metrics
	report.Segments = result.Segments
	// PR-3.7: surface cache + blob counters as dotted-key entries.
	// Merge runs AFTER assign so a nil result.Metrics is preserved as
	// a fresh map, and an executor-provided map is widened rather than
	// overwritten.
	if r.cacheStats != nil || r.blobStats != nil {
		if report.Metrics == nil {
			report.Metrics = make(map[string]interface{})
		}
		r.mergeStatsInto(report, report.Metrics)
	}
	return *report, nil
}

// mergeStatsInto writes the cache + blob counters into m using
// dotted-key names (legacy PR-3.7 shape) AND populates the typed
// mirror on report.TypedMetrics (Scorecard v1 F3 shape). Both shapes
// are produced until downstream consumers finish adopting the typed
// envelope.
//
// The TypedMetrics fields populated today are limited to what the
// worker's cache + blob stats providers actually expose:
//   - InputBytes / OutputBytes / BytesFromDrive / BytesFromBlobstore:
//     executor-supplied dotted keys (queue_bytes, drive_bytes, ...).
//     Falls back to 0 if absent.
//   - BytesFromLocalCache: cache.bytes (the local cache's authoritative
//     "currently occupied bytes" gauge).
//   - CpuTimeMs / PeakRssBytes / frames*: executor-supplied dotted keys.
//   - FfmpegSpeedRatio / EncodePasses / FinalConcatStreamCopy /
//     ConcatMode: executor-supplied dotted keys.
//   - CpuPricePerSecond / StoragePricePerGb / NetworkPricePerGb: 0 on
//     the worker — the master multiplies utilization × price to derive
//     cost. PR-3.6 will let the worker carry these into the typed
//     envelope once a sampler lands.
//
// Safe under zero-valued providers (noop fallbacks keep the merge
// safe and idempotent for tests).
func (r *TaskRunner) mergeStatsInto(report *TaskExecutionReport, m map[string]interface{}) {
	if r.cacheStats != nil {
		cs := r.cacheStats.Stats()
		m["cache.hits"] = cs.Hits
		m["cache.misses"] = cs.Misses
		m["cache.evictions"] = cs.Evictions
		m["cache.corruptions"] = cs.Corruptions
		m["cache.entries"] = cs.Entries
		m["cache.bytes"] = cs.BytesUsed
		m["cache.pinned"] = cs.PinnedEntries
	}
	if r.blobStats != nil {
		bs := r.blobStats.Stats()
		m["blob.publish"] = bs.Publish
		m["blob.publish_failed"] = bs.PublishFailed
		m["blob.fetch"] = bs.Fetch
		m["blob.fetch_miss"] = bs.FetchMiss
		m["blob.fetch_corruption"] = bs.FetchCorruption
		m["blob.entries"] = bs.Entries
		m["blob.bytes"] = bs.Bytes
	}

	// ── Scorecard v1 typed mirror ────────────────────────────────────
	// Built on top of the dotted-key map so the canonical typed shape
	// survives the F3 transition window. If the executor never produced
	// any metric counters, the typed mirror carries the cache.bytes
	// value alone (the only field CacheStatsProvider is authoritative
	// for today) and zeros elsewhere — correct behavior.
	typed := telemetry.TypedExecutionMetrics{
		BytesFromLocalCache: positiveIntegerToInt64(m["cache.bytes"]),
		InputBytes:          positiveIntegerToInt64(m["input.bytes"]),
		OutputBytes:         positiveIntegerToInt64(m["output.bytes"]),
		BytesFromDrive:      positiveIntegerToInt64(m["drive.bytes"]),
		BytesFromBlobstore:  positiveIntegerToInt64(m["blobstore.bytes"]),
		CpuTimeMs:           positiveIntegerToInt64(m["cpu.ms"]),
		PeakRssBytes:        positiveIntegerToInt64(m["rss.peak.bytes"]),
		FramesDecoded:       positiveIntegerToInt64(m["frames.decoded"]),
		FramesComposited:    positiveIntegerToInt64(m["frames.composited"]),
		FramesEncoded:       positiveIntegerToInt64(m["frames.encoded"]),
		ConcatMode:          stringFromMap(m["concat.mode"]),

		// Scorecard v2 resource / cache / quality counters surfaced by
		// executors as dotted keys. Missing keys stay zero.
		GpuTimeMs:              positiveIntegerToInt64(m["gpu.time.ms"]),
		PeakVramBytes:          positiveIntegerToInt64(m["vram.peak.bytes"]),
		TempBytesWritten:       positiveIntegerToInt64(m["temp.bytes.written"]),
		DuplicateDownloadBytes: positiveIntegerToInt64(m["duplicate.download.bytes"]),
		MediaDurationSeconds:   floatFromMap(m["media.duration.seconds"]),
		WallClockSeconds:       floatFromMap(m["wall.clock.seconds"]),

		FfprobeValid:      int32(positiveIntegerToInt64(m["ffprobe.valid"])),
		DurationDiffSec:   floatFromMap(m["duration.diff.sec"]),
		HasVideoStream:    boolFromMap(m["has.video.stream"]),
		HasAudioStream:    boolFromMap(m["has.audio.stream"]),
		OutputFileSize:    positiveIntegerToInt64(m["output.file.size"]),
		BlackFrameRatio:   floatFromMap(m["black.frame.ratio"]),
		AudioSyncOffsetMs: positiveIntegerToInt64(m["audio.sync.offset.ms"]),
		OutputSha256:      stringFromMap(m["output.sha256"]),

		CpuPercentPeak: floatFromMap(m["cpu.percent.peak"]),
		DiskReadBytes:  positiveIntegerToInt64(m["disk.read.bytes"]),
		DiskWriteBytes: positiveIntegerToInt64(m["disk.write.bytes"]),
		NetworkRxBytes: positiveIntegerToInt64(m["network.rx.bytes"]),
		NetworkTxBytes: positiveIntegerToInt64(m["network.tx.bytes"]),
		IowaitMs:       positiveIntegerToInt64(m["iowait.ms"]),
		OpenFdsPeak:    positiveIntegerToInt64(m["open.fds.peak"]),

		AssetCacheHitCount:  positiveIntegerToInt64(m["asset.cache.hit.count"]),
		AssetCacheMissCount: positiveIntegerToInt64(m["asset.cache.miss.count"]),
		BlobCacheHitCount:   positiveIntegerToInt64(m["blob.cache.hit.count"]),
		BlobCacheMissCount:  positiveIntegerToInt64(m["blob.cache.miss.count"]),
		RenderCacheHitCount: positiveIntegerToInt64(m["render.cache.hit.count"]),
	}
	if v, ok := m["ffmpeg.speed_ratio"].(float64); ok {
		typed.FfmpegSpeedRatio = v
	}
	// encode.passes is proto3 int32 — the legacy dotted-key producer
	// may emit it as int32 or int64 depending on platform.
	if v, ok := m["encode.passes"].(int32); ok {
		typed.EncodePasses = v
	} else if v, ok := m["encode.passes"].(int64); ok && v >= 0 && v <= 0x7fffffff {
		typed.EncodePasses = int32(v)
	}
	// final_concat_stream_copy is conventionally a bool in the proto
	// and a JSON-style key in the legacy map.
	if v, ok := m["final.concat.stream_copy"].(bool); ok {
		typed.FinalConcatStreamCopy = v
	}
	report.TypedMetrics = &typed
	// ── End typed mirror ─────────────────────────────────────────────
}

// positiveIntegerToInt64 reads dotted-key counters (int64 / int32 /
// int / float64 / uint64 / uint32) and returns a non-negative int64
// compatible with proto3 wire shape. Negatives are floored to 0.
// uint64 inputs are clipped at MaxInt64 to keep varint serialization
// honest rather than silently wrapping at -1.
func positiveIntegerToInt64(v any) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	switch x := v.(type) {
	case nil:
		return 0
	case int64:
		if x < 0 {
			return 0
		}
		return x
	case int32:
		if x < 0 {
			return 0
		}
		return int64(x)
	case int:
		if x < 0 {
			return 0
		}
		return int64(x)
	case uint64:
		if x > uint64(maxInt64) {
			return maxInt64
		}
		return int64(x)
	case uint32:
		return int64(x)
	case float64:
		if x <= 0 {
			return 0
		}
		return int64(x)
	}
	return 0
}

func stringFromMap(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func floatFromMap(v any) float64 {
	if v == nil {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case int:
		return float64(x)
	}
	return 0
}

func boolFromMap(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

// runExecute is the heart of PR-3.3: it invokes Executor.Execute under a
// recover() guard so a panic never escapes the runner. It also checks
// the parent ctx after Execute returns so we can hand back lease /
// cancel / deadline errors cleanly.
func (r *TaskRunner) runExecute(rc *runnerContext, exec executor.Executor, spec executor.TaskSpec, appendPhase func(PhaseMarker)) (executor.ExecutionResult, error) {
	execStart := r.now()
	var result executor.ExecutionResult
	var execErr error
	var recovered any

	func() {
		defer func() {
			recovered = recover()
		}()
		result, execErr = exec.Execute(rc.ctx, rc, spec)
	}()

	execEnd := r.now()
	final := PhaseMarker{Name: PhaseExecute, StartedAt: execStart, CompletedAt: execEnd, Status: "ok"}
	if recovered != nil {
		final.Status = "failed"
		final.Notes = fmt.Sprintf("panic: %v", recovered)
		execErr = fmt.Errorf("%w: panic in executor.Execute: %v", ErrInternalRunnerFault, recovered)
	} else if execErr != nil {
		final.Status = "failed"
		final.Notes = execErr.Error()
	} else if result.Status != "" && result.Status != "succeeded" {
		final.Status = "failed"
		final.Notes = fmt.Sprintf("status=%q code=%q detail=%q",
			result.Status, result.ErrorCode, result.ErrorDetail)
	}
	appendPhase(final)

	if recovered != nil || execErr != nil {
		// Reset result so the caller sees the failure post-mapping.
		if recovered != nil {
			result = executor.ExecutionResult{
				Status:      "failed",
				ErrorCode:   CodeExecutorPanicContained,
				ErrorDetail: fmt.Sprintf("panic in executor.Execute: %v\n%s", recovered, debug.Stack()),
				StartedAt:   execStart,
				CompletedAt: execEnd,
			}
		}
		return result, execErr
	}

	// Post-Execute ctx check: lease / cancel / deadline.
	if err := rc.Err(); err != nil {
		return executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   mapCtxErr(err),
			ErrorDetail: err.Error(),
			StartedAt:   execStart,
			CompletedAt: execEnd,
		}, err
	}
	return result, nil
}

// runUpload publishes executor outputs. PR-3.3 minimum: identity
// publication of outputs into the report; real upload arrives when
// PersistentLocalArtifactCache (PR-3.7) lands.
func (r *TaskRunner) runUpload(rc *runnerContext, result executor.ExecutionResult, appendPhase func(PhaseMarker)) error {
	start := r.now()
	if len(result.Outputs) == 0 {
		appendPhase(PhaseMarker{Name: PhaseUpload, StartedAt: start, CompletedAt: r.now(), Status: "ok", Notes: "skipped: no outputs"})
		return nil
	}
	// PR-3.7 will publish each output through the ArtifactAccess backend.
	// Today we only record the phase marker.
	appendPhase(PhaseMarker{Name: PhaseUpload, StartedAt: start, CompletedAt: r.now(), Status: "ok", Notes: "stub: PR-3.7 wires real upload"})
	return nil
}

// runPhase records one canonical phase timing.
func (r *TaskRunner) runPhase(name string, fn func() error, fallbackStart time.Time) PhaseMarker {
	start := r.now()
	err := fn()
	end := r.now()
	m := PhaseMarker{Name: name, StartedAt: start, CompletedAt: end, Status: "ok"}
	if err != nil {
		m.Status = "failed"
		m.Notes = err.Error()
	}
	return m
}

// completeError finalizes the report under the given code and detail,
// then runs the report phase to keep the 5-phase invariant intact.
// PR-3.7: failure paths also surface cache + blob counters so operators
// see real hit/miss/eviction activity on failed-task reports rather
// than a misleading zero-map.
func (r *TaskRunner) completeError(report *TaskExecutionReport, appendPhase func(PhaseMarker), code, detail string) TaskExecutionReport {
	report.Status = "failed"
	report.ErrorCode = code
	report.ErrorDetail = detail
	report.CompletedAt = r.now()
	// Always have at least one marker (the report phase) so consumers
	// that check `len(phaseMarkers) == 0` can rely on truth: failure
	// means a phase WAS run.
	appendPhase(PhaseMarker{Name: PhaseReport, StartedAt: r.now(), CompletedAt: r.now(), Status: "ok", Notes: "failure recorded"})
	// Mirror the success-path merge; init Metrics if nil so the merge
	// does not lose cache+blob data when the executor short-circuited.
	if r.cacheStats != nil || r.blobStats != nil {
		if report.Metrics == nil {
			report.Metrics = make(map[string]interface{})
		}
		r.mergeStatsInto(report, report.Metrics)
	}
	return *report
}

func (r *TaskRunner) now() time.Time {
	if r.clock != nil {
		return r.clock.Now()
	}
	return time.Now().UTC()
}

// specVersion picks the (id, version) tuple to query.
//
// PR-3.3 ships with a single default version (1). The master will start
// announcing versioned ExecutorIDs once the task graph gains the
// ExecutorID+Version split (PR-1 contracts territory); today the runner
// uses r.version.
func (r *TaskRunner) specVersion(_ executor.TaskSpec) int {
	if r.version > 0 {
		return r.version
	}
	return 1
}

func isPanicErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrInternalRunnerFault)
}

func isPanicContained(r executor.ExecutionResult) bool {
	return r.ErrorCode == CodeExecutorPanicContained && r.Status == "failed"
}

func mapCtxErr(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return CodeContextDeadlineExceeded
	case errors.Is(err, context.Canceled):
		return CodeCanceled
	default:
		return CodeCanceled
	}
}

// ── Logger adapter ────────────────────────────────────────────────────────

// workerExecLogger wraps pkg/logger.Logger so it satisfies the
// executor.Logger interface (Info/Warn/Error taking a string + fields).
// PR-3.2 invariant: every log line emitted from an executor surfaces
// the executor_id + job_id fields.
type workerExecLogger struct {
	inner  *logger.Logger
	fields map[string]interface{}
}

func (w *workerExecLogger) prefix() string {
	if w.fields == nil || len(w.fields) == 0 {
		return ""
	}
	// Stable, deterministic field order isn't required for human logs.
	keys := make([]string, 0, len(w.fields))
	for k := range w.fields {
		keys = append(keys, k)
	}
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%s=%v", k, w.fields[k])
	}
	return "[" + out + "]"
}

func (w *workerExecLogger) with(msg string, fields map[string]interface{}) string {
	return w.prefix() + " " + msg + " " + formatFields(fields)
}

func (w *workerExecLogger) Info(msg string, fields map[string]interface{}) {
	if w.inner == nil {
		return
	}
	w.inner.Info("%s", w.with(msg, fields))
}
func (w *workerExecLogger) Warn(msg string, fields map[string]interface{}) {
	if w.inner == nil {
		return
	}
	w.inner.Warn("%s", w.with(msg, fields))
}
func (w *workerExecLogger) Error(msg string, err error, fields map[string]interface{}) {
	if w.inner == nil {
		return
	}
	extra := formatFields(fields)
	if err != nil {
		extra += " err=" + err.Error()
	}
	w.inner.Error("%s %s %s", w.prefix(), msg, extra)
}

func formatFields(fields map[string]interface{}) string {
	if len(fields) == 0 {
		return ""
	}
	out := ""
	first := true
	for k, v := range fields {
		if !first {
			out += " "
		}
		first = false
		out += fmt.Sprintf("%s=%v", k, v)
	}
	return out
}
