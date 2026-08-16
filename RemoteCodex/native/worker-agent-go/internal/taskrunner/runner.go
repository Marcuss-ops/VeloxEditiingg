// Package taskrunner / runner.go
//
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
//
// File split:
//   - runner.go          : TaskRunner struct, NewTaskRunner, With* setters,
//     Run orchestrator, runPhase, now, specVersion.
//   - runner_report.go   : completeError, attachDetailedPhases,
//     AppendDetailedPhases — report finalization.
//   - runner_logger.go   : workerExecLogger adapter + formatFields.
//   - execution.go       : runExecute — the panic-contained Execute wrapper
//     and the post-Execute ctx check (pre-existing).
//   - upload_lifecycle.go: runUpload — the upload hand-off marker.
//   - error_mapping.go   : isPanicErr, isPanicContained, mapCtxErr —
//     the error-classification helpers (pre-existing).
//   - report_metrics.go  : mergeStatsInto + the type-coercion helpers
//     (pre-existing).
//
// PR-3.7: mergeStatsInto reads cache.CacheStats / blob.BlobStats values
// through the CacheStatsProvider / BlobStatsProvider interfaces declared
// in context.go. The interfaces themselves (and the explicit cache+blob
// imports) live in context.go, so runner.go does not need to import
// cache/ blob directly here. Field accesses like cs.Hits and bs.Publish
// resolve through the interface return types.
package taskrunner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/oteltrace"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

// TaskRunner is the generic per-task lifecycle orchestrator.
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

	// storage is the canonical StorageResolver (Fase E1) threaded into the
	// per-task ExecutionContext so executors resolve output placement
	// (ARTIFACT_STAGING) through the single central decision instead of
	// scattering os.TempDir()/filepath.Join calls. nil keeps the legacy
	// outputBase fallback in executors that predate the resolver.
	storage *storage.Resolver

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

// WithStorageResolver threads the canonical StorageResolver into every
// per-task ExecutionContext. Executors that produce a final artifact read
// the ARTIFACT_STAGING placement through it (tmpfs-with-reservation,
// NVMe fallback) instead of hardcoding an output dir. nil keeps the
// executor's legacy outputBase fallback.
func (r *TaskRunner) WithStorageResolver(s *storage.Resolver) *TaskRunner {
	r.storage = s
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
	if r.cacheStats != nil {
		cs := r.cacheStats.Stats()
		report.CacheBaseline = map[string]int64{
			"hits": cs.Hits, "misses": cs.Misses,
			"evictions": cs.Evictions, "corruptions": cs.Corruptions,
		}
		report.CacheBaselineSet = true
	}
	// The per-attempt phase recorder accumulates the detailed event
	// stream that TaskResult.phase_timings (proto field 20) carries to
	// the master. attachDetailedPhases snapshots it onto the report before
	// every return path, so success AND failure reports both carry the
	// full phase history without consuming the journal.
	rec := telemetry.RecorderFromContext(parent)
	if rec == nil {
		rec = telemetry.NewEventRecorder()
	}
	report.AttemptRecorder = rec
	report.AttemptEvents = telemetry.AttemptEventMachineFromContext(parent)
	// Attempt-scoped FFmpeg profile accumulator: executors push every
	// canonical FFmpegResult; the report finalization stamps the aggregate
	// (mergeStatsInto) on success AND failure paths.
	report.FFmpegProfiles = ffmpegrunner.NewAggregator()
	// appendPhase writes directly to report.PhaseMarkers so the
	// returned TaskExecutionReport always carries the recorded phases.
	// Run is single-goroutine; no mutex needed.
	appendPhase := func(m PhaseMarker) {
		report.PhaseMarkers = append(report.PhaseMarkers, m)
	}

	// Defensive: nil registry would brick Run. Externally we already
	// panic in NewTaskRunner; this catches post-construction mutation.
	if r.registry == nil {
		return r.completeError(rec, report, appendPhase, CodeInternalRunnerFault, "nil registry at Run time"), nil
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
		return r.completeError(rec, report, appendPhase, CodeValidationFailed,
			fmt.Sprintf("spec validation: %v", err)), nil
	}
	validateSpan.End()
	appendPhase(r.runDeferredPhase(rec, PhaseCacheLookup, "asset resolution is owned by the worker asset bridge"))

	// Phase: resolve executor from the registry.
	version := r.specVersion(spec)
	exec, lookupErr := r.registry.Resolve(spec.ExecutorID, version)
	if lookupErr != nil {
		return r.completeError(rec, report, appendPhase, CodeUnsupportedExecutor,
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
		Spec:            spec,
		ParentCtx:       parent,
		Logger:          execLog,
		Clock:           r.clock,
		Telemetry:       r.telemetry,
		Resources:       r.resources,
		LocalCache:      r.cache,
		Artifacts:       r.artifacts,
		CacheStats:      r.cacheStats,
		BlobStats:       r.blobStats,
		FFmpegProfiles:  report.FFmpegProfiles,
		StorageResolver: r.storage,
	})
	if err != nil {
		return r.completeError(rec, report, appendPhase, CodeInternalRunnerFault,
			fmt.Sprintf("build ExecutionContext: %v", err)), nil
	}

	// Phase: Executor.Validate BEFORE Execute. PR-3 invariant.
	if err := exec.Validate(spec); err != nil {
		return r.completeError(rec, report, appendPhase, CodeValidationFailed,
			fmt.Sprintf("executor.Validate: %v", err)), nil
	}
	appendPhase(r.runDeferredPhase(rec, PhasePrefetch, "prefetch is owned by the worker prefetch scheduler"))

	// Phase: Execute with panic containment + cancellation mapping.
	// Scorecard v2 / Step 15: starts a "render" span for distributed tracing.
	_, renderSpan := oteltrace.StartSpan(rc.ctx, "render",
		oteltrace.AttrJobID(spec.JobID),
		oteltrace.AttrExecutorID(spec.ExecutorID),
	)
	result, execErr := r.runExecute(rc, exec, spec, appendPhase, rec)
	renderSpan.End()

	// Preserve executor telemetry before classifying the outcome. Native
	// phases enter the canonical Attempt journal here before any report,
	// receipt, heartbeat, or TaskResult projection. Migrated executors
	// provide RawMetrics directly; legacy executors still provide Metrics
	// and are adapted below without making the map canonical.
	report.RawMetrics = result.RawMetrics
	if report.RawMetrics != nil {
		report.TypedMetrics = report.RawMetrics
	}
	// Keep the executor's legacy map as a compatibility projection only;
	// migrated executors provide RawMetrics above. The report exposes the
	// map through LegacyMetrics so remaining compatibility consumers are
	// visible and auditable.
	report.AdoptLegacyMetrics(result.Metrics)
	report.Segments = result.Segments
	if importErr := importExecutorDetailedPhases(rec, result.DetailedPhases); importErr != nil {
		report.SetLegacyMetric("telemetry.cpp_import_error", importErr.Error())
	}

	// Map internal err into a stable Code for the report.
	switch {
	case execErr == nil && (result.Status == "" || result.Status == "succeeded"):
		// success path
	case execErr == nil && isPanicContained(result):
		code := CodeExecutorPanicContained
		return r.completeError(rec, report, appendPhase, code, result.ErrorDetail), nil
	case errors.Is(execErr, context.DeadlineExceeded):
		return r.completeError(rec, report, appendPhase, CodeContextDeadlineExceeded, execErr.Error()), nil
	case errors.Is(execErr, context.Canceled):
		// PR-3.5 will split lease-loss vs operator-cancel. Today both
		// map to CodeCanceled.
		return r.completeError(rec, report, appendPhase, CodeCanceled, execErr.Error()), nil
	case execErr != nil:
		// The Executor returned an error or panicked; classify.
		if isPanicErr(execErr) {
			return r.completeError(rec, report, appendPhase, CodeExecutorPanicContained, execErr.Error()), nil
		}
		return r.completeError(rec, report, appendPhase, CodeExecuteFailed, execErr.Error()), nil
	default:
		// Executor returned a non-"succeeded" status string. Preserve a
		// canonical executor error code when one is supplied; otherwise use
		// the generic runner code. This keeps domain failures such as the
		// strict copy-only contract visible through the central report.
		code := result.ErrorCode
		if code == "" {
			code = CodeExecuteFailed
		}
		return r.completeError(rec, report, appendPhase, code,
			fmt.Sprintf("executor returned non-success status %q (code=%q detail=%q)",
				result.Status, result.ErrorCode, result.ErrorDetail)), nil
	}

	// Phase: upload (skipped if no outputs).
	uploadErr := r.runUpload(rc, result, appendPhase, rec)
	if uploadErr != nil {
		return r.completeError(rec, report, appendPhase, CodeUploadFailed, uploadErr.Error()), nil
	}

	// Phase: report - already built; mark final.
	appendPhase(r.runPhase(rec, PhaseReport, func() error { return nil }))
	report.Status = "succeeded"
	report.Outputs = result.Outputs
	// Project both legacy dotted metrics and the typed wire mirror on every
	// outcome. This must not depend on cache/blob providers because native
	// engine metrics are executor-provided.
	r.mergeStatsInto(report, report.LegacyMetrics())
	r.attachDetailedPhases(rec, report)
	return *report, nil
}

// runPhase records one canonical phase timing into the report marker
// list and, when a recorder is attached, as a detailed worker-origin
// event. The recorded event reuses the marker's wall stamps and
// duration so the task_phase_timings summary and the
// task_execution_events detail correlate exactly.
func (r *TaskRunner) runPhase(rec *telemetry.EventRecorder, name string, fn func() error) PhaseMarker {
	start := r.now()
	var phaseResources telemetry.PhaseResourceDelta
	var phaseSnapshot telemetry.PhaseResourceSnapshot
	if rec != nil {
		if session := rec.AttemptTelemetry(); session != nil {
			phaseSnapshot = session.BeginPhase()
		}
	}
	err := fn()
	end := r.now()
	if rec != nil {
		if session := rec.AttemptTelemetry(); session != nil {
			phaseResources = session.EndPhaseResource(phaseSnapshot)
		}
	}
	m := PhaseMarker{Name: name, StartedAt: start, CompletedAt: end, Status: "ok"}
	if err != nil {
		m.Status = "failed"
		m.Notes = err.Error()
	}
	if rec != nil {
		rec.Record(telemetry.EventSpec{
			Origin:       telemetry.OriginWorker,
			Scope:        telemetry.ScopeAttempt,
			Component:    "runner",
			Action:       name,
			Phase:        name,
			CPUMS:        float64(phaseResources.CPUTimeMs),
			MetadataJSON: telemetry.PhaseResourceMetadataJSON(phaseResources),
		}, start, end, end.Sub(start).Milliseconds(), m.Status, "", m.Notes)
	}
	return m
}

// runDeferredPhase records a canonical phase boundary whose work is owned by
// another lifecycle component. It deliberately does not use Status=ok: the
// marker is evidence of hand-off, not evidence that this runner completed the
// underlying operation.
func (r *TaskRunner) runDeferredPhase(rec *telemetry.EventRecorder, name, notes string) PhaseMarker {
	start := r.now()
	end := r.now()
	marker := PhaseMarker{Name: name, StartedAt: start, CompletedAt: end, Status: "deferred", Notes: notes}
	if rec != nil {
		rec.Record(telemetry.EventSpec{
			Origin:    telemetry.OriginWorker,
			Scope:     telemetry.ScopeAttempt,
			Component: "runner",
			Action:    name,
			Phase:     name,
		}, start, end, 0, marker.Status, "", marker.Notes)
	}
	return marker
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
