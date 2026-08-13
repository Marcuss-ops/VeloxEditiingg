// Package worker — task dispatch to TaskRunner.
//
// task_dispatch.go owns the dispatch path invoked by executeTask:
// runJobTask (per-job timeout budget) and dispatchTaskRunner (asset
// resolution, lease protection, TaskRunner.Run and the canonical
// error/report mapping). The active-task registration/cleanup helpers
// live in task_lifecycle.go and the detailed-progress callback in
// task_progress.go; all share the activeTasksMu-managed in-memory
// state that the dispatch path mutates while a task is in flight.
package worker

import (
	"context"
	"fmt"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/artifactgraph"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

// runJobTask executes the actual task via the TaskRunner.
//
// Job timeout is 30 minutes; this matches the worker-side budget
// defined for the canonical task-native dispatch path. The deadline
// cancels the dispatch context but does NOT short-circuit the
// telemetry/result reporting on the caller side (executeTask records
// outcome via recordTaskOutcome regardless).
func (w *Worker) runJobTask(ctx context.Context, pte *PendingTaskExecution) (*taskrunner.TaskExecutionReport, error) {
	w.logger.Info("[JOB] Starting execution: id=%s executor=%s", pte.JobID, pte.ExecutorID)

	jobTimeout := 30 * time.Minute
	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()

	w.logger.Info("[JOB] Phase: registry dispatch for executor=%s", pte.ExecutorID)
	report, err := w.dispatchTaskRunner(jobCtx, pte)
	if err != nil {
		return report, err
	}
	return report, nil
}

// dispatchTaskRunner runs the TaskRunner with the pre-compiled TaskSpec
// from PendingTaskExecution.
//
// The dispatch path:
//  1. If the spec carries a payload, resolve it via the worker asset
//     bridge (resolveTaskAssets). A failure here aborts before the
//     task runner is invoked, so the executor never sees a partially
//     resolved payload.
//  2. Invoke taskRunner.Run with the (possibly resolved) spec.
//  3. On taskRunner.Run error: wrap with "taskrunner.Run: %w" and
//     surface the (possibly partial) report alongside.
//  4. On non-success report: if ErrorCode == taskrunner.CodeCanceled
//     preserve context.Canceled identity on the wire; otherwise wrap
//     with "executor <key> failed: code=%q detail=%q".
//  5. fix/artifact-metadata: validate every output artifact has a
//     non-empty Hash before declaring the task succeeded.
func (w *Worker) dispatchTaskRunner(ctx context.Context, pte *PendingTaskExecution) (*taskrunner.TaskExecutionReport, error) {
	if w.taskRunner == nil {
		return nil, fmt.Errorf("worker has no taskRunner configured; call worker.New with options to install one")
	}

	spec := pte.Spec
	assetTracker := &assetOperationTracker{cacheEnabled: w.canonicalAssetCache != nil}
	// One recorder belongs to this attempt and is shared with asset
	// resolution and TaskRunner. Binding it before resolving assets keeps
	// cache/download events in the same ordered report as runner/engine
	// events; it is never global across attempts.
	rec := telemetry.NewEventRecorder()
	session := telemetry.AttemptTelemetryFromContext(ctx)
	rec.BindAttemptTelemetry(session)
	// The pipeline (driven by the session) snapshots the journal at Stop:
	// bind the recorder now so Process/Media collectors can project their
	// facts from the canonical events.
	if session != nil {
		session.BindRecorder(rec)
	}
	attemptEvents := telemetry.NewAttemptEventMachine(rec, pte.AttemptID)
	// Fase E2: one AttemptArtifactGraph per attempt, bound to the active
	// task and threaded through the dispatch context so executors can
	// attribute intermediate files via artifactgraph.GraphFromContext. The
	// profiling log fires on EVERY exit path (defer) but only when the
	// graph has records, so idle attempts add no noise.
	artifactGraph := artifactgraph.New()
	defer w.logArtifactGraphProfiling(pte, artifactGraph)
	w.activeTasksMu.Lock()
	if active := w.activeTasks[pte.TaskID]; active != nil {
		active.AttemptEvents = attemptEvents
		active.ArtifactGraph = artifactGraph
	}
	w.activeTasksMu.Unlock()
	if attemptEvents != nil {
		attemptEvents.AttemptStarted()
	}
	ctx = telemetry.WithRecorder(ctx, rec)
	ctx = telemetry.WithAttemptEventMachine(ctx, attemptEvents)
	ctx = artifactgraph.WithGraph(ctx, artifactGraph)
	ctx = withAssetOperationTracker(ctx, assetTracker)
	ctx = withCacheAccessContext(ctx, pte.JobID, "asset")
	ctx = telemetry.WithCacheAccessWorkerID(ctx, w.config.WorkerID)
	partialReport := &taskrunner.TaskExecutionReport{
		JobID:           pte.JobID,
		ExecutorID:      pte.ExecutorID,
		Status:          "failed",
		StartedAt:       time.Now().UTC(),
		AttemptRecorder: rec,
	}
	failBeforeRun := func(code string, err error) (*taskrunner.TaskExecutionReport, error) {
		partialReport.ErrorCode = code
		partialReport.ErrorDetail = err.Error()
		partialReport.CompletedAt = time.Now().UTC()
		attachAssetOperations(partialReport, assetTracker)
		taskrunner.AppendDetailedPhases(partialReport, rec)
		return partialReport, err
	}
	if spec.Payload != nil {
		resolvedPayload, err := w.resolveTaskAssets(ctx, spec.Payload)
		if err != nil {
			return failBeforeRun("asset_resolution_failed", err)
		}
		spec.Payload = resolvedPayload
	}
	// V2 carries the canonical plan as an opaque JSON string, so the legacy
	// payload resolver intentionally leaves it untouched. Resolve every typed
	// AssetRefV2 through the same verified resolver separately, then carry only
	// local runtime bindings in context.Context. The canonical JSON and its
	// SHA remain byte-identical across workers and retries.
	_, isCompiledPlan := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON]
	if isCompiledPlan {
		bindings, err := w.resolveCompiledRenderPlanAssets(ctx, spec.Payload)
		if err != nil {
			return failBeforeRun("compiled_plan_asset_resolution_failed", err)
		}
		ctx = runtimeassets.WithBindings(ctx, bindings)
	}

	// Pass 9 — Wrap the render in a per-job clip lease so the
	// workercache.Cleanup loop never deletes an asset the executor
	// is actively reading. The lease is acquired AFTER
	// resolveTaskAssets (which stores MarkDownloadComplete=true for
	// any clip the resolver just fetched) and BEFORE taskRunner.Run
	// (so the executor reads leased rows).
	//
	// Legacy payloads may skip leases when no clip cache is wired. V2
	// plans are fail-closed above because every resolved V2 asset,
	// including final_audio, must be protected before execution. An
	// empty asset-key slice remains a valid no-lease path for legacy
	// jobs with no clip references.
	var clipLease *ClipLease
	if isCompiledPlan && w.clipCache == nil {
		return failBeforeRun("clip_lease_failed", fmt.Errorf("compiled render plan v2 requires a configured clip cache for asset leases"))
	}
	reservationStore := w.canonicalAssetCache
	if reservationStore == nil && w.clipCache != nil {
		// Headless tests and legacy worker literals may only provide the
		// concrete cache. Adapt it to the same typed protection interface
		// used by prefetch.Scheduler.SetProtectionStore.
		reservationStore = w.clipCache.AsCanonicalStore()
	}
	if w.clipCache != nil {
		assetKeys := extractAssetKeysFromJSON(spec.Payload)
		if len(assetKeys) > 0 {
			var v2Reservation *v2AssetReservation
			var leaseReleaseErr error
			if isCompiledPlan {
				var reservationErr error
				workerID := ""
				if w.config != nil {
					workerID = w.config.WorkerID
				}
				v2Reservation, reservationErr = reserveV2AssetProtection(ctx, reservationStore, workerID, pte.JobID, pte.AttemptID, assetKeys, time.Now().UTC().Add(compiledPlanReservationTTL))
				if reservationErr != nil {
					return failBeforeRun("clip_reservation_failed", fmt.Errorf("reserve V2 asset protection: %w", reservationErr))
				}
				// Register before the lease cleanup defer. Go's LIFO ordering
				// then releases the active lease first and this reservation second.
				defer func() {
					// If active lease cleanup failed, keep this reservation until
					// its bounded TTL so the durable lease reconciler can finish
					// without an eviction gap.
					if leaseReleaseErr != nil {
						if w.logger != nil {
							w.logger.Warn("[LEASE] retaining V2 reservation after lease release failure for job=%s: %v", pte.JobID, leaseReleaseErr)
						}
						return
					}
					if releaseErr := v2Reservation.ReleaseAll(leaseCleanupContext(ctx)); releaseErr != nil && w.logger != nil {
						w.logger.Warn("[LEASE] V2 reservation release failed for job=%s: %v", pte.JobID, releaseErr)
					}
				}()
			}
			leased, leaseErr := AcquireJobClips(ctx, w.clipCache, pte.JobID, assetKeys)
			if leaseErr != nil {
				return failBeforeRun("clip_lease_failed", fmt.Errorf("acquire clip lease: %w", leaseErr))
			}
			clipLease = leased
			// Detach cleanup from the job context: a timeout/cancel is exactly
			// when the lease must still be released, and workercache.Release
			// must not inherit the already-done execution context. Register
			// this defer first so the renewal stop/join defer below runs first
			// under Go's LIFO defer ordering.
			defer func() {
				leaseReleaseErr = clipLease.ReleaseAll(leaseCleanupContext(ctx))
				if leaseReleaseErr != nil && w.logger != nil {
					w.logger.Warn("[LEASE] release failed for job=%s: %v", pte.JobID, leaseReleaseErr)
				}
			}()
			// Long V2 renders need a periodic cache-lease heartbeat. Stop and
			// join the renewal goroutine before final cleanup so a tick cannot
			// race ReleaseAll after the executor returns or times out.
			if isCompiledPlan {
				renewCtx, stopRenew := context.WithCancel(ctx)
				renewDone := make(chan struct{})
				go func() {
					defer close(renewDone)
					clipLease.runRenewalLoop(renewCtx, compiledPlanLeaseRenewalInterval, func(renewErr error) {
						if w.logger != nil {
							w.logger.Warn("[LEASE] renewal failed for job=%s: %v", pte.JobID, renewErr)
						}
					})
				}()
				defer func() {
					stopRenew()
					<-renewDone
				}()
			}
		}
	}

	report, runErr := w.taskRunner.Run(ctx, spec)
	if report.AttemptEvents == nil {
		report.AttemptEvents = attemptEvents
	}
	attachAssetOperations(&report, assetTracker)
	attachAssetOperationsToPhaseMarkers(&report)
	if runErr != nil {
		return &report, fmt.Errorf("taskrunner.Run: %w", runErr)
	}
	if report.AttemptEvents != nil {
		report.AttemptEvents.ArtifactVerifyStarted()
	}
	if report.Status != "succeeded" {
		// Preserve cancellation identity for the wire result. Wrapping every
		// non-success report as a generic error would turn operator aborts
		// into FAILED attempts on the master.
		if report.ErrorCode == taskrunner.CodeCanceled {
			return &report, context.Canceled
		}
		return &report, fmt.Errorf("executor %s failed: code=%q detail=%q",
			report.ExecutorKey, report.ErrorCode, report.ErrorDetail)
	}
	// fix/artifact-metadata: validate every output artifact has a non-empty
	// Hash before declaring the task succeeded.
	for i, ref := range report.Outputs {
		if ref.Hash == "" {
			if report.AttemptEvents != nil {
				report.AttemptEvents.ArtifactVerified(telemetry.StatusFailed, fmt.Errorf("output artifact %d has empty hash", i))
			}
			return &report, fmt.Errorf("executor %s succeeded but output artifact %d has empty hash (type=%q uri=%q) — executor must provide a content hash for every produced artifact",
				report.ExecutorKey, i, ref.Type, ref.URI)
		}
	}
	if report.AttemptEvents != nil {
		report.AttemptEvents.ArtifactVerified(telemetry.StatusOK, nil)
	}
	return &report, nil
}
