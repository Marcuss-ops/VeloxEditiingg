// Package worker — task dispatch to TaskRunner + active-task lifecycle.
//
// task_dispatch.go owns the dispatch path invoked by executeTask AND
// the active-task lifecycle helpers that run alongside it:
//
//	runJobTask                — wraps dispatchTaskRunner with a 30-minute
//	                            per-job timeout context (the canonical
//	                            worker-side budget).
//	dispatchTaskRunner        — resolves the pre-compiled TaskSpec's
//	                            asset payload via the worker asset
//	                            bridge, then invokes TaskRunner.Run.
//	                            Surface area:
//	                            - wraps taskRunner.Run errors with
//	                              "taskrunner.Run: %w"
//	                            - maps a non-success report to a wrapped
//	                              error that preserves the canonical
//	                              (executor_key, code, detail) tuple
//	                              on the wire
//	                            - preserves context.Canceled identity
//	                              when report.ErrorCode ==
//	                              taskrunner.CodeCanceled (operator
//	                              aborts must NOT be flattened to a
//	                              generic FAILED attempt on the master)
//	                            - enforces "every successful output has
//	                              a non-empty hash" so the executor
//	                              cannot declare success with empty
//	                              content hashes
//
//	registerActiveTask        — builds *ActiveTaskExecution, inserts it
//	                            into the activeTasks + taskIDsByJob maps
//	                            under activeTasksMu. Returns the pointer
//	                            so the caller can assign
//	                            activeTask.Cancel = jobCancel AFTER
//	                            wakeHeartbeat (preserving the original
//	                            ordering).
//	unregisterActiveTask      — deferred cleanup that mirrors the
//	                            original closure: deletes from the maps,
//	                            removes empty jobID entries, wakes the
//	                            heartbeat.
//	withJobProgressCallback   — wraps the parent context with the
//	                            progress callback that updates
//	                            activeTask.Progress under the
//	                            activeTasksMu lock.
//
// The dispatch path and the active-task lifecycle helpers live in the
// same file because the lifecycle helpers (registration, progress
// tracking, cleanup) are part of the dispatch flow's resource
// management surface — they own the in-memory state that the dispatch
// path mutates while a task is in flight.
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
	"velox-worker-agent/pkg/video/pipeline"
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
	rec.BindAttemptTelemetry(telemetry.AttemptTelemetryFromContext(ctx))
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
	if _, isCompiledPlan := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON]; isCompiledPlan {
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
	// A nil w.clipCache is the documented skip path (legacy
	// bootstrap profiles, headless tests). A empty asset-key slice
	// is also a skip path (jobs with no clip references — e.g.
	// audio-only renderings — are legitimate input and must not
	// panic the dispatch path).
	var clipLease *ClipLease
	if w.clipCache != nil {
		assetKeys := extractAssetKeysFromJSON(spec.Payload)
		if len(assetKeys) > 0 {
			leased, leaseErr := AcquireJobClips(ctx, w.clipCache, pte.JobID, assetKeys)
			if leaseErr != nil {
				return failBeforeRun("clip_lease_failed", fmt.Errorf("acquire clip lease: %w", leaseErr))
			}
			clipLease = leased
			defer clipLease.ReleaseAll(ctx)
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

// registerActiveTask builds the ActiveTaskExecution entry, inserts
// it under activeTasksMu, and returns the pointer. The caller MUST
// call wakeHeartbeat immediately after, then assign
// activeTask.Cancel = jobCancel — preserving the original ordering
// where the heartbeat goroutine sees the new entry BEFORE the cancel
// function is wired up.
func (w *Worker) registerActiveTask(taskID, attemptID string, pte *PendingTaskExecution) *ActiveTaskExecution {
	activeTask := &ActiveTaskExecution{
		TaskID:    taskID,
		AttemptID: attemptID,
		JobID:     pte.JobID,
		Task:      pte,
		LeaseID:   pte.LeaseID,
		StartedAt: time.Now(),
	}
	w.activeTasksMu.Lock()
	w.activeTasks[taskID] = activeTask
	w.taskIDsByJob[pte.JobID] = append(w.taskIDsByJob[pte.JobID], taskID)
	w.activeTasksMu.Unlock()
	return activeTask
}

// unregisterActiveTask is the deferred cleanup that mirrors the
// original closure: deletes the active task from both maps, removes
// the jobID entry when its task list drains to zero, then wakes the
// heartbeat so the next tick reports the updated state.
func (w *Worker) unregisterActiveTask(taskID string, pte *PendingTaskExecution) {
	w.activeTasksMu.Lock()
	delete(w.activeTasks, taskID)
	taskIDs := w.taskIDsByJob[pte.JobID]
	for i, tid := range taskIDs {
		if tid == taskID {
			w.taskIDsByJob[pte.JobID] = append(taskIDs[:i], taskIDs[i+1:]...)
			break
		}
	}
	if len(w.taskIDsByJob[pte.JobID]) == 0 {
		delete(w.taskIDsByJob, pte.JobID)
	}
	w.activeTasksMu.Unlock()
	w.wakeHeartbeat()
}

// logArtifactGraphProfiling emits the per-attempt intermediate-file
// profiling summary (Fase E2). Empty graphs (no records registered by the
// executor) are skipped entirely. Write-then-read candidates — the files a
// later optimization phase should consider eliminating — surface at INFO;
// the full ledger rides at DEBUG. This is the evidence base: nothing is
// removed a priori, candidates are only flagged for a decision.
func (w *Worker) logArtifactGraphProfiling(pte *PendingTaskExecution, g *artifactgraph.Graph) {
	if w == nil || g == nil || pte == nil {
		return
	}
	summary := g.Summary()
	if summary.FileCount == 0 {
		return
	}
	if len(summary.Candidates) > 0 {
		w.logger.Info("[ARTIFACT-GRAPH] attempt=%s files=%d candidates=%d reread_bytes=%d",
			pte.AttemptID, summary.FileCount, len(summary.Candidates), summary.TotalReReadBytes)
		for _, c := range summary.Candidates {
			w.logger.Info("[ARTIFACT-GRAPH]   reread_bytes=%d lifetime=%s producer=%s consumer=%s path=%s",
				c.ReReadBytes, c.Lifetime.Round(time.Millisecond), c.ProducerPhase, c.ConsumerPhase, c.Path)
		}
	}
	w.logger.Debug("[ARTIFACT-GRAPH] attempt=%s files=%d written_bytes=%d read_bytes=%d",
		pte.AttemptID, summary.FileCount, summary.TotalWrittenBytes, summary.TotalReadBytes)
}

func cumulativeMetricsEqual(left, right map[string]float64) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if other, ok := right[key]; !ok || other != value {
			return false
		}
	}
	return true
}

// withJobProgressCallback returns a child context carrying the
// canonical progress callback that updates activeTask.Progress under
// the activeTasksMu lock. The callback uses taskID to dynamically
// look up the current entry — never the captured pointer — so a
// later replace (which the original code does NOT do) would still
// route to the fresh entry.
func (w *Worker) withJobProgressCallback(parent context.Context, taskID string) context.Context {
	return pipeline.WithDetailedProgressCallback(parent, func(snapshot pipeline.ProgressSnapshot) {
		now := time.Now().UTC()
		w.activeTasksMu.Lock()
		if current := w.activeTasks[taskID]; current != nil {
			previous := current.Progress
			phaseChanged := previous.Phase != snapshot.Phase
			segmentChanged := previous.Segment != snapshot.Segment
			segmentCompleted := snapshot.SegmentCompleted &&
				(!previous.SegmentCompleted || previous.Segment != snapshot.Segment)
			identical := previous.Percent == snapshot.Percent &&
				previous.Scene == snapshot.Scene && previous.TotalScenes == snapshot.TotalScenes &&
				previous.Segment == snapshot.Segment && previous.TotalSegments == snapshot.TotalSegments &&
				previous.SegmentCompleted == snapshot.SegmentCompleted &&
				previous.Phase == snapshot.Phase && !segmentCompleted &&
				previous.FramesEncoded == snapshot.FramesEncoded &&
				previous.FramesDecoded == snapshot.FramesDecoded &&
				previous.FramesComposited == snapshot.FramesComposited &&
				previous.FfmpegSpeedX == snapshot.FfmpegSpeedX &&
				previous.ElapsedMS == snapshot.ElapsedMS &&
				cumulativeMetricsEqual(previous.CumulativeMetrics, snapshot.CumulativeMetrics)
			publishDue := !identical && (previous.LastPublishedAt.IsZero() ||
				now.Sub(previous.LastPublishedAt) >= 2*time.Second || phaseChanged || segmentChanged || segmentCompleted)

			metrics := make(map[string]float64, len(snapshot.CumulativeMetrics))
			for key, value := range snapshot.CumulativeMetrics {
				metrics[key] = value
			}
			// Keep the latest snapshot in the same canonical Attempt
			// projection even when heartbeat publication is throttled.
			// LastProgressAt describes the newest engine observation;
			// LastPublishedAt is only the local wake/throttle clock and
			// is never serialized as operator telemetry.
			if current.AttemptEvents != nil {
				// Emit lifecycle edges before the progress sample updates the
				// machine's last phase/segment context. Segment completion is
				// emitted after the sample because ProgressUpdated resets the
				// completion edge for the next segment.
				if phaseChanged {
					current.AttemptEvents.PhaseChanged(snapshot.Phase)
				}
				if segmentChanged {
					current.AttemptEvents.SegmentStarted(snapshot.Segment, snapshot.Phase)
				}
				current.AttemptEvents.ProgressUpdated(snapshot.Phase, snapshot.Segment, snapshot.Percent, snapshot.ElapsedMS, snapshot.FramesEncoded, now)
				if segmentCompleted {
					current.AttemptEvents.SegmentCompleted(snapshot.Segment, snapshot.Phase)
				}
			}
			current.Progress = JobProgress{
				Percent:     snapshot.Percent,
				Scene:       snapshot.Scene,
				TotalScenes: snapshot.TotalScenes, Segment: snapshot.Segment,
				TotalSegments:     snapshot.TotalSegments,
				SegmentCompleted:  snapshot.SegmentCompleted,
				Phase:             snapshot.Phase,
				Stage:             snapshot.Phase,
				FramesEncoded:     snapshot.FramesEncoded,
				FramesDecoded:     snapshot.FramesDecoded,
				FramesComposited:  snapshot.FramesComposited,
				FfmpegSpeedX:      snapshot.FfmpegSpeedX,
				ElapsedMS:         snapshot.ElapsedMS,
				LastProgressAt:    now,
				LastPublishedAt:   previous.LastPublishedAt,
				CumulativeMetrics: metrics,
			}
			if publishDue {
				current.Progress.LastPublishedAt = now
				w.wakeHeartbeat()
			}
		}
		w.activeTasksMu.Unlock()
	})
}
