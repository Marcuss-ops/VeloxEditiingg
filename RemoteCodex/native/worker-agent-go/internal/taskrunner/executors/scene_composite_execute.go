// Package executors — scene.composite.v1 execution path.
//
// scene_composite_execute.go owns Execute and its direct helpers
// (compiledPlanClipCount, rawMetricsFromPipeline, renderErrorCode,
// resolveOutputPath, PayloadOutputPath). The executor surface (type,
// constructor, Descriptor, Validate) lives in scene_composite.go.
package executors

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
)

// Execute performs the canonical work. It delegates to the existing
// pipeline.Runner using the explicit payload `pipeline_id` (the legacy
// hybrid.v1 fallback was retired).
//
// CAVEAT: the C++ engine runs as a synchronous subprocess; context
// cancellation propagates only AFTER the engine finishes. The
// descriptor's TemporalMode=global + Deterministic=true advertise this
// property to the master scheduler.
func (s *SceneComposite) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	startedAt := time.Now().UTC()
	timer := jobPhaseTimerFromExecutionContext(execCtx)
	gpuTracker := gpuTransferTrackerFromExecutionContext(execCtx)
	// Legacy compatibility projection for pre-typed report consumers. The
	// canonical producer output is rawMetrics below; this map is retired
	// incrementally as downstream consumers adopt RawMetrics.
	metrics := make(map[string]interface{})
	rec := recorderFromExecutionContext(execCtx)
	planHandle := rec.Begin(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask, Component: "worker.plan", Action: "compile"})
	if planHandle != nil {
		planHandle.SetMetadata("pipeline_id", resolvePipelineID(spec.Payload))
	}
	planCompleted := false
	defer func() {
		if planHandle == nil || planCompleted {
			return
		}
		if execCtx.Err() != nil {
			planHandle.Abort("context_cancelled", execCtx.Err().Error())
			return
		}
		planHandle.Abort("execution_failed", "scene composite exited before plan completion")
	}()

	outputPath, err := s.resolveOutputPath(execCtx, spec)
	if err != nil {
		return executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   "output_path_invalid",
			ErrorDetail: err.Error(),
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		}, nil
	}

	// Phase: pipeline run. We use RunWithMetrics so callers get
	// per-phase pipeline timings AND the native C++ engine sidecar
	// counters (frames, speed_x, encode_passes, temp_bytes,
	// duration_seconds) merged into the final task-scoped metrics map.
	pipelineID := resolvePipelineID(spec.Payload)
	pipelineStart := time.Now()

	// Fine-grained phase timer: wrap the pipeline run for the top-level video
	// phases. Sub-phase timings are populated from the engine's own sidecar
	// detailed phases after the run completes.
	var spanDecode, spanSubtitle, spanBlur, spanWatermark, spanFilter, spanComposite, spanEncode string
	if timer != nil {
		spanDecode = timer.Begin(telemetry.PhaseVideoDecode)
		spanSubtitle = timer.Begin(telemetry.PhaseVideoSubtitle)
		spanBlur = timer.Begin(telemetry.PhaseVideoBlur)
		spanWatermark = timer.Begin(telemetry.PhaseVideoWatermark)
		spanFilter = timer.Begin(telemetry.PhaseVideoFilter)
		spanComposite = timer.Begin(telemetry.PhaseVideoComposite)
		spanEncode = timer.Begin(telemetry.PhaseVideoEncode)
	}

	runMetrics, err := s.pipelineRunner.RunWithMetrics(ctx, pipelineID, spec.JobID, spec.Payload, outputPath)

	// Populate fine-grained phase timings from the C++ engine's detailed
	// phase ledger. Each engine component/action is mapped to a fine-grained
	// phase and accumulated into the shared timer.
	if timer != nil {
		// Close all spans (the engine run is synchronous).
		timer.End(spanDecode)
		timer.End(spanSubtitle)
		timer.End(spanBlur)
		timer.End(spanWatermark)
		timer.End(spanFilter)
		timer.End(spanComposite)
		timer.End(spanEncode)
		// Map engine detailed phases into fine-grained timer data.
		populateTimerFromEnginePhases(timer, runMetrics.RenderMetrics.DetailedPhases)
		// Populate per-scene breakdown from engine segments and detailed phases.
		populateSceneTimingsFromEngine(timer, runMetrics.RenderMetrics.Segments, runMetrics.RenderMetrics.DetailedPhases)
	}
	// Feed GPU transfer tracker from engine detailed phases.
	if gpuTracker != nil {
		ingestGPUTransferPhases(gpuTracker, runMetrics.RenderMetrics.DetailedPhases)
	}

	// Materialize native telemetry before handling the render error. The
	// engine may have emitted useful completed phases and segment timings
	// even when the process fails or is cancelled.
	clipCount := compiledPlanClipCount(spec.Payload)
	derivedIO, derivedCPU := projectRunMetrics(metrics, pipelineID, pipelineStart, runMetrics, clipCount)
	emitEngineProcessTelemetry(rec, runMetrics)
	segments := projectSegments(runMetrics.RenderMetrics)
	detailedPhases := projectDetailedPhases(runMetrics.RenderMetrics)
	rawMetrics := rawMetricsFromPipeline(runMetrics, derivedIO, derivedCPU)

	if err != nil {
		// A cancelled render is not a renderer failure. The native client
		// already terminates its process group; remove any partial output
		// before returning the cancellation sentinel to TaskRunner.
		_ = os.Remove(outputPath)
		_ = os.Remove(outputPath + ".progress.json")
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return executor.ExecutionResult{
				Status:    "failed",
				ErrorCode: "cancelled", ErrorDetail: err.Error(),
				RawMetrics:     rawMetrics,
				Metrics:        metrics,
				Segments:       segments,
				DetailedPhases: detailedPhases,
				StartedAt:      startedAt,
				CompletedAt:    time.Now().UTC(),
			}, err
		}
		return executor.ExecutionResult{
			Status:         "failed",
			ErrorCode:      renderErrorCode(err),
			ErrorDetail:    fmt.Sprintf("pipeline.Runner.RunWithMetrics(%s): %v", pipelineID, err),
			RawMetrics:     rawMetrics,
			Metrics:        metrics,
			Segments:       segments,
			DetailedPhases: detailedPhases,
			StartedAt:      startedAt,
			CompletedAt:    time.Now().UTC(),
		}, nil
	}
	// Fail-closed artifact boundary: the primary output and its progress
	// receipt must both exist and have a real manifest before success.
	outputs, outputManifest, failResult := verifyAndBuildOutputs(ctx, outputPath, startedAt, metrics, rawMetrics, runMetrics, clipCount, planHandle, rec)
	if failResult != nil {
		return *failResult, nil
	}

	planHandle.CompleteWith(0, outputManifest.SizeBytes, runMetrics.RenderMetrics.Frames, telemetry.StatusOK, "", "")
	planCompleted = true
	if rec != nil {
		status := telemetry.StatusOK
		if !outputManifest.FfprobeValid {
			status = telemetry.StatusFailed
		}
		rec.Emit(telemetry.EventSpec{Origin: telemetry.OriginValidation, Scope: telemetry.ScopeAttempt, Component: "quality", Action: "ffprobe"}, status, outputManifest.FfprobeErr, "")
	}
	return executor.ExecutionResult{
		Status:         "succeeded",
		Outputs:        outputs,
		RawMetrics:     rawMetrics,
		Metrics:        metrics,
		Segments:       segments,
		DetailedPhases: detailedPhases,
		StartedAt:      startedAt,
		CompletedAt:    time.Now().UTC(),
	}, nil
}

