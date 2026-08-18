// Package executors — scene.composite.v1 execution path.
//
// scene_composite_execute.go owns Execute and its direct helpers
// (compiledPlanClipCount, rawMetricsFromPipeline, renderErrorCode,
// resolveOutputPath, PayloadOutputPath). The executor surface (type,
// constructor, Descriptor, Validate) lives in scene_composite.go.
package executors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/performance"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/pipeline"
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
	// Legacy compatibility projection for pre-typed report consumers. The
	// canonical producer output is rawMetrics below; this map is retired
	// incrementally as downstream consumers adopt RawMetrics.
	metrics := newLegacyMetricsProjection()
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
	runMetrics, err := s.pipelineRunner.RunWithMetrics(ctx, pipelineID, spec.JobID, spec.Payload, outputPath)

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
				Metrics:        metrics.Map(),
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
			Metrics:        metrics.Map(),
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
		Metrics:        metrics.Map(),
		Segments:       segments,
		DetailedPhases: detailedPhases,
		StartedAt:      startedAt,
		CompletedAt:    time.Now().UTC(),
	}, nil
}

func compiledPlanClipCount(payload map[string]interface{}) int {
	raw, ok := payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0
	}
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(raw))
	if err != nil || plan == nil {
		return 0
	}
	count := 0
	for _, track := range plan.VideoTracks {
		count += len(track.Segments)
	}
	return count
}

func rawMetricsFromPipeline(run pipeline.RunMetrics, io performance.IOMetrics, cpu performance.CPUMetrics) *telemetry.RawExecutionMetrics {
	rm := run.RenderMetrics
	return &telemetry.RawExecutionMetrics{
		InputBytes:           io.AssetBytesRead,
		OutputBytes:          io.FinalBytesWritten,
		CpuTimeMs:            cpu.CPUTotalMs,
		PeakRssBytes:         rm.PeakRSSBytes,
		FramesDecoded:        rm.FramesDecoded,
		FramesComposited:     rm.FramesComposited,
		FramesEncoded:        rm.Frames,
		FfmpegSpeedRatio:     rm.SpeedX,
		EncodePasses:         int32(rm.EncodePasses),
		ConcatMode:           rm.ConcatMode,
		TempBytesWritten:     io.TempBytesWritten,
		MediaDurationSeconds: rm.DurationSec,
		WallClockSeconds:     float64(run.TotalMs) / 1000,
		OutputFileSize:       io.FinalBytesWritten,
		DiskReadBytes:        io.TotalBytesRead,
		DiskWriteBytes:       io.TotalBytesWritten,
	}
}

func renderErrorCode(err error) string {
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "copy_only media contract rejected") {
		return "COPY_ONLY_MEDIA_INCOMPATIBLE"
	}
	return "execute_failed"
}

// resolveOutputPath resolves where the final artifact is produced. When the
// task's ExecutionContext carries the canonical StorageResolver, the
// ARTIFACT_STAGING placement decision (tmpfs-with-reservation → NVMe
// fallback) owns the path; otherwise the executor falls back to its legacy
// <outputBase>/<jobID>.mp4 root. The payload's "output_path" is
// intentionally ignored — it refers to a path on the master and is not
// reachable inside the worker container. The worker always renders into its
// own storage and uploads the result to the master via the artifact API.
func (s *SceneComposite) resolveOutputPath(execCtx executor.ExecutionContext, spec executor.TaskSpec) (string, error) {
	if spec.JobID == "" {
		return "", fmt.Errorf("scene.composite.v1: missing JobID; cannot synthesize output path")
	}
	rel := spec.JobID + ".mp4"
	if resolver := storageResolverFromExecutionContext(execCtx); resolver != nil {
		placement, err := resolver.Place(storage.ArtifactStaging, rel, estimateOutputBytes(spec.Payload))
		if err == nil {
			return placement.Path, nil
		}
	}
	return filepath.Join(s.outputBase, rel), nil
}

// storageResolverFromExecutionContext extracts the canonical StorageResolver
// from the optional-interface seam exposed by taskrunner.runnerContext. It
// returns nil when the context does not provide one (legacy/headless
// executors, test stubs) so callers fall back to the outputBase root.
func storageResolverFromExecutionContext(execCtx executor.ExecutionContext) *storage.Resolver {
	if provider, ok := execCtx.(interface{ StorageResolver() *storage.Resolver }); ok && provider != nil {
		return provider.StorageResolver()
	}
	return nil
}

// Estimate the final artifact size for the tmpfs reservation. Over-reserving
// only costs a spurious NVMe fallback; under-reserving risks ENOSPC at 98%
// of the render. We therefore use a generous composite bitrate and a 20%
// margin. Unknown durations return -1, the resolver's "unknown size → NVMe"
// sentinel.
const (
	// outputEstimateConservativeBps is the assumed video+audio composite
	// bitrate for the reservation estimate.
	outputEstimateConservativeBps = 12_000_000
	// outputEstimateMargin inflates the byte estimate for container/mux
	// overhead.
	outputEstimateMargin = 1.20
)

func estimateOutputBytes(payload map[string]interface{}) int64 {
	dur := timelineDurationSeconds(payload)
	if dur <= 0 {
		return -1
	}
	return int64(float64(dur) * outputEstimateConservativeBps / 8 * outputEstimateMargin)
}

// estimateOutputBytesFromDuration estimates the final artifact size from a
// declared media duration in microseconds, using the same conservative
// composite bitrate and margin as estimateOutputBytes. Non-positive durations
// return -1, the resolver's "unknown size → NVMe" sentinel.
func estimateOutputBytesFromDuration(durationUS int64) int64 {
	if durationUS <= 0 {
		return -1
	}
	return int64(float64(durationUS) / 1_000_000 * outputEstimateConservativeBps / 8 * outputEstimateMargin)
}

// timelineDurationSeconds sums the payload's declared timeline duration,
// following the hybrid compiler's canonical precedence: items[] first, then
// scenes_json, then the top-level duration_seconds.
func timelineDurationSeconds(payload map[string]interface{}) float64 {
	if payload == nil {
		return 0
	}
	if items, ok := payload["items"].([]interface{}); ok {
		var total float64
		for _, it := range items {
			if im, ok := it.(map[string]interface{}); ok {
				total += durationSecondsOf(im)
			}
		}
		if total > 0 {
			return total
		}
	}
	if encoded, ok := payload["scenes_json"].(string); ok && strings.TrimSpace(encoded) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(encoded), &scenes); err == nil {
			var total float64
			for _, sc := range scenes {
				total += durationSecondsOf(sc)
			}
			if total > 0 {
				return total
			}
		}
	}
	return durationSecondsOf(payload)
}

// durationSecondsOf reads "duration_seconds" then "duration" (items use the
// shorter key) from a map, accepting the numeric shapes Go's JSON decoder
// can produce.
func durationSecondsOf(m map[string]interface{}) float64 {
	for _, key := range []string{"duration_seconds", "duration"} {
		switch v := m[key].(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case json.Number:
			if f, err := v.Float64(); err == nil {
				return f
			}
		}
	}
	return 0
}

// PayloadOutputPath returns the master-originated output_path stored in
// the spec payload (if any). Callers use this as the upload target
// filename, NOT as a local render path.
func PayloadOutputPath(spec executor.TaskSpec) string {
	p, _ := spec.Payload["output_path"].(string)
	return p
}
