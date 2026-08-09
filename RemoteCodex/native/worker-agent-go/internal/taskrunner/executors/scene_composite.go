// Package executors contains concrete Executor implementations that
// delegate to the existing pkg/video pipeline path. PR-3.4 invariant:
// adapters do NOT duplicate rendering logic — they only translate the
// canonical Executor contract onto the existing pipeline runner.
//
// The observability/metrics helpers (appendObservabilitySummaryPhases,
// flattenObservabilityMetric, resolvePipelineID) live in the sibling
// file scene_composite_metrics.go.
package executors

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/pipeline"
)

const (
	// SceneCompositeID is the canonical executor ID registered in the
	// worker bootstrap. Masters will see this ID in worker hello
	// capability payloads (PR-3.5).
	SceneCompositeID = "scene.composite.v1"

	// SceneCompositeVersion is the only version registered today. Bump
	// when the descriptor semantics change incompatibly; the registry
	// resolves by (id, version).
	SceneCompositeVersion = 1
)

// SceneComposite composes a scene from heterogeneous sources
// (images + clips + audio) by delegating to the existing
// pipeline.Runner. The pipeline registry's "hybrid.v1" compiler
// handles the actual render plan compilation; SceneComposite adds the
// Executor contract layer (Descriptor, Validate, Execute, error
// mapping).
//
// PR-3.4 invariant: no duplicated rendering. Every byte of video
// produced by this executor comes from the canonical pipeline path.
//
// CAVEAT (documented for PR-3.5 hello): pipeline.Runner shells out to
// the C++ render client synchronously. Parent context cancellation
// cannot preempt the C++ process once it is running; the executor's
// TemporalMode=global + Deterministic=true advertise this property to
// the master scheduler so it can plan around the blocking nature.
type SceneComposite struct {
	pipelineRunner *pipeline.Runner
	outputBase     string
}

// NewSceneComposite returns a SceneComposite executor that delegates to
// the given pipeline.Runner. outputBase is the directory under which
// per-job .mp4 paths are constructed when the spec's payload does not
// already specify one. Pass "." or "" for the current working directory
// (we default to /tmp/velox/scene-composite).
//
// Panics if runner is nil — adapters without a real pipeline are
// always a programmer error; surface loudly at driver startup.
func NewSceneComposite(runner *pipeline.Runner, outputBase string) *SceneComposite {
	if runner == nil {
		panic("taskrunner/executors: NewSceneComposite requires a non-nil pipeline.Runner")
	}
	if outputBase == "" {
		outputBase = "/tmp/velox/scene-composite"
	}
	return &SceneComposite{
		pipelineRunner: runner,
		outputBase:     outputBase,
	}
}

// Descriptor returns the canonical scene-composite descriptor. Fields
// reflect the executor's capabilities for the master's capability
// matching (PR-3.5 hello).
func (s *SceneComposite) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID:            SceneCompositeID,
		Version:       SceneCompositeVersion,
		InputTypes:    []string{"render.input", "audio.input"},
		OutputTypes:   []string{"render.output", "engine.progress.sidecar"},
		ResourceClass: executor.ResourceCPU,
		Deterministic: true,
		Cacheable:     true,
		TemporalMode:  executor.TemporalGlobal,
		SupportsAlpha: true,
	}
}

// Validate is the executor-side pre-flight check. The TaskRunner calls
// this BEFORE resource acquisition (PR-3.3 invariant). We require at
// least one media source slice; the canonical pipeline.DetectPipelineID
// drives which compiler actually renders.
//
// We deliberately do NOT validate specific schema fields here — the
// pipeline.Compiler is the authoritative validator for its own input
// shape. Validate at this layer only enforces "is there ANY media to
// composite?".
func (s *SceneComposite) Validate(spec executor.TaskSpec) error {
	if spec.Payload == nil {
		return fmt.Errorf("scene.composite.v1: payload is required")
	}
	if !hasAnyMediaSource(spec.Payload) {
		return fmt.Errorf("scene.composite.v1: payload must contain at least one of images, clips, intro_clip_paths, stock_clip_paths, scene_image_paths, scenes_json")
	}
	return nil
}

// Execute performs the canonical work. It delegates to the existing
// pipeline.Runner using the explicit payload `pipeline_id` when present;
// otherwise it falls back to the historical "hybrid.v1" route.
//
// CAVEAT: the C++ engine runs as a synchronous subprocess; context
// cancellation propagates only AFTER the engine finishes. The
// descriptor's TemporalMode=global + Deterministic=true advertise this
// property to the master scheduler.
func (s *SceneComposite) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	startedAt := time.Now().UTC()
	metrics := map[string]interface{}{}
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

	outputPath, err := s.resolveOutputPath(spec)
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
	metrics["pipeline.total_ms"] = time.Since(pipelineStart).Milliseconds()
	metrics["pipeline.id"] = pipelineID
	metrics["pipeline.resolve_ms"] = runMetrics.ResolveMs
	metrics["pipeline.validate_ms"] = runMetrics.ValidateMs
	metrics["pipeline.compile_ms"] = runMetrics.CompileMs
	metrics["pipeline.render_ms"] = runMetrics.RenderMs
	metrics["pipeline.timeline_items"] = int64(runMetrics.TimelineItems)
	metrics["pipeline.audio_tracks"] = int64(runMetrics.AudioTracks)
	metrics["native.total_ms"] = runMetrics.RenderMetrics.TotalMs
	metrics["native.plan_write_ms"] = runMetrics.RenderMetrics.PlanWriteMs
	metrics["native.process_wait_ms"] = runMetrics.RenderMetrics.ProcessWaitMs
	metrics["engine.frames"] = runMetrics.RenderMetrics.Frames
	metrics["engine.frames_decoded"] = runMetrics.RenderMetrics.FramesDecoded
	metrics["engine.frames_composited"] = runMetrics.RenderMetrics.FramesComposited
	metrics["engine.fps"] = runMetrics.RenderMetrics.Fps
	metrics["engine.speed_x"] = runMetrics.RenderMetrics.SpeedX
	metrics["engine.encode_passes"] = runMetrics.RenderMetrics.EncodePasses
	metrics["engine.temp_bytes"] = runMetrics.RenderMetrics.TempBytes
	metrics["engine.duration_seconds"] = runMetrics.RenderMetrics.DurationSec
	metrics["engine.concat_mode"] = runMetrics.RenderMetrics.ConcatMode
	metrics["engine.bitrate"] = runMetrics.RenderMetrics.Bitrate
	metrics["engine.dup_frames"] = runMetrics.RenderMetrics.DupFrames
	metrics["engine.drop_frames"] = runMetrics.RenderMetrics.DropFrames
	for k, v := range runMetrics.RenderMetrics.PhaseMS {
		metrics["engine."+k] = v
	}
	for key, value := range runMetrics.RenderMetrics.Observability {
		flattenObservabilityMetric(metrics, key, value)
	}

	segments := make([]executor.SegmentTiming, 0, len(runMetrics.RenderMetrics.Segments))
	for _, seg := range runMetrics.RenderMetrics.Segments {
		segments = append(segments, executor.SegmentTiming{
			SegmentIndex:     seg.SegmentIndex,
			SceneWorkerIndex: seg.SceneWorkerIndex,
			SceneID:          seg.SceneID,
			SourceType:       seg.SourceType,
			DurationMS:       seg.DurationMS,
			AssetDownloadMS:  seg.AssetDownloadMS,
			FfmpegEncodeMS:   seg.FfmpegEncodeMS,
			SourceBytes:      seg.SourceBytes,
			OutputBytes:      seg.OutputBytes,
			FramesEncoded:    seg.FramesEncoded,
			FramesDecoded:    seg.FramesDecoded,
			FramesComposited: seg.FramesComposited,
			FfmpegSpeedX:     seg.FfmpegSpeedX,
			Codec:            seg.Codec,
			Preset:           seg.Preset,
			FfmpegThreads:    seg.FfmpegThreads,
			Status:           seg.Status,
			ErrorCode:        seg.ErrorCode,
			ErrorMessage:     seg.ErrorMessage,
			SourceURLHash:    seg.SourceURLHash,
			CacheKey:         seg.CacheKey,
			InputDurationMS:  seg.InputDurationMS,
			OutputDurationMS: seg.OutputDurationMS,
			MetadataJSON:     seg.MetadataJSON,
			StartedOffsetMS:  seg.StartedOffsetMS,
			FinishedOffsetMS: seg.FinishedOffsetMS,
			WorkerSlot:       seg.WorkerSlot,
			CPUThreads:       seg.CPUThreads,
			ParallelGroup:    seg.ParallelGroup,
		})
	}

	detailedPhases := make([]executor.DetailedPhaseTiming, 0, len(runMetrics.RenderMetrics.DetailedPhases))
	for _, phase := range runMetrics.RenderMetrics.DetailedPhases {
		detailedPhases = append(detailedPhases, executor.DetailedPhaseTiming{
			Origin:           phase.Origin,
			Scope:            phase.Scope,
			Component:        phase.Component,
			Action:           phase.Action,
			Phase:            phase.Phase,
			EventType:        phase.EventType,
			EventName:        phase.EventName,
			EventIndex:       phase.EventIndex,
			StartedAt:        phase.StartedAt,
			CompletedAt:      phase.CompletedAt,
			DurationMS:       phase.DurationMS,
			Status:           phase.Status,
			ErrorCode:        phase.ErrorCode,
			ErrorMessage:     phase.ErrorMessage,
			BytesIn:          phase.BytesIn,
			BytesOut:         phase.BytesOut,
			Frames:           phase.Frames,
			MetadataJSON:     phase.MetadataJSON,
			SegmentIndex:     phase.SegmentIndex,
			TrackKind:        phase.TrackKind,
			TrackIndex:       phase.TrackIndex,
			StartedOffsetMS:  phase.StartedOffsetMS,
			FinishedOffsetMS: phase.FinishedOffsetMS,
			CPUMS:            phase.CPUMS,
			QueueWaitMS:      phase.QueueWaitMS,
			FramesIn:         phase.FramesIn,
			FramesOut:        phase.FramesOut,
		})
	}
	appendObservabilitySummaryPhases(&detailedPhases, runMetrics.RenderMetrics.Observability)

	if err != nil {
		// A cancelled render is not a renderer failure. The native client
		// already terminates its process group; remove any partial output
		// before returning the cancellation sentinel to TaskRunner.
		_ = os.Remove(outputPath)
		_ = os.Remove(outputPath + ".progress.json")
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return executor.ExecutionResult{
				Status:         "failed",
				ErrorCode:      "cancelled",
				ErrorDetail:    err.Error(),
				Metrics:        metrics,
				Segments:       segments,
				DetailedPhases: detailedPhases,
				StartedAt:      startedAt,
				CompletedAt:    time.Now().UTC(),
			}, err
		}
		return executor.ExecutionResult{
			Status:         "failed",
			ErrorCode:      "execute_failed",
			ErrorDetail:    fmt.Sprintf("pipeline.Runner.RunWithMetrics(%s): %v", pipelineID, err),
			Metrics:        metrics,
			Segments:       segments,
			DetailedPhases: detailedPhases,
			StartedAt:      startedAt,
			CompletedAt:    time.Now().UTC(),
		}, nil
	}
	// Compute output file hash and size for artifact metadata.
	// A successful renderer invocation is not sufficient: both the
	// primary output and its progress receipt must exist and have a
	// real manifest before this executor can report success. This is
	// the fail-closed boundary that prevents a mock/partial renderer
	// from producing a misleading succeeded task.
	var outputHash string
	var outputSize int64
	hashStart := time.Now()
	if rec != nil {
		rec.Emit(telemetry.EventSpec{Origin: telemetry.OriginValidation, Scope: telemetry.ScopeAttempt, Component: "quality", Action: "sha256"}, telemetry.StatusOK, "", "")
	}
	outputManifest, manifestErr := publisher.ComputeLocalManifest(ctx, outputPath)
	metrics["output.hash_ms"] = time.Since(hashStart).Milliseconds()
	if manifestErr != nil {
		planHandle.Abort("quality_manifest", manifestErr.Error())
		metrics["output.manifest_error"] = manifestErr.Error()
		return executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   "output_manifest_missing",
			ErrorDetail: fmt.Sprintf("render output manifest: %v", manifestErr),
			Metrics:     metrics,
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		}, nil
	}
	outputHash = outputManifest.SHA256Hex
	outputSize = outputManifest.SizeBytes
	if outputSize <= 0 {
		planHandle.Abort("quality_empty", "render output manifest has zero bytes")
		metrics["output.manifest_error"] = "render output is empty"
		return executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   "output_manifest_empty",
			ErrorDetail: "render output manifest has zero bytes",
			Metrics:     metrics,
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		}, nil
	}
	metrics["output.bytes"] = outputSize
	metrics["executor.total_ms"] = time.Since(startedAt).Milliseconds()

	outputs := []executor.ArtifactRef{{Type: "render.output", Hash: outputHash, URI: outputPath, SizeBytes: outputSize}}
	sidecarPath := outputPath + ".progress.json"
	if sidecarManifest, sidecarErr := publisher.ComputeLocalManifest(ctx, sidecarPath); sidecarErr == nil && sidecarManifest.SizeBytes > 0 {
		// The renderer owns this file as a durable receipt. Do not remove it
		// here: the worker must keep it available through declaration,
		// upload, and the master's commit acknowledgement.
		outputs = append(outputs, executor.ArtifactRef{
			Type:      "engine.progress.sidecar",
			Hash:      sidecarManifest.SHA256Hex,
			URI:       sidecarPath,
			SizeBytes: sidecarManifest.SizeBytes,
		})
		metrics["sidecar.present"] = true
		metrics["sidecar.bytes"] = sidecarManifest.SizeBytes
	} else {
		// The sidecar is the renderer's progress receipt and is part of
		// the artifact contract. Do not silently report success without
		// it: the worker cannot register a complete operation receipt.
		metrics["sidecar.present"] = false
		if sidecarErr == nil {
			sidecarErr = errors.New("render progress sidecar is empty")
		}
		metrics["sidecar.error"] = sidecarErr.Error()
		return executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   "progress_sidecar_missing",
			ErrorDetail: fmt.Sprintf("render progress sidecar manifest: %v", sidecarErr),
			Metrics:     metrics,
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		}, nil
	}

	planHandle.CompleteWith(0, outputSize, runMetrics.RenderMetrics.Frames, telemetry.StatusOK, "", "")
	planCompleted = true
	if rec != nil {
		rec.Emit(telemetry.EventSpec{Origin: telemetry.OriginValidation, Scope: telemetry.ScopeAttempt, Component: "quality", Action: "ffprobe"}, telemetry.StatusOK, "", "")
	}
	return executor.ExecutionResult{
		Status:         "succeeded",
		Outputs:        outputs,
		Metrics:        metrics,
		Segments:       segments,
		DetailedPhases: detailedPhases,
		StartedAt:      startedAt,
		CompletedAt:    time.Now().UTC(),
	}, nil
}

// resolveOutputPath synthesises <outputBase>/<jobID>.mp4 for the local
// filesystem. The payload's "output_path" is intentionally ignored — it
// refers to a path on the master and is not reachable inside the worker
// container. The worker always renders into its own temp directory and
// uploads the result to the master via the artifact API.
func (s *SceneComposite) resolveOutputPath(spec executor.TaskSpec) (string, error) {
	if spec.JobID == "" {
		return "", fmt.Errorf("scene.composite.v1: missing JobID; cannot synthesize output path")
	}
	return filepath.Join(s.outputBase, spec.JobID+".mp4"), nil
}

// PayloadOutputPath returns the master-originated output_path stored in
// the spec payload (if any). Callers use this as the upload target
// filename, NOT as a local render path.
func PayloadOutputPath(spec executor.TaskSpec) string {
	p, _ := spec.Payload["output_path"].(string)
	return p
}

// hasAnyMediaSource scans the payload for any one of the canonical
// sources. Used by Validate and the synthetic-output-path branch.
func hasAnyMediaSource(payload map[string]interface{}) bool {
	keys := []string{"items", "images", "clips", "intro_clip_paths", "stock_clip_paths", "scene_image_paths", "scenes_json"}
	for _, k := range keys {
		if v, ok := payload[k]; ok && v != nil {
			switch vv := v.(type) {
			case []interface{}:
				if len(vv) > 0 {
					return true
				}
			case []string:
				if len(vv) > 0 {
					return true
				}
			case string:
				if vv != "" {
					return true
				}
			}
		}
	}
	return false
}
