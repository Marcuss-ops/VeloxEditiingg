// Package executors contains concrete Executor implementations that
// delegate to the existing pkg/video pipeline path. PR-3.4 invariant:
// adapters do NOT duplicate rendering logic — they only translate the
// canonical Executor contract onto the existing pipeline runner.
package executors

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-worker-agent/internal/executor"
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
		OutputTypes:   []string{"render.output"},
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
func (s *SceneComposite) Execute(ctx context.Context, _ executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	startedAt := time.Now().UTC()
	metrics := map[string]interface{}{}

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
	if err != nil {
		return executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   "execute_failed",
			ErrorDetail: fmt.Sprintf("pipeline.Runner.RunWithMetrics(%s): %v", pipelineID, err),
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		}, nil
	}
	metrics["pipeline.total_ms"] = time.Since(pipelineStart).Milliseconds()
	metrics["pipeline.id"] = pipelineID
	// Per-phase pipeline timings
	metrics["pipeline.resolve_ms"] = runMetrics.ResolveMs
	metrics["pipeline.validate_ms"] = runMetrics.ValidateMs
	metrics["pipeline.compile_ms"] = runMetrics.CompileMs
	metrics["pipeline.render_ms"] = runMetrics.RenderMs
	metrics["pipeline.timeline_items"] = int64(runMetrics.TimelineItems)
	metrics["pipeline.audio_tracks"] = int64(runMetrics.AudioTracks)
	// Native engine sidecar counters
	metrics["native.total_ms"] = runMetrics.RenderMetrics.TotalMs
	metrics["native.plan_write_ms"] = runMetrics.RenderMetrics.PlanWriteMs
	metrics["native.process_wait_ms"] = runMetrics.RenderMetrics.ProcessWaitMs
	metrics["engine.frames"] = runMetrics.RenderMetrics.Frames
	metrics["engine.fps"] = runMetrics.RenderMetrics.Fps
	metrics["engine.speed_x"] = runMetrics.RenderMetrics.SpeedX
	metrics["engine.encode_passes"] = runMetrics.RenderMetrics.EncodePasses
	metrics["engine.temp_bytes"] = runMetrics.RenderMetrics.TempBytes
	metrics["engine.duration_seconds"] = runMetrics.RenderMetrics.DurationSec
	metrics["engine.concat_mode"] = runMetrics.RenderMetrics.ConcatMode
	metrics["engine.bitrate"] = runMetrics.RenderMetrics.Bitrate
	metrics["engine.dup_frames"] = runMetrics.RenderMetrics.DupFrames
	metrics["engine.drop_frames"] = runMetrics.RenderMetrics.DropFrames
	// C++ engine phase-level timings from sidecar phase_ms
	for k, v := range runMetrics.RenderMetrics.PhaseMS {
		metrics["engine."+k] = v
	}

	// Compute output file hash and size for artifact metadata.
	// Hash is mandatory per fix/artifact-metadata — dispatchTaskRunner
	// rejects succeeded tasks with empty-hash outputs.
	// Uses streaming hash (io.Copy) to avoid loading large video files
	// into memory. Time is recorded so operators can distinguish
	// slow-disk from slow-encode.
	var outputHash string
	var outputSize int64
	hashStart := time.Now()
	if f, err := os.Open(outputPath); err == nil {
		defer f.Close()
		h := sha256.New()
		if n, copyErr := io.Copy(h, f); copyErr == nil {
			outputHash = fmt.Sprintf("%x", h.Sum(nil))
			outputSize = n
		}
	}
	metrics["output.hash_ms"] = time.Since(hashStart).Milliseconds()
	metrics["output.bytes"] = outputSize
	metrics["executor.total_ms"] = time.Since(startedAt).Milliseconds()

	return executor.ExecutionResult{
		Status:      "succeeded",
		Outputs:     []executor.ArtifactRef{{Type: "render.output", Hash: outputHash, URI: outputPath, SizeBytes: outputSize}},
		Metrics:     metrics,
		StartedAt:   startedAt,
		CompletedAt: time.Now().UTC(),
	}, nil
}

func resolvePipelineID(payload map[string]interface{}) string {
	if payload != nil {
		if pipelineID, _ := payload["pipeline_id"].(string); strings.TrimSpace(pipelineID) != "" {
			return strings.TrimSpace(pipelineID)
		}
	}
	return "hybrid.v1"
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
