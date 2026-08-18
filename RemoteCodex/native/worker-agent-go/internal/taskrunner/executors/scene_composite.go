// Package executors contains concrete Executor implementations that
// delegate to the existing pkg/video pipeline path. PR-3.4 invariant:
// adapters do NOT duplicate rendering logic — they only translate the
// canonical Executor contract onto the existing pipeline runner.
//
// The scene.composite.v1 executor is split across focused files:
//   - scene_composite.go                 : type, constructor, Descriptor, Validate.
//   - scene_composite_execute.go         : Execute + orchestration helpers.
//   - scene_composite_metrics.go         : observability/metrics helpers
//     (appendObservabilitySummaryPhases, flattenObservabilityMetric, resolvePipelineID).
//   - scene_composite_metrics_projection.go : pure RunMetrics projections
//     (projectRunMetrics, emitEngineProcessTelemetry, projectSegments, projectDetailedPhases).
//   - scene_composite_output.go          : fail-closed artifact/sidecar
//     verification (verifyAndBuildOutputs).
package executors

import (
	"fmt"

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

// SceneComposite composes a scene from heterogeneous sources by
// delegating to the existing pipeline.Runner. The payload's explicit
// pipeline_id selects the compiler (images.v1 / clips.v1 / entities.v1);
// SceneComposite adds the Executor contract layer (Descriptor, Validate,
// Execute, error mapping). The legacy hybrid.v1 fallback was retired.
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

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
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
