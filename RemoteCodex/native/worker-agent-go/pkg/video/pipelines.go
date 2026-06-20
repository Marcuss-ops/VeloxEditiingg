package video

import (
	"context"

	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/plan"
	"velox-worker-agent/pkg/video/services/audio"

	imgCompiler "velox-worker-agent/pkg/video/pipelines/images"
	clipCompiler "velox-worker-agent/pkg/video/pipelines/clips"
	entityCompiler "velox-worker-agent/pkg/video/pipelines/entities"
	hybridCompiler "velox-worker-agent/pkg/video/pipelines/hybrid"
)

// registerPipelines registers all pipeline compilers in the registry.
func registerPipelines(registry *pipeline.Registry, probe audio.Probe) {
	registry.Register(&pipelineAdapter{id: "images.v1", validate: imgCompiler.Validate, compile: imgCompiler.Compile, probe: probe})
	registry.Register(&pipelineAdapter{id: "clips.v1", validate: clipCompiler.Validate, compile: clipCompiler.Compile, probe: probe})
	registry.Register(&pipelineAdapter{id: "entities.v1", validate: entityCompiler.Validate, compile: entityCompiler.Compile, probe: probe})
	registry.Register(&pipelineAdapter{id: "hybrid.v1", validate: hybridCompiler.Validate, compile: hybridCompiler.Compile, probe: probe})
}

// pipelineAdapter wraps a compile function as a pipeline.Compiler.
type pipelineAdapter struct {
	id       string
	validate func(map[string]interface{}) error
	compile  func(ctx context.Context, jobID string, input map[string]interface{}, outputPath string, probe audio.Probe) (*plan.RenderPlan, error)
	probe    audio.Probe
}

func (a *pipelineAdapter) ID() string                                     { return a.id }
func (a *pipelineAdapter) Validate(input map[string]interface{}) error     { return a.validate(input) }
func (a *pipelineAdapter) Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string) (*plan.RenderPlan, error) {
	return a.compile(ctx, jobID, input, outputPath, a.probe)
}
