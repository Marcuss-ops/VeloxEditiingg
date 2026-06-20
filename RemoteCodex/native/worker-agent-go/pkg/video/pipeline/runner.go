package pipeline

import (
	"context"
	"fmt"

	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/plan"
)

// RenderClient is the interface for executing a RenderPlan.
// Implemented by the native C++ render client.
type RenderClient interface {
	Render(ctx context.Context, p *plan.RenderPlan) error
}

// Runner orchestrates: resolve compiler → validate → compile → render.
type Runner struct {
	registry     *Registry
	renderClient RenderClient
	logger       *logger.Logger
}

// NewRunner creates a pipeline runner with the given registry and render client.
func NewRunner(registry *Registry, client RenderClient, log *logger.Logger) *Runner {
	return &Runner{
		registry:     registry,
		renderClient: client,
		logger:       log,
	}
}

// Run executes the full pipeline for a job.
// It resolves the compiler, validates input, compiles the plan, and renders.
func (r *Runner) Run(ctx context.Context, pipelineID string, jobID string, input map[string]interface{}, outputPath string) error {
	compiler, err := r.registry.Resolve(pipelineID)
	if err != nil {
		return fmt.Errorf("pipeline: resolve: %w", err)
	}

	r.logger.Info("[PIPELINE] Validating %s for job %s", pipelineID, jobID)
	if err := compiler.Validate(input); err != nil {
		return fmt.Errorf("pipeline: validate %s: %w", pipelineID, err)
	}

	r.logger.Info("[PIPELINE] Compiling %s for job %s", pipelineID, jobID)
	p, err := compiler.Compile(ctx, jobID, input, outputPath)
	if err != nil {
		return fmt.Errorf("pipeline: compile %s: %w", pipelineID, err)
	}

	if len(p.Timeline) == 0 {
		return fmt.Errorf("pipeline: compile %s produced empty timeline", pipelineID)
	}

	r.logger.Info("[PIPELINE] Rendering %s for job %s (%d timeline items)", pipelineID, jobID, len(p.Timeline))
	if err := r.renderClient.Render(ctx, p); err != nil {
		return fmt.Errorf("pipeline: render %s: %w", pipelineID, err)
	}

	r.logger.Info("[PIPELINE] Completed %s for job %s", pipelineID, jobID)
	return nil
}
