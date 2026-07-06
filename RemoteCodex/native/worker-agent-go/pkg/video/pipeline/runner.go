package pipeline

import (
	"context"
	"fmt"
	"time"

	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/plan"
)

// RenderClient is the interface for executing a RenderPlan.
// Implemented by the native C++ render client.
type RenderClient interface {
	Render(ctx context.Context, p *plan.RenderPlan) error
	RenderWithMetrics(ctx context.Context, p *plan.RenderPlan) (RenderMetrics, error)
}

// RenderMetrics captures the native engine sidecar + subprocess wall-clock
// counters. The zero value is safe — executors that don't use native
// rendering return it unpopulated.
type RenderMetrics struct {
	Frames         int64
	Fps            float64
	SpeedX         float64
	EncodePasses   int64
	TempBytes      int64
	DurationSec    float64
	ConcatMode     string
	TotalSize      int64
	OutTimeMs      int64
	Bitrate        float64
	DupFrames      int64
	DropFrames     int64
	PlanMarshalMs  int64
	PlanWriteMs    int64
	ProcessStartMs int64
	ProcessWaitMs  int64
	TotalMs        int64
}

// RenderClient exposes the underlying render client so callers outside
// pkg/video (currently only pkg/bootstrap, for the engine self-test)
// can drive a single Render without rebuilding the renderer. The
// returned value is the SAME pointer the constructor was given;
// mutating it via the returned interface affects subsequent Run()
// calls — callers MUST treat it as read-only.
//
// This is a pure accessor — no side-effects, no logging, no caching.
// The intent is to keep pkg/bootstrap decoupled from the pipeline
// constructor's identity-check obligations while still letting the
// self-render step drive the canonical worker-side renderer.
func (r *Runner) RenderClient() RenderClient {
	if r == nil {
		return nil
	}
	return r.renderClient
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

// RunMetrics aggregates the per-phase wall-clock timings and the
// native engine sidecar counters from a single pipeline.Runner.Run/
// RunWithMetrics invocation. Zero values are valid when the phase was
// skipped or the metric is unavailable.
type RunMetrics struct {
	ResolveMs  int64
	ValidateMs int64
	CompileMs  int64
	RenderMs   int64
	TotalMs    int64

	TimelineItems int
	AudioTracks   int

	// Native engine metrics (zero when no native rendering occurred)
	RenderMetrics
}

// RunWithMetrics executes the full pipeline and returns phase-level
// timings plus the native engine sidecar counters. Run() delegates to
// this method so existing callers are source-compatible.
func (r *Runner) RunWithMetrics(ctx context.Context, pipelineID string, jobID string, input map[string]interface{}, outputPath string) (RunMetrics, error) {
	m := RunMetrics{}
	start := time.Now()

	// Phase: resolve compiler
	resolveStart := time.Now()
	compiler, err := r.registry.Resolve(pipelineID)
	if err != nil {
		return m, fmt.Errorf("pipeline: resolve: %w", err)
	}
	m.ResolveMs = time.Since(resolveStart).Milliseconds()

	// Phase: validate
	r.logger.Info("[PIPELINE] Validating %s for job %s", pipelineID, jobID)
	validateStart := time.Now()
	if err := compiler.Validate(input); err != nil {
		return m, fmt.Errorf("pipeline: validate %s: %w", pipelineID, err)
	}
	m.ValidateMs = time.Since(validateStart).Milliseconds()

	// Phase: compile
	r.logger.Info("[PIPELINE] Compiling %s for job %s", pipelineID, jobID)
	compileStart := time.Now()
	p, err := compiler.Compile(ctx, jobID, input, outputPath)
	if err != nil {
		return m, fmt.Errorf("pipeline: compile %s: %w", pipelineID, err)
	}
	m.CompileMs = time.Since(compileStart).Milliseconds()

	if len(p.Timeline) == 0 {
		return m, fmt.Errorf("pipeline: compile %s produced empty timeline", pipelineID)
	}
	m.TimelineItems = len(p.Timeline)
	m.AudioTracks = len(p.AudioTracks)

	// Phase: render via native client (with metrics)
	r.logger.Info("[PIPELINE] Rendering %s for job %s (%d timeline items)", pipelineID, jobID, len(p.Timeline))
	renderStart := time.Now()
	nativeMetrics, renderErr := r.renderClient.RenderWithMetrics(ctx, p)
	m.RenderMs = time.Since(renderStart).Milliseconds()
	if renderErr != nil {
		return m, fmt.Errorf("pipeline: render %s: %w", pipelineID, renderErr)
	}
	// Copy native engine sidecar counters into the pipeline metrics
	m.RenderMetrics = nativeMetrics

	r.logger.Info("[PIPELINE] Completed %s for job %s", pipelineID, jobID)
	m.TotalMs = time.Since(start).Milliseconds()
	return m, nil
}

// Run executes the full pipeline for a job.
// It resolves the compiler, validates input, compiles the plan, and renders.
func (r *Runner) Run(ctx context.Context, pipelineID string, jobID string, input map[string]interface{}, outputPath string) error {
	_, err := r.RunWithMetrics(ctx, pipelineID, jobID, input, outputPath)
	return err
}
