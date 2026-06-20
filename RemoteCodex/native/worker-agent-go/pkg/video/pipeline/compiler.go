// Package pipeline defines the compiler abstraction for video pipelines.
// Each pipeline type (images, clips, entities, hybrid) implements the
// Compiler interface. The registry resolves pipeline IDs to compilers,
// and the runner orchestrates compilation + rendering.
package pipeline

import (
	"context"

	"velox-worker-agent/pkg/video/plan"
)

// Compiler transforms a pipeline-specific request into a RenderPlan.
// Each video endpoint (images, clips, entities, hybrid) has its own
// compiler that knows the rules for that pipeline type.
type Compiler interface {
	// ID returns the unique pipeline identifier (e.g. "images.v1").
	ID() string

	// Validate checks the raw request parameters before compilation.
	// Returns an error if required fields are missing or invalid.
	Validate(input map[string]interface{}) error

	// Compile transforms the request into a RenderPlan.
	// The compiler uses shared services (audio probe, duration allocator, etc.)
	// to resolve assets and compute timelines.
	Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string) (*plan.RenderPlan, error)
}
