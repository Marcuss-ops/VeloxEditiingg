// pkg/bootstrap/view.go — narrow interface decoupler.
//
// pkg/bootstrap deliberately does NOT import pkg/video. The
// composition root passes a RunnerView that exposes only the two
// methods the bootstrap needs (RenderClient + Run), which keeps:
//
//   - The test surface contained. Tests stub out RunnerView with a
//     fake that returns canned plans + computed SHA — no need to
//     build a real pipeline.NewRunner + native.RenderClient just to
//     exercise the boot orchestration.
//   - pkg/bootstrap free of CGO / shellout coupling. The bootstrap
//     file is rendered in tests via in-memory byte streams instead
//     of through pkg/video.
//   - The package reusable. The "RW-PROD-002 --doctor" command can
//     also pass a RunnerView-flavoured shim without dragging the
//     full pipeline state in.
//
// The interface MUST stay minimal. Each new method is a new coupling;
// openpkg/video callers should reach for the canonical constructor.
package bootstrap

import (
	"context"

	"velox-worker-agent/pkg/video/plan"
)

// RenderClientIface is the minimum surface the engine self-render
// step needs. Mirrors pkg/video/pipeline.RenderClient but lives in
// this package so we avoid an import loop.
type RenderClientIface interface {
	Render(ctx context.Context, p *plan.RenderPlan) error
}

// RunnerView is the entry point the bootstrap consumes. The (only)
// production implementation is *pipeline.Runner via an adapter in
// cmd/velox-worker-agent. Tests substitute hand-rolled fakes.
type RunnerView interface {
	RenderClient() RenderClientIface
}
