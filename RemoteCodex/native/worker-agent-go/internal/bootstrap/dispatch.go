// Package bootstrap / dispatch.go — RW-PROD-003 bootstrap-gate dispatch.
//
// Dispatch bridges the canonical *pipeline.Runner (built by
// video.NewPipelineRunner in the composition root) into the narrow
// bootstrap.RunnerView interface used by pkg/bootstrap. We use a tiny
// adapter because pkg/bootstrap keeps pkg/video at arm's length to keep
// its test surface free of CGO coupling.
package bootstrap

import (
	"context"
	"path/filepath"

	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"

	pbb "velox-worker-agent/pkg/bootstrap"
)

// Dispatch (RW-PROD-003 A5) runs the synchronous bootstrap-OK gate
// between the C++ engine construction and the executor wiring. The gate
// proves:
//
//	A8:  bundle hash on disk == cfg.BundleHash
//	A3:  ffmpeg + ffprobe are present and libx264 is enumerable
//	A4:  OutputDir is mkdir-able + write-able + removable
//	A1+A2: engine self-render of a 1×1 black frame matches the SHA-256
//	      baseline committed at <WorkDir>/tests/fixtures/engine_selftest_baseline.sha256
//	      within a hard 5s budget
//
// On success and failure alike we ALWAYS dump the JSON boot report to
// stderr so ops triage stays in one place — the per-step record travels
// with the short-form error caught by the composition root. The
// --bootstrap-report certifier reads the same [BOOTSTRAP_REPORT] block
// off stderr to assert verdict+steps without re-instrumenting the worker.
//
// Returns the *Report so --bootstrap-report can exit with the verdict
// code (0=OK, !0=FAIL) without re-deriving verdict from message text.
func Dispatch(
	ctx context.Context,
	cfg *config.WorkerConfig,
	runner *pipeline.Runner,
	log *logger.Logger,
) (*pbb.Report, error) {
	// The creator profile does not use the C++ pipeline, so runner may be
	// nil. Pass a nil interface (not a typed nil pointer) so bootstrap.Run
	// can detect the absence of a runner cleanly.
	var adapter pbb.RunnerView
	if runner != nil {
		adapter = &pipelineRunnerAdapter{runner: runner}
	}
	report, err := pbb.Run(ctx, cfg, adapter, pbb.Options{
		Logger:             log,
		OutputDir:          cfg.OutputDir,
		BaselineSHA256Path: filepath.Join(cfg.WorkDir, pbb.DefaultBaselineFixtureRel),
	})
	if report != nil {
		_ = pbb.DumpReport(report)
	}
	return report, err
}

// pipelineRunnerAdapter is the one-method shim required by
// bootstrap.RunnerView. We do NOT export it (lowercase type) because the
// only caller is Dispatch in this package.
//
// Crucially: bootstrap.RenderClientIface and pipeline.RenderClient
// have IDENTICAL signatures (Render(ctx, *plan.RenderPlan) error) so the
// runner's render client can flow through to bootstrap without any
// struct wrapping — the adapter merely exposes the runner's accessor
// through a different interface name.
type pipelineRunnerAdapter struct {
	runner *pipeline.Runner
}

func (a *pipelineRunnerAdapter) RenderClient() pbb.RenderClientIface {
	if a == nil || a.runner == nil {
		return nil
	}
	return a.runner.RenderClient()
}
