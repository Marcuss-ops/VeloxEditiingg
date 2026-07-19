// Package native provides the client for the C++ video engine.
package native

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/plan"
)

// render_client.go owns the *RenderClient surface — the exported
// type, constructor, callback setter, and the thin orchestrator that
// composes the helpers living in the sibling files:
//
//   engine_process.go    — subprocess lifecycle + signal handling
//   engine_progress.go   — stream + JSON progress parsing
//   engine_sidecar.go    — sidecar types + reader
//   binary_resolver.go   — binary resolution + plan temp +
//                          sidecar→metrics mapping + output verify
//
// RenderWithMetrics below is the orchestrator: it sequences those
// helpers with explicit measurement of marshal/write/start/wait
// wallclock counters. SAFETY-critical code lives in engine_process.go
// (the Setpgid+Pdeathsig+grace-10s+SIGKILL block) and is not touched
// here — this file only composes it.

// ProgressFunc is called with progress updates from the C++ engine.
type ProgressFunc func(percent int, scene, total int, stage string)

// RenderClient executes RenderPlans via the C++ video engine.
type RenderClient struct {
	binaryPath string
	logger     *logger.Logger
	onProgress ProgressFunc
	tempFiles  []string
}

// NewRenderClient creates a new native render client.
func NewRenderClient(log *logger.Logger) (*RenderClient, error) {
	bin, err := resolveBinary()
	if err != nil {
		return nil, fmt.Errorf("locate native engine: %w", err)
	}
	return &RenderClient{
		binaryPath: bin,
		logger:     log,
	}, nil
}

// SetProgressCallback sets the progress callback.
func (c *RenderClient) SetProgressCallback(fn ProgressFunc) {
	c.onProgress = fn
}

// Render is a convenience wrapper around RenderWithMetrics for callers
// that only need error semantics (e.g. bootstrap self-test).
func (c *RenderClient) Render(ctx context.Context, p *plan.RenderPlan) error {
	_, err := c.RenderWithMetrics(ctx, p)
	return err
}

// RenderWithMetrics writes the plan to disk, launches
// velox_video_engine --render --plan, and returns the parsed engine
// sidecar + subprocess wall-clock counters. The sidecar is read from
// <outputPath>.progress.json as emitted by C++ RenderEngine::emitSidecar.
func (c *RenderClient) RenderWithMetrics(ctx context.Context, p *plan.RenderPlan) (pipeline.RenderMetrics, error) {
	metrics := pipeline.RenderMetrics{}
	start := time.Now()

	tempDir, planPath, marshalMs, writeMs, err := preparePlanTemp(p)
	if err != nil {
		return metrics, err
	}
	// preparePlanTemp cleans up on its own partial-failure path; only
	// the success path leaves a live tempDir, which we own here.
	defer os.RemoveAll(tempDir)
	metrics.PlanMarshalMs = marshalMs
	metrics.PlanWriteMs = writeMs

	c.logger.Info("[NATIVE] Launching: %s --render --plan %s", c.binaryPath, planPath)
	// SAFETY-CRITICAL subprocess lifecycle lives in engine_process.go.
	processStartMs, processWaitMs, stderrBuf, stdoutBuf, err := runEngineProcess(ctx, c.binaryPath, planPath, c.onProgress)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Cancellation path — ProcessStartMs is set, ProcessWaitMs
			// is intentionally zero (matches original). TotalMs is
			// not set on either error path.
			metrics.ProcessStartMs = processStartMs
			return metrics, err
		}
		// Subprocess failed — populate ProcessWaitMs + wrap with
		// stderr/stdout context exactly as the original did.
		metrics.ProcessStartMs = processStartMs
		metrics.ProcessWaitMs = processWaitMs
		return metrics, fmt.Errorf("engine failed: %w (stderr=%s stdout=%s)",
			err, strings.TrimSpace(stderrBuf.String()), strings.TrimSpace(stdoutBuf.String()))
	}
	metrics.ProcessStartMs = processStartMs
	metrics.ProcessWaitMs = processWaitMs

	if stderr := strings.TrimSpace(stderrBuf.String()); stderr != "" {
		c.logger.Info("[NATIVE] stderr: %s", stderr)
	}

	if err := verifyOutputExists(p.OutputPath); err != nil {
		return metrics, err
	}

	sidecar, scErr := readEngineSidecar(p.OutputPath)
	if scErr != nil {
		c.logger.Warn("[NATIVE] sidecar read failed: %s", scErr.Error())
	} else {
		mapEngineSidecar(&sidecar, &metrics)
	}

	metrics.TotalMs = time.Since(start).Milliseconds()
	return metrics, nil
}
