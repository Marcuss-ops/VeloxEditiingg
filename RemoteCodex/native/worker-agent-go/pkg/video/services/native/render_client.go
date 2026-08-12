// Package native provides the client for the C++ video engine.
package native

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// ProgressFunc is the legacy callback contract retained for source
// compatibility. DetailedProgressFunc is the canonical render telemetry
// callback used by the worker Attempt projection.
type ProgressFunc = pipeline.ProgressFunc
type DetailedProgressFunc = pipeline.DetailedProgressFunc

// RenderClient executes RenderPlans via the C++ video engine.
type RenderClient struct {
	binaryPath     string
	logger         *logger.Logger
	onProgress     DetailedProgressFunc
	legacyProgress ProgressFunc
	tempFiles      []string
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

// SetProgressCallback retains the legacy callback API. Legacy callbacks
// are delivered by the engine stream parser without replacing detailed
// Attempt telemetry.
func (c *RenderClient) SetProgressCallback(fn ProgressFunc) {
	c.legacyProgress = fn
}

// SetDetailedProgressCallback sets the canonical detailed render callback.
func (c *RenderClient) SetDetailedProgressCallback(fn DetailedProgressFunc) {
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
	if chrononBackendEnabled() {
		data, convertErr := chrononPlanJSON(p)
		if convertErr != nil {
			return metrics, convertErr
		}
		planPath = filepath.Join(tempDir, "chronon.render-plan.v1.json")
		if writeErr := os.WriteFile(planPath, data, 0o644); writeErr != nil {
			return metrics, fmt.Errorf("write Chronon render plan: %w", writeErr)
		}
	}

	c.logger.Info("[NATIVE] Launching: %s --render --plan %s", c.binaryPath, planPath)
	// SAFETY-CRITICAL subprocess lifecycle lives in engine_process.go.
	processStartMs, processWaitMs, stderrBuf, stdoutBuf, processCounts, err := runEngineProcess(ctx, c.binaryPath, planPath, c.onProgress, c.legacyProgress)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Cancellation path — preserve any sidecar phases already
			// flushed before the process stopped, while retaining the
			// original cancellation error semantics.
			applyProcessCounters(&metrics, processStartMs, processWaitMs, processCounts)
			if sidecar, sidecarErr := readEngineSidecar(p.OutputPath); sidecarErr == nil {
				mapEngineSidecar(&sidecar, &metrics)
			}
			return metrics, err
		}
		// Subprocess failed — preserve any partial sidecar telemetry before
		// returning the process error. Failed renders can still contain
		// completed phases and segment timing that are valuable for retry
		// and waste analysis; reading them is strictly best-effort.
		applyProcessCounters(&metrics, processStartMs, processWaitMs, processCounts)
		if sidecar, sidecarErr := readEngineSidecar(p.OutputPath); sidecarErr == nil {
			mapEngineSidecar(&sidecar, &metrics)
		}
		return metrics, fmt.Errorf("engine failed: %w (stderr=%s stdout=%s)",
			err, strings.TrimSpace(stderrBuf.String()), strings.TrimSpace(stdoutBuf.String()))
	}
	applyProcessCounters(&metrics, processStartMs, processWaitMs, processCounts)

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

// applyProcessCounters records the subprocess lifecycle counters on the
// metrics struct. EngineSpawnCount is 1 exactly when the engine process
// started (ProcessStartMs > 0) — including failed and cancelled
// renders, where the spawn still happened and its cost is part of the
// attempt. The exec counts come from the /proc sampler that ran while
// the engine process was alive.
func applyProcessCounters(m *pipeline.RenderMetrics, processStartMs, processWaitMs int64, counts ProcessCounts) {
	if m == nil {
		return
	}
	m.EngineSpawnMs = processStartMs
	m.ChildWaitMs = processWaitMs
	if processStartMs > 0 {
		m.EngineSpawnCount = 1
	}
	m.ExternalProcessCount = counts.ExternalProcessCount
	m.FfmpegExecCount = counts.FfmpegExecCount
	m.FfprobeExecCount = counts.FfprobeExecCount
	m.ShellExecCount = counts.ShellExecCount
	m.CurlExecCount = counts.CurlExecCount
}
