// Package native provides the client for the C++ video engine.
package native

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"velox-worker-agent/pkg/binaryresolver"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/plan"
)

// ProgressFunc is called with progress updates from the C++ engine.
type ProgressFunc func(percent int, scene, total int, stage string)

// engineSidecar mirrors the C++ <output>.progress.json sidecar written
// by RenderEngine::emitSidecar. Fields are a subset of the emitted JSON
// needed for operator-visible telemetry. We parse only what the
// sidecar contract guarantees — unrecognised keys are silently ignored.
type engineSidecar struct {
	Frames         int64   `json:"frames"`
	Fps            float64 `json:"fps"`
	SpeedX         float64 `json:"speed_x"`
	EncodePasses   int64   `json:"encode_passes"`
	TempBytes      int64   `json:"temp_bytes"`
	DurationSec    float64 `json:"duration_seconds"`
	ConcatMode     string  `json:"concat_mode"`
	TotalSize      int64   `json:"total_size"`
	OutTimeUs      int64   `json:"out_time_us"`
	OutTimeMs      int64   `json:"out_time_ms"`
	Bitrate        float64 `json:"bitrate"`
	DupFrames      int64   `json:"dup_frames"`
	DropFrames     int64   `json:"drop_frames"`
}

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

	tempDir, err := os.MkdirTemp("", "velox_render_*")
	if err != nil {
		return metrics, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// ── Plan marshal + write ──────────────────────────────────
	planPath := filepath.Join(tempDir, "render_plan.json")
	marshalStart := time.Now()
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return metrics, fmt.Errorf("marshal plan: %w", err)
	}
	metrics.PlanMarshalMs = time.Since(marshalStart).Milliseconds()

	writeStart := time.Now()
	if err := os.WriteFile(planPath, data, 0o644); err != nil {
		return metrics, fmt.Errorf("write plan: %w", err)
	}
	metrics.PlanWriteMs = time.Since(writeStart).Milliseconds()

	// ── Subprocess launch ─────────────────────────────────────
	c.logger.Info("[NATIVE] Launching: %s --render --plan %s", c.binaryPath, planPath)
	cmd := exec.Command(c.binaryPath, "--render", "--plan", planPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return metrics, fmt.Errorf("stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return metrics, fmt.Errorf("stderr pipe: %w", err)
	}

	processStart := time.Now()
	if err := cmd.Start(); err != nil {
		return metrics, fmt.Errorf("start engine: %w", err)
	}
	metrics.ProcessStartMs = time.Since(processStart).Milliseconds()

	var stderrBuf strings.Builder
	stderrReader := bufio.NewReader(stderrPipe)
	progressDone := make(chan struct{})

	go func() {
		defer close(progressDone)
		for {
			line, err := stderrReader.ReadString('\n')
			if len(line) > 0 {
				line = strings.TrimRight(line, "\n\r")
				stderrBuf.WriteString(line)
				stderrBuf.WriteString("\n")
				var prog struct {
					Percent int    `json:"percent"`
					Scene   int    `json:"scene"`
					Total   int    `json:"total_scenes"`
					Stage   string `json:"stage"`
				}
				if json.Unmarshal([]byte(line), &prog) == nil && prog.Percent > 0 {
					if c.onProgress != nil {
						c.onProgress(prog.Percent, prog.Scene, prog.Total, prog.Stage)
					}
				}
			}
			if err != nil {
				break
			}
		}
	}()

	var stdoutBuf strings.Builder
	stdoutReader := bufio.NewReader(stdoutPipe)
	go func() {
		for {
			line, err := stdoutReader.ReadString('\n')
			if len(line) > 0 {
				stdoutBuf.WriteString(line)
			}
			if err != nil {
				break
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	waitStart := time.Now()
	select {
	case <-ctx.Done():
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		}
		<-progressDone
		return metrics, ctx.Err()
	case execErr := <-done:
		<-progressDone
		metrics.ProcessWaitMs = time.Since(waitStart).Milliseconds()
		if execErr != nil {
			return metrics, fmt.Errorf("engine failed: %w (stderr=%s stdout=%s)",
				execErr, strings.TrimSpace(stderrBuf.String()), strings.TrimSpace(stdoutBuf.String()))
		}
	}

	if stderr := strings.TrimSpace(stderrBuf.String()); stderr != "" {
		c.logger.Info("[NATIVE] stderr: %s", stderr)
	}

	if _, err := os.Stat(p.OutputPath); err != nil {
		return metrics, fmt.Errorf("output file not created %s: %w", p.OutputPath, err)
	}

	// ── Read C++ engine sidecar ───────────────────────────────
	sidecar, err := readEngineSidecar(p.OutputPath)
	if err != nil {
		c.logger.Warn("[NATIVE] sidecar read failed: %s", err.Error())
	} else {
		metrics.Frames = sidecar.Frames
		metrics.Fps = sidecar.Fps
		metrics.SpeedX = sidecar.SpeedX
		metrics.EncodePasses = sidecar.EncodePasses
		metrics.TempBytes = sidecar.TempBytes
		metrics.DurationSec = sidecar.DurationSec
		metrics.ConcatMode = sidecar.ConcatMode
		metrics.TotalSize = sidecar.TotalSize
		metrics.OutTimeMs = sidecar.OutTimeMs
		metrics.Bitrate = sidecar.Bitrate
		metrics.DupFrames = sidecar.DupFrames
		metrics.DropFrames = sidecar.DropFrames
	}

	metrics.TotalMs = time.Since(start).Milliseconds()
	return metrics, nil
}

// readEngineSidecar reads and parses the C++ sidecar at
// <outputPath>.progress.json. Returns a zero-value EngineSidecar if the
// file does not exist or cannot be parsed — callers treat missing
// sidecar as a non-fatal condition.
func readEngineSidecar(outputPath string) (engineSidecar, error) {
	var sc engineSidecar
	sidecarPath := outputPath + ".progress.json"
	f, err := os.Open(sidecarPath)
	if err != nil {
		return sc, fmt.Errorf("open sidecar %s: %w", sidecarPath, err)
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&sc); err != nil {
		return sc, fmt.Errorf("decode sidecar %s: %w", sidecarPath, err)
	}
	return sc, nil
}

func resolveBinary() (string, error) {
	r := binaryresolver.Resolver{
		Name:   "velox_video_engine",
		EnvVar: "VELOX_VIDEO_ENGINE_CPP_BIN",
		AbsCandidates: []string{
			"/usr/local/bin/velox_video_engine",
		},
		RelOffsets: []string{
			filepath.Join("..", "..", "..", "video-engine-cpp", "build", "velox_video_engine"),
			filepath.Join("..", "..", "..", "video-engine-cpp", "velox_video_engine"),
			filepath.Join("..", "..", "..", "..", "video-engine-cpp", "build", "velox_video_engine"),
			filepath.Join("..", "..", "..", "..", "video-engine-cpp", "velox_video_engine"),
		},
	}
	return r.Resolve(0)
}
