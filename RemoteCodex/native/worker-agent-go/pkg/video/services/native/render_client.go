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
	"velox-worker-agent/pkg/video/plan"
)

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

// Render writes the plan to disk and launches velox_video_engine --render --plan.
func (c *RenderClient) Render(ctx context.Context, p *plan.RenderPlan) error {
	tempDir, err := os.MkdirTemp("", "velox_render_*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	planPath := filepath.Join(tempDir, "render_plan.json")
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal plan: %w", err)
	}
	if err := os.WriteFile(planPath, data, 0o644); err != nil {
		return fmt.Errorf("write plan: %w", err)
	}

	c.logger.Info("[NATIVE] Launching: %s --render --plan %s", c.binaryPath, planPath)
	cmd := exec.Command(c.binaryPath, "--render", "--plan", planPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start engine: %w", err)
	}

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
		return ctx.Err()
	case execErr := <-done:
		<-progressDone
		if execErr != nil {
			return fmt.Errorf("engine failed: %w (stderr=%s stdout=%s)",
				execErr, strings.TrimSpace(stderrBuf.String()), strings.TrimSpace(stdoutBuf.String()))
		}
	}

	if stderr := strings.TrimSpace(stderrBuf.String()); stderr != "" {
		c.logger.Info("[NATIVE] stderr: %s", stderr)
	}

	if _, err := os.Stat(p.OutputPath); err != nil {
		return fmt.Errorf("output file not created %s: %w", p.OutputPath, err)
	}

	return nil
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
