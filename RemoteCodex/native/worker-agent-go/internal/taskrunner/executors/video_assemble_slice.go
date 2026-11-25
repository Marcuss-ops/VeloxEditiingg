package executors

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/video/pipeline"
)

const (
	VideoAssembleSliceID      = "video.assemble.slice.v1"
	VideoAssembleSliceVersion = 1
)

// videoAssembleSliceExecutor preserves the persisted legacy executor ID and
// delegates all assembly behavior to the canonical packet-copy executor.
type videoAssembleSliceExecutor struct {
	runner     *pipeline.Runner
	outputRoot string
}

func NewVideoAssembleSlice(outputRoot string, runner *pipeline.Runner) executor.Executor {
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "video-assemble-slice")
	}
	return &videoAssembleSliceExecutor{runner: runner, outputRoot: outputRoot}
}

func (e *videoAssembleSliceExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID: VideoAssembleSliceID, Version: VideoAssembleSliceVersion,
		InputTypes: []string{"render.compiled.v2.drive"}, OutputTypes: []string{"video/mp4"},
		ResourceClass: executor.ResourceCPU, Deterministic: true, Cacheable: true,
		TemporalMode: executor.TemporalGlobal,
	}
}

func (e *videoAssembleSliceExecutor) copySpec(spec executor.TaskSpec) (executor.TaskSpec, error) {
	if spec.ExecutorID != VideoAssembleSliceID {
		return executor.TaskSpec{}, fmt.Errorf("video.assemble.slice.v1: executor_id must be %q", VideoAssembleSliceID)
	}
	if e == nil || e.runner == nil || e.runner.RenderClient() == nil {
		return executor.TaskSpec{}, errors.New("video.assemble.slice.v1: native packet-copy renderer is not configured")
	}
	spec.ExecutorID = VideoAssembleCopyID
	return spec, nil
}

func (e *videoAssembleSliceExecutor) Validate(spec executor.TaskSpec) error {
	copySpec, err := e.copySpec(spec)
	if err != nil {
		return err
	}
	return NewVideoAssembleCopy(e.runner, e.outputRoot).Validate(copySpec)
}

func (e *videoAssembleSliceExecutor) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	copySpec, err := e.copySpec(spec)
	if err != nil {
		return executor.ExecutionResult{}, err
	}
	return NewVideoAssembleCopy(e.runner, e.outputRoot).Execute(ctx, execCtx, copySpec)
}
