// Package executors — render_batch@1 executor.
//
// render_batch_executor.go owns the executor surface (descriptor type,
// constructor, Descriptor, Validate, registration). The Execute orchestration
// and its runCommand helper live in render_batch_execute.go; the ffmpeg argv
// builders live in render_batch_args.go; validation helpers live in
// render_batch_validate.go; observability lives in render_batch_observability.go.
package executors

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

const (
	// RenderBatchID is the canonical executor ID for the V2 compiled-plan path.
	RenderBatchID = "render_batch"

	// RenderBatchVersion is the first version of the V2 batch contract.
	RenderBatchVersion = 1
)

var (
	ErrMissingRenderBatchBindings = errors.New("render_batch@1: resolved asset bindings are required")
	ErrRenderBatchAssetIntegrity  = errors.New("render_batch@1: asset binding integrity mismatch")
	ErrCopyOnlyVideoIncompatible  = errors.New("render_batch@1: video packet-copy contract rejected")
)

type renderBatchExecutor struct {
	descriptor executor.Descriptor
	runner     ffmpegrunner.FFmpegRunner
	outputRoot string
	probe      func(context.Context, string) (publisher.MediaProbe, error)
}

// NewRenderBatch constructs the canonical render_batch@1 executor.
func NewRenderBatch(runner ffmpegrunner.FFmpegRunner, outputRoot string) executor.Executor {
	if runner == nil {
		runner = ffmpegrunner.NewProcessRunner()
	}
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "render-batch")
	}
	return &renderBatchExecutor{
		descriptor: executor.Descriptor{
			ID:            RenderBatchID,
			Version:       RenderBatchVersion,
			InputTypes:    []string{"render.compiled.v2"},
			OutputTypes:   []string{"video/mp4"},
			ResourceClass: executor.ResourceCPU,
			Deterministic: true,
			Cacheable:     true,
			TemporalMode:  executor.TemporalGlobal,
		},
		runner:     runner,
		outputRoot: outputRoot,
		probe:      publisher.ProbeMediaDetails,
	}
}

func (e *renderBatchExecutor) Descriptor() executor.Descriptor { return e.descriptor }

// Validate admits only a complete, strict, canonical V2 envelope. Legacy
// render_plan/render_plan_json payloads remain owned by the V1 executors.
func (e *renderBatchExecutor) Validate(spec executor.TaskSpec) error {
	if spec.ExecutorID != RenderBatchID {
		return fmt.Errorf("render_batch@1: executor_id must be %q, got %q", RenderBatchID, spec.ExecutorID)
	}
	if spec.Payload == nil {
		return errors.New("render_batch@1: payload is required")
	}
	if raw, ok := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string); !ok || strings.TrimSpace(raw) == "" {
		return fmt.Errorf("render_batch@1: %q is required", contract.PayloadKeyCompiledRenderPlanJSON)
	}
	if raw, ok := spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA].(string); !ok || strings.TrimSpace(raw) == "" {
		return fmt.Errorf("render_batch@1: %q is required", contract.PayloadKeyCompiledRenderPlanSHA)
	}
	if _, err := contract.DecodeCompiledRenderPlanV2Payload(spec.Payload); err != nil {
		return fmt.Errorf("render_batch@1: invalid CompiledRenderPlanV2: %w", err)
	}
	return nil
}

// RegisterRenderBatchExecutor adds exactly one render_batch@1 entry to the
// canonical registry. Existing V1 registrations are neither replaced nor
// modified.
func RegisterRenderBatchExecutor(reg *executor.Registry, runner ffmpegrunner.FFmpegRunner, outputRoot string) error {
	if reg == nil {
		return errors.New("render_batch@1: registry is nil")
	}
	return reg.Register(NewRenderBatch(runner, outputRoot))
}
