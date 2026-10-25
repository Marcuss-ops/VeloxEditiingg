// Package video — canonical pipeline runner constructor (PR-3.9).
//
// Background: the V2 packet-copy executor wraps a pipeline.Runner. Worker
// bootstrap needs to build that Runner from the
// canonical pipeline registry + native render client. Prior to PR-3.9
// the worker package carried executeWorkflowJob + newVideoWorkflow that
// duplicated this wiring (the duplicate-routing that PR-3.9 deletes).
//
// NewPipelineRunner is the SINGLE place that builds and returns a
// pipeline.Runner ready for executor wiring. The production
// composition root (cmd/velox-worker-agent/main.go) consumes this
// constructor to wire the V2 packet-copy adapter.
package video

import (
	"fmt"

	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/services/audio"
	"velox-worker-agent/pkg/video/services/native"
)

// NewPipelineRunner builds the canonical pipeline.Runner used by
// the V2 packet-copy executor registered in the worker executor.Registry
// at boot (PR-3.9).
//
// log MUST be non-nil — production callers come from main.go where
// the canonical logger is always available, and silently installing a
// noop logger here would swallow renderer errors at PR-3.9 boot.
// Panicking on nil-log surfaces the caller error loudly, mirroring
// executor wiring's nil-runner contract.
//
// Errors surface ONLY when the native render client cannot be located.
// Pipeline registration is a no-allocation operation and cannot fail.
// A nil binary path (C++ engine not installed) is a deploy-time
// problem the caller must surface — we wrap the error so the worker
// fails closed.
func NewPipelineRunner(log *logger.Logger) (*pipeline.Runner, error) {
	if log == nil {
		panic("video.NewPipelineRunner: logger is required — production callers must pass the worker's canonical logger")
	}
	registry := pipeline.NewRegistry()
	probe := &audio.FFprobe{}
	registerPipelines(registry, probe)

	client, err := native.NewRenderClient(log)
	if err != nil {
		return nil, fmt.Errorf("video.NewPipelineRunner: native render client: %w", err)
	}
	return pipeline.NewRunner(registry, client, log), nil
}
