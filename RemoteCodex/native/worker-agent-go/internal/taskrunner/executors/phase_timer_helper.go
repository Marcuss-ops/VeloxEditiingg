// Package executors — phase timer & GPU transfer integration helpers.
//
// These helpers extract the shared JobPhaseTimer and GPUTransferTracker from
// the ExecutionContext (threaded from the taskrunner). Executors use these
// to record fine-grained per-phase wall-clock durations and GPU↔CPU frame
// transfers that feed the PerformanceReport at job completion.
package executors

import (
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
)

// jobPhaseTimerFromExecutionContext returns the shared fine-grained phase
// timer from the execution context, or nil when none was wired (e.g. tests
// or executors that predate the timer integration).
func jobPhaseTimerFromExecutionContext(execCtx executor.ExecutionContext) *telemetry.JobPhaseTimer {
	if execCtx == nil {
		return nil
	}
	if provider, ok := execCtx.(interface {
		PhaseTimer() *telemetry.JobPhaseTimer
	}); ok {
		return provider.PhaseTimer()
	}
	return nil
}

// gpuTransferTrackerFromExecutionContext returns the shared GPU transfer
// tracker from the execution context, or nil when none was wired.
func gpuTransferTrackerFromExecutionContext(execCtx executor.ExecutionContext) *telemetry.GPUTransferTracker {
	if execCtx == nil {
		return nil
	}
	if provider, ok := execCtx.(interface {
		GPUTransferTracker() *telemetry.GPUTransferTracker
	}); ok {
		return provider.GPUTransferTracker()
	}
	return nil
}
