// Package taskrunner / execution.go
//
// Execution phase — the panic-contained wrapper around Executor.Execute.
// runExecute is the heart of PR-3.3: it invokes Executor.Execute under a
// recover() guard so a panic never escapes the runner. It also checks
// the parent ctx after Execute returns so we can hand back lease /
// cancel / deadline errors cleanly.
package taskrunner

import (
	"fmt"
	"runtime/debug"

	"velox-worker-agent/internal/executor"
)

// runExecute is the heart of PR-3.3: it invokes Executor.Execute under a
// recover() guard so a panic never escapes the runner. It also checks
// the parent ctx after Execute returns so we can hand back lease /
// cancel / deadline errors cleanly.
func (r *TaskRunner) runExecute(rc *runnerContext, exec executor.Executor, spec executor.TaskSpec, appendPhase func(PhaseMarker)) (executor.ExecutionResult, error) {
	execStart := r.now()
	var result executor.ExecutionResult
	var execErr error
	var recovered any

	func() {
		defer func() {
			recovered = recover()
		}()
		result, execErr = exec.Execute(rc.ctx, rc, spec)
	}()

	execEnd := r.now()
	final := PhaseMarker{Name: PhaseExecute, StartedAt: execStart, CompletedAt: execEnd, Status: "ok"}
	if recovered != nil {
		final.Status = "failed"
		final.Notes = fmt.Sprintf("panic: %v", recovered)
		execErr = fmt.Errorf("%w: panic in executor.Execute: %v", ErrInternalRunnerFault, recovered)
	} else if execErr != nil {
		final.Status = "failed"
		final.Notes = execErr.Error()
	} else if result.Status != "" && result.Status != "succeeded" {
		final.Status = "failed"
		final.Notes = fmt.Sprintf("status=%q code=%q detail=%q",
			result.Status, result.ErrorCode, result.ErrorDetail)
	}
	appendPhase(final)

	if recovered != nil || execErr != nil {
		// Reset result so the caller sees the failure post-mapping.
		if recovered != nil {
			result = executor.ExecutionResult{
				Status:      "failed",
				ErrorCode:   CodeExecutorPanicContained,
				ErrorDetail: fmt.Sprintf("panic in executor.Execute: %v\n%s", recovered, debug.Stack()),
				StartedAt:   execStart,
				CompletedAt: execEnd,
			}
		}
		return result, execErr
	}

	// Post-Execute ctx check: lease / cancel / deadline.
	if err := rc.Err(); err != nil {
		return executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   mapCtxErr(err),
			ErrorDetail: err.Error(),
			StartedAt:   execStart,
			CompletedAt: execEnd,
		}, err
	}
	return result, nil
}
