// Package taskrunner / error_mapping.go
//
// Error mapping — the small, pure helpers Run uses to turn free-form
// errors (panics, ctx errors, executor results) into the closed Code*
// enum that TaskExecutionReport carries.
//
// Lives in its own file so the mapping is easy to audit: every path
// from a free-form error to a Code* must pass through one of these
// functions, no inline string comparison against a Code constant.
package taskrunner

import (
	"context"
	"errors"

	"velox-worker-agent/internal/executor"
)

func isPanicErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrInternalRunnerFault)
}

func isPanicContained(r executor.ExecutionResult) bool {
	return r.ErrorCode == CodeExecutorPanicContained && r.Status == "failed"
}

func mapCtxErr(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return CodeContextDeadlineExceeded
	case errors.Is(err, context.Canceled):
		return CodeCanceled
	default:
		return CodeCanceled
	}
}
