// Package taskrunner is the generic per-task lifecycle orchestrator for
// the Velox worker agent. It owns the canonical 5-phase execution
// sequence (cache_lookup, prefetch, execute, upload, report), panic
// containment, error-code mapping, and TaskExecutionReport emission.
//
// PR-3.2 owns the typed ExecutionContext and concrete sub-interface
// implementations; PR-3.3 adds the lifecycle runner; PR-3.4 contributes
// one concrete Executor adapter (scene.composite.v1) that wraps the
// existing pkg/video pipeline path.
package taskrunner

import "errors"

// Sentinel errors. Match with errors.Is.
var (
	// ErrRegistryNil: the TaskRunner was constructed without a registry.
	// We panic in NewTaskRunner instead of returning this — leaving here
	// as the documented invariant so external test code can match it.
	ErrRegistryNil = errors.New("taskrunner: nil executor registry")

	// ErrInternalRunnerFault: a bug in the runner itself, not in an
	// executor. Returned wrapped with %w when an internal invariant is
	// violated (e.g. nil registry after construction). Code
	// CodeInternalRunnerFault covers the same condition.
	ErrInternalRunnerFault = errors.New("taskrunner: internal runner fault")
)

// Closed set of stable error codes that surface in TaskExecutionReport.ErrorCode.
//
// The runner NEVER emits a free-form string in ErrorCode. Callers can rely
// on a closed-string enum to branch safely.
const (
	// CodeSuccess: empty string; the convention is "no error code == success".
	CodeSuccess = ""

	// CodeValidationFailed: spec.Validate or Executor.Validate returned an
	// error before any work was performed.
	CodeValidationFailed = "validation_failed"

	// CodeUnsupportedExecutor: the Registry could not resolve the requested
	// ExecutorID at the (id, version) the master announced.
	CodeUnsupportedExecutor = "unsupported_executor"

	// CodeCacheLookupFailed: required cached inputs could not be retrieved
	// (PR-3.7 stub today; reserved for the real cache).
	CodeCacheLookupFailed = "cache_lookup_failed"

	// CodePrefetchFailed: a required artifact hash could not be fetched
	// for the executor's inputs.
	CodePrefetchFailed = "prefetch_failed"

	// CodeUploadFailed: publishing executor outputs to the canonical
	// artifact store failed after Execute succeeded.
	CodeUploadFailed = "upload_failed"

	// CodeExecuteFailed: Executor.Execute returned an error or yielded
	// a non-"succeeded" Status string.
	CodeExecuteFailed = "execute_failed"

	// CodeExecutorPanicContained: a panic during Executor.Execute was caught
	// by TaskRunner and converted into a Failed report. The original stack
	// trace is preserved in TaskExecutionReport.ErrorDetail.
	CodeExecutorPanicContained = "executor_panic_contained"

	// CodeLeaseLost: the parent context was canceled with the lease-loss
	// signal attached. Reserved for PR-3.5 worker-side lease plumbing;
	// in PR-3.3 we lump all ctx cancelation into CodeCanceled.
	CodeLeaseLost = "lease_lost"

	// CodeCanceled: the parent context was canceled without a lease-loss
	// signal (e.g. worker shutdown, drain mode, manual cancel).
	CodeCanceled = "canceled"

	// CodeContextDeadlineExceeded: the parent context deadline elapsed.
	CodeContextDeadlineExceeded = "context_deadline_exceeded"

	// CodeInternalRunnerFault: the runner itself faulted (nil registry,
	// programming error). This is a worker-side bug, never an executor
	// problem; safe for downstream alerting.
	CodeInternalRunnerFault = "internal_runner_fault"
)

// Canonical phase names. PR-3.3 invariant: TaskExecutionReport.PhaseMarkers
// contains at most ONE marker per phase, listed in this fixed order.
const (
	PhaseCacheLookup = "cache_lookup"
	PhasePrefetch    = "prefetch"
	PhaseExecute     = "execute"
	PhaseUpload      = "upload"
	PhaseReport      = "report"
)
