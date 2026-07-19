// job_executor.go is intentionally a shell. The original
// job_executor.go (a single 329-line file mixing four concerns:
// the executeTask orchestrator, the dispatch path to TaskRunner,
// the pb.TaskResult wire-format builder, and the active-task
// lifecycle / telemetry helpers) has been redistributed across
// these focused files, each with a single concern:
//
//	task_execution.go       - executeTask: the orchestrator that
//	                          wires concurrency acquire, state
//	                          transition, active-task registration,
//	                          progress callback, telemetry seeds,
//	                          dispatch, upload, outcome telemetry,
//	                          TaskResult submission, and the
//	                          error backoff sleep into a single
//	                          linear pipeline. Owns the canonical
//	                          package doc.
//	task_dispatch.go        - runJobTask (30-minute timeout wrapper)
//	                          + dispatchTaskRunner (asset resolve +
//	                          taskRunner.Run invocation + status
//	                          mapping + artifact-hash validation).
//	task_result_builder.go  - submitTaskResult: the single wire-format
//	                          entry point that builds pb.TaskResult,
//	                          stamps the report hash, and sends via
//	                          transport.
//	active_task_lifecycle.go - extracted helpers: registerActiveTask,
//	                          unregisterActiveTask,
//	                          withJobProgressCallback, recordTaskStart,
//	                          recordTaskOutcome (3-branch outcome
//	                          telemetry), recordTaskFinish.
//
// Single-instance contract enforced by this split:
//   - executeTask lives only on *Worker, only in task_execution.go.
//   - runJobTask + dispatchTaskRunner live only in task_dispatch.go.
//   - submitTaskResult lives only in task_result_builder.go.
//   - registerActiveTask / unregisterActiveTask / withJobProgressCallback
//     / recordTaskStart / recordTaskOutcome / recordTaskFinish live only
//     in active_task_lifecycle.go.
//   - The 3-branch outcome telemetry (cancelled / failed / succeeded)
//     is implemented exactly once in recordTaskOutcome, with
//     RecordJobRuntime called in every branch.
//
// Pure structural refactor: zero behaviour, schema, API, protocol,
// or telemetry-ordering change. Every error-wrap message, telemetry
// metric, log line, and lock acquisition is preserved verbatim.
// uploadTaskOutputs remains in output_upload.go (separate concern).
package worker
