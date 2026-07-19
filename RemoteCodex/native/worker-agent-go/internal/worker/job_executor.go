// job_executor.go is intentionally a shell. The original
// job_executor.go (a single 329-line file mixing four concerns:
// the executeTask orchestrator, the dispatch path to TaskRunner,
// the pb.TaskResult wire-format builder, and the active-task
// lifecycle / telemetry / upload helpers) has been redistributed
// across these focused files, each with a single concern:
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
//	task_dispatch.go        - dispatch path + active-task lifecycle
//	                          helpers:
//	                          - runJobTask (30-minute timeout wrapper)
//	                          - dispatchTaskRunner (asset resolve +
//	                            taskRunner.Run invocation + status
//	                            mapping + artifact-hash validation)
//	                          - registerActiveTask (builds +
//	                            inserts *ActiveTaskExecution into
//	                            the maps under activeTasksMu)
//	                          - unregisterActiveTask (deferred
//	                            cleanup mirroring the original
//	                            closure)
//	                          - withJobProgressCallback (wraps the
//	                            parent context with the progress
//	                            callback that updates
//	                            activeTask.Progress).
//	task_result_builder.go  - submitTaskResult: the single wire-format
//	                          entry point that builds pb.TaskResult,
//	                          stamps the report hash, and sends via
//	                          transport.
//	active_task_lifecycle.go - metriche + upload helpers:
//	                          - recordTaskStart (telemetry seeds:
//	                            SetWorkerStatus=2,
//	                            SetWorkerActiveJobs, LogJobStart).
//	                          - recordTaskOutcome (3-branch outcome
//	                            telemetry: cancelled / failed /
//	                            succeeded, with RecordJobRuntime
//	                            in every branch).
//	                          - recordTaskFinish (idle-side telemetry
//	                            restoration).
//	                          - uploadTaskOutputs (uploads the
//	                            canonical render.output artifact to
//	                            the master API; starts an "upload"
//	                            OTel span).
//	                          - selectUploadableOutput (picks the
//	                            canonical upload candidate).
//
// Single-instance contract enforced by this split:
//   - executeTask lives only on *Worker, only in task_execution.go.
//   - runJobTask + dispatchTaskRunner live only in task_dispatch.go.
//   - submitTaskResult lives only in task_result_builder.go.
//   - registerActiveTask / unregisterActiveTask /
//     withJobProgressCallback live only in task_dispatch.go.
//   - recordTaskStart / recordTaskOutcome / recordTaskFinish /
//     uploadTaskOutputs / selectUploadableOutput live only in
//     active_task_lifecycle.go.
//   - The 3-branch outcome telemetry (cancelled / failed / succeeded)
//     is implemented exactly once in recordTaskOutcome, with
//     RecordJobRuntime called in every branch.
//
// Pure structural refactor: zero behaviour, schema, API, protocol,
// or telemetry-ordering change. Every error-wrap message, telemetry
// metric, log line, and lock acquisition is preserved verbatim.
package worker
