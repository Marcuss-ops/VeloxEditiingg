// output_upload.go is intentionally a shell. The original
// output_upload.go (the two upload helpers: uploadTaskOutputs +
// selectUploadableOutput) has been redistributed into the
// active-task lifecycle surface — see active_task_lifecycle.go.
//
// File layout decision (post-refactor):
//
//	output_upload.go          — this shell: redistributive comment,
//	                            kept for git history.
//	active_task_lifecycle.go  — owns the upload helpers
//	                            (uploadTaskOutputs + selectUploadableOutput)
//	                            alongside the metriche helpers
//	                            (recordTaskStart + recordTaskOutcome
//	                            + recordTaskFinish). The upload
//	                            surface is part of the active-task
//	                            lifecycle because it runs in the same
//	                            "task is finishing" phase as the
//	                            outcome telemetry: executeTask calls
//	                            uploadTaskOutputs after dispatch,
//	                            then recordTaskOutcome wraps the
//	                            upload failure (if any) into execErr.
//
// Single-instance contract enforced by this redistribution:
//   - uploadTaskOutputs exists only on *Worker, only in
//     active_task_lifecycle.go.
//   - selectUploadableOutput exists only in active_task_lifecycle.go.
//   - The Scorecard v2 / Step 15 "upload" OTel span starts in
//     uploadTaskOutputs via oteltrace.StartSpan and ends on function
//     return via defer span.End() — same lifecycle as the original.
//
// Pure structural refactor: zero behaviour, schema, API, or
// telemetry change. The error-wrap messages
// ("worker output upload: api client is not configured",
// "worker output upload: no uploadable output with a local file path",
// "worker output upload: output file %q is not readable: %w",
// "worker output upload: master rejected upload for job %s: %s")
// are preserved verbatim.
package worker
