// Package taskattempts no longer hosts TaskReportIngestionService.
//
// As of feat/task-report-ingestion, the canonical ingestion entry point
// lives in `internal/taskingestion` (a leaf package) so that:
//   - taskattempts can keep importing the canonical drivers
//     (taskgraph, job-related reader/writer primitives)
//   - the new taskingestion service can import BOTH taskattempts AND
//     taskgraph without producing an import cycle
//     (taskgraph.Repository references taskattempts.TaskAttempt)
//
// See `internal/taskingestion/service.go` for the new home.
package taskattempts
