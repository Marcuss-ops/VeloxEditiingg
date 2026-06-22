// Package taskattempts no longer hosts the TaskReportIngestionService tests.
// See `internal/taskingestion/service_test.go` for the migrated coverage:
//   - TestIngestionService_HappyPathSucceeded
//   - TestIngestionService_IdempotentReplay
//   - TestIngestionService_SiblingsStillRunningNoJobRollUp
//   - TestIngestionService_RequiresAllDeps
//
// Migration rationale: feat/task-report-ingestion moved the service out
// of taskattempts (which imports taskgraph) into a leaf taskingestion
// package that imports BOTH taskattempts AND taskgraph without
// producing an import cycle.
package taskattempts
