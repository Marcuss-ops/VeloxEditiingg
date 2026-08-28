package store

// attempt_metrics.go: typed metrics / cache-stats / cost-basis persistence
// + drift-snapshot reads for the attempts side. Single-row, non-tx
// writes against three flat read-models (`task_attempt_metrics`,
// `task_attempt_cache_stats`, `task_attempt_cost_basis`); the only
// Tx-encapsulated writes on the repo live in attempt_reports.go
// (per-phase / per-segment sidecars). Lifecycle row-CRUD lives in
// attempt_lifecycle.go.
// Extracted from sqlite_task_attempt_repository.go.
//
// Split into:
//   - attempt_metrics_write.go: PersistMetrics
//   - attempt_metrics_read.go:  GetMetrics, ListMetricsByGitSHA
//   - attempt_cache_stats.go:   PersistCacheStats, GetCacheStats
//   - attempt_cost_basis.go:    PersistCostBasis, GetCostBasis
