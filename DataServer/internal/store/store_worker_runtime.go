// store_worker_runtime.go is intentionally a shell. The original
// store_worker_runtime.go has been redistributed across these focused
// files, each with a single concern:
//
//	store_worker_heartbeat.go        - PersistWorkerHeartbeat (the single
//	                                  transaction owner; only file that
//	                                  calls s.db.BeginTx) and the
//	                                  orchestrator that composes the
//	                                  helpers below
//	store_worker_runtime_projection.go - DeleteWorkerTaskRuntime +
//	                                  activeTasksFromSnapshot +
//	                                  reconcileWorkerRuntime
//	store_worker_metrics.go          - maybeInsertWorkerMetric +
//	                                  pruneWorkerMetricSamples
//	store_worker_events.go           - appendWorkerStateChangedEvent +
//	                                  appendTaskRuntimeDisappearedEvent
//	worker_value_decode.go           - clampPercent, defaultString,
//	                                  boolInt, int64Value,
//	                                  int64OrDefault, floatValue,
//	                                  floatOrMetric (shared by the
//	                                  snapshot file too)
//
// Single-writer contract enforced by this split:
//   - PersistWorkerHeartbeat (store_worker_heartbeat.go) is the only
//     heartbeat-path opener of *sql.Tx (s.db.BeginTx). Every other
//     helper in the heartbeat path files receives the *sql.Tx as a
//     parameter; none of them call BeginTx themselves.
//   - DOCUMENTED EXCEPTION (background recovery loop):
//     reconcileOnePartition
//     (in store_worker_runtime_recovery.go, called by the public
//     ReconcileWorkerPartitions) DOES open its own s.db.BeginTx —
//     one transaction per candidate worker. It cannot piggyback
//     on PersistWorkerHeartbeat: it is a cron-style recovery loop
//     for the case where the heartbeat stream has stopped entirely.
//     See the file header of store_worker_runtime_recovery.go for
//     the full rationale and the per-package single-writer
//     invariant that the exception preserves.
//   - DeleteWorkerTaskRuntime uses s.db.Exec directly (no transaction:
//     the canonical TaskResult transaction has already committed by the
//     time this is invoked).
//
// This split is purely structural: no SQL, no schema, no threshold,
// no retention window, no event_type, and no LOGIC semantics change.
// store_worker_runtime_test.go is unchanged (the public surface of
// *SQLiteStore — PersistWorkerHeartbeat + DeleteWorkerTaskRuntime —
// keeps identical signatures).
package store
