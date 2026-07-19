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
//   - Only PersistWorkerHeartbeat opens a *sql.Tx (s.db.BeginTx).
//   - Every helper in the sibling files receives the *sql.Tx as a
//     parameter. None of them ever call BeginTx themselves.
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
