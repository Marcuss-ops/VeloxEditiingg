// store_workers.go is intentionally a shell. The original
// store_workers.go (366 lines) has been redistributed across these
// focused files, each with a single concern:
//
//	store_worker_snapshot.go       - snapshot row CRUD (UpsertWorker,
//	                                EnsureWorkerRecord, GetWorker,
//	                                DeleteWorker, ListWorkers) and the
//	                                SQL UPSERT itself
//	worker_snapshot_mapping.go     - helpers shared by the snapshot file
//	                                (workerSQLExec interface,
//	                                workerActiveTaskCount, jsonString)
//	store_worker_flags.go          - worker_flags row (SetWorkerRevoked,
//	                                GetRevokedWorkers) — three-key audit
//	                                shape on worker_flags.raw_json
//	store_worker_validation.go     - worker_validations table
//	                                (WorkerValidationStatus type +
//	                                CreateValidationTableIfNotExists /
//	                                Save / Get / GetAll methods)
//	repository_workers.go          - WorkersRepository interface +
//	                                SQLiteWorkersRepository adapter
//
// This split keeps existing call sites intact (all methods remain
// receivers on *SQLiteStore or *SQLiteWorkersRepository, all exported
// names unchanged) so the refactor is purely structural: no behaviour,
// schema, query, or interface change.
package store
