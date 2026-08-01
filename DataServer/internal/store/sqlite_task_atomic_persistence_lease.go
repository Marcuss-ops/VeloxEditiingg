package store

// sqlite_task_atomic_persistence_lease.go is the home for lease-entity
// write helpers used by IngestTaskResultAtomic. Each helper receives the
// coordinator-owned transaction; none opens, commits, or rolls back a
// transaction. Split out of sqlite_task_atomic_persistence_helpers.go.
//
// Today the lease claim/renew/expire persistence lives in
// sqlite_task_lease.go (the standalone lease path); this file is kept as
// the dedicated lease surface for the atomic persistence layer so future
// lease persistence added to IngestTaskResultAtomic has a stable home.
// No lease helpers are defined here yet.
