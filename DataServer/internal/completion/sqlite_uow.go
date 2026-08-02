// Package completion / sqlite_uow.go
//
// SQLite-backed Unit of Work adapter. Implements the six repository
// interfaces declared in unitofwork.go against the canonical
// migrations 010/030/039/041/045/061/062/014/022/013 schemas.
//
// sql-allowlist: completion UnitOfWork adapter — owns the six typed
// repositories (attempt_commits, task_attempts, tasks, jobs, deliveries,
// outbox); sole SQL gateway per the UnitOfWork pattern documented in
// docs/architecture/unit-of-work.md. The Coordinator speaks only typed
// Go parameters; no SQL leaks beyond this file's package boundary.
//
// Every method receives the underlying *sql.Tx at construction time
// (via sqliteUnitOfWork.WithTx) and holds it as private state. No
// SQL is exposed beyond this file's package boundary; the
// coordinator speaks only typed Go parameters.
//
// Tx lifecycle stays at the coordinator layer (open, commit, defer
// rollback). This file does not start or commit transactions.
// loadCommitResult is invoked by the coordinator BEFORE Commit() so
// the snapshot is part of the same LevelSerializable write lock.
//
// File split by responsibility:
//   - sqlite_uow.go                → factory + UnitOfWork wiring
//   - sqlite_uow_attempt_commit.go → sqliteAttemptCommitRepo (attempt_commits)
//   - sqlite_uow_repos.go          → task_attempts, tasks, jobs, deliveries, outbox
package completion

import "database/sql"

// Compile-time assertion: sqliteUnitOfWork satisfies UnitOfWork.
var _ UnitOfWork = (*sqliteUnitOfWork)(nil)

// sqliteUnitOfWorkFactory produces UnitOfWork bundles bound to a tx.
type sqliteUnitOfWorkFactory struct {
	db *sql.DB
}

// NewSQLiteUnitOfWorkFactory constructs the canonical SQLite-backed
// UnitOfWorkFactory. Exposed for callers that want to opt out of the
// implicit factory creation in NewCoordinator (mostly tests).
func NewSQLiteUnitOfWorkFactory(db *sql.DB) UnitOfWorkFactory {
	return &sqliteUnitOfWorkFactory{db: db}
}

// WithTx returns a UnitOfWork bound to the supplied tx.
func (f *sqliteUnitOfWorkFactory) WithTx(tx *sql.Tx) UnitOfWork {
	return &sqliteUnitOfWork{tx: tx, db: f.db}
}

// sqliteUnitOfWork holds the tx shared by all six repos.
type sqliteUnitOfWork struct {
	tx *sql.Tx
	db *sql.DB
}

func (u *sqliteUnitOfWork) AttemptCommits() AttemptCommitRepository {
	return &sqliteAttemptCommitRepo{u: u}
}
func (u *sqliteUnitOfWork) TaskAttempts() TaskAttemptRepository {
	return &sqliteTaskAttemptRepo{u: u}
}
func (u *sqliteUnitOfWork) Tasks() TaskRepository {
	return &sqliteTaskRepo{u: u}
}
func (u *sqliteUnitOfWork) Jobs() JobFinalizationRepository {
	return &sqliteJobRepo{u: u}
}
func (u *sqliteUnitOfWork) Deliveries() DeliveryRepository {
	return &sqliteDeliveryRepo{u: u}
}
func (u *sqliteUnitOfWork) Outbox() OutboxRepository {
	return &sqliteOutboxRepo{u: u}
}
