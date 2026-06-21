// Package database — executor.go
//
// Executor interface declared once for the whole codebase. Producers
// and store implementations pass *sql.DB or *sql.Tx interchangeably via
// this interface, so application code stays backend-neutral.
//
// Historically each narrow store (outbox, workflow, ...) re-declared
// the same interface locally. Aliasing outbox.Executor to this single
// declaration is the dedupe target so future stores reuse the same
// surface directly without redeclaration.
//
// *sql.DB and *sql.Tx from the standard library both satisfy the three
// methods — no adapter or wrapper required.
package database

import (
	"context"
	"database/sql"
)

// Executor is the minimum surface database-aware code uses to run
// statements. It exists in platform/database so producers
// (outbox/store.go etc.) can declare their parameter types against the
// single canonical interface instead of redeclaring the same shape
// per package.
//
// Both *sql.DB and *sql.Tx satisfy Executor out of the box — no
// wrappers needed on either side of the repository boundary.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
