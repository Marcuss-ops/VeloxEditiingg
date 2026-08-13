// Package storecore provides the minimal database primitive shared by the
// leaf repository packages extracted from the internal/store god-package.
//
// Leaves depend on this tiny surface (instead of importing database/sql or
// internal/store) so internal/store stays a composition/compatibility facade
// rather than a dependency magnet. New leaf packages MUST import storecore,
// never internal/store.
package storecore

import (
	"context"
	"database/sql"
)

// DBTX is the minimal SQL execution surface a leaf repository needs. Both
// *sql.DB and *sql.Tx satisfy it, so a leaf function works identically inside
// and outside a caller-owned transaction without importing database/sql itself.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
