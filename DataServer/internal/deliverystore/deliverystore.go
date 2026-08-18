// Package deliverystore is the SQLite persistence for job_deliveries,
// delivery_destinations, and the delivery lease/retry state machine. It was
// split out of the internal/store god-package: the domain model (status
// vocabulary + lease/row shapes) lives here, the business state machine lives
// in internal/deliveries, and this package owns only the SQLite SQL/CAS.
//
// It depends on deliverycontract, repository (the DBTelemetry seam) and
// storecore (the shared DB primitive), never on internal/store —
// internal/store re-exports its surface as a compatibility facade.
package deliverystore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/repository"
	"velox-server/internal/sqliteerr"
)

// ParentJobFinalizer is the cross-domain injection point for the delivery
// terminal-transition transaction. MarkDeliverySucceeded / Failed /
// BlockedAuth must flip the parent job's aggregate state (jobs table) inside
// the SAME transaction as the job_deliveries CAS, so this leaf accepts a
// narrow Tx-scoped finalizer instead of importing the store god-package
// (which would create a store ↔ deliverystore cycle). SQLiteStore implements
// it.
type ParentJobFinalizer interface {
	FinalizeParentJobIfDeliveriesDone(ctx context.Context, tx *sql.Tx, deliveryID, now string) error
}

// SQLiteDeliveryStore implements the delivery persistence surface against a
// *sql.DB. It is a pure repository: no business policy, no provider dispatch.
type SQLiteDeliveryStore struct {
	db                 *sql.DB
	dbTelemetry        repository.DBTelemetry
	parentJobFinalizer ParentJobFinalizer
}

// NewSQLiteDeliveryStore wraps an existing *sql.DB as a SQLiteDeliveryStore.
func NewSQLiteDeliveryStore(db *sql.DB) *SQLiteDeliveryStore {
	if db == nil {
		panic("deliverystore: NewSQLiteDeliveryStore requires a non-nil *sql.DB")
	}
	return &SQLiteDeliveryStore{db: db}
}

// WithDBTelemetry injects the persistence-observability seam. Without it the
// leaf performs no observation (identical to the store facade's dormant
// telemetry path before extraction).
func (w *SQLiteDeliveryStore) WithDBTelemetry(t repository.DBTelemetry) *SQLiteDeliveryStore {
	if w != nil {
		w.dbTelemetry = t
	}
	return w
}

// WithParentJobFinalizer injects the cross-domain jobs finalizer used by the
// terminal MarkDelivery* transitions. Without it those transitions fail
// closed, because the job_deliveries CAS and the parent-job aggregate flip
// must share one transaction (see marks.go).
func (w *SQLiteDeliveryStore) WithParentJobFinalizer(f ParentJobFinalizer) *SQLiteDeliveryStore {
	if w != nil {
		w.parentJobFinalizer = f
	}
	return w
}

// nowRFC3339 returns the current UTC time formatted for a store timestamp
// column (second precision, RFC3339). Mirrors the store-package helper so the
// leaf's timestamp serialization stays identical.
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// nowRFC3339Nano returns the current UTC time formatted at nanosecond
// precision (RFC3339Nano), used by columns that must preserve lease/fence
// ordering boundaries.
func nowRFC3339Nano() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// nullIfEmpty returns nil for empty strings, otherwise the string itself.
func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func (w *SQLiteDeliveryStore) observeDBOperation(write bool) {
	if w != nil && w.dbTelemetry != nil {
		w.dbTelemetry.RecordDBOperation(write)
		w.observeDBStats()
	}
}

func (w *SQLiteDeliveryStore) observeDBTransaction(waitMS, transactionMS float64, busy, busyTimeout, retried bool, writeOps, readOps uint64) {
	if w != nil && w.dbTelemetry != nil {
		w.dbTelemetry.ObserveDBTransaction(waitMS, transactionMS, busy, busyTimeout, retried, writeOps, readOps)
		w.observeDBStats()
	}
}

func (w *SQLiteDeliveryStore) observeDBStats() {
	if w == nil || w.db == nil || w.dbTelemetry == nil {
		return
	}
	stats := w.db.Stats()
	w.dbTelemetry.ObserveDBStats(
		int64(stats.OpenConnections), int64(stats.InUse), int64(stats.Idle),
		stats.WaitCount, float64(stats.WaitDuration.Microseconds())/1000,
	)
}

// runInTx opens a *sql.Tx, invokes fn(ctx, tx), and commits iff fn returns
// nil. Any non-nil return from fn rolls back the transaction; the fn's error
// is propagated to the caller. It mirrors the store TxManager.RunInTx
// lifecycle so the leaf's transactions behave identically.
func (w *SQLiteDeliveryStore) runInTx(ctx context.Context, fn func(ctx context.Context, tx *sql.Tx) error) error {
	if w == nil || w.db == nil {
		return fmt.Errorf("deliverystore: runInTx: not initialized")
	}
	if fn == nil {
		return fmt.Errorf("deliverystore: runInTx: nil callback")
	}

	started := time.Now()
	tx, err := w.db.BeginTx(ctx, nil)
	waitMS := float64(time.Since(started).Microseconds()) / 1000
	if err != nil {
		transactionMS := float64(time.Since(started).Microseconds()) / 1000
		busy, busyTimeout := sqliteBusyFlags(err, transactionMS)
		w.observeDBTransaction(waitMS, transactionMS, busy, busyTimeout, false, 1, 0)
		return fmt.Errorf("deliverystore: runInTx begin: %w", err)
	}

	fnErr := fn(ctx, tx)
	if fnErr != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			transactionMS := float64(time.Since(started).Microseconds()) / 1000
			busy, busyTimeout := sqliteBusyFlags(fnErr, transactionMS)
			w.observeDBTransaction(waitMS, transactionMS, busy, busyTimeout, false, 1, 0)
			return fmt.Errorf("deliverystore: runInTx: callback returned error (rollback also failed: %v): %w", rbErr, fnErr)
		}
		transactionMS := float64(time.Since(started).Microseconds()) / 1000
		busy, busyTimeout := sqliteBusyFlags(fnErr, transactionMS)
		w.observeDBTransaction(waitMS, transactionMS, busy, busyTimeout, false, 1, 0)
		return fnErr
	}

	if cErr := tx.Commit(); cErr != nil {
		transactionMS := float64(time.Since(started).Microseconds()) / 1000
		busy, busyTimeout := sqliteBusyFlags(cErr, transactionMS)
		w.observeDBTransaction(waitMS, transactionMS, busy, busyTimeout, false, 1, 0)
		return fmt.Errorf("deliverystore: runInTx commit: %w", cErr)
	}
	w.observeDBTransaction(waitMS, float64(time.Since(started).Microseconds())/1000, false, false, false, 1, 0)
	return nil
}

// sqliteBusyFlags distinguishes any busy/locked result from one that spent
// approximately the configured 10s SQLite busy timeout before surfacing.
func sqliteBusyFlags(err error, transactionMS float64) (busy, busyTimeout bool) {
	busy = sqliteerr.IsBusy(err)
	return busy, busy && transactionMS >= 9000
}
