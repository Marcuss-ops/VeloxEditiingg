package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"velox-server/internal/deliverycontract"
)

// SQLiteJobDeliveryCounter is the production implementation of the
// consumer-owned JobDeliveryCounter port on top of *sql.DB. All methods
// tolerate sql.ErrNoRows by returning 0 (the caller treats 0 as "no rows",
// which differs semantically from a real error).
//
// The interface itself lives in the consumer package (internal/artifacts)
// so the consumer owns the contract. This package does not import that
// consumer package to avoid an import cycle; instead, a local anonymous
// interface provides a compile-time assertion that the concrete type
// satisfies the contract's method shape (structural interface matching).
type SQLiteJobDeliveryCounter struct {
	db *sql.DB
}

// NewSQLiteJobDeliveryCounter constructs the typed reader. db must
// outlive the reader.
func NewSQLiteJobDeliveryCounter(db *sql.DB) *SQLiteJobDeliveryCounter {
	if db == nil {
		panic("store: NewSQLiteJobDeliveryCounter requires a non-nil *sql.DB")
	}
	return &SQLiteJobDeliveryCounter{db: db}
}

// NewSQLiteJobDeliveryCounterFromStore binds the read-only counter to the
// canonical SQLiteStore. Raw database handles remain inside store.
func NewSQLiteJobDeliveryCounterFromStore(s *SQLiteStore) *SQLiteJobDeliveryCounter {
	if s == nil || s.db == nil {
		panic("store: NewSQLiteJobDeliveryCounterFromStore requires a non-nil SQLiteStore")
	}
	return &SQLiteJobDeliveryCounter{db: s.db}
}

// Compile-time assertion using an anonymous interface. The consumer-owned
// port in internal/artifacts has the identical method signature, so
// SQLiteJobDeliveryCounter satisfies it structurally without forcing an
// import cycle.
var _ interface {
	CountExpectedDeliveries(ctx context.Context, jobID, overrideDestID string) (int, error)
} = (*SQLiteJobDeliveryCounter)(nil)

// CountExpectedDeliveries returns the destinations-count the
// finalize tx would stamp for the given job and override. See the
// artifacts.JobDeliveryCounter interface docstring for the
// resolution-order mirror rationale.
func (c *SQLiteJobDeliveryCounter) CountExpectedDeliveries(ctx context.Context, jobID, overrideDestID string) (int, error) {
	if overrideDestID != "" {
		// Single-destination explicit path — the writer hard-codes
		// 1 here regardless of any per-job plan; the gate mirrors
		// that exactly. Override identity is opaque to this method;
		// the writer does not validate it.
		return 1, nil
	}
	if jobID == "" {
		return 0, fmt.Errorf("store: JobDeliveryCounter.CountExpectedDeliveries: empty jobID (overrideDestID was also empty)")
	}
	// Branch 1: per-job plan (production path). Mirror the ORDER BY
	// used by SQLiteDeliveryPlanResolver::ResolvePlan so the count
	// is identical to what the resolver would have returned as a
	// slice length.
	var n int
	if err := c.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_delivery_plans WHERE job_id = ? AND enabled = 1`,
		jobID,
	).Scan(&n); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Defensive: COUNT(*) never returns ErrNoRows; treat it
			// as an absent explicit plan and fail closed below.
			n = 0
		} else {
			return 0, fmt.Errorf("store: JobDeliveryCounter.CountExpectedDeliveries plans count: %w", err)
		}
	}
	if n > 0 {
		return n, nil
	}
	// No explicit plan exists. Never count unrelated global
	// delivery_destinations: finalization must fail closed.
	return 0, fmt.Errorf("%w: job_id=%s", deliverycontract.ErrNoExplicitPlan, jobID)
}
