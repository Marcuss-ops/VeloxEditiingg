package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"velox-server/internal/deliverycontract"
)

// PostgresJobDeliveryCounter is the pgx-native Postgres implementation
// of the consumer-owned JobDeliveryCounter port on top of pgxpool.Pool.
//
// The interface itself lives in the consumer package (internal/artifacts)
// so the consumer owns the contract. This package does not import that
// consumer package to avoid an import cycle; instead, a local anonymous
// interface provides a compile-time assertion that the concrete type
// satisfies the contract's method shape (structural interface matching).
type PostgresJobDeliveryCounter struct {
	pool *pgxpool.Pool
}

// NewPostgresJobDeliveryCounter constructs the typed reader. pool must
// outlive the reader. Panics on nil pool so a production wiring never
// silently disables the gate via a nil pointer.
func NewPostgresJobDeliveryCounter(pool *pgxpool.Pool) *PostgresJobDeliveryCounter {
	if pool == nil {
		panic("store: NewPostgresJobDeliveryCounter requires a non-nil *pgxpool.Pool")
	}
	return &PostgresJobDeliveryCounter{pool: pool}
}

// Compile-time assertion using an anonymous interface. The consumer-owned
// port in internal/artifacts has the identical method signature, so
// PostgresJobDeliveryCounter satisfies it structurally without forcing an
// import cycle.
var _ interface {
	CountExpectedDeliveries(ctx context.Context, jobID, overrideDestID string) (int, error)
} = (*PostgresJobDeliveryCounter)(nil)

// CountExpectedDeliveries returns the destinations-count the finalize tx
// would stamp for the given job and override when the orchestrator is
// talking to a Postgres backend. Branch logic is identical to
// SQLiteJobDeliveryCounter.CountExpectedDeliveries; see the SQLite
// reader's godoc for the mirror-policy rationale.
func (c *PostgresJobDeliveryCounter) CountExpectedDeliveries(ctx context.Context, jobID, overrideDestID string) (int, error) {
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
	// slice length. Postgres uses $1 numbered positional placeholders
	// (pgx rejects the SQLite ? syntax).
	var n int
	if err := c.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM job_delivery_plans WHERE job_id = $1 AND enabled = 1`,
		jobID,
	).Scan(&n); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Defensive: COUNT(*) should never return ErrNoRows;
			// treat it as an absent explicit plan and fail closed below.
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
