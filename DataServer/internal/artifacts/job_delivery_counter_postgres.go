// Package artifacts / job_delivery_counter_postgres.go
//
// Postgres-native parallel of job_delivery_counter.go's
// SQLiteJobDeliveryCounter. Implements the JobDeliveryCounter
// interface on top of github.com/jackc/pgx/v5/pgxpool.Pool so the
// production DB swap (SQLite → Postgres) is mechanically a swap
// of the typed reader passed to artifacts.NewService — Service and
// the gate's logic are driver-agnostic.
//
// Type-only portability: this file is committed without touching
// bootstrap_assets.go (production still uses SQLite). Compile-
// time `go build ./...` + `go vet ./...` are the portability gate
// until a real Postgres target is wired in a follow-up.
//
// Mirror policy: this implementation mirrors the SQLite reader's
// 3-branch resolution order (overrideDestID → 1, else
// job_delivery_plans WHERE enabled=1, else fallback to
// delivery_destinations WHERE enabled=1). Divergence between
// SQLite and Postgres impls will surface in CI via the existing
// finalization integrity tests; if a refactor changes the
// semantics of any branch, update both files together (the SQLite
// reader's godoc explicitly documents this same mirror policy so
// the warning is visible at both impl sites).
package artifacts

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresJobDeliveryCounter is the pgx-native Postgres
// implementation of the JobDeliveryCounter interface. Mirrors
// SQLiteJobDeliveryCounter branch-for-branch; differs only in
// driver (pgxpool.Pool vs *sql.DB) and Postgres dialect (numbered
// $N placeholders, pgx.ErrNoRows sentinel).
type PostgresJobDeliveryCounter struct {
	pool *pgxpool.Pool
}

// NewPostgresJobDeliveryCounter constructs the Postgres typed
// reader. pool must outlive the reader. Panics on nil pool —
// mirrors the SQLite constructor's defensive stance so a future
// refactor that swaps in a Postgres wiring never silently
// disables the gate via a nil pointer.
func NewPostgresJobDeliveryCounter(pool *pgxpool.Pool) *PostgresJobDeliveryCounter {
	if pool == nil {
		panic("artifacts: NewPostgresJobDeliveryCounter requires a non-nil *pgxpool.Pool")
	}
	return &PostgresJobDeliveryCounter{pool: pool}
}

// CountExpectedDeliveries returns the destinations-count the
// finalize tx WOULD stamp for the given job and override when the
// orchestrator is talking to a Postgres backend. Branch logic is
// identical to SQLiteJobDeliveryCounter.CountExpectedDeliveries;
// see the SQLite reader's godoc for the rationale on the
// mirror-policy contract.
//
// Differences from the SQLite impl:
//   - pgxpool.Pool.QueryRow(ctx, sql, args...) replaces
//     *sql.DB.QueryRowContext(ctx, sql, args...) (pgx drops the
//     "Context" suffix; the *pool already implicitly carries
//     connection-level context).
//   - $1 placeholder replaces ? (Postgres numbered positional
//     syntax; pgx rejects ?).
//   - pgx.ErrNoRows replaces sql.ErrNoRows (distinct sentinels
//     because pgx returns its own; mapping both to (0, nil) keeps
//     the no-rows defensive normalize consistent).
func (c *PostgresJobDeliveryCounter) CountExpectedDeliveries(ctx context.Context, jobID, overrideDestID string) (int, error) {
	if overrideDestID != "" {
		// Single-destination explicit path — the writer hard-codes
		// 1 here regardless of any per-job plan; the gate (and
		// both typed readers) mirror that exactly. Override
		// identity is opaque to this method; the writer does not
		// validate it.
		return 1, nil
	}
	if jobID == "" {
		return 0, fmt.Errorf("artifacts: JobDeliveryCounter.CountExpectedDeliveries: empty jobID (overrideDestID was also empty)")
	}
	// Branch 1: per-job plan (production path). Mirror the ORDER BY
	// used by SQLiteDeliveryPlanResolver::ResolvePlan so the count
	// is identical to what the resolver would have returned as a
	// slice length. Postgres uses $1 numbered positional
	// placeholders (pgx rejects the SQLite ? syntax).
	var n int
	if err := c.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM job_delivery_plans WHERE job_id = $1 AND enabled = 1`,
		jobID,
	).Scan(&n); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Defensive: COUNT(*) should never return ErrNoRows,
			// but pgx drivers can surface it in pathological
			// cases; treat as zero-plan and fall through to the
			// fallback branch below so we don't silently
			// miscount.
			n = 0
		} else {
			return 0, fmt.Errorf("artifacts: JobDeliveryCounter.CountExpectedDeliveries plans count: %w", err)
		}
	}
	if n > 0 {
		return n, nil
	}
	// Branch 2: legacy fallback — all-enabled global destinations.
	// Mirrors the writer's `delivery_destinations WHERE enabled = 1`
	// SELECT in resolveDeliveryDestinationsTx branch 3.
	if err := c.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM delivery_destinations WHERE enabled = 1`,
	).Scan(&n); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("artifacts: JobDeliveryCounter.CountExpectedDeliveries fallback count: %w", err)
	}
	return n, nil
}
