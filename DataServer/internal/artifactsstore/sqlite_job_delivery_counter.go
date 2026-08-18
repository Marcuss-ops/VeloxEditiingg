// Package artifactsstore is the SQLite persistence for the artifacts /
// verified-finalization pipeline. It was split out of the internal/store
// god-package: the consumer-owned ports (JobDeliveryCounter, FinalizationWriter,
// …) live in internal/artifacts, the orchestration lives in internal/artifacts,
// and this package owns only the SQLite SQL/CAS. It depends on leaves
// (deliverycontract, storecore) and never on internal/store.
package artifactsstore

import (
	"context"
	"database/sql"
	"encoding/json"
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
	// A render-only job is the one intentional no-delivery contract. The
	// finalizer accepts it explicitly and therefore the pre-commit probe must
	// compare against zero expected delivery streams rather than turning the
	// valid render-only path into a missing-plan error.
	var requestJSON string
	if err := c.db.QueryRowContext(ctx,
		`SELECT COALESCE(request_json, '{}') FROM jobs WHERE job_id = ?`, jobID).
		Scan(&requestJSON); err != nil {
		return 0, fmt.Errorf("store: JobDeliveryCounter.CountExpectedDeliveries job contract: %w", err)
	}
	var contract map[string]interface{}
	if err := json.Unmarshal([]byte(requestJSON), &contract); err != nil {
		return 0, fmt.Errorf("store: JobDeliveryCounter.CountExpectedDeliveries invalid request_json: %w", err)
	}
	if renderOnly, _ := contract["render_only"].(bool); renderOnly {
		return 0, nil
	}
	// No explicit plan exists for a normal job. Never count unrelated global
	// delivery_destinations: finalization must fail closed.
	return 0, fmt.Errorf("%w: job_id=%s", deliverycontract.ErrNoExplicitPlan, jobID)
}
