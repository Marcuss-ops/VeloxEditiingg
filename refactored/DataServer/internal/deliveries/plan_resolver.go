// Package deliveries / plan_resolver.go — PR delivery plan resolver.
//
// SQLiteDeliveryPlanResolver implements artifacts.DeliveryPlanResolver by
// querying job_delivery_plans first and falling back to all enabled
// delivery_destinations when no per-job plan exists.
//
// This is the bridge between the global delivery_destinations table and
// the per-job delivery plan model required by the ideal architecture.
package deliveries

import (
	"context"
	"database/sql"
	"fmt"
)

// SQLiteDeliveryPlanResolver implements artifacts.DeliveryPlanResolver
// against a *sql.DB. It queries job_delivery_plans first; if no per-job
// plans exist, it falls back to all enabled delivery_destinations.
type SQLiteDeliveryPlanResolver struct {
	db *sql.DB
}

// NewSQLiteDeliveryPlanResolver creates a resolver backed by *sql.DB.
func NewSQLiteDeliveryPlanResolver(db *sql.DB) *SQLiteDeliveryPlanResolver {
	return &SQLiteDeliveryPlanResolver{db: db}
}

// ResolveDestinations returns the destination IDs that should receive the
// artifact for the given job. Resolution order:
//
//  1. Look for per-job plans in job_delivery_plans WHERE enabled = 1.
//     Returns these only (exact match for the ideal "piano esplicito").
//  2. If no per-job plans exist, fall back to all enabled
//     delivery_destinations (backward compatible).
func (r *SQLiteDeliveryPlanResolver) ResolveDestinations(ctx context.Context, jobID, artifactID string) ([]string, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	_ = artifactID // available for future per-artifact routing

	// Step 1: check for per-job plans.
	rows, err := r.db.QueryContext(ctx,
		`SELECT destination_id FROM job_delivery_plans
		 WHERE job_id = ? AND enabled = 1
		 ORDER BY priority ASC, destination_id ASC`, jobID)
	if err != nil {
		return nil, fmt.Errorf("deliveries: ResolveDestinations plans query: %w", err)
	}
	defer rows.Close()

	var destIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("deliveries: ResolveDestinations plans scan: %w", err)
		}
		destIDs = append(destIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("deliveries: ResolveDestinations plans rows: %w", err)
	}
	if len(destIDs) > 0 {
		return destIDs, nil
	}

	// Step 2: fallback — all enabled global destinations.
	fallback, err := r.db.QueryContext(ctx,
		`SELECT destination_id FROM delivery_destinations WHERE enabled = 1
		 ORDER BY destination_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("deliveries: ResolveDestinations fallback query: %w", err)
	}
	defer fallback.Close()

	var fallbackIDs []string
	for fallback.Next() {
		var id string
		if err := fallback.Scan(&id); err != nil {
			return nil, fmt.Errorf("deliveries: ResolveDestinations fallback scan: %w", err)
		}
		fallbackIDs = append(fallbackIDs, id)
	}
	return fallbackIDs, fallback.Err()
}
