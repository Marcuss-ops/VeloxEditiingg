// Package deliveries / plan_resolver.go — PR delivery plan resolver.
//
// SQLiteDeliveryPlanResolver implements artifacts.DeliveryPlanResolver by
// querying job_delivery_plans first. In production, it REQUIRES an explicit
// plan — no global delivery_destinations fallback is performed. A
// configurable dev-mode flag (GlobalFallback) restores the legacy fallback
// for development environments.
package deliveries

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNoExplicitPlan is returned when no per-job delivery plan exists and
// the global fallback to all enabled delivery_destinations is disabled.
// The caller (typically FinalizeVerified) should surface this as a
// WAITING_FOR_PLAN status so operators can create the missing plan rows.
var ErrNoExplicitPlan = errors.New("deliveries: no explicit delivery plan and global fallback is disabled")

// SQLiteDeliveryPlanResolver implements artifacts.DeliveryPlanResolver
// against a *sql.DB. It queries job_delivery_plans first; only falls back
// to all enabled delivery_destinations when GlobalFallback is true.
type SQLiteDeliveryPlanResolver struct {
	db             *sql.DB
	GlobalFallback bool // true = dev mode: fall back to all delivery_destinations
}

// NewSQLiteDeliveryPlanResolver creates a resolver backed by *sql.DB.
// globalFallback enables the legacy global-destinations fallback (dev only).
func NewSQLiteDeliveryPlanResolver(db *sql.DB, globalFallback bool) *SQLiteDeliveryPlanResolver {
	return &SQLiteDeliveryPlanResolver{db: db, GlobalFallback: globalFallback}
}

// ResolveDestinations returns the destination IDs that should receive the
// artifact for the given job. Resolution order:
//
//  1. Look for per-job plans in job_delivery_plans WHERE enabled = 1.
//     Returns these only (exact match for the "piano esplicito").
//  2. If no per-job plans exist AND GlobalFallback is true, fall back to
//     all enabled delivery_destinations (legacy dev mode).
//  3. If no per-job plans exist AND GlobalFallback is false, return
//     ErrNoExplicitPlan (production mode — operator must create a plan).
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

	// Step 2: if GlobalFallback is disabled (production), require an explicit plan.
	if !r.GlobalFallback {
		return nil, fmt.Errorf("%w: job_id=%s (create a job_delivery_plans row for this job)",
			ErrNoExplicitPlan, jobID)
	}

	// Step 3: fallback — all enabled global destinations (dev mode only).
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
