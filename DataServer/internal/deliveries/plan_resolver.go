// Package deliveries / plan_resolver.go — PR delivery plan resolver.
//
// SQLiteDeliveryPlanResolver implements artifacts.DeliveryPlanResolver by
// querying job_delivery_plans first. In production, it REQUIRES an explicit
// plan — no global delivery_destinations fallback is performed. A
// configurable dev-mode flag (GlobalFallback) restores the legacy fallback
// for development environments.
//
// Phase 5.1-5.5: per-plan retry_budget. Each row in job_delivery_plans
// carries an integer retry_budget; the resolver exposes it via
// PlanContext so the DeliveryRunner can override the runner-wide
// MaxAttempts on a per-delivery basis. The plan is acquired at the
// Job level (FinalizeVerified → DeliveryPlanResolver) and stamped
// onto job_deliveries rows at INSERT time, making retry decisions
// durable across restarts.
package deliveries

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrNoExplicitPlan is returned when no per-job delivery plan exists and
// the global fallback to all enabled delivery_destinations is disabled.
// The caller (typically FinalizeVerified) should surface this as a
// WAITING_FOR_PLAN status so operators can create the missing plan rows.
var ErrNoExplicitPlan = errors.New("deliveries: no explicit delivery plan and global fallback is disabled")

// PlanContext is the per-destination slice of the resolver's result.
// One row per (destination_id, retry_budget, priority). The runner
// reads MaxAttempts = retry_budget at INSERT time and stamps it on
// job_deliveries so the durable attempt counter does NOT regress on
// restart.
//
// Priority orders parallel workers (lower = first). The runner
// preserves the order in the SQL response.
//
// AcquiredAt is set to time.Now().UTC() at Resolve time. The
// DeliveryRunner uses it to mark the "since when this plan has been
// in effect" boundary for any audit log / re-resolve decision.
type PlanContext struct {
	DestinationID string
	Priority      int
	RetryBudget   int           // max attempts before FAILED
	Backoff       []time.Duration // optional per-plan override; nil → runner default
	AcquiredAt    time.Time
}

// Plan is the full resolver response: per-destination contexts and
// the job_id the plan was bound to. Used by FinalizeVerified to pass
// the plan to DeliveryRunner at job_deliveries INSERT time.
type Plan struct {
	JobID       string
	Destinations []PlanContext
	ResolvedAt  time.Time
}

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
	plan, err := r.ResolvePlan(ctx, jobID, artifactID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(plan.Destinations))
	for _, d := range plan.Destinations {
		ids = append(ids, d.DestinationID)
	}
	return ids, nil
}

// ResolvePlan returns the full plan (per-destination retry_budget + priority)
// for a job. Use this instead of ResolveDestinations when the caller
// (FinalizeVerified, DeliveryRunner) needs the retry_budget.
//
// Phase 5.1-5.5: the plan is acquired at the JOB level (not per-
// artifact) and INSERTed into job_deliveries in step 9 of the
// finalization tx (coordinator.CommitAttempt). Each job_deliveries
// row carries the retry_budget it was stamped with, so the runner
// reads it back at claim time and overrides cfg.MaxAttempts
// per-delivery.
//
// Idempotency: the per-plan rows are not stateful (a re-resolve
// returns the same priority/retry_budget). The plan itself is a
// configuration, not an attempt tracker; the attempt counter lives
// on job_deliveries.attempt_count.
func (r *SQLiteDeliveryPlanResolver) ResolvePlan(ctx context.Context, jobID, artifactID string) (*Plan, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	_ = artifactID // available for future per-artifact routing

	// Step 1: check for per-job plans.
	rows, err := r.db.QueryContext(ctx,
		`SELECT destination_id, priority, retry_budget FROM job_delivery_plans
		 WHERE job_id = ? AND enabled = 1
		 ORDER BY priority ASC, destination_id ASC`, jobID)
	if err != nil {
		return nil, fmt.Errorf("deliveries: ResolvePlan plans query: %w", err)
	}
	defer rows.Close()

	plan := &Plan{JobID: jobID, ResolvedAt: time.Now().UTC()}
	for rows.Next() {
		var (
			destID    string
			priority  int
			retryBud  int
		)
		if err := rows.Scan(&destID, &priority, &retryBud); err != nil {
			return nil, fmt.Errorf("deliveries: ResolvePlan plans scan: %w", err)
		}
		plan.Destinations = append(plan.Destinations, PlanContext{
			DestinationID: destID,
			Priority:      priority,
			RetryBudget:   retryBud,
			AcquiredAt:    plan.ResolvedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("deliveries: ResolvePlan plans rows: %w", err)
	}
	if len(plan.Destinations) > 0 {
		return plan, nil
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
		return nil, fmt.Errorf("deliveries: ResolvePlan fallback query: %w", err)
	}
	defer fallback.Close()

	for fallback.Next() {
		var id string
		if err := fallback.Scan(&id); err != nil {
			return nil, fmt.Errorf("deliveries: ResolvePlan fallback scan: %w", err)
		}
		// Default retry_budget = 5 (matches DefaultRunnerConfig.MaxAttempts)
		// when no explicit plan is in effect (dev mode).
		plan.Destinations = append(plan.Destinations, PlanContext{
			DestinationID: id,
			Priority:      100,
			RetryBudget:   5,
			AcquiredAt:    plan.ResolvedAt,
		})
	}
	return plan, fallback.Err()
}
