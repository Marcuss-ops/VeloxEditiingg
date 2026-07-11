// Package artifacts / job_delivery_counter.go
//
// Purpose-built typed reader for the pre-commit ffprobe invariant
// (RW-PROD-008 A4). Service.Finalize shells out to ffprobe BEFORE
// the verified-finalization tx commits and asserts the artifact's
// audio stream count matches the count of delivery destinations the
// finalize tx WOULD stamp at Step 5 (per
// SQLiteFinalizeWriter::resolveDeliveryDestinationsTx + the
// delivery_plan resolver in deliveries/plan_resolver.go). Pre-commit
// means a count mismatch aborts the finalize tx cleanly — the
// artifact row stays in STAGING, jobs.status stays RUNNING, and
// deliveries remain undelivered.
//
// Why a separate reader (vs wiring the resolver through Service):
// CountExpectedDeliveries mirrors the writer's resolution order
// (job_delivery_plans WHERE enabled=1, else fall back to all-enabled
// delivery_destinations) so the gate and the INSERT agree on
// exactly the same count without sharing a resolver instance.
// Mirroring is small and read-only; if it ever drifts from the
// writer, the gate will fire ErrFFProbeAudioCountMismatch and fail
// CI loudly.
//
// Surface kept narrow on purpose: one count query, scoped to
// (jobID, override). Adding more would grow this interface beyond
// its scope; future concerns should land on a sibling reader.
package artifacts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// JobDeliveryCounter is the typed read-only surface Service uses to
// derive the expected audio-stream count for the pre-commit gate.
//
// Implemented in the artifacts package on top of *sql.DB; future
// Postgres support can wire a parallel implementation without
// touching Service.
type JobDeliveryCounter interface {
	// CountExpectedDeliveries returns the number of delivery
	// destinations the finalize tx WILL stamp at Step 5 for this
	// job, accounting for both the per-job plan path and the
	// override path:
	//
	//   - overrideDestID != ""  → 1 (the single-destination
	//     explicit path mirrored from
	//     SQLiteFinalizeWriter::resolveDeliveryDestinationsTx
	//     branch 1).
	//   - job_delivery_plans WHERE job_id = ? AND enabled = 1 has
	//     ≥1 row → that count (production plan path).
	//   - Otherwise, count of delivery_destinations WHERE enabled = 1
	//     (legacy fallback path — only relevant in tests or dev
	//     mode; production GlobalFallback is false so the writer
	//     will reach the writer's ErrNoExplicitPlan branch).
	//
	// Mirror policy: this method intentionally mirrors the
	// SQLiteFinalizeWriter's resolution order so the gate's
	// expected count is exactly the count the writer would stamp.
	// If a future refactor changes writer resolution, update this
	// method too — divergence will surface as a true
	// ErrFFProbeAudioCountMismatch in CI.
	CountExpectedDeliveries(ctx context.Context, jobID, overrideDestID string) (int, error)
}

// SQLiteJobDeliveryCounter is the production implementation on top
// of *sql.DB. All methods tolerate sql.ErrNoRows by returning 0
// (the caller treats 0 as "no rows", which differs semantically
// from a real error).
type SQLiteJobDeliveryCounter struct {
	db *sql.DB
}

// NewSQLiteJobDeliveryCounter constructs the typed reader. db must
// outlive the reader.
func NewSQLiteJobDeliveryCounter(db *sql.DB) *SQLiteJobDeliveryCounter {
	if db == nil {
		panic("artifacts: NewSQLiteJobDeliveryCounter requires a non-nil *sql.DB")
	}
	return &SQLiteJobDeliveryCounter{db: db}
}

// CountExpectedDeliveries returns the destinations-count the
// finalize tx would stamp for the given job and override. See the
// interface docstring above for the resolution-order mirror
// rationale.
func (c *SQLiteJobDeliveryCounter) CountExpectedDeliveries(ctx context.Context, jobID, overrideDestID string) (int, error) {
	if overrideDestID != "" {
		// Single-destination explicit path — the writer hard-codes
		// 1 here regardless of any per-job plan; the gate mirrors
		// that exactly. Override identity is opaque to this method;
		// the writer does not validate it.
		return 1, nil
	}
	if jobID == "" {
		return 0, fmt.Errorf("artifacts: JobDeliveryCounter.CountExpectedDeliveries: empty jobID (overrideDestID was also empty)")
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
			// Defensive: COUNT(*) never returns ErrNoRows; treat
			// any such driver quirk as zero-plan (fall through to
			// fallback below so we don't silently miscount).
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
	if err := c.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM delivery_destinations WHERE enabled = 1`,
	).Scan(&n); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("artifacts: JobDeliveryCounter.CountExpectedDeliveries fallback count: %w", err)
	}
	return n, nil
}
