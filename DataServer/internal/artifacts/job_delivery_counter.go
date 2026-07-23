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
)

// JobDeliveryCounter is the typed read-only surface Service uses to
// derive the expected audio-stream count for the pre-commit gate.
//
// The consumer-owned contract lives here; concrete SQL adapters live
// in the store package (e.g. store.SQLiteJobDeliveryCounter). Future
// Postgres support can wire a parallel store adapter without
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
