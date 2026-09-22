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
// (explicit override or enabled per-job job_delivery_plans) so the gate
// and the materialized delivery set agree without selecting global destinations.
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
// in the artifactsstore leaf (e.g. artifactsstore.SQLiteJobDeliveryCounter).
// Future Postgres support can wire a parallel adapter without touching Service.
type JobDeliveryCounter interface {
	// CountExpectedDeliveries returns the expected audio stream count for
	// the finalized artifact, accounting for whether the finalization tx
	// has a delivery target:
	//
	//   - overrideDestID != ""  → 1 (the explicit delivery path).
	//   - job_delivery_plans WHERE job_id = ? AND enabled = 1 has
	//     ≥1 row → 1. One rendered MP4 has one final audio stream even
	//     when it is delivered to many destinations.
	//   - Otherwise, no explicit plan exists and finalization must fail
	//     closed; global delivery_destinations are never selected.
	//
	//   - render_only=true with no plan → 0.
	//
	// Mirror policy: this method intentionally mirrors the
	// SQLiteFinalizeWriter's resolution order so the gate knows whether
	// the artifact is a render-only output or a published output. It does
	// not confuse destination fan-out with media stream multiplicity.
	// If a future refactor changes writer resolution, update this
	// method too — divergence will surface as a true
	// ErrFFProbeAudioCountMismatch in CI.
	CountExpectedDeliveries(ctx context.Context, jobID, overrideDestID string) (int, error)
}
