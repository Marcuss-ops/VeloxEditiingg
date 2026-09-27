package workercache

import "time"

// evictionDecision is the shared, side-effect-free policy result used by both
// the legacy cleanup pass and the snapshot-aware cleanup pass.
type evictionDecision string

const (
	evictionKeepLease       evictionDecision = "active_lease"
	evictionKeepReservation evictionDecision = "active_reservation"
	evictionKeepInFlight    evictionDecision = "download_in_flight"
	evictionKeepProtected   evictionDecision = "protected_snapshot"
	evictionKeepGrace       evictionDecision = "recent_use_grace"
	evictionEligible        evictionDecision = "not_protected_and_grace_expired"
)

// evaluateEviction applies the canonical protection order. It performs no I/O
// and does not mutate the entry, so callers retain control over auditing and
// statistics while all cleanup paths share exactly the same policy semantics.
func evaluateEviction(entry Entry, protected map[string]struct{}, recentUseGrace time.Duration, now time.Time) evictionDecision {
	if entry.ActiveLeaseCount > 0 {
		return evictionKeepLease
	}
	if entry.ActiveReservationCount > 0 {
		return evictionKeepReservation
	}
	if !entry.DownloadComplete && (recentUseGrace <= 0 || now.Sub(entry.LastUsedAt) < recentUseGrace) {
		// An incomplete blob is protected while its transfer is recent. Old
		// incomplete rows are abandoned downloads: after the safety window,
		// continue checking the future-job snapshot and idle grace so they do
		// not pin cache metadata and partial files forever.
		return evictionKeepInFlight
	}
	if _, keep := protected[string(entry.AssetKey)]; keep {
		return evictionKeepProtected
	}
	if recentUseGrace > 0 && now.Sub(entry.LastUsedAt) < recentUseGrace {
		return evictionKeepGrace
	}
	return evictionEligible
}
