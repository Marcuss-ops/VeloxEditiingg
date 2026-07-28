package opsalerts

import (
	"sync"
	"time"
)

// dedup.go — Step 16/15 in-memory dedup state machine for the
// 12-rule fleet-operator alert catalog.
//
// User spec semantics for "Sopprimere eventi normali":
//   - INFO  → never write an AlertEvent; only log.
//   - WARNING → write AlertEvent ONCE per (worker_id, rule_id,
//               severity) per 5-minute window. Re-fires only
//               after the window expires or the severity
//               escalates.
//   - CRITICAL → write AlertEvent ONCE per (worker_id, rule_id,
//               severity). No window. Auto-resolves on
//               next observation that no longer trips.
//
// The dedup state lives in-memory (process-local); a master
// restart resets the window. This is acceptable because:
//   (a) the alert_events table is the durable audit; the engine
//       re-derives the dedup state from the table on startup.
//   (b) the canonical panic-mode alert (CRITICAL) doesn't dedup
//       at all, so a restart IS the safe fallback.
//
// Escalation: WARNING fires today; tomorrow the value crosses
// the CRITICAL boundary; both rows exist with different
// (rule_id, severity) dedup keys. The 12-rule catalog encodes
// this for disk_pressure (85% WARNING / 95% CRITICAL) and
// cert_expiring (15d WARNING / 5d CRITICAL).
//
// Resolution: when the evaluator returns NO hitting event for
// a previously-seen (worker_id, rule_id, severity) triple, the
// engine calls Resolve() which stamps alert_events.resolved_at
// and removes the in-memory key.

// DedupKey is the (worker_id, rule_id, severity) tuple. Identical
// schema to the dedup state-tracker key the outbox and
// fleet_operations use for idempotent retries.
type DedupKey struct {
	WorkerID string
	RuleID   RuleID
	Severity Severity
}

// DedupState is the in-memory entry for one key.
type DedupState struct {
	FirstFiredAt time.Time
	LastSeenAt   time.Time
	CurrentValue string
	Message      string
}

// DedupStore is the in-memory dedup state machine. Thread-safe;
// multiple engine ticks may consult + mutate concurrently.
type DedupStore struct {
	mu      sync.Mutex
	entries map[DedupKey]DedupState

	// warningWindow is the 5-minute dedup window for WARNING.
	// CRITICAL is unbounded by default (matches the
	// "Sopprimere eventi normali" semantics: WARNING can be
	// noisy, CRITICAL must always trip).
	warningWindow time.Duration
}

// NewDedupStore builds a fresh dedup state machine.
func NewDedupStore() *DedupStore {
	return &DedupStore{
		entries:       make(map[DedupKey]DedupState, 16),
		warningWindow: 5 * time.Minute,
	}
}

// ShouldFire consults the dedup state for a (worker_id, rule_id,
// severity) hit. Returns:
//   - true  when the state is fresh (no prior entry OR the
//     window has elapsed) AND the caller should persist a new
//     AlertEvent row.
//   - false when the state is in-window (caller should
//     touch the existing AlertEvent row to bump
//     last_observed_at without creating a duplicate).
//
// CRITICAL: never consults the window (always trip).
// WARNING: consults the 5-minute window.
// INFO: should NEVER be passed; the engine drops INFO events
// at the call site.
func (d *DedupStore) ShouldFire(key DedupKey, severity Severity, now time.Time) bool {
	if severity == Info {
		return false
	}
	if severity == Critical {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, ok := d.entries[key]
	if !ok {
		return true
	}
	return now.Sub(entry.LastSeenAt) >= d.warningWindow
}

// Observe records a fresh firing. Caller invokes after
// ShouldFire returns true. Updates the in-memory key to the
// current observation timestamp + message.
func (d *DedupStore) Observe(key DedupKey, hit AlertEventHit) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries[key] = DedupState{
		FirstFiredAt: hit.FiredAt,
		LastSeenAt:   hit.FiredAt,
		CurrentValue: hit.CurrentValueText,
		Message:      hit.Message,
	}
}

// Touch bumps last_observed_at without re-firing. Caller
// invokes when ShouldFire returns false. The engine mirrors
// this against the alert_events table via TouchActiveAlertEvent.
func (d *DedupStore) Touch(key DedupKey, observedAt time.Time, currentValue, message string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries[key] = DedupState{
		FirstFiredAt: d.entries[key].FirstFiredAt,
		LastSeenAt:   observedAt,
		CurrentValue: currentValue,
		Message:      message,
	}
}

// Forget removes the dedup key. Caller invokes when the
// evaluator returns no event for a previously-seen triple —
// the engine calls Forget + Resolve concurrently with the
// audit-table transition.
func (d *DedupStore) Forget(key DedupKey) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.entries, key)
}

// SnapshotForTest export the dedup state for a key (test-only).
func (d *DedupStore) SnapshotForTest(key DedupKey) (DedupState, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.entries[key]
	return e, ok
}

// iterateWorker returns every dedup key belonging to the given
// workerID. Used by the engine's resolveWalk to detect when a
// previously-active (rule_id, severity) triple has stopped
// tripping. Copies keys under the lock to avoid holding it
// during store writes on the caller side.
//
// Performance: each per-tick call walks the full dedup map; with
// the documented fleet-size cap (50 workers × 12 rules ~ 600
// keys max) this is sub-millisecond cost.
func (d *DedupStore) iterateWorker(workerID string) []DedupKey {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DedupKey, 0, 4)
	for k := range d.entries {
		if k.WorkerID == workerID {
			out = append(out, k)
		}
	}
	return out
}
