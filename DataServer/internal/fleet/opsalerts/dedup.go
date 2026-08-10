package opsalerts

import (
	"sync"
	"time"

	runtimealerts "velox-server/internal/alerts"
)

type DedupKey struct {
	WorkerID string
	RuleID   RuleID
	Severity Severity
}

type DedupState struct {
	FirstFiredAt time.Time
	LastSeenAt   time.Time
	CurrentValue string
	Message      string
}

type occurrence struct {
	id    string
	event runtimealerts.AlertEvent
}

type DedupStore struct {
	mu            sync.Mutex
	entries       map[DedupKey]DedupState
	pending       map[DedupKey]struct{}
	occurrences   map[DedupKey]occurrence
	warningWindow time.Duration
}

func NewDedupStore() *DedupStore {
	return &DedupStore{
		entries:       make(map[DedupKey]DedupState, 16),
		pending:       make(map[DedupKey]struct{}, 16),
		occurrences:   make(map[DedupKey]occurrence, 16),
		warningWindow: 5 * time.Minute,
	}
}

func (d *DedupStore) ShouldFire(key DedupKey, severity Severity, now time.Time) bool {
	if severity == Info || severity == Critical {
		return severity == Critical
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, ok := d.entries[key]
	return !ok || now.Sub(entry.LastSeenAt) >= d.warningWindow
}

type dedupClaim struct {
	dedup *DedupStore
	key   DedupKey
	hit   AlertEventHit
	once  sync.Once
}

func (c *dedupClaim) Commit() {
	c.once.Do(func() {
		c.dedup.Observe(c.key, c.hit)
		c.dedup.mu.Lock()
		delete(c.dedup.pending, c.key)
		c.dedup.mu.Unlock()
	})
}

func (c *dedupClaim) Release() {
	c.once.Do(func() {
		c.dedup.mu.Lock()
		delete(c.dedup.pending, c.key)
		c.dedup.mu.Unlock()
	})
}

type DedupClaim interface {
	Commit()
	Release()
}

func (d *DedupStore) Prepare(event runtimealerts.AlertEvent, now time.Time) runtimealerts.AlertEvent {
	key := DedupKey{WorkerID: event.Subject, RuleID: RuleID(event.RuleID), Severity: Severity(event.Severity)}
	d.mu.Lock()
	defer d.mu.Unlock()
	if saved, ok := d.occurrences[key]; ok {
		return saved.event
	}
	return event
}

func (d *DedupStore) EventID(event runtimealerts.AlertEvent, now time.Time) string {
	key := DedupKey{WorkerID: event.Subject, RuleID: RuleID(event.RuleID), Severity: Severity(event.Severity)}
	d.mu.Lock()
	defer d.mu.Unlock()
	if saved, ok := d.occurrences[key]; ok {
		if _, committed := d.entries[key]; !committed || key.Severity != Critical && saved.event.FiredAt.Add(d.warningWindow).After(now) {
			return saved.id
		}
	}
	prepared := event
	prepared.FiredAt = now
	id := runtimealerts.EventIDFor(prepared)
	d.occurrences[key] = occurrence{id: id, event: prepared}
	return id
}

func (d *DedupStore) Claim(key DedupKey, severity Severity, now time.Time, hit AlertEventHit) (DedupClaim, bool) {
	if severity == Info {
		return nil, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.pending[key]; exists {
		return nil, false
	}
	if severity != Critical {
		if entry, exists := d.entries[key]; exists && now.Sub(entry.LastSeenAt) < d.warningWindow {
			return nil, false
		}
	}
	d.pending[key] = struct{}{}
	return &dedupClaim{dedup: d, key: key, hit: hit}, true
}

func (d *DedupStore) Observe(key DedupKey, hit AlertEventHit) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries[key] = DedupState{FirstFiredAt: hit.FiredAt, LastSeenAt: hit.FiredAt, CurrentValue: hit.CurrentValueText, Message: hit.Message}
}

func (d *DedupStore) Touch(key DedupKey, observedAt time.Time, currentValue, message string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	state := d.entries[key]
	state.LastSeenAt = observedAt
	state.CurrentValue = currentValue
	state.Message = message
	d.entries[key] = state
}

func (d *DedupStore) Forget(key DedupKey) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.entries, key)
	delete(d.occurrences, key)
}

func (d *DedupStore) SnapshotForTest(key DedupKey) (DedupState, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	state, ok := d.entries[key]
	return state, ok
}

func (d *DedupStore) iterateWorker(workerID string) []DedupKey {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DedupKey, 0, 4)
	for key := range d.entries {
		if key.WorkerID == workerID {
			out = append(out, key)
		}
	}
	return out
}
