package assets

import "sync"

// KeyframeIndexSnapshot is the bounded hit/miss surface for the persistent
// per-source probe index. It intentionally carries no source identity.
type KeyframeIndexSnapshot struct {
	Hits   uint64
	Misses uint64
}

// KeyframeIndexObserver mirrors one index outcome into an outer metrics
// adapter without coupling the assets package to the metrics registry.
type KeyframeIndexObserver func(hit bool)

// KeyframeIndexMetrics is an in-process, bounded-cardinality counter surface.
// A hit means a complete, size-matching persisted probe was reused; a miss
// means a persisted record was absent, unreadable, stale or size-mismatched,
// so the caller had to run ffprobe. Calls rejected before a lookup (disabled
// index, invalid SHA/size) are not counted.
type KeyframeIndexMetrics struct {
	mu        sync.Mutex
	hits      uint64
	misses    uint64
	observers []KeyframeIndexObserver
}

// NewKeyframeIndexMetrics returns an empty counter surface.
func NewKeyframeIndexMetrics() *KeyframeIndexMetrics {
	return &KeyframeIndexMetrics{}
}

// AddObserver registers an outer mirroring adapter.
func (m *KeyframeIndexMetrics) AddObserver(observer KeyframeIndexObserver) {
	if m == nil || observer == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observers = append(m.observers, observer)
}

// Observe records one lookup outcome. Observers are invoked after the
// counter lock is released so a metrics adapter cannot extend the critical
// section or deadlock against a second lookup.
func (m *KeyframeIndexMetrics) Observe(hit bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if hit {
		m.hits++
	} else {
		m.misses++
	}
	observers := append([]KeyframeIndexObserver(nil), m.observers...)
	m.mu.Unlock()
	for _, observer := range observers {
		observer(hit)
	}
}

// Snapshot returns a copy of the cumulative counters.
func (m *KeyframeIndexMetrics) Snapshot() KeyframeIndexSnapshot {
	if m == nil {
		return KeyframeIndexSnapshot{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return KeyframeIndexSnapshot{Hits: m.hits, Misses: m.misses}
}
