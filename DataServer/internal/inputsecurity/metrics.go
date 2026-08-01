package inputsecurity

import (
	"sort"
	"sync"
)

// Metrics is an in-process, bounded-cardinality counter surface. The master
// can expose or periodically persist this snapshot without ever using a URL,
// path, job ID or hash as a label.
type Metrics struct {
	mu          sync.Mutex
	rejections  map[string]uint64
	bytes       map[string]uint64
	quarantines map[string]uint64
	observers   []Observer
}

// Observer is used by an outer metrics adapter to mirror bounded counters
// into its own exposition registry without coupling this security boundary to
// the registry package.
type Observer func(kind Kind, code ErrorCode, rejectedBytes uint64, quarantined bool)

type MetricsSnapshot struct {
	Rejections  map[string]uint64
	Bytes       map[string]uint64
	Quarantines map[string]uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		rejections:  make(map[string]uint64),
		bytes:       make(map[string]uint64),
		quarantines: make(map[string]uint64),
	}
}

func (m *Metrics) AddObserver(observer Observer) {
	if m == nil || observer == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observers = append(m.observers, observer)
}

func (m *Metrics) ObserveRejected(kind Kind, code ErrorCode, bytes uint64) {
	if m == nil {
		return
	}
	key := string(kind) + ":" + string(code)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rejections == nil {
		m.rejections = make(map[string]uint64)
		m.bytes = make(map[string]uint64)
		m.quarantines = make(map[string]uint64)
	}
	m.rejections[key]++
	m.bytes[key] += bytes
	for _, observer := range m.observers {
		observer(kind, code, bytes, false)
	}
}

func (m *Metrics) ObserveQuarantined(kind Kind, code ErrorCode) {
	if m == nil {
		return
	}
	key := string(kind) + ":" + string(code)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.quarantines == nil {
		m.quarantines = make(map[string]uint64)
	}
	m.quarantines[key]++
	for _, observer := range m.observers {
		observer(kind, code, 0, true)
	}
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{Rejections: map[string]uint64{}, Bytes: map[string]uint64{}, Quarantines: map[string]uint64{}}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	copyMap := func(source map[string]uint64) map[string]uint64 {
		out := make(map[string]uint64, len(source))
		for key, value := range source {
			out[key] = value
		}
		return out
	}
	return MetricsSnapshot{Rejections: copyMap(m.rejections), Bytes: copyMap(m.bytes), Quarantines: copyMap(m.quarantines)}
}

// MetricKeys returns sorted keys for deterministic diagnostics and tests.
func MetricKeys(values map[string]uint64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
