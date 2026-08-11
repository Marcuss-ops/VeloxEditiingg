// Package assets / media_metadata_metrics.go
//
// Bounded counter surface for the canonical asset-metadata probe pipeline.
// Operators can see how many media assets registered WITHOUT verified
// metadata (probe_failed / persist_failed) instead of grepping logs — the
// data that drives the Fase C2 fail-closed decision (reject vs one-time
// canonical probe).
package assets

import "sync"

// MediaMetadataOutcome is a closed enum of probe-pipeline outcomes.
type MediaMetadataOutcome string

// Closed outcome set for asset media-metadata probing. Non-media assets
// (fonts, subtitles, project files) are expected to have no metadata row
// and are intentionally NOT counted.
const (
	MetadataOutcomeVerified      MediaMetadataOutcome = "verified"
	MetadataOutcomeProbeFailed   MediaMetadataOutcome = "probe_failed"
	MetadataOutcomePersistFailed MediaMetadataOutcome = "persist_failed"
)

// MediaMetadataObserver mirrors bounded counters into an outer metrics
// adapter without coupling the assets package to the registry package.
type MediaMetadataObserver func(outcome MediaMetadataOutcome)

// MediaMetadataMetrics is an in-process, bounded-cardinality counter
// surface. The master can expose this snapshot without ever using an
// asset ID, URL or hash as a label.
type MediaMetadataMetrics struct {
	mu        sync.Mutex
	counts    map[MediaMetadataOutcome]uint64
	observers []MediaMetadataObserver
}

// NewMediaMetadataMetrics returns an empty counter surface.
func NewMediaMetadataMetrics() *MediaMetadataMetrics {
	return &MediaMetadataMetrics{counts: make(map[MediaMetadataOutcome]uint64)}
}

// AddObserver registers an outer mirroring adapter.
func (m *MediaMetadataMetrics) AddObserver(observer MediaMetadataObserver) {
	if m == nil || observer == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observers = append(m.observers, observer)
}

// Observe records one probe-pipeline outcome. Outcome MUST come from the
// closed MediaMetadataOutcome constants.
func (m *MediaMetadataMetrics) Observe(outcome MediaMetadataOutcome) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = make(map[MediaMetadataOutcome]uint64)
	}
	m.counts[outcome]++
	for _, observer := range m.observers {
		observer(outcome)
	}
}

// Snapshot returns a copy of the outcome counts.
func (m *MediaMetadataMetrics) Snapshot() map[MediaMetadataOutcome]uint64 {
	if m == nil {
		return map[MediaMetadataOutcome]uint64{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[MediaMetadataOutcome]uint64, len(m.counts))
	for outcome, count := range m.counts {
		out[outcome] = count
	}
	return out
}
