package metrics

import "velox-server/internal/assets"

// NewKeyframeIndexFamilies adapts the bounded persisted-keyframe-index
// counters to the master Prometheus registry without making the assets
// package depend on this package. Both families are label-free because a
// source SHA-256 or path would be high-cardinality.
func NewKeyframeIndexFamilies(source *assets.KeyframeIndexMetrics) []*Family {
	if source == nil {
		return nil
	}
	hits := NewCounterFamily(
		"velox_keyframe_index_hits_total",
		"Persisted per-source keyframe/probe index hits",
		nil,
	)
	misses := NewCounterFamily(
		"velox_keyframe_index_misses_total",
		"Persisted per-source keyframe/probe index misses",
		nil,
	)
	source.AddObserver(func(hit bool) {
		if hit {
			hits.Inc(nil, 1)
			return
		}
		misses.Inc(nil, 1)
	})
	return []*Family{hits, misses}
}
