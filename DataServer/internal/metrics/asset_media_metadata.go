package metrics

import "velox-server/internal/assets"

// NewAssetMediaMetadataFamilies adapts the bounded asset-metadata probe
// counters to the master Prometheus registry without making the assets
// package depend on this package. The outcome label is a closed enum
// (verified / probe_failed / persist_failed) so operators can see how many
// media assets registered without verified metadata — the data that drives
// the fail-closed decision in Fase C2.
func NewAssetMediaMetadataFamilies(source *assets.MediaMetadataMetrics) []*Family {
	if source == nil {
		return nil
	}
	family := NewCounterFamily(
		"velox_assets_media_metadata_probes_total",
		"Asset media-metadata probe pipeline outcomes (verified / probe_failed / persist_failed)",
		[]string{"outcome"},
	)
	source.AddObserver(func(outcome assets.MediaMetadataOutcome) {
		family.Inc([]string{string(outcome)}, 1)
	})
	return []*Family{family}
}
