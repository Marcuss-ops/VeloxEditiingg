package metrics

import (
	"strings"
	"testing"

	"velox-server/internal/assets"
)

func TestNewAssetMediaMetadataFamilies_PinsNameLabelsAndIncrements(t *testing.T) {
	source := assets.NewMediaMetadataMetrics()
	families := NewAssetMediaMetadataFamilies(source)
	if len(families) != 1 {
		t.Fatalf("families = %d, want 1", len(families))
	}
	family := families[0]
	if family.Name != "velox_assets_media_metadata_probes_total" {
		t.Errorf("family name = %q", family.Name)
	}
	if len(family.labels) != 1 || family.labels[0] != "outcome" {
		t.Errorf("labels = %v, want [outcome]", family.labels)
	}

	source.Observe(assets.MetadataOutcomeVerified)
	source.Observe(assets.MetadataOutcomeProbeFailed)
	source.Observe(assets.MetadataOutcomeVerified)

	var buf strings.Builder
	if err := family.write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	text := buf.String()
	if !strings.Contains(text, `outcome="verified"} 2`) {
		t.Errorf("exposition missing verified=2:\n%s", text)
	}
	if !strings.Contains(text, `outcome="probe_failed"} 1`) {
		t.Errorf("exposition missing probe_failed=1:\n%s", text)
	}
}
