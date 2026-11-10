package metrics

import (
	"strings"
	"testing"

	"velox-server/internal/assets"
)

func TestNewKeyframeIndexFamilies_PinsNamesAndIncrements(t *testing.T) {
	source := assets.NewKeyframeIndexMetrics()
	families := NewKeyframeIndexFamilies(source)
	if len(families) != 2 {
		t.Fatalf("families = %d, want 2", len(families))
	}
	if families[0].Name != "velox_keyframe_index_hits_total" {
		t.Errorf("hits family name = %q", families[0].Name)
	}
	if families[1].Name != "velox_keyframe_index_misses_total" {
		t.Errorf("misses family name = %q", families[1].Name)
	}
	for _, family := range families {
		if len(family.labels) != 0 {
			t.Errorf("%s labels = %v, want none", family.Name, family.labels)
		}
	}

	source.Observe(true)
	source.Observe(false)
	source.Observe(true)

	var text strings.Builder
	for _, family := range families {
		if err := family.write(&text); err != nil {
			t.Fatalf("write %s: %v", family.Name, err)
		}
	}
	got := text.String()
	if !strings.Contains(got, "velox_keyframe_index_hits_total 2") {
		t.Errorf("hits exposition missing count 2:\n%s", got)
	}
	if !strings.Contains(got, "velox_keyframe_index_misses_total 1") {
		t.Errorf("misses exposition missing count 1:\n%s", got)
	}
}
