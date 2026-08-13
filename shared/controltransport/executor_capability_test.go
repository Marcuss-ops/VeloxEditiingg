package controltransport

import (
	"encoding/json"
	"testing"
)

func TestExecutorRegistryFromLegacyPayload(t *testing.T) {
	raw := map[string]interface{}{
		"executors": []interface{}{
			map[string]interface{}{
				"id":             "scene.composite.v1",
				"version":        float64(2),
				"resource_class": "gpu",
				"temporal_mode":  "global",
				"deterministic":  true,
				"cacheable":      true,
				"output_types":   []interface{}{"video/mp4"},
			},
		},
	}
	registry, err := ExecutorRegistryFromLegacy(raw)
	if err != nil {
		t.Fatalf("decode legacy payload: %v", err)
	}
	all := registry.All()
	if len(all) != 1 || all[0].ID != "scene.composite.v1" || all[0].Version != 2 {
		t.Fatalf("registry = %+v, want scene.composite.v1@2", all)
	}
	if !registry.Has("scene.composite.v1", 2) {
		t.Fatal("typed registry lost executor identity")
	}
}

func TestExecutorRegistryJSONRoundTrip(t *testing.T) {
	registry, err := NewExecutorRegistry(
		ExecutorCapability{ID: "z.last", Version: 1},
		ExecutorCapability{ID: "a.first", Version: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(registry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded []map[string]interface{}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded) != 2 || decoded[0]["id"] != "a.first" || decoded[1]["id"] != "z.last" {
		t.Fatalf("encoded order = %s, want deterministic ID order", encoded)
	}
}

// TestExecutorRegistryHasBinarySearch pins the binary-search lookups against
// a multi-version registry so the (ID, Version)-sorted ordering invariant
// stays correct across both Has and HasID.
func TestExecutorRegistryHasBinarySearch(t *testing.T) {
	registry, err := NewExecutorRegistry(
		ExecutorCapability{ID: "scene.composite.v1", Version: 3},
		ExecutorCapability{ID: "scene.composite.v1", Version: 1},
		ExecutorCapability{ID: "audio.mix.v1", Version: 2},
		ExecutorCapability{ID: "scene.composite.v1", Version: 2},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		id      string
		version int
		want    bool
	}{
		{"scene.composite.v1", 1, true},
		{"scene.composite.v1", 2, true},
		{"scene.composite.v1", 3, true},
		{"scene.composite.v1", 4, false},
		{"scene.composite.v1", 0, false},
		{"audio.mix.v1", 2, true},
		{"audio.mix.v1", 1, false},
		{"missing.v1", 1, false},
		{"", 0, false},
	} {
		if got := registry.Has(tc.id, tc.version); got != tc.want {
			t.Errorf("Has(%q, %d) = %v, want %v", tc.id, tc.version, got, tc.want)
		}
	}

	for _, tc := range []struct {
		id   string
		want bool
	}{
		{"scene.composite.v1", true},
		{"audio.mix.v1", true},
		{"missing.v1", false},
		{"", false},
		{"scene", false},
	} {
		if got := registry.HasID(tc.id); got != tc.want {
			t.Errorf("HasID(%q) = %v, want %v", tc.id, got, tc.want)
		}
	}
}

func TestExecutorRegistryCopiesOutputTypes(t *testing.T) {
	registry, err := NewExecutorRegistry(ExecutorCapability{
		ID: "copy.test", Version: 1, OutputTypes: []string{"video/mp4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	all := registry.All()
	all[0].OutputTypes[0] = "mutated"
	again := registry.All()
	if again[0].OutputTypes[0] != "video/mp4" {
		t.Fatal("registry exposed mutable OutputTypes backing storage")
	}
}
