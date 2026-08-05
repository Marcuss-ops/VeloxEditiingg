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
