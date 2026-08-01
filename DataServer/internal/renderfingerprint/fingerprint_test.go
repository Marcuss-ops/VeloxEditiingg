package renderfingerprint

import "testing"

func TestBuildCanonicalizesObjectKeysButPreservesAssetOrder(t *testing.T) {
	a, err := Build(Input{
		RenderPlan:       map[string]any{"b": 2, "a": 1},
		CanonicalPayload: map[string]any{"title": "x"},
		InputManifest:    map[string]any{"assets": []string{"a", "b"}},
		AssetHashes:      []string{"asset-a", "asset-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build(Input{
		RenderPlan:       map[string]any{"a": 1, "b": 2},
		CanonicalPayload: map[string]any{"title": "x"},
		InputManifest:    map[string]any{"assets": []string{"a", "b"}},
		AssetHashes:      []string{"asset-a", "asset-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.Value != b.Value {
		t.Fatalf("map key order changed fingerprint: %s != %s", a.Value, b.Value)
	}
	b.AssetHashes = []string{"asset-b", "asset-a"}
	c, err := Build(Input{
		RenderPlan: map[string]any{"a": 1, "b": 2}, CanonicalPayload: map[string]any{"title": "x"},
		InputManifest: map[string]any{"assets": []string{"a", "b"}}, AssetHashes: b.AssetHashes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.Value == c.Value {
		t.Fatal("asset order must affect fingerprint")
	}
}

func TestBuildRejectsUnsupportedJSON(t *testing.T) {
	if _, err := Build(Input{RenderPlan: func() {}, CanonicalPayload: nil, InputManifest: nil}); err == nil {
		t.Fatal("expected unsupported render plan value to fail")
	}
}
