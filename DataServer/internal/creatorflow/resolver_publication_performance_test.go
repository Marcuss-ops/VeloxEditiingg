package creatorflow

import (
	"encoding/json"
	"reflect"
	"testing"

	"velox-shared/publication"
)

func benchmarkPublicationSpecs() []publication.Spec {
	budget := 3
	madeForKids := true
	return []publication.Spec{{
		Version:         1,
		PublicationID:   "pub-performance",
		OutputRef:       publication.OutputRef{ArtifactRole: "final_video"},
		Language:        "en",
		DefaultLanguage: "en",
		Metadata: publication.Metadata{
			Title:       "Published title",
			Description: "Published description",
			Tags:        []string{"one", "two"},
			Privacy:     "private",
			MadeForKids: &madeForKids,
		},
		Localizations: map[string]publication.Localization{
			"en": {Title: "Published title", Description: "Published description"},
		},
		Destinations: []publication.Destination{{
			DestinationID:    "youtube-en",
			Priority:         1,
			RetryBudget:      &budget,
			MetadataOverride: &publication.Metadata{Title: "Destination title"},
			ProviderOptions: map[string]interface{}{
				"nested": map[string]interface{}{"enabled": true},
				"items":  []interface{}{map[string]interface{}{"name": "destination-item"}},
			},
		}},
		ProviderOptions: map[string]interface{}{
			"channel": "youtube",
			"nested":  map[string]interface{}{"attempt": 1},
			"items":   []interface{}{map[string]interface{}{"name": "publication-item"}},
		},
	}}
}

func jsonPublicationProjection(specs []publication.Spec) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(specs))
	for _, spec := range specs {
		encoded, err := json.Marshal(spec)
		if err != nil {
			continue
		}
		var value map[string]interface{}
		if err := json.Unmarshal(encoded, &value); err == nil {
			out = append(out, value)
		}
	}
	return out
}

func canonicalJSONValue(t *testing.T, value interface{}) interface{} {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal canonical value: %v", err)
	}
	var decoded interface{}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal canonical value: %v", err)
	}
	return decoded
}

func TestPublicationSpecToMapMatchesJSONProjection(t *testing.T) {
	specs := benchmarkPublicationSpecs()
	got := clonePublicationSpecs(specs)
	want := jsonPublicationProjection(specs)

	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal manual publication projection: %v", err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal JSON publication projection: %v", err)
	}
	if !reflect.DeepEqual(gotJSON, wantJSON) {
		t.Fatalf("manual publication projection differs from JSON bytes:\n got=%s\nwant=%s", gotJSON, wantJSON)
	}
	if !reflect.DeepEqual(canonicalJSONValue(t, got), canonicalJSONValue(t, want)) {
		t.Fatalf("manual publication projection differs from JSON contract:\n got=%v\nwant=%v", got, want)
	}
}

func TestPublicationSpecToMapPreservesNilShapes(t *testing.T) {
	got := clonePublicationSpecs([]publication.Spec{{
		PublicationID: "pub-nil-shapes",
		Destinations:  nil,
		Localizations: nil,
	}})
	if _, present := got[0]["localizations"]; present {
		t.Fatal("nil localizations should respect omitempty")
	}
	if destinations, present := got[0]["destinations"]; !present || destinations != nil {
		t.Fatalf("nil destinations should remain explicit null, got present=%v value=%#v", present, destinations)
	}
}

func TestClonePublicationValuePreservesNilMapSliceElements(t *testing.T) {
	input := []map[string]interface{}{nil, {"name": "item"}}
	cloned := clonePublicationValue(input).([]map[string]interface{})
	if cloned[0] != nil {
		t.Fatalf("nil map element changed during clone: %#v", cloned[0])
	}
	cloned[1]["name"] = "changed"
	if input[1]["name"] != "item" {
		t.Fatal("map slice clone shares nested map storage")
	}
}

func TestPublicationSpecToMapDeepCopiesNestedValues(t *testing.T) {
	specs := benchmarkPublicationSpecs()
	got := clonePublicationSpecs(specs)
	got[0]["provider_options"].(map[string]interface{})["nested"].(map[string]interface{})["attempt"] = 99
	got[0]["provider_options"].(map[string]interface{})["items"].([]interface{})[0].(map[string]interface{})["name"] = "changed"

	got[0]["destinations"].([]interface{})[0].(map[string]interface{})["provider_options"].(map[string]interface{})["nested"].(map[string]interface{})["enabled"] = false
	got[0]["destinations"].([]interface{})[0].(map[string]interface{})["provider_options"].(map[string]interface{})["items"].([]interface{})[0].(map[string]interface{})["name"] = "changed-destination"

	if specs[0].ProviderOptions["nested"].(map[string]interface{})["attempt"] != 1 {
		t.Fatal("manual projection mutated top-level provider options")
	}
	if specs[0].ProviderOptions["items"].([]interface{})[0].(map[string]interface{})["name"] != "publication-item" {
		t.Fatal("manual projection mutated nested publication provider option list")
	}
	if specs[0].Destinations[0].ProviderOptions["nested"].(map[string]interface{})["enabled"] != true {
		t.Fatal("manual projection mutated destination provider options")
	}
	if specs[0].Destinations[0].ProviderOptions["items"].([]interface{})[0].(map[string]interface{})["name"] != "destination-item" {
		t.Fatal("manual projection mutated destination provider option list")
	}
}

func TestPublicationSpecToMapPreservesJSONEdgeSemantics(t *testing.T) {
	zero := 0
	falseValue := false
	spec := publication.Spec{
		PublicationID: "pub-edge",
		OutputRef:     publication.OutputRef{ArtifactRole: "final_video"},
		Metadata: publication.Metadata{
			Tags:        []string{},
			MadeForKids: &falseValue,
		},
		Destinations: []publication.Destination{{
			DestinationID:   "drive",
			RetryBudget:     &zero,
			ProviderOptions: map[string]interface{}{},
		}},
		ProviderOptions: map[string]interface{}{},
	}

	got := clonePublicationSpecs([]publication.Spec{spec})
	want := jsonPublicationProjection([]publication.Spec{spec})
	if !reflect.DeepEqual(canonicalJSONValue(t, got), canonicalJSONValue(t, want)) {
		t.Fatalf("edge projection differs from JSON contract:\n got=%v\nwant=%v", got, want)
	}
	metadata := got[0]["metadata"].(map[string]interface{})
	if _, present := metadata["tags"]; present {
		t.Fatal("empty tags should respect omitempty")
	}
	if metadata["made_for_kids"] != false {
		t.Fatalf("false pointer value was lost: %#v", metadata["made_for_kids"])
	}
	destination := got[0]["destinations"].([]interface{})[0].(map[string]interface{})
	if destination["retry_budget"] != 0 {
		t.Fatalf("explicit zero retry budget was lost: %#v", destination["retry_budget"])
	}
	if _, present := destination["provider_options"]; present {
		t.Fatal("empty destination provider options should respect omitempty")
	}
	if _, present := got[0]["provider_options"]; present {
		t.Fatal("empty publication provider options should respect omitempty")
	}
}

var publicationProjectionBenchmarkSink interface{}

func BenchmarkClonePublicationSpecsJSONRoundTrip(b *testing.B) {
	specs := benchmarkPublicationSpecs()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		publicationProjectionBenchmarkSink = jsonPublicationProjection(specs)
	}
}

func BenchmarkClonePublicationSpecsManual(b *testing.B) {
	specs := benchmarkPublicationSpecs()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		publicationProjectionBenchmarkSink = clonePublicationSpecs(specs)
	}
}
