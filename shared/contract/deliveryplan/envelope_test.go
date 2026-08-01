package deliveryplan

import "testing"

func TestExtractEnvelopeCopiesOnlyRoutingAliases(t *testing.T) {
	payload := map[string]interface{}{
		"delivery_plan":            []interface{}{map[string]interface{}{"destination_id": "drive"}},
		"delivery_destination_ids": []string{"drive"},
		"delivery_destination_id":  "drive",
		"delivery_metadata":        map[string]interface{}{"legacy": true},
		"destinations":             []string{"drive"},
		"delivery_destinations":    []string{"drive"},
		"destination_ids":          []string{"drive"},
		"destination_id":           "drive",
		"publications":             []interface{}{"must stay outside"},
		"video_metadata":           map[string]interface{}{"title": "renderer input"},
	}

	got := ExtractEnvelope(payload)
	if got == nil {
		t.Fatal("ExtractEnvelope returned nil for routing fields")
	}
	if len(got) != 8 {
		t.Fatalf("ExtractEnvelope returned %d fields, want 8: %#v", len(got), got)
	}
	for _, key := range []string{"publications", "video_metadata"} {
		if _, present := got[key]; present {
			t.Fatalf("control-plane publication field %q leaked into delivery envelope", key)
		}
	}
	if got["delivery_plan"] == nil || got["destination_id"] != "drive" {
		t.Fatalf("routing values were not preserved: %#v", got)
	}
}

func TestExtractEnvelopeNilWhenNoRoutingFields(t *testing.T) {
	if got := ExtractEnvelope(nil); got != nil {
		t.Fatalf("ExtractEnvelope(nil) = %#v, want nil", got)
	}
	if got := ExtractEnvelope(map[string]interface{}{"video_name": "render"}); got != nil {
		t.Fatalf("ExtractEnvelope without routing = %#v, want nil", got)
	}
}

func TestExtractEnvelopeDoesNotMutateInput(t *testing.T) {
	payload := map[string]interface{}{
		"delivery_plan": []interface{}{"original"},
		"video_name":    "render",
	}
	got := ExtractEnvelope(payload)
	got["delivery_plan"] = []interface{}{"changed"}
	if payload["delivery_plan"].([]interface{})[0] != "original" {
		t.Fatalf("ExtractEnvelope changed the input map value")
	}
}

func TestStripEnvelopeRemovesRoutingButPreservesRendererAndPublicationBoundary(t *testing.T) {
	payload := map[string]interface{}{
		"delivery_plan":  []interface{}{map[string]interface{}{"destination_id": "drive"}},
		"destination_id": "drive",
		"video_name":     "render",
		"scenes":         []interface{}{map[string]interface{}{"text": "scene"}},
		"publications":   []interface{}{"control-plane"},
		"video_metadata": map[string]interface{}{"title": "publication metadata"},
	}

	got := StripEnvelope(payload)
	for _, key := range envelopeKeys {
		if _, present := got[key]; present {
			t.Fatalf("StripEnvelope retained routing key %q", key)
		}
	}
	for _, key := range []string{"video_name", "scenes", "publications", "video_metadata"} {
		if _, present := got[key]; !present {
			t.Fatalf("StripEnvelope removed non-routing key %q", key)
		}
	}
	if _, present := payload["delivery_plan"]; !present {
		t.Fatal("StripEnvelope mutated the input map")
	}
}

func TestStripEnvelopeNilInputIsEmptyNonNilMap(t *testing.T) {
	got := StripEnvelope(nil)
	if got == nil {
		t.Fatal("StripEnvelope(nil) returned nil, want empty map")
	}
	if len(got) != 0 {
		t.Fatalf("StripEnvelope(nil) = %#v, want empty map", got)
	}
}
