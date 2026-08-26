package contract

import (
	"strings"
	"testing"
)

func TestParseSceneMapsJSONNormalizesOneCanonicalShape(t *testing.T) {
	scenes, err := ParseSceneMapsJSON([]byte(`[{"description":"hello","image_url":"img.png"}]`))
	if err != nil {
		t.Fatalf("ParseSceneMapsJSON: %v", err)
	}
	if len(scenes) != 1 {
		t.Fatalf("got %d scenes, want 1", len(scenes))
	}
	if scenes[0]["text"] != "hello" || scenes[0]["image_link"] != "img.png" {
		t.Fatalf("normalized scene = %#v", scenes[0])
	}
	if got, ok := scenes[0]["duration_seconds"].(float64); !ok || got != 5 {
		t.Fatalf("duration_seconds = %#v, want 5", scenes[0]["duration_seconds"])
	}
}

func TestParseSceneMapsRejectsNonObjectEntriesAndMalformedJSON(t *testing.T) {
	if _, err := ParseSceneMaps([]interface{}{map[string]interface{}{"text": "ok"}, "bad"}); err == nil {
		t.Fatal("accepted non-object scene entry")
	}
	if _, err := ParseSceneMapsJSON([]byte(`[{"text":"ok"}] trailing`)); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}

func TestParsePayloadObjectJSONRejectsNonObjectAndTrailingValue(t *testing.T) {
	if _, err := ParsePayloadObjectJSON([]byte(`{"job_id":"one"} {}`)); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing payload error = %v", err)
	}
	if _, err := ParsePayloadObjectJSON([]byte(`[]`)); err == nil {
		t.Fatal("accepted array as payload object")
	}
}
