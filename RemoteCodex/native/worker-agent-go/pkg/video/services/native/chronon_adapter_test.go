package native

import (
	"encoding/json"
	"testing"

	"velox-worker-agent/pkg/video/plan"
)

func TestChrononPlanJSONConvertsTimeline(t *testing.T) {
	payload, err := chrononPlanJSON(&plan.RenderPlan{
		JobID:  "job-1",
		Canvas: plan.CanvasSpec{Width: 320, Height: 180, Fps: 30},
		Timeline: []plan.TimelineItem{
			{Source: plan.MediaSource{Type: "color", ColorHex: "#102040"}, DurationSeconds: 1},
			{Source: plan.MediaSource{Type: "image", URL: "still.png"}, DurationSeconds: 0.5},
		},
		OutputPath: "/work/out.mp4",
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	if got["schema"] != "chronon.render-plan" || got["version"] != float64(1) {
		t.Fatalf("unexpected schema header: %#v", got)
	}
	layers, ok := got["layers"].([]any)
	if !ok || len(layers) != 2 {
		t.Fatalf("unexpected layers: %#v", got["layers"])
	}
	if layers[1].(map[string]any)["start_frame"] != float64(30) {
		t.Fatalf("second layer was not placed after first duration: %#v", layers[1])
	}
}

func TestChrononPlanJSONKeepsEditorialLayersSeparate(t *testing.T) {
	payload, err := chrononPlanJSON(&plan.RenderPlan{
		JobID:  "job-mixed",
		Canvas: plan.CanvasSpec{Width: 640, Height: 360, Fps: 30},
		Timeline: []plan.TimelineItem{{
			Source: plan.MediaSource{Type: "video", URL: "main.mp4"}, DurationSeconds: 4,
		}},
		Layers: []plan.Layer{
			{ID: "name", Type: "text", Role: "name", Text: "Ada Lovelace", FontSize: 72, StartSeconds: 1, DurationSeconds: 2, Animation: "slide_up"},
			{ID: "important", Type: "text", Role: "important_phrase", Text: "La frase chiave", StartSeconds: 2, DurationSeconds: 1, Preset: "glow"},
			{ID: "extra-image", Type: "image", Role: "overlay", Asset: "badge.png", StartSeconds: 0, DurationSeconds: 4},
		},
		Subtitles:  []plan.SubtitleTrack{{Source: "captions.srt", Preset: "active_word_pop"}},
		OutputPath: "/work/out.mp4",
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	layers := got["layers"].([]any)
	if len(layers) != 5 {
		t.Fatalf("want timeline + 3 overlays + subtitle, got %d", len(layers))
	}
	if layers[1].(map[string]any)["role"] != "name" || layers[2].(map[string]any)["role"] != "important_phrase" {
		t.Fatalf("editorial roles were not preserved: %#v", layers)
	}
	if layers[4].(map[string]any)["type"] != "subtitle_track" {
		t.Fatalf("subtitle was not kept as a separate layer: %#v", layers[4])
	}
}
