package contract

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/rendermanifest"
)

func TestJobPayloadV2ContractParity(t *testing.T) {
	jsonTagKeys := jobPayloadV2JSONTagKeys(t)
	canonicalKeys := stringSet(CanonicalTopLevelKeys)
	payloadKeys := mapKeys(t, mustPayloadV2Map(t))

	assertKeySetEqual(t, "JobPayloadV2 JSON tags", jsonTagKeys, "CanonicalTopLevelKeys", canonicalKeys)
	assertKeySetEqual(t, "JobPayloadV2 JSON tags", jsonTagKeys, "ToMap", payloadKeys)
}

func jobPayloadV2JSONTagKeys(t *testing.T) map[string]bool {
	t.Helper()
	typeOfPayload := reflect.TypeOf(JobPayloadV2{})
	keys := make(map[string]bool, typeOfPayload.NumField())
	for index := 0; index < typeOfPayload.NumField(); index++ {
		field := typeOfPayload.Field(index)
		tag := field.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		key := strings.Split(tag, ",")[0]
		if key == "" {
			t.Fatalf("JobPayloadV2.%s has an empty JSON key in tag %q", field.Name, tag)
		}
		if keys[key] {
			t.Fatalf("duplicate JSON key %q in JobPayloadV2", key)
		}
		keys[key] = true
	}
	return keys
}

func mustPayloadV2Map(t *testing.T) map[string]any {
	t.Helper()
	payload := &JobPayloadV2{
		ContractVersion:        2,
		PayloadContractVersion: 2,
		JobID:                  "job-1", JobRunID: "run-1", CorrelationID: "corr-1",
		JobType: "process_video", TemplateID: "test-template", TemplateVersion: 1, Version: "v2",
		CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
		VideoName: "video", ScriptText: "script",
		RenderManifest: map[string]any{"schema": "manifest"},
		ManifestRef:    map[string]any{"uri": "velox-asset://manifest"},
		ManifestSHA256: "manifest-sha",
		RenderPlanJSON: "{}", RenderPlanSHA256: "plan-sha",
		CompiledRenderPlanJSON: "{}", CompiledRenderPlanSHA256: "compiled-plan-sha",
		ScenesJSON:     "[]",
		Scenes:         []map[string]any{{"id": "scene-1"}},
		Clips:          []map[string]any{{"url": "velox-asset://clip-1", "duration": 5}},
		Layers:         []rendermanifest.Layer{{ID: "layer-1", Type: "text"}},
		Items:          []map[string]any{{"role": "scene"}},
		VoiceoverPaths: []string{"voiceover.mp3"},
		AudioLanguage:  "en", VideoMode: "scene_image", Effect: "slow_zoom", Orientation: "landscape", OutputPath: "/tmp/video.mp4",
		DriveOutput: "drive-folder", ChannelID: "channel-1", OutputVideoID: "output-1",
		SceneImagePaths: []string{"scene.jpg"}, ImageSourceMap: "{}",
		VideoMetadata: map[string]any{"width": 1920},
		Priority:      1, TimeoutSecs: 3600, SceneCount: 1, VoiceoverCount: 1,
		TotalDurationSecs: 5, SceneDurationSecs: 5,
		SubmittedVia: "test", Source: "test", JobFingerprint: "fingerprint", Status: InputAssemblyPending,
		DeliveryPlan: []deliveryplan.Entry{{DestinationID: "destination-1", RetryBudget: 5, Enabled: true}},
	}
	output, err := payload.ToMap()
	if err != nil {
		t.Fatalf("JobPayloadV2.ToMap(): %v", err)
	}
	return output
}

func mapKeys(t *testing.T, payload map[string]any) map[string]bool {
	t.Helper()
	keys := make(map[string]bool, len(payload))
	for key := range payload {
		keys[key] = true
	}
	return keys
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func assertKeySetEqual(t *testing.T, leftName string, left map[string]bool, rightName string, right map[string]bool) {
	t.Helper()
	var missing, extra []string
	for key := range left {
		if !right[key] {
			extra = append(extra, key)
		}
	}
	for key := range right {
		if !left[key] {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		return
	}
	sort.Strings(missing)
	sort.Strings(extra)
	t.Errorf("%s and %s differ: missing from %s=%v, extra in %s=%v", leftName, rightName, leftName, missing, rightName, extra)
}

func TestNewJobPayloadV2PreservesRenderTimelineItems(t *testing.T) {
	raw := map[string]any{
		"items":  []any{map[string]any{"role": "scene"}},
		"scenes": []any{map[string]any{"text": "scene", "subtitles": map[string]any{"url": "subtitles.srt", "format": "srt"}}},
	}
	payload := NewJobPayloadV2(raw)
	mapped, err := payload.ToMap()
	if err != nil {
		t.Fatalf("JobPayloadV2.ToMap(): %v", err)
	}
	values, ok := mapped["items"].([]map[string]any)
	if !ok || len(values) != 1 {
		t.Fatalf("items = %#v, want one object", mapped["items"])
	}
}

func TestJobPayloadV2JSONAndToMapHaveTheSameKeys(t *testing.T) {
	payload := &JobPayloadV2{
		ContractVersion: 2, PayloadContractVersion: 2, JobID: "job-1", JobRunID: "run-1", CorrelationID: "corr-1",
		JobType: "process_video", Version: "v2", CreatedAt: "now", UpdatedAt: "now",
		VideoName: "video", ScriptText: "script", Priority: 1, TimeoutSecs: 1,
		Items: []map[string]any{{"role": "scene"}},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(JobPayloadV2): %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal(JobPayloadV2): %v", err)
	}
	mapped, err := payload.ToMap()
	if err != nil {
		t.Fatalf("JobPayloadV2.ToMap(): %v", err)
	}
	assertKeySetEqual(t, "MarshalJSON", mapKeys(t, raw), "ToMap", mapKeys(t, mapped))
}

// TestJobPayloadV2LayersAreTypedAndRoundTrip pins the typed layers boundary:
// NewJobPayloadV2 converts authoring layer maps into []rendermanifest.Layer
// (defaulting missing IDs via LayerFromMap) and ToMap projects them back to
// the generic []map[string]any shape downstream readers expect.
// TestJobPayloadV2CanvasIsTypedFromVideoMetadata pins the typed canvas
// boundary: NewJobPayloadV2 derives the single-source rendermanifest.Canvas
// from the video_metadata canvas keys (with per-field defaults), while the
// untyped VideoMetadata map remains the wire compatibility projection.
func TestJobPayloadV2CanvasIsTypedFromVideoMetadata(t *testing.T) {
	payload := NewJobPayloadV2(map[string]any{
		"video_metadata": map[string]any{
			"width":   1280,
			"height":  720,
			"fps_num": float64(24),
			// fps_den and pixel_format omitted: defaults must fill them.
		},
	})
	if payload.Canvas.Width != 1280 || payload.Canvas.Height != 720 || payload.Canvas.FPSNum != 24 {
		t.Fatalf("Canvas = %#v, want 1280x720@24", payload.Canvas)
	}
	if payload.Canvas.FPSDen != 1 || payload.Canvas.PixelFormat != "yuv420p" {
		t.Fatalf("Canvas defaults = %#v, want fps_den=1 pixel_format=yuv420p", payload.Canvas)
	}

	// No video_metadata at all still yields the canonical default geometry.
	empty := NewJobPayloadV2(map[string]any{})
	if empty.Canvas != rendermanifest.DefaultCanvas() {
		t.Fatalf("default Canvas = %#v, want %#v", empty.Canvas, rendermanifest.DefaultCanvas())
	}
}

func TestJobPayloadV2LayersAreTypedAndRoundTrip(t *testing.T) {
	raw := map[string]any{
		"layers": []any{
			map[string]any{"type": "text", "role": "title", "text": "Title", "position": []any{0.5, 0.25}},
			map[string]any{"id": "explicit", "type": "image"},
		},
	}
	payload := NewJobPayloadV2(raw)
	if len(payload.Layers) != 2 {
		t.Fatalf("layers = %d, want 2", len(payload.Layers))
	}
	if payload.Layers[0].ID != "layer-000" || payload.Layers[0].Type != "text" || len(payload.Layers[0].Position) != 2 {
		t.Fatalf("layers[0] = %#v, want defaulted id, parsed type and position", payload.Layers[0])
	}
	if payload.Layers[1].ID != "explicit" || payload.Layers[1].Type != "image" {
		t.Fatalf("layers[1] = %#v, want explicit id and type", payload.Layers[1])
	}

	mapped, err := payload.ToMap()
	if err != nil {
		t.Fatalf("JobPayloadV2.ToMap(): %v", err)
	}
	layerMaps, ok := mapped["layers"].([]map[string]any)
	if !ok || len(layerMaps) != 2 {
		t.Fatalf("ToMap layers = %#v, want two generic maps", mapped["layers"])
	}
	if layerMaps[0]["id"] != "layer-000" || layerMaps[0]["type"] != "text" {
		t.Fatalf("layerMaps[0] = %#v, want defaulted id and type", layerMaps[0])
	}
	if layerMaps[1]["id"] != "explicit" || layerMaps[1]["type"] != "image" {
		t.Fatalf("layerMaps[1] = %#v, want explicit id and type", layerMaps[1])
	}
}
