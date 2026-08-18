package rendermanifest

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

const testSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validManifest() Manifest {
	return Manifest{
		Schema: Schema,
		Canvas: Canvas{Width: 1920, Height: 1080, FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p"},
		Assets: []Asset{
			{ID: "clip-001", URI: "velox-asset://clip-001", Kind: "video", SHA256: testSHA, SizeBytes: 1000, DurationMS: 8400},
			{ID: "voiceover", URI: "velox-asset://voiceover", Kind: "audio", SHA256: testSHA, SizeBytes: 2000, DurationMS: 10000},
			{ID: "music", URI: "velox-asset://music", Kind: "audio", SHA256: testSHA, SizeBytes: 3000, DurationMS: 12000},
			{ID: "captions", URI: "velox-asset://captions", Kind: "subtitle", Format: "ass", SHA256: testSHA, SizeBytes: 100},
		},
		Tracks: []Track{
			{ID: "main-video", Kind: "video", Events: []Event{{AssetID: "clip-001", TimelineStartMS: 0, DurationMS: 8400}}},
			{ID: "voiceover-track", Kind: "voiceover", Events: []Event{{AssetID: "voiceover", TimelineStartMS: 0, DurationMS: 10000}}},
			{ID: "music-track", Kind: "music", Events: []Event{{AssetID: "music", TimelineStartMS: 0, DurationMS: 12000, GainDB: -24, FadeInMS: 1500, DuckUnderVoiceover: true}}},
			{ID: "caption-track", Kind: "captions", AssetID: "captions", BurnIn: true},
		},
		Output: Output{Container: "mp4", VideoCodec: "h264", AudioCodec: "aac", AudioSampleRate: 48000, AudioChannels: 2},
	}
}

func TestManifestValidate_Valid(t *testing.T) {
	if err := validManifest().Validate(); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
}

func TestParse_RejectsUnknownFieldsStrictly(t *testing.T) {
	data := []byte(`{"schema":"velox.render-manifest.v1","canvas":{"width":1920,"height":1080,"fps_num":30,"fps_den":1,"pixel_format":"yuv420p"},"assets":[],"tracks":[],"output":{},"unexpected":true}`)
	if _, err := Parse(data); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected strict unknown-field error, got %v", err)
	}
}

func TestValidate_AssetRules(t *testing.T) {
	manifest := validManifest()
	manifest.Assets[0].URI = "https://example.invalid/clip.mp4"
	manifest.Assets[0].SHA256 = strings.Repeat("A", 64)
	manifest.Assets[0].DurationMS = -1
	manifest.Assets = append(manifest.Assets, manifest.Assets[1])

	err := manifest.Validate()
	var violations ValidationErrors
	if !errors.As(err, &violations) {
		t.Fatalf("expected ValidationErrors, got %T (%v)", err, err)
	}
	for _, path := range []string{"assets[0].uri", "assets[0].sha256", "assets[0].duration_ms", "assets[4].id"} {
		if !hasPath(violations, path) {
			t.Errorf("missing asset violation for %s: %+v", path, violations)
		}
	}
}

func TestValidate_CanvasRules(t *testing.T) {
	manifest := validManifest()
	manifest.Canvas = Canvas{Width: 0, Height: -1, FPSNum: 121, FPSDen: 1, PixelFormat: "rgb24"}
	err := manifest.Validate()
	var violations ValidationErrors
	if !errors.As(err, &violations) {
		t.Fatalf("expected ValidationErrors, got %T (%v)", err, err)
	}
	for _, path := range []string{"canvas.width", "canvas.height", "canvas", "canvas.pixel_format"} {
		if !hasPath(violations, path) {
			t.Errorf("missing canvas violation for %s: %+v", path, violations)
		}
	}
}

func TestValidate_TrackAndEventRules(t *testing.T) {
	manifest := validManifest()
	manifest.Tracks[0].Events[0] = Event{AssetID: "voiceover", TimelineStartMS: -1, SourceStartMS: 9000, DurationMS: 2000, FadeInMS: -1}
	manifest.Tracks[1].Events[0].AssetID = "does-not-exist"
	manifest.Tracks[2].Kind = "unknown"
	manifest.Tracks[3].AssetID = "clip-001"

	err := manifest.Validate()
	var violations ValidationErrors
	if !errors.As(err, &violations) {
		t.Fatalf("expected ValidationErrors, got %T (%v)", err, err)
	}
	for _, path := range []string{"tracks[0].events[0].asset_id", "tracks[0].events[0].timeline_start_ms", "tracks[0].events[0].source_start_ms", "tracks[0].events[0].fade_in_ms", "tracks[1].events[0].asset_id", "tracks[2].kind", "tracks[3].asset_id"} {
		if !hasPath(violations, path) {
			t.Errorf("missing track/event violation for %s: %+v", path, violations)
		}
	}
}

func TestValidate_LayerRules(t *testing.T) {
	manifest := validManifest()
	manifest.Layers = []Layer{
		{ID: "duplicate", Type: "text", StartSeconds: -1, DurationSeconds: math.NaN(), Position: []float64{math.Inf(1)}},
		{ID: "duplicate", Type: "", FontSize: -1},
	}
	err := manifest.Validate()
	var violations ValidationErrors
	if !errors.As(err, &violations) {
		t.Fatalf("expected ValidationErrors, got %T (%v)", err, err)
	}
	for _, path := range []string{"layers[0].start_seconds", "layers[0].duration_seconds", "layers[0].position[0]", "layers[1].type", "layers[1].font_size", "layers[1].id"} {
		if !hasPath(violations, path) {
			t.Errorf("missing layer violation for %s: %+v", path, violations)
		}
	}
}

func TestValidate_OutputRules(t *testing.T) {
	manifest := validManifest()
	manifest.Output = Output{Container: "mkv", VideoCodec: "vp9", AudioCodec: "opus", AudioSampleRate: 0, AudioChannels: 6}
	err := manifest.Validate()
	var violations ValidationErrors
	if !errors.As(err, &violations) {
		t.Fatalf("expected ValidationErrors, got %T (%v)", err, err)
	}
	for _, path := range []string{"output.container", "output.video_codec", "output.audio_codec", "output.audio_sample_rate", "output.audio_channels"} {
		if !hasPath(violations, path) {
			t.Errorf("missing output violation for %s: %+v", path, violations)
		}
	}
}

func TestValidateMap_UsesSameStrictContract(t *testing.T) {
	manifest := validManifest()
	raw := map[string]interface{}{
		"schema": manifest.Schema,
		"canvas": map[string]interface{}{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1, "pixel_format": "yuv420p"},
		"assets": []interface{}{},
		"tracks": []interface{}{},
		"output": map[string]interface{}{"container": "mp4", "video_codec": "h264", "audio_codec": "aac", "audio_sample_rate": 48000, "audio_channels": 2},
		"typo":   true,
	}
	if err := ValidateMap(raw); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected strict map validation error, got %v", err)
	}
}

func hasPath(errs ValidationErrors, want string) bool {
	for _, err := range errs {
		if err.Path == want {
			return true
		}
	}
	return false
}

func TestLayerFromMapDefaultsIDAndParsesFields(t *testing.T) {
	layer := LayerFromMap(map[string]any{
		"type":     "text",
		"role":     "title",
		"position": []any{0.5, 0.25},
	}, 0)
	if layer.ID != "layer-000" {
		t.Fatalf("ID = %q, want layer-000", layer.ID)
	}
	if layer.Type != "text" || layer.Role != "title" {
		t.Fatalf("layer = %#v, want type/role parsed", layer)
	}
	if len(layer.Position) != 2 || layer.Position[0] != 0.5 || layer.Position[1] != 0.25 {
		t.Fatalf("Position = %v, want [0.5 0.25]", layer.Position)
	}
}

func TestCanvasFromMapParsesFieldsAndAppliesDefaults(t *testing.T) {
	defaults := DefaultCanvas()
	canvas := CanvasFromMap(map[string]any{
		"width":   1280,
		"height":  float64(720),
		"fps_num": json.Number("24"),
	}, defaults)
	if canvas.Width != 1280 || canvas.Height != 720 || canvas.FPSNum != 24 {
		t.Fatalf("canvas = %#v, want 1280x720@24", canvas)
	}
	if canvas.FPSDen != defaults.FPSDen || canvas.PixelFormat != defaults.PixelFormat {
		t.Fatalf("canvas defaults = %#v, want fps_den=%d pixel_format=%q", canvas, defaults.FPSDen, defaults.PixelFormat)
	}

	// Non-numeric and absent values must fall back to defaults, never panic.
	fallback := CanvasFromMap(map[string]any{"width": "nope", "pixel_format": "  "}, defaults)
	if fallback != defaults {
		t.Fatalf("fallback canvas = %#v, want defaults %#v", fallback, defaults)
	}
	if got := CanvasFromMap(nil, defaults); got != defaults {
		t.Fatalf("nil map canvas = %#v, want defaults %#v", got, defaults)
	}
}

func TestDefaultCanvasIsAValueNotAMutableGlobal(t *testing.T) {
	first := DefaultCanvas()
	first.Width = 0
	if got := DefaultCanvas(); got.Width != 1920 {
		t.Fatalf("DefaultCanvas width = %d, want 1920 (mutated shared value)", got.Width)
	}
}

func TestLayersToMapsRoundTripsThroughLayerFromMap(t *testing.T) {
	layers := []Layer{
		{ID: "layer-1", Type: "text", Role: "title", Text: "Title", StartSeconds: 0, DurationSeconds: 2},
	}
	maps, err := LayersToMaps(layers)
	if err != nil {
		t.Fatalf("LayersToMaps: %v", err)
	}
	if len(maps) != 1 || maps[0]["id"] != "layer-1" || maps[0]["type"] != "text" || maps[0]["role"] != "title" {
		t.Fatalf("LayersToMaps = %#v, want typed layer maps", maps)
	}
	round := LayerFromMap(maps[0], 0)
	if !reflect.DeepEqual(round, layers[0]) {
		t.Fatalf("round-trip = %#v, want %#v", round, layers[0])
	}
}
