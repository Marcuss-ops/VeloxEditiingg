// Package contract_test verifica che le strutture Go corrispondano esattamente
// alle controparti C++ in RemoteCodex/native/video-engine-cpp/include/video_contract.hpp.
//
// I test marshallano ogni struct Go in JSON e verificano che:
//   1. Tutti i field name JSON corrispondano ai field name C++ (snake_case)
//   2. Il round-trip JSON (marshal → unmarshal) sia fedele
//   3. Non ci siano field in eccesso o in difetto
package contract

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Expected JSON key names from C++ video_contract.hpp structs.
// These MUST match the snake_case field names in the C++ struct definitions.

var sceneAssetFields = map[string]bool{
	"text":             true,
	"image_link":       true,
	"image_links":      true,
	"duration_seconds": true,
}

var clipAssetFields = map[string]bool{
	"text":             true,
	"clip_link":        true,
	"clip_links":       true,
	"duration_seconds": true,
	"kind":             true,
}

// sceneVideoRequestCppFields elenca i JSON key di VideoEngineRequest che corrispondono
// ESATTAMENTE ai field di C++ video::SceneVideoRequest.
// Due differenze note NON sono incluse qui (gestite in test separati):
//   - clip_segments:      Go []ClipRequest  ↔  C++ std::string clip_segments_json
//   - scene_image_paths:  Go-only field, non presente in SceneVideoRequest C++
var sceneVideoRequestFields = map[string]bool{
	"job_id":                 true,
	"video_name":             true,
	"script_text":            true,
	"voiceover_paths":        true,
	"scenes":                 true,
	"scenes_json":            true,
	"output_path":            true,
	"video_mode":             true,
	"intro_clip_paths":       true,
	"stock_clip_paths":       true,
	"drive_output_folder":    true,
	"audio_language_for_srt": true,
}

// jsonKeys returns the set of keys present in the JSON encoding of v.
func jsonKeys(t *testing.T, v interface{}) map[string]bool {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal(%T) failed: %v", v, err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal of %T output failed: %v", v, err)
	}
	keys := make(map[string]bool, len(raw))
	for k := range raw {
		keys[k] = true
	}
	return keys
}

// assertKeysMatch fails if the set of JSON keys from v differs from expected.
func assertKeysMatch(t *testing.T, v interface{}, expected map[string]bool, typeName string) {
	t.Helper()
	keys := jsonKeys(t, v)

	for k := range expected {
		if !keys[k] {
			t.Errorf("%s JSON è m Missante il field C++: %q", typeName, k)
		}
	}
	for k := range keys {
		if !expected[k] {
			t.Errorf("%s JSON ha un field extra non presente in C++: %q", typeName, k)
		}
	}
	if t.Failed() {
		data, _ := json.MarshalIndent(v, "", "  ")
		t.Logf("JSON attuale:\n%s", string(data))
	}
}

// assertRoundTrip marshals v, unmarshals into T, and verifies all fields match.
func assertRoundTrip[T any](t *testing.T, original T) {
	t.Helper()
	var zero T
	if reflect.DeepEqual(original, zero) {
		t.Skip("skipping round-trip for zero value")
		return
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	var decoded T
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if !reflect.DeepEqual(original, decoded) {
		t.Errorf("round-trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

// ──────────────────────────────────────────────
// SceneRequest ↔ C++ SceneAsset
// ──────────────────────────────────────────────

func TestSceneRequest_MatchesSceneAsset(t *testing.T) {
	v := SceneRequest{
		Text:            "Ciao mondo",
		ImageLink:       "https://example.com/img.jpg",
		ImageLinks:      []string{"https://example.com/img1.jpg", "https://example.com/img2.jpg"},
		DurationSeconds: 5.5,
	}
	assertKeysMatch(t, v, sceneAssetFields, "SceneRequest")
}

func TestSceneRequest_JSONRoundTrip(t *testing.T) {
	v := SceneRequest{
		Text:            "Test scene",
		ImageLink:       "https://example.com/img.jpg",
		ImageLinks:      []string{"https://example.com/a.jpg"},
		DurationSeconds: 3.0,
	}
	assertRoundTrip(t, v)
}

func TestSceneRequest_EmptyOptionalFields(t *testing.T) {
	// When ImageLinks is nil and DurationSeconds is 0, omitempty should exclude them.
	// The C++ side must handle missing optional fields gracefully.
	v := SceneRequest{Text: "solo testo"}
	keys := jsonKeys(t, v)
	if keys["image_link"] {
		t.Error("image_link should be omitted when empty")
	}
	if keys["image_links"] {
		t.Error("image_links should be omitted when empty")
	}
	if keys["duration_seconds"] {
		t.Error("duration_seconds should be omitted when zero")
	}
	if !keys["text"] {
		t.Error("text should always be present")
	}
}

// ──────────────────────────────────────────────
// ClipRequest ↔ C++ ClipAsset
// ──────────────────────────────────────────────

func TestClipRequest_MatchesClipAsset(t *testing.T) {
	v := ClipRequest{
		Text:            "Clip uno",
		ClipLink:        "https://example.com/clip.mp4",
		ClipLinks:       []string{"https://example.com/c1.mp4", "https://example.com/c2.mp4"},
		DurationSeconds: 4.0,
		Kind:            "intro",
	}
	assertKeysMatch(t, v, clipAssetFields, "ClipRequest")
}

func TestClipRequest_JSONRoundTrip(t *testing.T) {
	v := ClipRequest{
		Text:            "Test clip",
		ClipLink:        "https://example.com/clip.mp4",
		ClipLinks:       []string{"https://example.com/clip.mp4"},
		DurationSeconds: 4.0,
		Kind:            "stock",
	}
	assertRoundTrip(t, v)
}

// ──────────────────────────────────────────────
// VideoEngineRequest ↔ C++ SceneVideoRequest
// ──────────────────────────────────────────────

func TestVideoEngineRequest_MatchesSceneVideoRequest(t *testing.T) {
	// NOTA: ClipSegments e SceneImagePaths sono volutamente omessi perché
	// non hanno corrispettivo diretto in C++ SceneVideoRequest.
	// Vedi TestVideoEngineRequest_KnownDifferences per i dettagli.
	v := VideoEngineRequest{
		JobID:               "job_123",
		VideoName:           "My Video",
		ScriptText:          "This is the script",
		VoiceoverPaths:      []string{"https://drive.google.com/vo.mp3"},
		Scenes: []SceneRequest{
			{Text: "Scene 1", ImageLink: "https://example.com/1.jpg", DurationSeconds: 5.0},
		},
		ScenesJSON:          `[{"text":"Scene 1","image_link":"https://example.com/1.jpg"}]`,
		OutputPath:          "/tmp/output.mp4",
		VideoMode:           "scene_image",
		IntroClipPaths:      []string{"https://example.com/intro.mp4"},
		StockClipPaths:      []string{"https://example.com/stock.mp4"},
		DriveOutputFolder:   "folder_abc",
		AudioLanguageForSRT: "it",
	}
	assertKeysMatch(t, v, sceneVideoRequestFields, "VideoEngineRequest")
}

func TestVideoEngineRequest_JSONRoundTrip(t *testing.T) {
	v := VideoEngineRequest{
		JobID:               "job_roundtrip",
		VideoName:           "Round Trip Video",
		ScriptText:          "Script content",
		VoiceoverPaths:      []string{"vo.mp3"},
		Scenes:              []SceneRequest{{Text: "S1"}},
		OutputPath:          "/tmp/out.mp4",
		AudioLanguageForSRT: "en",
	}
	assertRoundTrip(t, v)
}

// ──────────────────────────────────────────────
// Known differences between Go and C++
// ──────────────────────────────────────────────

func TestVideoEngineRequest_KnownDifferences(t *testing.T) {
	// Differenza 1: clip_segments vs clip_segments_json
	// Go serializza i clip segments come array tipizzato:       "clip_segments": [{...}]
	// C++ li riceve come stringa JSON pre-serializzata:        "clip_segments_json": "[{...}]"
	v := VideoEngineRequest{
		JobID:      "diff_test",
		VideoName:  "Diff",
		ScriptText: "Diff script",
		Scenes:     []SceneRequest{{Text: "S1"}},
		OutputPath: "/tmp/out.mp4",
		ClipSegments: []ClipRequest{
			{Text: "C1", ClipLink: "https://example.com/c1.mp4", DurationSeconds: 4.0},
		},
	}
	keys := jsonKeys(t, v)

	if !keys["clip_segments"] {
		t.Error("clip_segments dovrebbe essere presente (popolato con ClipSegments)")
	} else {
		t.Logf("OK — Go serializza clip_segments come array JSON")
		t.Logf("NOTA: C++ riceve questo dato come stringa JSON in clip_segments_json — " +
			"la conversione avviene nel worker-agent-go prima di inviarlo al C++ engine")
	}
	if keys["clip_segments_json"] {
		t.Log("NOTA: clip_segments_json NON è un JSON key Go — è solo il nome del field C++")
	}

	// Differenza 2: scene_image_paths è Go-only
	v.SceneImagePaths = []string{"https://example.com/s1.jpg"}
	keys2 := jsonKeys(t, v)
	if !keys2["scene_image_paths"] {
		t.Error("scene_image_paths dovrebbe essere presente (popolato)")
	} else {
		t.Logf("OK — scene_image_paths è un field aggiuntivo Go")
		t.Logf("NOTA: Non presente in SceneVideoRequest C++ — viene parsato separatamente")
	}

	// Verifica che scenes_json sia presente quando impostato
	v.ScenesJSON = `[{"text":"S1"}]`
	keys3 := jsonKeys(t, v)
	if !keys3["scenes_json"] {
		t.Error("scenes_json dovrebbe essere presente quando impostato")
	}
}

// ──────────────────────────────────────────────
// UnmarshalSceneRequest / UnmarshalClipRequest
// ──────────────────────────────────────────────

func TestUnmarshalSceneRequest(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		want    SceneRequest
		wantErr bool
	}{
		{
			name:    "full scene",
			jsonStr: `{"text":"Ciao","image_link":"https://ex.com/img.jpg","image_links":["https://ex.com/a.jpg"],"duration_seconds":5.5}`,
			want: SceneRequest{
				Text:            "Ciao",
				ImageLink:       "https://ex.com/img.jpg",
				ImageLinks:      []string{"https://ex.com/a.jpg"},
				DurationSeconds: 5.5,
			},
		},
		{
			name:    "only text",
			jsonStr: `{"text":"Solo testo"}`,
			want:    SceneRequest{Text: "Solo testo"},
		},
		{
			name:    "empty JSON object",
			jsonStr: `{}`,
			want:    SceneRequest{},
		},
		{
			name:    "empty string",
			jsonStr: "",
			want:    SceneRequest{},
		},
		{
			name:    "malformed JSON",
			jsonStr: `{invalid`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnmarshalSceneRequest([]byte(tt.jsonStr))
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.jsonStr == "" {
				if got != nil {
					t.Error("expected nil for empty input")
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil result")
			}
			if got.Text != tt.want.Text ||
				got.ImageLink != tt.want.ImageLink ||
				got.DurationSeconds != tt.want.DurationSeconds {
				t.Errorf("SceneRequest mismatch:\n  got:  %+v\n  want: %+v", got, tt.want)
			}
			if len(got.ImageLinks) != len(tt.want.ImageLinks) {
				t.Errorf("ImageLinks length: got %d, want %d", len(got.ImageLinks), len(tt.want.ImageLinks))
			}
		})
	}
}

func TestUnmarshalClipRequest(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		want    ClipRequest
		wantErr bool
	}{
		{
			name:    "full clip",
			jsonStr: `{"text":"Clip","clip_link":"https://ex.com/clip.mp4","clip_links":["https://ex.com/c1.mp4"],"duration_seconds":4.0,"kind":"intro"}`,
			want: ClipRequest{
				Text:            "Clip",
				ClipLink:        "https://ex.com/clip.mp4",
				ClipLinks:       []string{"https://ex.com/c1.mp4"},
				DurationSeconds: 4.0,
				Kind:            "intro",
			},
		},
		{
			name:    "minimal clip",
			jsonStr: `{"clip_link":"https://ex.com/c.mp4"}`,
			want:    ClipRequest{ClipLink: "https://ex.com/c.mp4", DurationSeconds: 0},
		},
		{
			name:    "empty string",
			jsonStr: "",
			want:    ClipRequest{},
		},
		{
			name:    "malformed JSON",
			jsonStr: `{broken`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnmarshalClipRequest([]byte(tt.jsonStr))
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.jsonStr == "" {
				if got != nil {
					t.Error("expected nil for empty input")
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil result")
			}
			if got.Text != tt.want.Text ||
				got.ClipLink != tt.want.ClipLink ||
				got.Kind != tt.want.Kind {
				t.Errorf("ClipRequest mismatch:\n  got:  %+v\n  want: %+v", got, tt.want)
			}
		})
	}
}

// ──────────────────────────────────────────────
// UnmarshalScenes / UnmarshalClips (array)
// ──────────────────────────────────────────────

func TestUnmarshalScenes(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		wantLen int
		wantErr bool
	}{
		{
			name:    "two scenes",
			jsonStr: `[{"text":"S1"},{"text":"S2"}]`,
			wantLen: 2,
		},
		{
			name:    "empty array",
			jsonStr: `[]`,
			wantLen: 0,
		},
		{
			name:    "empty string",
			jsonStr: "",
			wantLen: 0,
		},
		{
			name:    "malformed",
			jsonStr: `[{bad`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnmarshalScenes([]byte(tt.jsonStr))
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("got %d scenes, want %d", len(got), tt.wantLen)
			}
		})
	}
}

func TestUnmarshalClips(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		wantLen int
		wantErr bool
	}{
		{
			name:    "two clips",
			jsonStr: `[{"clip_link":"a.mp4"},{"clip_link":"b.mp4"}]`,
			wantLen: 2,
		},
		{
			name:    "empty array",
			jsonStr: `[]`,
			wantLen: 0,
		},
		{
			name:    "malformed",
			jsonStr: `[`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnmarshalClips([]byte(tt.jsonStr))
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("got %d clips, want %d", len(got), tt.wantLen)
			}
		})
	}
}

// ──────────────────────────────────────────────
// ParseClipsJSON (simmetrica a ParseScenes)
// ──────────────────────────────────────────────

func TestParseClipsJSON(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		wantLen int
	}{
		{
			name:    "two clips",
			jsonStr: `[{"text":"C1"},{"text":"C2"}]`,
			wantLen: 2,
		},
		{
			name:    "empty array",
			jsonStr: `[]`,
			wantLen: 0,
		},
		{
			name:    "empty string returns nil",
			jsonStr: "",
			wantLen: 0,
		},
		{
			name:    "malformed returns nil silently",
			jsonStr: `[{bad`,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseClipsJSON(tt.jsonStr)
			if tt.jsonStr == "" && got != nil {
				t.Error("expected nil for empty input")
			}
			if len(got) != tt.wantLen {
				t.Errorf("got %d clips, want %d", len(got), tt.wantLen)
			}
		})
	}
}

func TestParseClipsJSON_RoundTrip(t *testing.T) {
	// Marshal → Unmarshal via ParseClipsJSON is lossless
	original := []ClipRequest{
		{Text: "C1", ClipLink: "https://ex.com/c1.mp4", DurationSeconds: 4.0, Kind: "intro"},
		{Text: "C2", ClipLink: "https://ex.com/c2.mp4", DurationSeconds: 3.0, Kind: "stock"},
	}
	data, _ := json.Marshal(original)
	parsed := ParseClipsJSON(string(data))
	if len(parsed) != len(original) {
		t.Fatalf("length: got %d, want %d", len(parsed), len(original))
	}
	for i := range original {
		if parsed[i].Text != original[i].Text ||
			parsed[i].ClipLink != original[i].ClipLink ||
			parsed[i].Kind != original[i].Kind {
			t.Errorf("index %d mismatch:\n  got:  %+v\n  want: %+v", i, parsed[i], original[i])
		}
	}
}

