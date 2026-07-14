// Package enqueue — clip_input_normalizer_test.go.
//
// Pure isolated unit tests for clip_input_normalizer.go. No DB,
// no fixtures, no migrations. Three input shapes dispatch to three
// adapters (scenes array, scenes_json string, raw clips array). The
// closest integration cousin is TestNormalizeScenesPayload
// (enqueue_test.go) which exercises the same dispatcher but goes
// through BuildSceneImagePayloadForMaster with a real fs temp dir.
// This file keeps the adapter calls atomic — no temp dir, no
// re-resolution paths, no scene-image writability check.
package enqueue

import (
	"strings"
	"testing"
)

// =====================================================================
// normalizeClipPayload: dispatcher routes the three input shapes to
// the right adapter.
// =====================================================================

func TestNormalizeClipPayload_Dispatch(t *testing.T) {
	t.Parallel()

	t.Run("scenes_array_routes_to_scenes_input", func(t *testing.T) {
		t.Parallel()
		raw := map[string]interface{}{
			"scenes": []interface{}{
				map[string]interface{}{
					"clip_link":        "https://clip/1.mp4",
					"duration_seconds": 3.0,
				},
			},
		}
		entries, _, clips, audioTracks, mode, err := normalizeClipPayload(raw)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if mode != "clips" {
			t.Errorf("mode = %q; want clips (flat-scene path)", mode)
		}
		if len(entries) != 1 || len(clips) != 1 {
			t.Errorf("entries=%d clips=%d; want 1/1", len(entries), len(clips))
		}
		// No voiceover binding on flat path → 0 audio tracks.
		if audioTracks != nil {
			t.Errorf("flat path: audioTracks = %v; want nil (no voiceover = no audio timeline)", audioTracks)
		}
	})

	t.Run("scenes_json_routes_to_json_input", func(t *testing.T) {
		t.Parallel()
		raw := map[string]interface{}{
			"scenes_json": `[{"clip_link":"https://clip/2.mp4","duration_seconds":2}]`,
		}
		entries, _, clips, _, mode, err := normalizeClipPayload(raw)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if mode != "clips" {
			t.Errorf("mode = %q; want clips", mode)
		}
		if len(entries) != 1 || len(clips) != 1 {
			t.Errorf("entries=%d clips=%d; want 1/1", len(entries), len(clips))
		}
	})

	t.Run("clips_array_routes_to_clips_input", func(t *testing.T) {
		t.Parallel()
		raw := map[string]interface{}{
			"clips": []interface{}{"https://c/a.mp4", "https://c/b.mp4"},
		}
		entries, items, clips, _, mode, err := normalizeClipPayload(raw)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if mode != "clips" {
			t.Errorf("mode = %q; want clips", mode)
		}
		if len(entries) != 2 || len(items) != 2 || len(clips) != 2 {
			t.Errorf("entries=%d items=%d clips=%d; want 2/2/2", len(entries), len(items), len(clips))
		}
	})

	t.Run("missing_all_three_errors", func(t *testing.T) {
		t.Parallel()
		_, _, _, _, _, err := normalizeClipPayload(map[string]interface{}{})
		if err == nil {
			t.Fatal("want error when no input shape matches")
		}
		if !strings.Contains(err.Error(), "scenes, scenes_json, or clips are required") {
			t.Errorf("error %q must mention the input shape union", err.Error())
		}
	})

	t.Run("nil_payload_errors", func(t *testing.T) {
		t.Parallel()
		// nil map is treated as empty map: same dispatch path.
		_, _, _, _, _, err := normalizeClipPayload(nil)
		if err == nil {
			t.Fatal("want error on nil payload")
		}
	})

	t.Run("scenes_array_with_voiceover_routes_to_narrated_path", func(t *testing.T) {
		t.Parallel()
		raw := map[string]interface{}{
			"scenes": []interface{}{
				map[string]interface{}{
					"clip_link":                   "https://clip/1.mp4",
					"duration_seconds":            3.0,
					"voiceover_duration_seconds":  3.0,
					"final_clip_duration_seconds": 1.0,
					"bindings": map[string]interface{}{
						"voiceover": map[string]interface{}{"link": "https://voice/1.mp3"},
						"clip":      map[string]interface{}{"drive_link": "https://clip/1.mp4"},
					},
				},
			},
		}
		_, _, _, audioTracks, mode, err := normalizeClipPayload(raw)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if mode != "clip_stock" {
			t.Errorf("mode = %q; want clip_stock (narrated path)", mode)
		}
		if len(audioTracks) != 2 {
			t.Errorf("audio_tracks = %d; want 2 (voiceover + scene_clip_audio for 1 scene)", len(audioTracks))
		}
	})
}

// =====================================================================
// normalizeScenesInput: flat path (no voiceover) vs narrated routing.
// =====================================================================

func TestNormalizeScenesInput_FlatPath(t *testing.T) {
	t.Parallel()
	scenes := []map[string]interface{}{
		{"clip_link": "https://clip/A.mp4", "duration_seconds": 5.0},
		{"clip_link": "https://clip/B.mp4"}, // → default 4.0
	}
	entries, items, clips, audioTracks, mode, err := normalizeScenesInput(
		map[string]interface{}{}, scenes,
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if mode != "clips" {
		t.Errorf("mode = %q; want clips (no voiceover)", mode)
	}
	if len(entries) != 2 || len(items) != 2 || len(clips) != 2 {
		t.Errorf("entries=%d items=%d clips=%d; want 2/2/2", len(entries), len(items), len(clips))
	}
	if audioTracks != nil {
		t.Errorf("flat path: audioTracks = %v; want nil", audioTracks)
	}
	// Default 4.0 fires on the second scene.
	if d := asFloat(entries[1]["duration_seconds"]); d != 4.0 {
		t.Errorf("missing duration_seconds: got %v; want 4.0 (default)", d)
	}
}

// =====================================================================
// normalizeClipsInput / normalizeClipsAsInterface: string slices and
// map entries; default 4.0 duration; missing URL rejected.
// =====================================================================

func TestNormalizeClipsInput_StringSlice(t *testing.T) {
	t.Parallel()
	entries, items, clips, _, _, err := normalizeClipsInput([]string{"https://a", "https://b"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(entries) != 2 || len(items) != 2 || len(clips) != 2 {
		t.Fatalf("entries=%d items=%d clips=%d; want 2/2/2", len(entries), len(items), len(clips))
	}
	for i, e := range entries {
		if d := asFloat(e["duration_seconds"]); d != 4.0 {
			t.Errorf("entry[%d] duration = %v; want 4.0 (default)", i, d)
		}
	}
}

func TestNormalizeClipsInput_UnsupportedShape(t *testing.T) {
	t.Parallel()
	_, _, _, _, _, err := normalizeClipsInput("not a list")
	if err == nil {
		t.Fatal("want error when input is a string (only []string / []interface{} supported)")
	}
}

func TestNormalizeClipsAsInterface_StringEntriesDefaultDuration(t *testing.T) {
	t.Parallel()
	entries, items, clips, _, _, err := normalizeClipsAsInterface([]interface{}{
		"https://a", "https://b",
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(entries) != 2 || len(items) != 2 || len(clips) != 2 {
		t.Fatalf("len mismatch: entries=%d items=%d clips=%d", len(entries), len(items), len(clips))
	}
	for _, e := range entries {
		if d := asFloat(e["duration_seconds"]); d != 4.0 {
			t.Errorf("entry %v: duration_seconds = %v; want 4.0 (default)", e, d)
		}
		if _, ok := e["text"]; !ok {
			t.Errorf("entry %v: text field must be auto-generated", e)
		}
	}
}

func TestNormalizeClipsAsInterface_MapEntriesHonorDurationPrecedence(t *testing.T) {
	t.Parallel()
	// Source-of-truth precedence (per clip_input_normalizer.go
	// normalizeClipsAsInterface, in this exact order):
	//   1. alias `duration` is checked first
	//   2. if it is <= 0, fall through to canonical `duration_seconds`
	//   3. if both are <= 0, default to 4.0
	// Each entry probes one rung of that ladder.
	entries, _, _, _, _, err := normalizeClipsAsInterface([]interface{}{
		map[string]interface{}{"url": "https://a", "duration_seconds": 7.0},                  // canonical only → 7.0
		map[string]interface{}{"url": "https://b", "duration_seconds": 7.0, "duration": 3.0}, // alias wins (source quirk: alias is checked first)
		map[string]interface{}{"url": "https://c", "duration": 9.0},                          // alias only → 9.0
		map[string]interface{}{"url": "https://d"},                                           // neither → 4.0 default
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := []float64{7.0, 3.0, 9.0, 4.0}
	for i, e := range entries {
		if d := asFloat(e["duration_seconds"]); d != want[i] {
			t.Errorf("entry[%d] duration = %v; want %v", i, d, want[i])
		}
	}
}

func TestNormalizeClipsAsInterface_MapEntryNoURLThenFallback(t *testing.T) {
	t.Parallel()
	entries, _, clips, _, _, err := normalizeClipsAsInterface([]interface{}{
		map[string]interface{}{"clip_links": []string{"https://a", "https://b"}},
		map[string]interface{}{"drive_links": []string{"https://c"}},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(entries) != 2 || len(clips) != 2 {
		t.Fatalf("len mismatch: entries=%d clips=%d; want 2/2", len(entries), len(clips))
	}
	if !equalStrings(clips, []string{"https://a", "https://c"}) {
		t.Errorf("clips = %v; want [https://a https://c] (clip_links[0] → a, drive_links[0] → c)", clips)
	}
}

func TestNormalizeClipsAsInterface_RejectMissingURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		entry   interface{}
		wantSub string
	}{
		{
			name:    "map_no_url_keys",
			entry:   map[string]interface{}{"text": "no URL here"},
			wantSub: "url is required",
		},
		{
			name:    "map_empty_url_string",
			entry:   map[string]interface{}{"url": "  "},
			wantSub: "url is required",
		},
		{
			name:    "string_entry_whitespace_only",
			entry:   "   ",
			wantSub: "url is required",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			_, _, _, _, _, err := normalizeClipsAsInterface([]interface{}{c.entry})
			if err == nil {
				t.Fatalf("want error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), c.wantSub)
			}
		})
	}
}

// =====================================================================
// normalizeScenesJSONInput — the parse hop surfaces a wrapped error.
// =====================================================================

func TestNormalizeScenesJSONInput_InvalidJSON(t *testing.T) {
	t.Parallel()
	_, _, _, _, _, err := normalizeScenesJSONInput(map[string]interface{}{}, "not json")
	if err == nil {
		t.Fatal("want error on invalid scenes_json")
	}
	if !strings.Contains(err.Error(), "invalid scenes_json") {
		t.Errorf("error %q must mention 'invalid scenes_json'", err.Error())
	}
}

// =====================================================================
// toInterfaceSlice — the []string→[]interface{} bridge must preserve
// order and length.
// =====================================================================

func TestToInterfaceSlice_RoundTrip(t *testing.T) {
	t.Parallel()
	in := []string{"alpha", "beta", "gamma"}
	out := toInterfaceSlice(in)
	if len(out) != 3 {
		t.Fatalf("out len = %d; want 3", len(out))
	}
	for i, v := range out {
		s, ok := v.(string)
		if !ok {
			t.Errorf("out[%d] not a string; got %T", i, v)
			continue
		}
		if s != in[i] {
			t.Errorf("out[%d] = %q; want %q", i, s, in[i])
		}
	}
}

func TestToInterfaceSlice_NilAndEmpty(t *testing.T) {
	t.Parallel()
	out := toInterfaceSlice(nil)
	if out != nil && len(out) != 0 {
		t.Errorf("nil input: got %v; want empty/nil", out)
	}
	out2 := toInterfaceSlice([]string{})
	if out2 != nil && len(out2) != 0 {
		t.Errorf("empty input: got %v; want empty/nil", out2)
	}
}
