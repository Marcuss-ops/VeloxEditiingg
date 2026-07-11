// Package enqueue — narrated_clip_timeline_test.go.
//
// Pure isolated unit tests for narrated_clip_timeline.go. No DB,
// no migrations, no filesystem fixtures. Audio probes are injected
// via the narratedClipOptions.probe field so the duration resolver
// is deterministic without touching sharedmedia.DetectAudioDurationSecs.
// The closest cousin
// is BuildClipPayloadForMaster_UsesDetectedVoiceoverDurationForOffsets
// (enqueue_test.go) which uses PATH-stubbed ffprobe; this file
// keeps the probe as an in-process closure so tests stay atomic and
// parallel-safe (no tmp shell-stubbing).
package enqueue

import (
	"strings"
	"testing"
)

// =====================================================================
// resolveSceneVoiceoverDuration: precedence rules
//   1. Explicit voiceover_duration_seconds wins.
//   2. Probe fallback fires only when explicit value is absent AND
//      probe is non-nil AND probe returns a positive number.
//   3. Both empty: error (canonical "voiceover can't be measured"
//      regression surface).
// =====================================================================

func TestResolveSceneVoiceoverDuration(t *testing.T) {
	t.Parallel()

	t.Run("empty_scene_and_no_url_returns_zero_nil", func(t *testing.T) {
		t.Parallel()
		d, err := resolveSceneVoiceoverDuration(nil, "", nil)
		if err != nil || d != 0 {
			t.Fatalf("got d=%v err=%v; want 0/nil", d, err)
		}
	})

	t.Run("explicit_value_wins_over_probe", func(t *testing.T) {
		t.Parallel()
		scene := map[string]interface{}{"voiceover_duration_seconds": 12.5}
		calls := 0
		probe := func(string) float64 { calls++; return 99 }
		d, err := resolveSceneVoiceoverDuration(scene, "https://voice/1.mp3", probe)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if d != 12.5 {
			t.Errorf("duration = %v; want 12.5 (explicit beats probe)", d)
		}
		if calls != 0 {
			t.Errorf("probe called %d times; want 0 (explicit value short-circuits)", calls)
		}
	})

	t.Run("probe_fires_when_no_explicit_value", func(t *testing.T) {
		t.Parallel()
		calls := 0
		probe := func(string) float64 { calls++; return 7 }
		d, err := resolveSceneVoiceoverDuration(nil, "https://voice/1.mp3", probe)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if d != 7 {
			t.Errorf("duration = %v; want 7", d)
		}
		if calls != 1 {
			t.Errorf("probe calls = %d; want 1", calls)
		}
	})

	t.Run("no_explicit_no_probe_returns_error", func(t *testing.T) {
		t.Parallel()
		d, err := resolveSceneVoiceoverDuration(nil, "https://voice/missing.mp3", nil)
		if err == nil {
			t.Fatalf("want error 'voiceover unavailable', got nil; d=%v", d)
		}
		if d != 0 {
			t.Errorf("duration = %v on error; want 0", d)
		}
		if !strings.Contains(err.Error(), "voiceover duration unavailable") {
			t.Errorf("error %q must mention 'voiceover duration unavailable'", err.Error())
		}
	})

	t.Run("probe_returns_zero_falls_through_to_error", func(t *testing.T) {
		t.Parallel()
		probe := func(string) float64 { return 0 }
		_, err := resolveSceneVoiceoverDuration(nil, "https://voice/short.mp3", probe)
		if err == nil {
			t.Fatal("want error when probe returns 0; got nil")
		}
	})
}

// =====================================================================
// resolveSceneFinalClipDuration: final_clip_duration_seconds is
// canonical; clip_duration_seconds is the documented legacy alias;
// duration_seconds is intentionally NOT consulted (it is a
// presentation placeholder, not a clip timing contract).
// =====================================================================

func TestResolveSceneFinalClipDuration(t *testing.T) {
	t.Parallel()

	t.Run("canonical_wins_over_legacy_alias", func(t *testing.T) {
		t.Parallel()
		scene := map[string]interface{}{
			"final_clip_duration_seconds": 3.0,
			"clip_duration_seconds":       10.0,
			"duration_seconds":            99.0, // ignored
		}
		if got := resolveSceneFinalClipDuration(scene); got != 3.0 {
			t.Errorf("duration = %v; want 3.0 (canonical key wins)", got)
		}
	})

	t.Run("legacy_alias_used_when_canonical_missing", func(t *testing.T) {
		t.Parallel()
		scene := map[string]interface{}{"clip_duration_seconds": 7.5}
		if got := resolveSceneFinalClipDuration(scene); got != 7.5 {
			t.Errorf("duration = %v; want 7.5 (legacy alias fallback)", got)
		}
	})

	t.Run("default_4s_when_both_missing_along_with_generic", func(t *testing.T) {
		t.Parallel()
		scene := map[string]interface{}{"duration_seconds": 99.0} // ignored
		if got := resolveSceneFinalClipDuration(scene); got != 4.0 {
			t.Errorf("duration = %v; want 4.0 (default fallback)", got)
		}
	})

	t.Run("nil_scene_returns_default", func(t *testing.T) {
		t.Parallel()
		if got := resolveSceneFinalClipDuration(nil); got != 4.0 {
			t.Errorf("nil scene: got %v; want 4.0", got)
		}
	})

	t.Run("zero_in_canonical_falls_through_to_legacy_then_default", func(t *testing.T) {
		t.Parallel()
		// The function uses NormalizedDuration which collapses 0+nonsense to 0;
		// canonical=0 → canonical skipped → legacy used.
		scene := map[string]interface{}{
			"final_clip_duration_seconds": 0.0,
			"clip_duration_seconds":       5.5,
		}
		if got := resolveSceneFinalClipDuration(scene); got != 5.5 {
			t.Errorf("canonical=0 must NOT block legacy: got %v; want 5.5", got)
		}
	})
}

// =====================================================================
// sceneFallbackNarrationClipURLs: stock_clip_paths preferred over
// intro_clip_paths. Empty / nil raw payloads must return nil.
// =====================================================================

func TestSceneFallbackNarrationClipURLs(t *testing.T) {
	t.Parallel()

	t.Run("nil_payload_returns_nil", func(t *testing.T) {
		t.Parallel()
		if got := sceneFallbackNarrationClipURLs(nil); got != nil {
			t.Errorf("nil payload: got %v; want nil", got)
		}
	})

	t.Run("stock_clip_paths_wins_over_intro_clip_paths", func(t *testing.T) {
		t.Parallel()
		raw := map[string]interface{}{
			"stock_clip_paths": []string{"https://stock/a.mp4", "https://stock/b.mp4"},
			"intro_clip_paths": []string{"https://intro/x.mp4"},
		}
		got := sceneFallbackNarrationClipURLs(raw)
		want := []string{"https://stock/a.mp4", "https://stock/b.mp4"}
		if !equalStrings(got, want) {
			t.Errorf("got %v; want %v", got, want)
		}
	})

	t.Run("intro_clip_paths_used_when_stock_missing", func(t *testing.T) {
		t.Parallel()
		raw := map[string]interface{}{
			"intro_clip_paths": []string{"https://intro/x.mp4"},
		}
		got := sceneFallbackNarrationClipURLs(raw)
		want := []string{"https://intro/x.mp4"}
		if !equalStrings(got, want) {
			t.Errorf("got %v; want %v", got, want)
		}
	})

	t.Run("alias_keys_accepted", func(t *testing.T) {
		t.Parallel()
		rawStock := map[string]interface{}{
			"stock_clip_sources": []string{"https://stock/alias.mp4"},
		}
		gotStock := sceneFallbackNarrationClipURLs(rawStock)
		if !equalStrings(gotStock, []string{"https://stock/alias.mp4"}) {
			t.Errorf("stock_clip_sources alias: got %v; want [https://stock/alias.mp4]", gotStock)
		}

		rawIntro := map[string]interface{}{
			"start_clip_paths": []string{"https://intro/start.mp4"},
		}
		gotIntro := sceneFallbackNarrationClipURLs(rawIntro)
		if !equalStrings(gotIntro, []string{"https://intro/start.mp4"}) {
			t.Errorf("start_clip_paths alias: got %v; want [https://intro/start.mp4]", gotIntro)
		}
	})

	t.Run("all_aliases_empty_returns_nil", func(t *testing.T) {
		t.Parallel()
		raw := map[string]interface{}{
			"stock_clip_paths": []string{},
			"intro_clip_paths": []string{},
		}
		if got := sceneFallbackNarrationClipURLs(raw); got != nil {
			t.Errorf("empty pools: got %v; want nil", got)
		}
	})
}

// =====================================================================
// supportsNarratedClipScenes — narrate iff any scene carries a
// voiceover binding (top-level alias OR bindings.voiceover.{link,url,
// drive_link,local_path}). Nil receiver and empty scenes return false.
// =====================================================================

func TestSupportsNarratedClipScenes(t *testing.T) {
	t.Parallel()
	if supportsNarratedClipScenes(nil) {
		t.Error("nil scenes: want false, got true")
	}
	if supportsNarratedClipScenes([]map[string]interface{}{}) {
		t.Error("empty scenes: want false, got true")
	}
	if supportsNarratedClipScenes([]map[string]interface{}{
		{"text": "scene-A — no voiceover"},
		{"text": "scene-B — also no voiceover"},
	}) {
		t.Error("scenes without voiceover: want false, got true")
	}

	// Hit the path on the FIRST scene with bindings.voiceover.link.
	if !supportsNarratedClipScenes([]map[string]interface{}{
		{"text": "scene-A"},
		{
			"bindings": map[string]interface{}{
				"voiceover": map[string]interface{}{"link": "https://voice/1.mp3"},
			},
		},
	}) {
		t.Error("scene with voiceover binding: want true, got false")
	}

	// Top-level voiceover_link alternate path.
	if !supportsNarratedClipScenes([]map[string]interface{}{
		{"reference_voiceover": "https://voice/ref.mp3"},
	}) {
		t.Error("top-level reference_voiceover: want true, got false")
	}
}

// =====================================================================
// buildNarratedClipPayload: 6-tuple return. Probe injection through
// narratedClipOptions.probe to verify the audio-track offset clock walks correctly
// across many scenes. Each scene contributes:
//   - 1 video item (voiceover_bed, if voiceover present)
//   - 1 video item (scene_clip — final)
//   - 2 audio tracks (voiceover at offsetSeconds, scene_clip_audio
//     at offsetSeconds+voiceoverDuration)
//   - offsetSeconds += voiceoverDuration + final_clip_duration
// =====================================================================

func TestBuildNarratedClipPayload_OffsetClock(t *testing.T) {
	t.Parallel()
	scenes := []map[string]interface{}{
		{
			"voiceover_duration_seconds": 3.0,
			"final_clip_duration_seconds": 2.0,
			"bindings": map[string]interface{}{
				"voiceover": map[string]interface{}{"link": "https://voice/1.mp3"},
				"clip":      map[string]interface{}{"drive_link": "https://clip/1.mp4"},
			},
		},
		{
			// Probe MUST fire here (no explicit duration).
			"final_clip_duration_seconds": 2.0,
			"bindings": map[string]interface{}{
				"voiceover": map[string]interface{}{"link": "https://voice/2.mp3"},
				"clip":      map[string]interface{}{"drive_link": "https://clip/2.mp4"},
			},
		},
		{
			"voiceover_duration_seconds": 2.0,
			"final_clip_duration_seconds": 2.0,
			"bindings": map[string]interface{}{
				"voiceover": map[string]interface{}{"link": "https://voice/3.mp3"},
				"clip":      map[string]interface{}{"drive_link": "https://clip/3.mp4"},
			},
		},
	}
	probeCalls := 0
	probe := func(string) float64 { probeCalls++; return 5.0 }

	_, items, clips, audioTracks, mode, err := buildNarratedClipPayload(
		scenes, narratedClipOptions{probe: probe},
	)
	if err != nil {
		t.Fatalf("buildNarratedClipPayload: %v", err)
	}
	if mode != "clip_stock" {
		t.Errorf("mode = %q; want clip_stock", mode)
	}
	if got, want := len(items), 6; got != want {
		t.Errorf("items len = %d; want %d (3 voiceover_bed + 3 final)", got, want)
	}
	if got, want := len(clips), 3; got != want {
		t.Errorf("clips len = %d; want %d", got, want)
	}
	if got, want := len(audioTracks), 6; got != want {
		t.Errorf("audio_tracks len = %d; want %d", got, want)
	}
	if probeCalls != 1 {
		t.Errorf("probeCalls = %d; want 1 (only scene 2 had no explicit duration)", probeCalls)
	}

	// Expected offsets:
	//   voiceover offsets:   scene 0: 0.0
	//                        scene 1: 3.0   (cumulative after scene 0)
	//                        scene 2: 3.0 + 5.0 = 8.0   (cumulative after scene 1)
	//   final-clip offsets:  scene 0: 0.0 + 3.0 = 3.0
	//                        scene 1: 3.0 + 5.0 = 8.0
	//                        scene 2: 8.0 + 2.0 = 10.0
	wantVoiceOffsets := []float64{0.0, 5.0, 12.0}
	wantFinalOffsets := []float64{3.0, 10.0, 14.0}
	for i, want := range wantVoiceOffsets {
		if got := asFloat(audioTracks[i*2]["start_time_offset"]); got != want {
			t.Errorf("voiceover offset[%d] = %v; want %v", i, got, want)
		}
	}
	for i, want := range wantFinalOffsets {
		if got := asFloat(audioTracks[i*2+1]["start_time_offset"]); got != want {
			t.Errorf("final_clip_audio offset[%d] = %v; want %v", i, got, want)
		}
	}

	// Role tags are partitioned correctly between voiceover and
	// scene_clip_audio lanes.
	expectedRoles := []string{"voiceover", "scene_clip_audio"}
	for i, tk := range audioTracks {
		got := tk["role"].(string)
		if got != expectedRoles[i%2] {
			t.Errorf("audioTracks[%d].role = %q; want %q", i, got, expectedRoles[i%2])
		}
	}
}

// TestBuildNarratedClipPayload_RequiresClipOrNarration — if a scene
// declares NEITHER narration NOR final-clip URL, the builder must fail
// rather than emit a half-blank entry.
func TestBuildNarratedClipPayload_RequiresClipOrNarration(t *testing.T) {
	t.Parallel()
	scenes := []map[string]interface{}{
		{"text": "scene with no clip nor narration"},
	}
	if _, _, _, _, _, err := buildNarratedClipPayload(scenes, narratedClipOptions{}); err == nil {
		t.Fatal("want error when scene has neither clip nor narration; got nil")
	}
}

// TestBuildNarratedClipPayload_FallbackPoolCoveredBySceneIndex — when
// a scene lacks stock_link but the payload ships a top-level
// stock_clip_paths array, the i-th entry is borrowed as the
// narration bed; the per-scene bindings.clip.drive_link is kept as
// the final clip. The two URLs MUST stay distinct — the narration
// bed gets the pool entry, the final clip keeps its own source.
func TestBuildNarratedClipPayload_FallbackPoolCoveredBySceneIndex(t *testing.T) {
	t.Parallel()
	scenes := []map[string]interface{}{
		{
			// No per-scene stock_link / narration_clip_link → narration
			// URL falls back to pool[0]. Per-scene bindings.clip is
			// kept as the final-clip URL (no conflation).
			"voiceover_duration_seconds":  1.0,
			"final_clip_duration_seconds": 2.0,
			"bindings": map[string]interface{}{
				"voiceover": map[string]interface{}{"link": "https://voice/0.mp3"},
				"clip":      map[string]interface{}{"drive_link": "https://clip/0.mp4"},
			},
		},
	}
	_, items, _, audioTracks, mode, err := buildNarratedClipPayload(
		scenes,
		narratedClipOptions{fallbackNarrationClipURLs: []string{"https://fallback/0.mp4"}},
	)
	if err != nil {
		t.Fatalf("want success when fallback pool + per-scene clip; got err: %v", err)
	}
	if mode != "clip_stock" {
		t.Errorf("mode = %q; want clip_stock", mode)
	}
	// Two items per scene: voiceover_bed (pool entry) and scene_clip.
	if len(items) != 2 {
		t.Fatalf("items len = %d; want 2 (voiceover_bed + scene_clip)", len(items))
	}
	if got := items[0]["url"].(string); got != "https://fallback/0.mp4" {
		t.Errorf("voiceover_bed.url = %q; want %q (pool[0] for narration bed)", got, "https://fallback/0.mp4")
	}
	if got := items[0]["role"].(string); got != "voiceover_bed" {
		t.Errorf("items[0].role = %q; want voiceover_bed", got)
	}
	if got := items[1]["url"].(string); got != "https://clip/0.mp4" {
		t.Errorf("scene_clip.url = %q; want %q (per-scene binding wins over pool)", got, "https://clip/0.mp4")
	}
	if got := items[1]["role"].(string); got != "scene_clip" {
		t.Errorf("items[1].role = %q; want scene_clip", got)
	}

	// Audio: voiceover at offsetSeconds=0 (cum=0), scene_clip_audio
	// at offsetSeconds+voiceoverDuration=0+1.0=1.0.
	if len(audioTracks) != 2 {
		t.Fatalf("audio_tracks len = %d; want 2 (voiceover + scene_clip_audio)", len(audioTracks))
	}
	if got := asFloat(audioTracks[0]["start_time_offset"]); got != 0.0 {
		t.Errorf("voiceover offset = %v; want 0.0 (first scene)", got)
	}
	if got := asFloat(audioTracks[1]["start_time_offset"]); got != 1.0 {
		t.Errorf("scene_clip_audio offset = %v; want 1.0 (voiceover ends at t=1.0)", got)
	}
	if got := audioTracks[0]["source_url"].(string); got != "https://voice/0.mp3" {
		t.Errorf("voiceover.source_url = %q; want voice binding link", got)
	}
	if got := audioTracks[1]["source_url"].(string); got != "https://clip/0.mp4" {
		t.Errorf("scene_clip_audio.source_url = %q; want per-scene clip", got)
	}
}

// =====================================================================
// Scene URL helpers: priority order for aliases. Pinned explicitly so
// future renames of one alias don't silently invert the priority.
// =====================================================================

func TestSceneURLHelpers(t *testing.T) {
	t.Parallel()

	t.Run("voiceover_aliases_top_level_precedence", func(t *testing.T) {
		t.Parallel()
		scene := map[string]interface{}{
			"voiceover_link":      "v1",
			"reference_voiceover": "v2",
			"voiceover_path":      "v3",
		}
		if got := sceneVoiceoverURL(scene); got != "v1" {
			t.Errorf("top-level: got %q; want v1 (voiceover_link is canonical)", got)
		}
		scene2 := map[string]interface{}{
			"reference_voiceover": "v2",
			"voiceover_path":      "v3",
		}
		if got := sceneVoiceoverURL(scene2); got != "v2" {
			t.Errorf("fallback 1: got %q; want v2", got)
		}
		if got := sceneVoiceoverURL(nil); got != "" {
			t.Errorf("nil scene: got %q; want ''", got)
		}
	})

	t.Run("voiceover_bindings_url_path_drive_link_precedence", func(t *testing.T) {
		t.Parallel()
		// FirstString order: link, url, drive_link, local_path.
		scene := map[string]interface{}{
			"bindings": map[string]interface{}{
				"voiceover": map[string]interface{}{
					"url":        "vu",
					"drive_link": "vd",
				},
			},
		}
		if got := sceneVoiceoverURL(scene); got != "vu" {
			t.Errorf("bindings.url wins over drive_link: got %q; want vu", got)
		}
		scene2 := map[string]interface{}{
			"bindings": map[string]interface{}{
				"voiceover": map[string]interface{}{"drive_link": "vd"},
			},
		}
		if got := sceneVoiceoverURL(scene2); got != "vd" {
			t.Errorf("bindings.drive_link: got %q; want vd", got)
		}
	})

	t.Run("narration_per_scene_wins_over_fallback_pool", func(t *testing.T) {
		t.Parallel()
		scene := map[string]interface{}{"stock_link": "scene-level"}
		if got := sceneNarrationClipURL(scene, []string{"fallback-a", "fallback-b"}, 0); got != "scene-level" {
			t.Errorf("per-scene stock_link wins: got %q; want scene-level", got)
		}

		// In-range fallback pool entry used when no per-scene stock.
		scene3 := map[string]interface{}{}
		if got := sceneNarrationClipURL(scene3, []string{"fallback-a", "fallback-b"}, 1); got != "fallback-b" {
			t.Errorf("in-range fallback pool: got %q; want fallback-b", got)
		}

		// Out-of-range index falls through to sceneFinalClipURL (empty here).
		scene2 := map[string]interface{}{}
		if got := sceneNarrationClipURL(scene2, []string{"fallback-a"}, 5); got != "" {
			t.Errorf("out-of-range fallback pool: got %q; want ''", got)
		}
	})

	t.Run("first_clip_url_aliases", func(t *testing.T) {
		t.Parallel()
		if got := firstClipURL(map[string]interface{}{"clip_link": "primary"}); got != "primary" {
			t.Errorf("clip_link: got %q; want primary", got)
		}
		if got := firstClipURL(map[string]interface{}{"drive_link": "drive-clip"}); got != "drive-clip" {
			t.Errorf("drive_link: got %q; want drive-clip", got)
		}
		if got := firstClipURL(map[string]interface{}{"clip_links": []string{"a", "b"}}); got != "a" {
			t.Errorf("clip_links[0]: got %q; want a", got)
		}
		if got := firstClipURL(map[string]interface{}{"drive_links": []string{"d1", "d2"}}); got != "d1" {
			t.Errorf("drive_links[0]: got %q; want d1", got)
		}
		if got := firstClipURL(map[string]interface{}{}); got != "" {
			t.Errorf("no URL field: got %q; want ''", got)
		}
		if got := firstClipURL(nil); got != "" {
			t.Errorf("nil scene: got %q; want ''", got)
		}
	})

	t.Run("scene_final_clip_url_falls_through_to_bindings", func(t *testing.T) {
		t.Parallel()
		scene := map[string]interface{}{
			"bindings": map[string]interface{}{
				"clip": map[string]interface{}{
					"drive_link": "https://clip/binding.mp4",
				},
			},
		}
		if got := sceneFinalClipURL(scene); got != "https://clip/binding.mp4" {
			t.Errorf("bindings clip drive_link: got %q; want https://clip/binding.mp4", got)
		}
	})
}

// =====================================================================
// local helpers
// =====================================================================

// equalStrings is a small pure helper. Defined locally rather than
// imported: tests should not depend on testify/assert or pull extra
// packages just for a slice equality check the spec already covers.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
