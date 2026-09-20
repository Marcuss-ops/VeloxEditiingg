package clips

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestCompileRequiresCopyOnlyPolicy(t *testing.T) {
	input := map[string]interface{}{"clips": []interface{}{map[string]interface{}{"url": "clip.mp4", "duration": 1.0}}}
	if _, err := Compile(context.Background(), "job", input, "/tmp/out.mp4", nil); err == nil {
		t.Fatal("Compile accepted a clip job without copy_only=true")
	}
}

func TestCompileCopyOnlyDoesNotAddWatermarkComposition(t *testing.T) {
	input := map[string]interface{}{
		"copy_only": true,
		"clips":     []interface{}{map[string]interface{}{"url": "clip.mp4", "duration": 1.0}},
	}
	got, err := Compile(context.Background(), "job", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !got.CopyOnly {
		t.Fatal("copy-only policy was not enabled")
	}
	if got.Timeline[0].Transform != nil {
		t.Fatal("copy-only clip unexpectedly carries a transform")
	}
}

func TestCompileReplaceOverlaySplitsSourceWindowWithoutResettingBase(t *testing.T) {
	input := map[string]interface{}{
		"copy_only": true,
		"clips":     []interface{}{map[string]interface{}{"url": "base.mp4", "duration": 40.0}},
		"overlays": []interface{}{map[string]interface{}{
			"id": "overlay-1", "asset_id": "overlay-asset", "url": "overlay.mp4",
			"start_frame": 120, "frame_count": 120, "mode": "replace",
			"z_index": 10, "audio_mode": "preserve_final_audio",
		}},
	}
	got, err := Compile(context.Background(), "job-overlay", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(got.Timeline) != 3 {
		t.Fatalf("timeline = %#v", got.Timeline)
	}
	if got.Timeline[0].Source.URL != "base.mp4" || got.Timeline[0].SourceDurationUS != 5_000_000 || got.Timeline[0].SourceInUS != 0 {
		t.Fatalf("prefix = %+v", got.Timeline[0])
	}
	if got.Timeline[0].Source.Type != "video" {
		t.Fatalf("prefix source type = %q, want video", got.Timeline[0].Source.Type)
	}
	if got.Timeline[1].Source.URL != "overlay.mp4" || got.Timeline[1].Source.Type != "video" || got.Timeline[1].DurationSeconds != 5 || got.Timeline[1].SourceInUS != 0 {
		t.Fatalf("overlay = %+v", got.Timeline[1])
	}
	if got.CopyOnly || got.Mixed || !got.RequiresEditorialRender {
		t.Fatalf("overlay must use the editorial renderer: copy_only=%v mixed=%v editorial=%v", got.CopyOnly, got.Mixed, got.RequiresEditorialRender)
	}
	if got.Timeline[2].Source.URL != "base.mp4" || got.Timeline[2].SourceInUS != 10_000_000 || got.Timeline[2].SourceDurationUS != 30_000_000 {
		t.Fatalf("suffix = %+v", got.Timeline[2])
	}
	if got.Timeline[2].Source.Type != "video" {
		t.Fatalf("suffix source type = %q, want video", got.Timeline[2].Source.Type)
	}
}

func TestCompileCompositeOverlayNeverFallsIntoNativeLayers(t *testing.T) {
	input := map[string]interface{}{
		"copy_only": true,
		"clips":     []interface{}{map[string]interface{}{"url": "base.mp4", "duration": 40.0}},
		"overlays": []interface{}{map[string]interface{}{
			"id": "overlay-1", "asset_id": "overlay-asset", "url": "overlay.mp4",
			"start_frame": 120, "frame_count": 120, "mode": "composite",
			"z_index": 10, "audio_mode": "preserve_final_audio",
		}},
	}
	_, err := Compile(context.Background(), "job-composite", input, "/tmp/out.mp4", nil)
	if err == nil {
		t.Fatal("composite overlay was accepted without Chronon preparation")
	}
	if !strings.Contains(err.Error(), "Chronon prepared fragments") {
		t.Fatalf("error = %v", err)
	}
}

func TestCompileUsesNormalizedAudioPathWhenAudioURLIsAbsent(t *testing.T) {
	input := map[string]interface{}{
		"copy_only":  true,
		"audio_path": "/var/cache/final-audio.m4a",
		"clips":      []interface{}{map[string]interface{}{"url": "clip.mp4", "duration": 1.0}},
	}
	got, err := Compile(context.Background(), "job", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(got.AudioTracks) != 1 || got.AudioTracks[0].SourceURL != "/var/cache/final-audio.m4a" {
		t.Fatalf("audio tracks = %#v, want normalized audio path", got.AudioTracks)
	}
}

func TestCompileProjectsRuntimeAudioIDsFromNestedPayload(t *testing.T) {
	input := map[string]interface{}{
		"copy_only": true,
		"clips":     []interface{}{map[string]interface{}{"url": "clip.mp4", "duration": 10.0}},
		"runtime_assets": []interface{}{
			map[string]interface{}{"asset_id": "tts-1", "url": "/cache/tts.m4a", "duration_ms": 4000.0},
			map[string]interface{}{"asset_id": "music-1", "url": "/cache/music.mp3", "duration_ms": 208306.0},
			map[string]interface{}{"asset_id": "sfx-1", "url": "/cache/sfx.wav", "duration_ms": 1250.0},
		},
		"runtime_payload": map[string]interface{}{
			"runtime_audio": map[string]interface{}{
				"tts_asset_id":      "tts-1",
				"music_asset_id":    "music-1",
				"sfx_asset_id":      "sfx-1",
				"sfx_start_seconds": 2.5,
			},
		},
	}

	got, err := Compile(context.Background(), "job-runtime-audio", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(got.AudioTracks) != 3 {
		t.Fatalf("audio tracks = %#v, want TTS, music and SFX", got.AudioTracks)
	}
	if got.AudioTracks[0].Role != "tts" || got.AudioTracks[0].SourceURL != "/cache/tts.m4a" || got.AudioTracks[0].DurationSeconds != 4 {
		t.Fatalf("tts track = %#v", got.AudioTracks[0])
	}
	if got.AudioTracks[1].Role != "background_music" || got.AudioTracks[1].SourceURL != "/cache/music.mp3" || !got.AudioTracks[1].Loop || got.AudioTracks[1].Volume != 0.25 {
		t.Fatalf("music track = %#v", got.AudioTracks[1])
	}
	if got.AudioTracks[2].Role != "sfx" || got.AudioTracks[2].SourceURL != "/cache/sfx.wav" || got.AudioTracks[2].StartTimeOffset != 2.5 || got.AudioTracks[2].DurationSeconds != 1.25 {
		t.Fatalf("sfx track = %#v", got.AudioTracks[2])
	}
}

func TestCompileRejectsRuntimeAudioWithoutResolvedAsset(t *testing.T) {
	input := map[string]interface{}{
		"copy_only": true,
		"clips":     []interface{}{map[string]interface{}{"url": "clip.mp4", "duration": 1.0}},
		"runtime_payload": map[string]interface{}{
			"runtime_audio": map[string]interface{}{"music_asset_id": "missing"},
		},
	}
	if _, err := Compile(context.Background(), "job-runtime-audio-missing", input, "/tmp/out.mp4", nil); err == nil {
		t.Fatal("Compile accepted runtime audio without a resolved asset")
	}
}

func TestCompileSceneTimelineLoopsAndTrimsShortStock(t *testing.T) {
	input := map[string]interface{}{
		"scenes_json": `[{
			"scene_id":"intro",
			"duration_seconds":5,
			"clip":{"asset_id":"intro-clip","url":"intro.mp4","duration_ms":5000},
			"stock":[{"asset_id":"stock-a","url":"stock-a.mp4","duration_ms":2000}],
			"voiceover":{"asset_id":"voice","url":"voice.mp3","duration_ms":5000}
		}]`,
	}

	got, err := Compile(context.Background(), "job-stock-loop", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got.CopyOnly {
		t.Fatal("stock scene timeline must use the mixed renderer")
	}
	if !got.Mixed {
		t.Fatal("stock scene timeline must emit mixed=true")
	}
	if len(got.Timeline) != 4 {
		t.Fatalf("timeline segments = %d, want 3 stock segments plus final clip", len(got.Timeline))
	}
	wantDurations := []float64{2, 2, 1, 5}
	for index, want := range wantDurations {
		if got.Timeline[index].DurationSeconds != want {
			t.Fatalf("timeline[%d].duration = %v, want %v", index, got.Timeline[index].DurationSeconds, want)
		}
		if got.Timeline[index].SceneID != "intro" {
			t.Fatalf("timeline[%d].scene_id = %q, want intro", index, got.Timeline[index].SceneID)
		}
	}
	if len(got.AudioTracks) != 2 || got.AudioTracks[0].DurationSeconds != 5 || got.AudioTracks[1].StartTimeOffset != 5 {
		t.Fatalf("audio tracks = %#v, want voiceover plus post-voiceover clip audio", got.AudioTracks)
	}
}

func TestCompileSceneTimelineKeepsClipOnlyAudioAtSceneBoundary(t *testing.T) {
	input := map[string]interface{}{
		"scenes_json": `[{
			"scene_id":"intro",
			"duration_seconds":19,
			"clip":{"url":"intro.mp4","duration_ms":19000}
		},{
			"scene_id":"narrated",
			"duration_seconds":247.224,
			"stock":[{"url":"stock.mp4","duration_ms":1000}],
			"voiceover":{"url":"voice.mp3","duration_ms":247224}
		}]`,
	}

	got, err := Compile(context.Background(), "job-boundary", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(got.AudioTracks) != 2 {
		t.Fatalf("audio tracks = %#v, want intro audio plus voiceover", got.AudioTracks)
	}
	if got.AudioTracks[0].StartTimeOffset != 0 || got.AudioTracks[0].DurationSeconds != 19 {
		t.Fatalf("intro audio track = %#v, want offset 0 and duration 19", got.AudioTracks[0])
	}
	if got.AudioTracks[1].StartTimeOffset != 19 {
		t.Fatalf("voiceover offset = %v, want 19", got.AudioTracks[1].StartTimeOffset)
	}
	for index, item := range got.Timeline {
		if item.IncludeAudio {
			t.Fatalf("timeline[%d] unexpectedly carries embedded audio", index)
		}
	}
}

func TestCompileSceneTimelineAppliesReplaceOverlays(t *testing.T) {
	input := map[string]interface{}{
		"scenes_json": `[{"scene_id":"intro","duration_seconds":19,"clip":{"url":"intro.mp4","duration_ms":19000}},{"scene_id":"stock","duration_seconds":41,"stock":[{"url":"stock.mp4","duration_ms":41000}],"voiceover":{"url":"voice.mp3","duration_ms":41000}}]`,
		"overlays": []interface{}{map[string]interface{}{
			"id": "overlay-1", "asset_id": "overlay-asset", "url": "overlay.mp4",
			"start_frame": 24, "frame_count": 120, "mode": "replace",
			"z_index": 10, "audio_mode": "preserve_final_audio",
		}},
	}

	got, err := Compile(context.Background(), "job-scene-overlay", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(got.Timeline) != 4 {
		t.Fatalf("timeline segments = %d, want intro prefix, overlay, intro suffix and stock", len(got.Timeline))
	}
	if got.Timeline[1].Source.URL != "overlay.mp4" || got.Timeline[1].Source.Type != "video" || got.Timeline[1].DurationSeconds != 5 {
		t.Fatalf("overlay segment = %+v", got.Timeline[1])
	}
	if got.Timeline[0].Source.Type != "video" || got.Timeline[2].Source.Type != "video" || !got.RequiresEditorialRender {
		t.Fatalf("base segment types/editorial = %q, %q, %v; want video/video/true", got.Timeline[0].Source.Type, got.Timeline[2].Source.Type, got.RequiresEditorialRender)
	}
	if got.Mixed {
		t.Fatal("replace overlay must not use the packet-only mixed renderer")
	}
}

func TestShuffleStockPoolIsDeterministicButJobSeeded(t *testing.T) {
	pool := []sceneTimelineAsset{
		{URL: "stock-a.mp4", DurationMS: 1000},
		{URL: "stock-b.mp4", DurationMS: 1000},
		{URL: "stock-c.mp4", DurationMS: 1000},
	}
	first := shuffleStockPool(pool, "job-one", 0)
	second := shuffleStockPool(pool, "job-one", 0)
	for index := range first {
		if first[index].URL != second[index].URL {
			t.Fatalf("same job seed changed stock order: %v != %v", first, second)
		}
	}

	different := false
	for seed := 2; seed < 20; seed++ {
		candidate := shuffleStockPool(pool, fmt.Sprintf("job-%d", seed), 0)
		if candidate[0].URL != first[0].URL || candidate[1].URL != first[1].URL {
			different = true
			break
		}
	}
	if !different {
		t.Fatal("different job seeds never changed stock order")
	}
}

func TestShuffleStockPoolChangesOrderPerCycle(t *testing.T) {
	pool := []sceneTimelineAsset{
		{URL: "stock-a.mp4", DurationMS: 1000},
		{URL: "stock-b.mp4", DurationMS: 1000},
		{URL: "stock-c.mp4", DurationMS: 1000},
	}
	first := shuffleStockPool(pool, "job-cycle", 0, 0)
	second := shuffleStockPool(pool, "job-cycle", 0, 1)
	for index := range first {
		if first[index].URL != second[index].URL {
			return
		}
	}
	t.Fatalf("cycle shuffle repeated the same order: %v", first)
}
