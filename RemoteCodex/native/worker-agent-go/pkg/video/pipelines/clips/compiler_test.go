package clips

import (
	"context"
	"fmt"
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
