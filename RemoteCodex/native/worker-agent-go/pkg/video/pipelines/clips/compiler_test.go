package clips

import (
	"context"
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
