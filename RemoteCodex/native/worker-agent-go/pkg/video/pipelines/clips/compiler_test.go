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

func TestCompileCopyOnlyPreservesEmbeddedWatermark(t *testing.T) {
	input := map[string]interface{}{
		"copy_only":                 true,
		"watermark_already_applied": true,
		"clips":                     []interface{}{map[string]interface{}{"url": "clip.mp4", "duration": 1.0}},
	}
	got, err := Compile(context.Background(), "job", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !got.CopyOnly || !got.WatermarkAlreadyApplied || got.WatermarkRequested {
		t.Fatalf("unexpected copy-only watermark policy: %+v", got)
	}
	if got.Timeline[0].Transform != nil {
		t.Fatal("copy-only clip unexpectedly carries a transform")
	}
}
