package hybrid

import (
	"context"
	"testing"
)

func TestCompile_DerivesInternalSubtitleTracksFromSceneSubtitles(t *testing.T) {
	input := map[string]interface{}{
		"scenes_json": `[{"text":"Scene 1","duration_seconds":5,"subtitles":{"url":"velox-asset://subtitle-1","format":"srt"}},{"text":"Scene 2","duration_seconds":4}]`,
		"items": []interface{}{
			map[string]interface{}{"type": "image", "url": "velox-asset://image-1", "duration": 5.0},
		},
	}

	plan, err := Compile(context.Background(), "job-subtitles", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(scene subtitles): %v", err)
	}
	if len(plan.Subtitles) != 1 {
		t.Fatalf("RenderPlan subtitle_tracks = %#v, want one track", plan.Subtitles)
	}
	if plan.Subtitles[0].Source != "velox-asset://subtitle-1" {
		t.Fatalf("subtitle source = %q, want canonical scene subtitle URL", plan.Subtitles[0].Source)
	}
	if plan.Subtitles[0].Preset != "" {
		t.Fatalf("subtitle preset = %q, want empty when omitted", plan.Subtitles[0].Preset)
	}
}
