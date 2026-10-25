package instaedit

import (
	"encoding/json"
	"testing"

	"velox-shared/contract"
)

func validRenderSpecV2(t *testing.T, name string) map[string]any {
	t.Helper()
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manifest := map[string]any{
		"schema": "velox.render-manifest.v1",
		"canvas": map[string]any{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1, "pixel_format": "yuv420p"},
		"assets": []any{
			map[string]any{"id": "clip", "uri": "velox-asset://clip", "kind": "video", "sha256": digest, "size_bytes": 100, "duration_ms": 1000},
			map[string]any{"id": "voice", "uri": "velox-asset://voice", "kind": "audio", "sha256": digest, "size_bytes": 100, "duration_ms": 1000},
			map[string]any{"id": "final-audio", "uri": "velox-asset://final-audio", "kind": "final_audio", "format": "audio/mp4", "sha256": digest, "size_bytes": 200, "duration_ms": 1000},
		},
		"tracks": []any{
			map[string]any{"id": "video", "kind": "video", "events": []any{map[string]any{"asset_id": "clip", "timeline_start_ms": 0, "duration_ms": 1000}}},
			map[string]any{"id": "voice", "kind": "voiceover", "events": []any{map[string]any{"asset_id": "voice", "timeline_start_ms": 0, "duration_ms": 1000}}},
		},
		"output": map[string]any{"container": "mp4", "video_codec": "h264", "audio_codec": "aac", "audio_sample_rate": 48000, "audio_channels": 2},
	}
	raw, sha, err := contract.CompileRenderPlanV2JSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"video_name": name, "script_text": "compiled render plan fixture",
		contract.PayloadKeyCompiledRenderPlanJSON: string(raw),
		contract.PayloadKeyCompiledRenderPlanSHA:  sha,
	}
}

func validRenderSpecV2JSON(t *testing.T, name string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(validRenderSpecV2(t, name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
