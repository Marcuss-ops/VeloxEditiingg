package creatorflow

import "velox-shared/contract"

func compiledV2TestPlan() (string, string) {
	const sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manifest := map[string]interface{}{
		"schema": "velox.render-manifest.v1",
		"canvas": map[string]interface{}{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1, "pixel_format": "yuv420p"},
		"assets": []interface{}{
			map[string]interface{}{"id": "clip", "uri": "velox-asset://clip", "kind": "video", "sha256": sha, "size_bytes": 100, "duration_ms": 1000},
			map[string]interface{}{"id": "voiceover", "uri": "velox-asset://voiceover", "kind": "audio", "sha256": sha, "size_bytes": 100, "duration_ms": 1000},
			map[string]interface{}{"id": "final-audio", "uri": "velox-asset://final-audio", "kind": "final_audio", "format": "audio/mp4", "sha256": sha, "size_bytes": 200, "duration_ms": 1000},
		},
		"tracks": []interface{}{
			map[string]interface{}{"id": "video", "kind": "video", "events": []interface{}{map[string]interface{}{"asset_id": "clip", "timeline_start_ms": 0, "duration_ms": 1000}}},
			map[string]interface{}{"id": "voiceover", "kind": "voiceover", "events": []interface{}{map[string]interface{}{"asset_id": "voiceover", "timeline_start_ms": 0, "duration_ms": 1000}}},
		},
		"output": map[string]interface{}{"container": "mp4", "video_codec": "h264", "audio_codec": "aac", "audio_sample_rate": 48000, "audio_channels": 2},
	}
	plan, sha256, err := contract.CompileRenderPlanV2JSON(manifest)
	if err != nil {
		panic(err)
	}
	return string(plan), sha256
}
