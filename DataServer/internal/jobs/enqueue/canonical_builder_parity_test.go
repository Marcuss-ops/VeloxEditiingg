package enqueue

import (
	"encoding/json"
	"reflect"
	"testing"

	"velox-server/internal/remoteengine"
)

// TestCanonicalBuilderParity_RichPayload compares the three lower-level
// canonical builders on fields that are owned by the renderer. Lifecycle
// labels are intentionally excluded because each ingress has a distinct
// producer identity; render inputs, technical metadata, canonical timeline
// tracks and output routing must not drift. Delivery plans are control-plane
// data and are intentionally asserted separately rather than compared as
// renderer fields. Legacy
// worker-wire fields (items/clips/images/video_mode) are intentionally out
// of this persisted canonical contract and are covered by the dedicated
// legacy projection tests.
func TestCanonicalBuilderParity_RichPayload(t *testing.T) {
	raw := map[string]interface{}{
		"job_id":                 "rich-parity-001",
		"video_name":             "Rich parity render",
		"script_text":            "Rich parity script",
		"voiceover_paths":        []interface{}{"velox-asset://voice/rich.mp3"},
		"output_path":            "/var/lib/velox/rich.mp4",
		"drive_output_folder":    "folder-rich",
		"audio_language_for_srt": "en",
		"video_metadata": map[string]interface{}{
			"width":             1920,
			"height":            1080,
			"fps_num":           30,
			"fps_den":           1,
			"pixel_format":      "yuv420p",
			"audio_sample_rate": 48000,
			"audio_channels":    2,
			"video_codec":       "h264",
			"title":             "must-not-cross-render-boundary",
		},
		"scenes": []interface{}{
			map[string]interface{}{
				"scene_id":         "scene-0",
				"text":             "Rich scene",
				"clip_link":        "velox-asset://clip/rich.mp4",
				"duration_seconds": 5.0,
				"voiceover": map[string]interface{}{
					"url":         "velox-asset://voice/rich.mp3",
					"duration_ms": 5000.0,
				},
				"subtitles": map[string]interface{}{
					"url":    "velox-asset://subtitle/rich.srt",
					"format": "srt",
				},
			},
		},
		"layers": []interface{}{
			map[string]interface{}{
				"id":               "layer-0",
				"type":             "text",
				"role":             "title",
				"text":             "Rich title",
				"font":             "Inter",
				"font_size":        48.0,
				"start_seconds":    0.0,
				"duration_seconds": 5.0,
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"source_url":        "velox-asset://music/rich.mp3",
				"role":              "background_music",
				"volume":            0.12,
				"start_time_offset": 0.0,
				"duration_seconds":  5.0,
			},
		},
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "drive-rich",
				"priority":       1,
				"retry_budget":   3,
			},
		},
	}

	normalized, err := normalizeSceneVideoPayload(cloneRichPayload(raw))
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}

	pipelinePayload, err := BuildPipelinePayload(map[string]interface{}{
		"status": "completed",
		"result": cloneRichPayload(raw),
	})
	if err != nil {
		t.Fatalf("BuildPipelinePayload: %v", err)
	}

	dto, err := remoteengine.ParseRemotePipelineResult(cloneRichPayload(raw))
	if err != nil {
		t.Fatalf("ParseRemotePipelineResult: %v", err)
	}
	remotePayload, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("remote-engine worker projection: %v", err)
	}

	wantPayload, err := cloneRendererPayload(normalized)
	if err != nil {
		t.Fatalf("project normalized renderer payload: %v", err)
	}
	want := richWorkerProjection(wantPayload)
	for name, payload := range map[string]map[string]interface{}{
		"pipeline":      pipelinePayload,
		"remote_engine": remotePayload,
	} {
		if got := richWorkerProjection(payload); !reflect.DeepEqual(got, want) {
			t.Errorf("%s drifted from normalized canonical payload:\nwant=%s\n got=%s", name, richJSON(want), richJSON(got))
		}
	}

	if metadata, ok := normalized["video_metadata"].(map[string]interface{}); !ok {
		t.Fatalf("normalizer lost technical metadata: %#v", normalized["video_metadata"])
	} else if width, ok := richNumber(metadata["width"]); !ok || width != 1920 {
		t.Fatalf("normalizer changed technical metadata width: %#v", metadata["width"])
	}
	// The normalizer returns the canonical envelope consumed by enqueue;
	// delivery_plan remains there for the control plane and is removed only
	// by the renderer projection. Assert that separation explicitly.
	if plan, ok := normalized["delivery_plan"].([]interface{}); !ok || len(plan) != 1 {
		t.Fatalf("canonical normalizer lost control-plane delivery_plan: %#v", normalized["delivery_plan"])
	}

	for name, payload := range map[string]map[string]interface{}{
		"pipeline":      pipelinePayload,
		"remote_engine": remotePayload,
	} {
		metadata, _ := payload["video_metadata"].(map[string]interface{})
		if width, ok := richNumber(metadata["width"]); !ok || width != 1920 {
			t.Errorf("%s lost technical video metadata: %#v", name, payload["video_metadata"])
		}
		if _, leaked := metadata["title"]; leaked {
			t.Errorf("%s leaked publication title into renderer metadata: %#v", name, metadata)
		}
		for _, key := range []string{"title", "metadata", "description", "tags", "privacy_status", "publish_at", "delivery_plan"} {
			if _, leaked := payload[key]; leaked {
				t.Errorf("%s leaked editorial field %q into renderer payload: %#v", name, key, payload[key])
			}
		}
	}
}

func richWorkerProjection(payload map[string]interface{}) map[string]interface{} {
	projection := make(map[string]interface{})
	for _, key := range []string{
		"video_name",
		"script_text",
		"scenes_json",
		"output_path",
		"drive_output_folder",
		"audio_language_for_srt",
		"video_metadata",
		"layers",
		"audio_tracks",
	} {
		value, ok := payload[key]
		if !ok {
			continue
		}
		if key == "scenes_json" {
			if encoded, ok := value.(string); ok {
				var decoded interface{}
				if json.Unmarshal([]byte(encoded), &decoded) == nil {
					projection[key] = decoded
					continue
				}
			}
		}
		projection[key] = richNormalizeJSON(value)
	}
	return projection
}

func richNumber(value interface{}) (float64, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true
	case int64:
		return float64(number), true
	case float64:
		return number, true
	default:
		return 0, false
	}
}

func richNormalizeJSON(value interface{}) interface{} {
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var normalized interface{}
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return value
	}
	return normalized
}

func cloneRichPayload(value map[string]interface{}) map[string]interface{} {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var clone map[string]interface{}
	if err := json.Unmarshal(encoded, &clone); err != nil {
		panic(err)
	}
	return clone
}

func richJSON(value interface{}) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<marshal error>"
	}
	return string(encoded)
}
