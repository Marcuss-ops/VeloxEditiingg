package projection

import (
	"testing"
)

func TestProjectWorkerPayload_ProjectsCanonicalFieldsAndStripsDeliveryPlan(t *testing.T) {
	raw := map[string]interface{}{
		"status":      "completed",
		"job_id":      "projection-job-1",
		"video_name":  "Projection video",
		"script_text": "Projection script",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":             "Scene one",
				"duration_seconds": float64(5),
			},
		},
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "drive",
				"retry_budget":   3,
			},
		},
	}

	got, err := ProjectWorkerPayload(raw, "")
	if err != nil {
		t.Fatalf("ProjectWorkerPayload() error: %v", err)
	}
	if got["job_id"] != "projection-job-1" || got["video_name"] != "Projection video" || got["script_text"] != "Projection script" {
		t.Fatalf("canonical fields = %#v", got)
	}
	if _, ok := got["scenes_json"]; !ok {
		t.Fatalf("scenes_json missing from worker payload: %#v", got)
	}
	if _, ok := got["delivery_plan"]; ok {
		t.Fatalf("delivery_plan leaked into worker payload: %#v", got)
	}
}

func TestProjectWorkerPayload_PreservesExplicitWorkerBoundaryFields(t *testing.T) {
	raw := map[string]interface{}{
		"status":                   "completed",
		"job_id":                   "projection-job-2",
		"audio_tracks":             []interface{}{map[string]interface{}{"source_url": "velox-asset://audio/a.mp3"}},
		"layers":                   []interface{}{map[string]interface{}{"id": "layer-1"}},
		"_placement_pin_worker_id": "worker-1",
	}

	got, err := ProjectWorkerPayload(raw, "")
	if err != nil {
		t.Fatalf("ProjectWorkerPayload() error: %v", err)
	}
	for _, key := range []string{"audio_tracks", "layers", "_placement_pin_worker_id"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("worker boundary field %q was dropped: %#v", key, got)
		}
	}
}

func TestProjectWorkerPayload_MapsClipStockToRegisteredClipsPipeline(t *testing.T) {
	raw := map[string]interface{}{
		"status":      "completed",
		"job_id":      "projection-clip-stock",
		"job_type":    "clip.stock.v1",
		"video_name":  "Clip stock",
		"script_text": "Clip narration",
		"scenes": []interface{}{
			map[string]interface{}{
				"scene_id":         "scene-0",
				"text":             "A clip scene",
				"duration_seconds": float64(5),
				"clip":             map[string]interface{}{"asset_id": "asset-1", "duration_ms": int64(5000)},
			},
		},
	}

	got, err := ProjectWorkerPayload(raw, "clip_stock")
	if err != nil {
		t.Fatalf("ProjectWorkerPayload() error: %v", err)
	}
	if got["pipeline_id"] != "clips.v1" {
		t.Fatalf("pipeline_id = %#v, want clips.v1; payload=%#v", got["pipeline_id"], got)
	}
}
