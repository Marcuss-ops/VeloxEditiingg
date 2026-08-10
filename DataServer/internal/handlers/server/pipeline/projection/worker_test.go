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
