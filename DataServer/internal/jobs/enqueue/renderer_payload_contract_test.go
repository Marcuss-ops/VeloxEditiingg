package enqueue

import (
	"context"
	"encoding/json"
	"testing"

	"velox-server/internal/costmodel"
)

func TestPrepareJobAndTask_TaskSpecPayloadIsRenderingOnly(t *testing.T) {
	enq := newTestEnqueuer(t)
	job, spec, _, err := enq.PrepareJobAndTask(context.Background(), map[string]interface{}{
		"job_id":          "render-only-contract",
		"video_name":      "Render only",
		"script_text":     "Technical render",
		"project_id":      "control-project",
		"render_spec":     map[string]interface{}{"legacy": true},
		"voiceover_paths": []interface{}{"velox-asset://voice"},
		"delivery_plan": []interface{}{map[string]interface{}{
			"destination_id": "drive-main",
			"metadata":       map[string]interface{}{"title": "publication title"},
		}},
		"publications": []interface{}{map[string]interface{}{
			"publication_id": "publication-main",
			"metadata": map[string]interface{}{
				"title":       "publication title",
				"description": "publication description",
				"tags":        []interface{}{"legacy"},
				"privacy":     "private",
				"publish_at":  "2026-08-04T12:00:00Z",
			},
		}},
		"video_metadata": map[string]interface{}{
			"width": 1920, "height": 1080, "title": "must not leak",
		},
		"scenes": []interface{}{map[string]interface{}{
			"text":       "Scene",
			"clip_link":  "velox-asset://legacy-clip",
			"image_link": "velox-asset://legacy-image",
			"local_path": "/tmp/legacy",
			"bindings":   map[string]interface{}{"clip": "legacy"},
			"clip": map[string]interface{}{
				"asset_id": "clip-1", "url": "velox-asset://clip-1", "duration_ms": 5000,
			},
		}},
	}, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("PrepareJobAndTask: %v", err)
	}
	if job == nil || spec == nil {
		t.Fatal("expected job and TaskSpec")
	}
	if spec.DeliveryPlan == nil {
		t.Fatal("delivery plan must remain available on the control-plane TaskSpec field")
	}
	if len(spec.PublicationSpecs) != 1 || spec.PublicationSpecs[0]["publication_id"] != "publication-main" {
		t.Fatalf("publication specs must remain on the control-plane TaskSpec field: %#v", spec.PublicationSpecs)
	}

	forbidden := []string{
		"voiceover_paths", "clip_link", "image_link", "local_path", "bindings",
		"project_id", "render_spec", "delivery_plan", "publications", "publication_specs",
		"publication_metadata", "delivery_metadata", "metadata", "metadata_override",
		"title", "description", "tags", "privacy", "privacy_status", "publish_at",
	}
	assertNoForbiddenRendererKeys(t, spec.Payload, forbidden)
	var persistedPayload map[string]interface{}
	if err := json.Unmarshal([]byte(job.Payload), &persistedPayload); err != nil {
		t.Fatalf("decode persisted job payload: %v", err)
	}
	assertNoForbiddenRendererKeys(t, persistedPayload, forbidden)
	if _, ok := spec.Payload["video_metadata"].(map[string]interface{}); !ok {
		t.Fatal("technical video_metadata should remain in TaskSpec.Payload")
	}
}

func assertNoForbiddenRendererKeys(t *testing.T, value interface{}, forbidden []string) {
	t.Helper()
	blocked := make(map[string]struct{}, len(forbidden))
	for _, key := range forbidden {
		blocked[key] = struct{}{}
	}
	var walk func(interface{}, string)
	walk = func(current interface{}, path string) {
		switch typed := current.(type) {
		case map[string]interface{}:
			for key, child := range typed {
				if _, found := blocked[key]; found {
					t.Errorf("forbidden renderer key %q at %s", key, path)
				}
				walk(child, path+"."+key)
			}
		case []interface{}:
			for index, child := range typed {
				walk(child, path)
				_ = index
			}
		case []map[string]interface{}:
			for _, child := range typed {
				walk(child, path)
			}
		}
	}
	walk(value, "payload")
}
