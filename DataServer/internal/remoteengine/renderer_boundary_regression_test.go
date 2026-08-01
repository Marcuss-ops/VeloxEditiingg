package remoteengine

import "testing"

// TestToWorkerPayload_RejectsAllControlPlanePublicationAndDeliveryData is a
// renderer-boundary regression. Publication specs, delivery metadata, and
// destinations belong to the control plane; none may survive the DTO
// projection that produces the payload consumed by the renderer.
func TestToWorkerPayload_RejectsAllControlPlanePublicationAndDeliveryData(t *testing.T) {
	raw := map[string]interface{}{
		"job_id":      "renderer-boundary-regression",
		"status":      "completed",
		"video_name":  "Technical renderer name",
		"script_text": "Render-only script.",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":             "A scene",
				"duration_seconds": float64(3),
				"image_link":       "velox-asset://image/scene.png",
			},
		},
		"video_metadata": map[string]interface{}{
			"width":          1920,
			"height":         1080,
			"fps_num":        30,
			"fps_den":        1,
			"video_codec":    "h264",
			"title":          "Publication title must not leak",
			"description":    "Publication description must not leak",
			"tags":           []interface{}{"editorial"},
			"privacy_status": "private",
		},
		"publications": []interface{}{
			map[string]interface{}{
				"publication_id": "publication-1",
				"metadata": map[string]interface{}{
					"title":       "Nested publication title",
					"description": "Nested publication description",
					"tags":        []interface{}{"tag"},
					"privacy":     "private",
				},
				"destinations": []interface{}{
					map[string]interface{}{"destination_id": "youtube-en"},
				},
			},
		},
		"publication_specs": []interface{}{
			map[string]interface{}{
				"metadata":       map[string]interface{}{"title": "Spec title"},
				"destination_id": "drive-en",
			},
		},
		"delivery_metadata": map[string]interface{}{
			"title":       "Delivery metadata title",
			"description": "Delivery metadata description",
		},
		"destinations": []interface{}{
			map[string]interface{}{"destination_id": "youtube-en"},
		},
		"delivery_destinations": []interface{}{
			map[string]interface{}{"destination_id": "drive-en"},
		},
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "youtube-en",
				"priority":       1,
				"retry_budget":   3,
				"metadata": map[string]interface{}{
					"title":       "Delivery title",
					"description": "Delivery description",
					"tags":        []interface{}{"delivery"},
					"privacy":     "unlisted",
				},
			},
		},
		"destination_id":  "youtube-en",
		"destination_ids": []interface{}{"youtube-en", "drive-en"},
	}

	dto, err := ParseRemotePipelineResult(raw)
	if err != nil {
		t.Fatalf("ParseRemotePipelineResult: %v", err)
	}
	workerPayload := dto.ToWorkerPayload()

	technical, ok := workerPayload["video_metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("technical video_metadata was lost from renderer payload: %#v", workerPayload["video_metadata"])
	}
	for _, key := range []string{"title", "description", "tags", "privacy_status", "publish_at"} {
		if _, present := technical[key]; present {
			t.Fatalf("publication video_metadata field %q leaked into renderer payload: %#v", key, technical[key])
		}
	}
	for _, key := range []string{"width", "height", "fps_num", "fps_den", "video_codec"} {
		if _, present := technical[key]; !present {
			t.Fatalf("technical video_metadata field %q was lost from renderer payload", key)
		}
	}
	for _, key := range []string{
		"publications",
		"publication_specs",
		"delivery_plan",
		"delivery_metadata",
		"destinations",
		"delivery_destinations",
		"destination_id",
		"destination_ids",
		"metadata",
		"metadata_override",
		"localizations",
		"title",
		"description",
		"tags",
		"privacy",
		"privacy_status",
		"publish_at",
		"schedule",
		"scheduling",
	} {
		if _, present := workerPayload[key]; present {
			t.Fatalf("control-plane field %q leaked into renderer payload: %#v", key, workerPayload[key])
		}
	}

	if workerPayload["video_name"] != "Technical renderer name" {
		t.Fatalf("technical video_name was lost: %#v", workerPayload["video_name"])
	}
	if _, present := workerPayload["scenes_json"]; !present {
		t.Fatal("render scenes were lost while stripping control-plane data")
	}
}
