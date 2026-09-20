package remoteengine

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"velox-shared/contract"
)

// ── ValidateInitialResponse ──────────────────────────────────────────────────

func TestValidateInitialResponse_Valid(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_123",
		"status": "queued",
		"ok":     true,
	}
	resp, err := ValidateInitialResponse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.JobID != "job_123" {
		t.Fatalf("JobID: got %q, want job_123", resp.JobID)
	}
	if resp.Status != "queued" {
		t.Fatalf("Status: got %q, want queued", resp.Status)
	}
	if resp.RawResult == nil {
		t.Fatal("RawResult should not be nil")
	}
}

func TestValidateInitialResponse_TraceIDFallback(t *testing.T) {
	raw := map[string]interface{}{
		"trace_id": "trace_456",
		"status":   "running",
	}
	resp, err := ValidateInitialResponse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.JobID != "trace_456" {
		t.Fatalf("JobID: got %q, want trace_456", resp.JobID)
	}
}

func TestValidateInitialResponse_IDFallback(t *testing.T) {
	raw := map[string]interface{}{
		"id":     "id_789",
		"status": "completed",
	}
	resp, err := ValidateInitialResponse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.JobID != "id_789" {
		t.Fatalf("JobID: got %q, want id_789", resp.JobID)
	}
}

func TestValidateInitialResponse_MissingJobID(t *testing.T) {
	raw := map[string]interface{}{
		"status": "queued",
	}
	_, err := ValidateInitialResponse(raw)
	if err == nil {
		t.Fatal("should error on missing job_id")
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("class: got %s, want PERMANENT", re.Class)
	}
	if re.Code != "CONTRACT_MISSING_JOB_ID" {
		t.Fatalf("code: got %s, want CONTRACT_MISSING_JOB_ID", re.Code)
	}
}

func TestValidateInitialResponse_MissingStatus(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_123",
	}
	_, err := ValidateInitialResponse(raw)
	if err == nil {
		t.Fatal("should error on missing status")
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("class: got %s, want PERMANENT", re.Class)
	}
}

func TestValidateInitialResponse_UnknownStatus(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_123",
		"status": "pending", // not in knownRemoteStatuses
	}
	_, err := ValidateInitialResponse(raw)
	if err == nil {
		t.Fatal("should error on unknown status")
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("class: got %s, want PERMANENT", re.Class)
	}
	if re.Code != "CONTRACT_UNKNOWN_STATUS" {
		t.Fatalf("code: got %s, want CONTRACT_UNKNOWN_STATUS", re.Code)
	}
}

func TestValidateInitialResponse_NilMap(t *testing.T) {
	_, err := ValidateInitialResponse(nil)
	if err == nil {
		t.Fatal("should error on nil map")
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorMalformed {
		t.Fatalf("class: got %s, want MALFORMED_RESPONSE", re.Class)
	}
}

func TestValidateInitialResponse_AllKnownStatuses(t *testing.T) {
	for _, status := range []string{"queued", "running", "completed", "failed", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			raw := map[string]interface{}{
				"job_id": "job_1",
				"status": status,
			}
			resp, err := ValidateInitialResponse(raw)
			if err != nil {
				t.Fatalf("status %q should be valid: %v", status, err)
			}
			if resp.Status != status {
				t.Fatalf("Status: got %q, want %q", resp.Status, status)
			}
		})
	}
}

// ── ParseRemotePipelineResult ────────────────────────────────────────────────

func TestParseRemotePipelineResult_Complete(t *testing.T) {
	scenesJSON := `[{"text":"Scene 1","image_link":"https://example.com/1.png"},{"text":"Scene 2","image_link":"https://example.com/2.png"}]`
	raw := map[string]interface{}{
		"job_id": "job_123",
		"status": "completed",
		"ok":     true,
		"result": map[string]interface{}{
			"video_name":  "Test Video",
			"script_text": "This is the script.",
			"scenes_json": scenesJSON,
			"voiceover": map[string]interface{}{
				"local_path": "/tmp/voice.mp3",
			},
		},
	}

	dto, err := ParseRemotePipelineResult(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if dto.RemoteJobID != "job_123" {
		t.Fatalf("RemoteJobID: got %q, want job_123", dto.RemoteJobID)
	}
	if dto.Script.Title != "Test Video" {
		t.Fatalf("Script.Title: got %q, want Test Video", dto.Script.Title)
	}
	if dto.Script.Text != "This is the script." {
		t.Fatalf("Script.Text: got %q", dto.Script.Text)
	}
	if len(dto.Scenes) != 2 {
		t.Fatalf("Scenes: got %d, want 2", len(dto.Scenes))
	}
	if dto.Scenes[0].Text != "Scene 1" {
		t.Fatalf("Scenes[0].Text: got %q", dto.Scenes[0].Text)
	}
	if dto.Scenes[0].ImageLink != "https://example.com/1.png" {
		t.Fatalf("Scenes[0].ImageLink: got %q", dto.Scenes[0].ImageLink)
	}
	if len(dto.Voiceover.Paths) != 1 {
		t.Fatalf("Voiceover.Paths: got %d, want 1", len(dto.Voiceover.Paths))
	}
	if dto.Voiceover.Paths[0] != "/tmp/voice.mp3" {
		t.Fatalf("Voiceover.Paths[0]: got %q", dto.Voiceover.Paths[0])
	}
}

func TestParseRemotePipelineResult_FlatShape(t *testing.T) {
	raw := map[string]interface{}{
		"job_id":         "job_flat",
		"status":         "running",
		"video_name":     "Flat Video",
		"script_text":    "Flat script.",
		"voiceover_path": "/tmp/flat.mp3",
	}

	dto, err := ParseRemotePipelineResult(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dto.Script.Title != "Flat Video" {
		t.Fatalf("Title: got %q", dto.Script.Title)
	}
	if dto.Script.Text != "Flat script." {
		t.Fatalf("Text: got %q", dto.Script.Text)
	}
	if len(dto.Voiceover.Paths) != 1 || dto.Voiceover.Paths[0] != "/tmp/flat.mp3" {
		t.Fatalf("Voiceover.Paths: got %v", dto.Voiceover.Paths)
	}
}

func TestParseRemotePipelineResult_NilMap(t *testing.T) {
	_, err := ParseRemotePipelineResult(nil)
	if err == nil {
		t.Fatal("should error on nil map")
	}
}

func TestParseRemotePipelineResult_VoiceoverPathsSlice(t *testing.T) {
	raw := map[string]interface{}{
		"job_id":          "job_vp",
		"status":          "completed",
		"voiceover_paths": []interface{}{"/tmp/v1.mp3", "/tmp/v2.mp3"},
	}

	dto, _ := ParseRemotePipelineResult(raw)
	if len(dto.Voiceover.Paths) != 2 {
		t.Fatalf("Voiceover.Paths: got %d, want 2", len(dto.Voiceover.Paths))
	}
	if dto.Voiceover.Paths[0] != "/tmp/v1.mp3" || dto.Voiceover.Paths[1] != "/tmp/v2.mp3" {
		t.Fatalf("Voiceover.Paths: got %v", dto.Voiceover.Paths)
	}
}

func TestParseRemotePipelineResult_Metadata(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_meta",
		"status": "completed",
		"video_metadata": map[string]interface{}{
			"title":          "Meta Title",
			"description":    "Meta Description",
			"tags":           []interface{}{"tag1", "tag2"},
			"privacy_status": "private",
		},
	}

	dto, _ := ParseRemotePipelineResult(raw)
	if dto.Metadata.Title != "Meta Title" {
		t.Fatalf("Metadata.Title: got %q", dto.Metadata.Title)
	}
	if dto.Metadata.Description != "Meta Description" {
		t.Fatalf("Metadata.Description: got %q", dto.Metadata.Description)
	}
	if len(dto.Metadata.Tags) != 2 || dto.Metadata.Tags[0] != "tag1" {
		t.Fatalf("Metadata.Tags: got %v", dto.Metadata.Tags)
	}
	if dto.Metadata.PrivacyStatus != "private" {
		t.Fatalf("Metadata.PrivacyStatus: got %q", dto.Metadata.PrivacyStatus)
	}
}

func TestParseRemotePipelineResult_ScenesArray(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_scenes",
		"status": "completed",
		"scenes": []interface{}{
			map[string]interface{}{"text": "A", "image_link": "https://a.png"},
			map[string]interface{}{"text": "B", "clip_link": "https://b.mp4", "duration_seconds": float64(5)},
		},
	}

	dto, _ := ParseRemotePipelineResult(raw)
	if len(dto.Scenes) != 2 {
		t.Fatalf("Scenes: got %d, want 2", len(dto.Scenes))
	}
	if dto.Scenes[0].Text != "A" || dto.Scenes[0].ImageLink != "https://a.png" {
		t.Fatalf("Scenes[0]: got %+v", dto.Scenes[0])
	}
	if dto.Scenes[1].Text != "B" || dto.Scenes[1].ClipLink != "https://b.mp4" {
		t.Fatalf("Scenes[1]: got %+v", dto.Scenes[1])
	}
	if dto.Scenes[1].DurationSeconds != 5 {
		t.Fatalf("Scenes[1].DurationSeconds: got %v, want 5", dto.Scenes[1].DurationSeconds)
	}
}

func TestParseRemotePipelineResult_PreservesCanonicalStockPool(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_stock_pool",
		"status": "completed",
		"scenes": []interface{}{map[string]interface{}{
			"scene_id":         "scene-0",
			"duration_seconds": float64(5),
			"stock": []interface{}{
				map[string]interface{}{"asset_id": "stock-a", "url": "velox-asset://stock-a", "duration_ms": float64(2000)},
				map[string]interface{}{"asset_id": "stock-b", "url": "velox-asset://stock-b", "duration_ms": float64(3000)},
			},
		}},
	}

	dto, err := ParseRemotePipelineResult(raw)
	if err != nil {
		t.Fatalf("ParseRemotePipelineResult: %v", err)
	}
	if len(dto.Scenes) != 1 || len(dto.Scenes[0].Stock) != 2 {
		t.Fatalf("stock pool = %#v, want two typed assets", dto.Scenes)
	}
	if dto.Scenes[0].Stock[0].AssetID != "stock-a" || dto.Scenes[0].Stock[1].DurationMS != 3000 {
		t.Fatalf("stock pool = %#v, want identity and duration preserved", dto.Scenes[0].Stock)
	}

	workerPayload, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("ToWorkerPayloadChecked: %v", err)
	}
	encoded, ok := workerPayload["scenes_json"].(string)
	if !ok || !strings.Contains(encoded, "stock-a") || !strings.Contains(encoded, "stock-b") {
		t.Fatalf("worker scenes_json = %q, want canonical stock assets", encoded)
	}
}

func TestRemotePipelineResultCanonicalizesTimedDriveOverlaysForWorker(t *testing.T) {
	dto, err := ParseRemotePipelineResult(map[string]interface{}{
		"job_id": "job_overlays",
		"status": "completed",
		"overlays": []interface{}{map[string]interface{}{
			"id":            "overlay-01",
			"asset_id":      "drive-overlay-01",
			"drive_file_id": "drive-overlay-01",
			"url":           "https://drive.google.com/file/d/drive-overlay-01/view?usp=drive_link",
			"start_frame":   float64(120),
			"frame_count":   float64(120),
			"mode":          "replace",
			"audio_mode":    "preserve_final_audio",
		}},
	})
	if err != nil {
		t.Fatalf("ParseRemotePipelineResult: %v", err)
	}
	workerPayload, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("ToWorkerPayloadChecked: %v", err)
	}
	overlays, ok := workerPayload["overlays"].([]contract.Overlay)
	if !ok || len(overlays) != 1 {
		t.Fatalf("worker overlays = %#v", workerPayload["overlays"])
	}
	if overlays[0].URL != "velox-drive://drive-overlay-01" || overlays[0].DriveFileID != "drive-overlay-01" || overlays[0].StartFrame != 120 || overlays[0].EndFrame != 240 {
		t.Fatalf("worker overlay = %+v", overlays[0])
	}
}

func TestParseRemotePipelineResult_AssetsFromScenes(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_assets",
		"status": "completed",
		"scenes": []interface{}{
			map[string]interface{}{"text": "A", "image_link": "https://a.png"},
			map[string]interface{}{"text": "B", "clip_link": "https://b.mp4"},
		},
	}

	dto, _ := ParseRemotePipelineResult(raw)
	if len(dto.Assets) != 2 {
		t.Fatalf("Assets: got %d, want 2", len(dto.Assets))
	}
	if dto.Assets[0].Type != "image" || dto.Assets[0].URL != "https://a.png" {
		t.Fatalf("Assets[0]: got %+v", dto.Assets[0])
	}
	if dto.Assets[1].Type != "clip" || dto.Assets[1].URL != "https://b.mp4" {
		t.Fatalf("Assets[1]: got %+v", dto.Assets[1])
	}
}

// ── ToWorkerPayload ──────────────────────────────────────────────────────────

func TestToWorkerPayload_RoundTrip(t *testing.T) {
	scenes := []SceneResult{
		{Text: "Scene 1", ImageLink: "https://example.com/1.png"},
		{Text: "Scene 2", ImageLink: "https://example.com/2.png"},
	}
	dto := &RemotePipelineResult{
		RemoteJobID: "job_123",
		Script: ScriptResult{
			Title:    "Test Video",
			Text:     "This is the script.",
			JSONPath: "/tmp/scenes.json",
		},
		Scenes: scenes,
		Voiceover: VoiceoverResult{
			Paths: []string{"/tmp/voice.mp3"},
		},
		Metadata: VideoMetadata{
			Title:         "Meta Title",
			PrivacyStatus: "private",
		},
	}

	m, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("ToWorkerPayloadChecked: %v", err)
	}

	if m["job_id"] != "job_123" {
		t.Fatalf("job_id: got %v", m["job_id"])
	}
	if m["trace_id"] != "job_123" {
		t.Fatalf("trace_id: got %v", m["trace_id"])
	}
	if m["video_name"] != "Test Video" {
		t.Fatalf("video_name: got %v", m["video_name"])
	}
	if m["script_text"] != "This is the script." {
		t.Fatalf("script_text: got %v", m["script_text"])
	}
	if m["json_path"] != "/tmp/scenes.json" {
		t.Fatalf("json_path: got %v", m["json_path"])
	}

	// scenes_json should be a JSON string.
	scenesJSON, ok := m["scenes_json"].(string)
	if !ok {
		t.Fatalf("scenes_json should be string, got %T", m["scenes_json"])
	}
	var parsed []SceneResult
	if err := json.Unmarshal([]byte(scenesJSON), &parsed); err != nil {
		t.Fatalf("scenes_json unmarshal: %v", err)
	}
	if len(parsed) != 2 || parsed[0].Text != "Scene 1" {
		t.Fatalf("parsed scenes: got %+v", parsed)
	}

	if _, present := m["voiceover_paths"]; present {
		t.Fatalf("voiceover_paths must not cross the renderer boundary: %v", m["voiceover_paths"])
	}

	// Publication metadata belongs to the control plane and must not be
	// included in the renderer payload, even when the typed DTO contains it.
	if _, present := m["video_metadata"]; present {
		t.Fatalf("video_metadata leaked into renderer payload: %v", m["video_metadata"])
	}
}

func TestToWorkerPayload_StripsPublicationFieldsFromRawPayload(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_publication_fields",
		"status": "completed",
		"video_metadata": map[string]interface{}{
			"title":          "Published title",
			"description":    "Published description",
			"tags":           []interface{}{"tag"},
			"privacy_status": "private",
			"publish_at":     "2026-07-20T18:00:00Z",
		},
		"publications": []interface{}{map[string]interface{}{"metadata": map[string]interface{}{"title": "Nested"}}},
		"delivery_plan": []interface{}{map[string]interface{}{
			"destination_id": "youtube-en",
			"retry_budget":   3,
			"metadata":       map[string]interface{}{"title": "Legacy nested"},
		}},
	}

	dto := &RemotePipelineResult{RemoteJobID: "job_publication_fields", Script: ScriptResult{Title: "Renderer name", Text: "Render script"}, Raw: raw}
	m, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("ToWorkerPayloadChecked: %v", err)
	}
	for _, key := range []string{"video_metadata", "publications", "publication_specs"} {
		if _, present := m[key]; present {
			t.Fatalf("%s leaked into renderer payload: %#v", key, m[key])
		}
	}
	if _, present := m["delivery_plan"]; present {
		t.Fatalf("delivery_plan leaked into renderer payload: %#v", m["delivery_plan"])
	}
}

func TestToWorkerPayload_PreservesRawFields(t *testing.T) {
	raw := map[string]interface{}{
		"job_id":        "job_1",
		"status":        "completed",
		"delivery_plan": []interface{}{map[string]interface{}{"destination_id": "drive-main"}},
		"output_path":   "/tmp/output",
	}
	dto := &RemotePipelineResult{
		RemoteJobID: "job_1",
		Script:      ScriptResult{Title: "V", Text: "S"},
		Voiceover:   VoiceoverResult{Paths: []string{"/tmp/v.mp3"}},
		Raw:         raw,
	}

	m, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("ToWorkerPayloadChecked: %v", err)
	}

	// Delivery routing is control-plane data and must not cross the
	// renderer boundary. Render-only fields such as output_path remain.
	if _, ok := m["delivery_plan"]; ok {
		t.Fatalf("delivery_plan leaked into renderer payload: %#v", m["delivery_plan"])
	}
	// output_path should be preserved.
	if m["output_path"] != "/tmp/output" {
		t.Fatalf("output_path: got %v", m["output_path"])
	}
}

func TestToWorkerPayload_NilReceiver(t *testing.T) {
	var dto *RemotePipelineResult
	m, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("ToWorkerPayloadChecked: %v", err)
	}
	if len(m) != 0 {
		t.Fatalf("nil receiver should return empty map, got %d keys", len(m))
	}
}

func TestToWorkerPayload_EmptyDTO(t *testing.T) {
	dto := &RemotePipelineResult{}
	m, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("ToWorkerPayloadChecked: %v", err)
	}
	// No Raw, no fields set — should be empty map.
	if len(m) != 0 {
		t.Fatalf("empty DTO should return empty map, got %d keys: %v", len(m), m)
	}
}

// ── knownRemoteStatuses ─────────────────────────────────────────────────────

func TestKnownRemoteStatuses(t *testing.T) {
	valid := []string{"queued", "running", "completed", "failed", "cancelled"}
	for _, s := range valid {
		if !knownRemoteStatuses[s] {
			t.Errorf("status %q should be known", s)
		}
	}
	invalid := []string{"pending", "done", "succeeded", "", "PAUSED"}
	for _, s := range invalid {
		if knownRemoteStatuses[s] {
			t.Errorf("status %q should NOT be known", s)
		}
	}
}
