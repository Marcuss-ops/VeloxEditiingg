package remoteengine

import (
	"encoding/json"
	"errors"
	"testing"
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
		"status": "pending", // not in KnownRemoteStatuses
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
		"job_id":  "job_123",
		"status":  "completed",
		"ok":      true,
		"result": map[string]interface{}{
			"video_name":    "Test Video",
			"script_text":   "This is the script.",
			"scenes_json":   scenesJSON,
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
		"job_id":        "job_flat",
		"status":        "running",
		"video_name":    "Flat Video",
		"script_text":   "Flat script.",
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
		"job_id":           "job_vp",
		"status":           "completed",
		"voiceover_paths":  []interface{}{"/tmp/v1.mp3", "/tmp/v2.mp3"},
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
		t.Fatalf("Scenes[1].DurationSeconds: got %d, want 5", dto.Scenes[1].DurationSeconds)
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
			Title:  "Test Video",
			Text:   "This is the script.",
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

	m := dto.ToWorkerPayload()

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

	// voiceover_paths should be []string.
	vp, ok := m["voiceover_paths"].([]string)
	if !ok {
		t.Fatalf("voiceover_paths should be []string, got %T", m["voiceover_paths"])
	}
	if len(vp) != 1 || vp[0] != "/tmp/voice.mp3" {
		t.Fatalf("voiceover_paths: got %v", vp)
	}

	// video_metadata should be a map.
	meta, ok := m["video_metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("video_metadata should be map, got %T", m["video_metadata"])
	}
	if meta["title"] != "Meta Title" {
		t.Fatalf("metadata title: got %v", meta["title"])
	}
	if meta["privacy_status"] != "private" {
		t.Fatalf("metadata privacy: got %v", meta["privacy_status"])
	}
}

func TestToWorkerPayload_PreservesRawFields(t *testing.T) {
	raw := map[string]interface{}{
		"job_id":       "job_1",
		"status":       "completed",
		"delivery_plan": []interface{}{map[string]interface{}{"destination_id": "drive-main"}},
		"output_path":  "/tmp/output",
	}
	dto := &RemotePipelineResult{
		RemoteJobID: "job_1",
		Script:      ScriptResult{Title: "V", Text: "S"},
		Voiceover:   VoiceoverResult{Paths: []string{"/tmp/v.mp3"}},
		Raw:         raw,
	}

	m := dto.ToWorkerPayload()

	// delivery_plan should be preserved from the raw map.
	if dp, ok := m["delivery_plan"]; !ok || dp == nil {
		t.Fatal("delivery_plan should be preserved from raw map")
	}
	// output_path should be preserved.
	if m["output_path"] != "/tmp/output" {
		t.Fatalf("output_path: got %v", m["output_path"])
	}
}

func TestToWorkerPayload_NilReceiver(t *testing.T) {
	var dto *RemotePipelineResult
	m := dto.ToWorkerPayload()
	if len(m) != 0 {
		t.Fatalf("nil receiver should return empty map, got %d keys", len(m))
	}
}

func TestToWorkerPayload_EmptyDTO(t *testing.T) {
	dto := &RemotePipelineResult{}
	m := dto.ToWorkerPayload()
	// No Raw, no fields set — should be empty map.
	if len(m) != 0 {
		t.Fatalf("empty DTO should return empty map, got %d keys: %v", len(m), m)
	}
}

// ── KnownRemoteStatuses ──────────────────────────────────────────────────────

func TestKnownRemoteStatuses(t *testing.T) {
	valid := []string{"queued", "running", "completed", "failed", "cancelled"}
	for _, s := range valid {
		if !KnownRemoteStatuses[s] {
			t.Errorf("status %q should be known", s)
		}
	}
	invalid := []string{"pending", "done", "succeeded", "", "PAUSED"}
	for _, s := range invalid {
		if KnownRemoteStatuses[s] {
			t.Errorf("status %q should NOT be known", s)
		}
	}
}
