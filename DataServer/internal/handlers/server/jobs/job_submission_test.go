package jobs

import "testing"

func TestBuildSingleJobSetsJobRunID(t *testing.T) {
	data := map[string]interface{}{
		"video_name":  "Test video",
		"start_clips": []interface{}{"https://example.com/clip.mp4"},
		"voiceovers":  []interface{}{"https://example.com/voice.mp3"},
		"created_at":  int64(1234567890),
		"job_type":    "process_video",
		"script_text": "hello world script",
	}

	jobID, normalized, fingerprint := buildSingleJob(data)
	if jobID == "" {
		t.Fatal("expected non-empty jobID")
	}
	if normalized["job_run_id"] == "" {
		t.Fatalf("expected non-empty job_run_id, got %v", normalized["job_run_id"])
	}
	if normalized["run_id"] == "" {
		t.Fatalf("expected non-empty run_id, got %v", normalized["run_id"])
	}
	if fingerprint == "" {
		t.Fatal("expected non-empty fingerprint")
	}
}
