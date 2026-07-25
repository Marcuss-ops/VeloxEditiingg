package pipeline

import "testing"

func TestNormalizeCreatorPushRequestUsesPayloadIdentityAndDefaults(t *testing.T) {
	normalized, err := normalizeCreatorPushRequest(creatorPushRequest{
		Payload: map[string]interface{}{
			"job_id":      "creator-job-123",
			"status":      "completed",
			"video_name":  "Creator supplied video",
			"script_text": "A complete script",
		},
	})
	if err != nil {
		t.Fatalf("normalizeCreatorPushRequest() error = %v", err)
	}
	if normalized.SourceProvider != defaultCreatorSourceProvider {
		t.Fatalf("SourceProvider = %q, want %q", normalized.SourceProvider, defaultCreatorSourceProvider)
	}
	if normalized.SourceJobID != "creator-job-123" {
		t.Fatalf("SourceJobID = %q, want creator-job-123", normalized.SourceJobID)
	}
	if normalized.TargetExecutorID != "scene.composite.v1" {
		t.Fatalf("TargetExecutorID = %q, want scene.composite.v1", normalized.TargetExecutorID)
	}
	if got := firstStringResolver(normalized.WorkerPayload, "job_id"); got != "creator-job-123" {
		t.Fatalf("WorkerPayload job_id = %q, want creator-job-123", got)
	}
}

func TestNormalizeCreatorPushRequestEnvelopeOverridesPayloadIdentity(t *testing.T) {
	normalized, err := normalizeCreatorPushRequest(creatorPushRequest{
		SourceProvider:   "creator_pc_2",
		SourceJobID:      "creator-source-456",
		TargetExecutorID: "scene.composite.v1@1",
		Payload: map[string]interface{}{
			"job_id": "payload-job-ignored-for-forwarding-identity",
			"status": "completed",
		},
	})
	if err != nil {
		t.Fatalf("normalizeCreatorPushRequest() error = %v", err)
	}
	if normalized.SourceProvider != "creator_pc_2" {
		t.Fatalf("SourceProvider = %q, want creator_pc_2", normalized.SourceProvider)
	}
	if normalized.SourceJobID != "creator-source-456" {
		t.Fatalf("SourceJobID = %q, want creator-source-456", normalized.SourceJobID)
	}
	if normalized.TargetExecutorID != "scene.composite.v1@1" {
		t.Fatalf("TargetExecutorID = %q, want scene.composite.v1@1", normalized.TargetExecutorID)
	}
}

func TestNormalizeCreatorPushRequestRejectsMissingPayload(t *testing.T) {
	if _, err := normalizeCreatorPushRequest(creatorPushRequest{}); err == nil {
		t.Fatal("normalizeCreatorPushRequest() error = nil, want missing payload error")
	}
}

func TestNormalizeCreatorPushRequestRejectsMissingSourceJobID(t *testing.T) {
	if _, err := normalizeCreatorPushRequest(creatorPushRequest{
		Payload: map[string]interface{}{"status": "completed"},
	}); err == nil {
		t.Fatal("normalizeCreatorPushRequest() error = nil, want missing source_job_id error")
	}
}

// TestNormalizeRemoteEngineIntakeRejectsMissingSourceJobID verifies
// the shared normalizer errors when no source_job_id is derivable
// (same contract as the creator_push wrapper).
func TestNormalizeRemoteEngineIntakeRejectsMissingSourceJobID(t *testing.T) {
	if _, err := normalizeRemoteEngineIntake(
		map[string]interface{}{"status": "completed"},
		"remote_engine",
		"",
		"",
	); err == nil {
		t.Fatal("normalizeRemoteEngineIntake() error = nil, want missing source_job_id error")
	}
}

