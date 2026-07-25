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

// TestNormalizeRemoteEngineIntakeLegacyPathDerivesIdentityFromRawMap
// locks in the drift-proof behavior of the shared normalizer for the
// legacy /api/remote/pipeline path. After the refactor, the legacy
// route routes its raw result map through normalizeRemoteEngineIntake
// with hardcoded source_provider="remote_engine" and empty envelope
// IDs. The identities (source_provider literal + source_job_id +
// target_executor_id) must be derived identically to the pre-refactor
// firstStringResolver behavior, otherwise existing remote-engine
// workers break.
func TestNormalizeRemoteEngineIntakeLegacyPathDerivesIdentityFromRawMap(t *testing.T) {
	normalized, err := normalizeRemoteEngineIntake(
		map[string]interface{}{
			"job_id":      "legacy-job-789",
			"executor_id": "scene.composite.v1",
			"status":      "completed",
			"video_name":  "Legacy video",
			"script_text": "Legacy script",
		},
		"remote_engine", // hardcoded by forwardPipelineResultToWorker
		"",              // envelope source_job_id empty → fallback to map
		"",              // envelope target_executor_id empty → fallback to map
	)
	if err != nil {
		t.Fatalf("normalizeRemoteEngineIntake() error = %v", err)
	}
	if normalized.SourceProvider != "remote_engine" {
		t.Fatalf("SourceProvider = %q, want remote_engine", normalized.SourceProvider)
	}
	if normalized.SourceJobID != "legacy-job-789" {
		t.Fatalf("SourceJobID = %q, want legacy-job-789", normalized.SourceJobID)
	}
	if normalized.TargetExecutorID != "scene.composite.v1" {
		t.Fatalf("TargetExecutorID = %q, want scene.composite.v1", normalized.TargetExecutorID)
	}
	if got := firstStringResolver(normalized.WorkerPayload, "job_id"); got != "legacy-job-789" {
		t.Fatalf("WorkerPayload job_id = %q, want legacy-job-789", got)
	}
}

// TestNormalizeRemoteEngineIntakeDefaultsTargetWhenMapSilent ensures
// the legacy path falls back to "scene.composite.v1" when the raw
// map has no executor_id/pipeline_id AND the envelope is silent —
// same default the creator_push path uses.
func TestNormalizeRemoteEngineIntakeDefaultsTargetWhenMapSilent(t *testing.T) {
	normalized, err := normalizeRemoteEngineIntake(
		map[string]interface{}{
			"job_id": "no-executor-job",
			"status": "completed",
		},
		"remote_engine",
		"",
		"",
	)
	if err != nil {
		t.Fatalf("normalizeRemoteEngineIntake() error = %v", err)
	}
	if normalized.TargetExecutorID != "scene.composite.v1" {
		t.Fatalf("TargetExecutorID = %q, want scene.composite.v1", normalized.TargetExecutorID)
	}
}

// TestNormalizeRemoteEngineIntakeRejectsNilPayload verifies the
// shared normalizer rejects a nil raw map (same contract as the
// creator_push wrapper).
func TestNormalizeRemoteEngineIntakeRejectsNilPayload(t *testing.T) {
	if _, err := normalizeRemoteEngineIntake(nil, "remote_engine", "", ""); err == nil {
		t.Fatal("normalizeRemoteEngineIntake(nil) error = nil, want missing payload error")
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
