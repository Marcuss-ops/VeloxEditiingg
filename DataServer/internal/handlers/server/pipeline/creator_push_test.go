package pipeline

import (
	"testing"

	"velox-shared/contract"
)

func TestNormalizeCreatorInputAssemblyPayloadPreservesWireValue(t *testing.T) {
	raw := map[string]interface{}{
		"status": " completed ",
		"job_id": "creator-wire-001",
	}

	got, err := normalizeCreatorInputAssemblyPayload(raw)
	if err != nil {
		t.Fatalf("normalizeCreatorInputAssemblyPayload() error = %v", err)
	}
	if got["status"] != string(contract.InputAssemblyCompleted) {
		t.Fatalf("normalized status = %v, want wire value %q", got["status"], "completed")
	}
	if raw["status"] != " completed " {
		t.Fatalf("normalization mutated the input status: %v", raw["status"])
	}
	if got["job_id"] != raw["job_id"] {
		t.Fatalf("normalized job_id = %v, want %v", got["job_id"], raw["job_id"])
	}
}

func TestNormalizeCreatorInputAssemblyPayloadRejectsJobLifecycleStatuses(t *testing.T) {
	for _, status := range []string{"SUCCEEDED", "DONE", "RUNNING", "FAILED"} {
		status := status
		t.Run(status, func(t *testing.T) {
			_, err := normalizeCreatorInputAssemblyPayload(map[string]interface{}{
				"status": status,
				"job_id": "creator-invalid-status",
			})
			if err == nil {
				t.Fatalf("status %q was accepted, want rejection as job lifecycle status", status)
			}
		})
	}
}

func TestNormalizeCreatorInputAssemblyPayloadRejectsNonStringStatus(t *testing.T) {
	if _, err := normalizeCreatorInputAssemblyPayload(map[string]interface{}{
		"status": true,
	}); err == nil {
		t.Fatal("non-string status was accepted, want validation error")
	}
}

func TestNormalizeCreatorInputAssemblyPayloadAllowsMissingLegacyStatus(t *testing.T) {
	got, err := normalizeCreatorInputAssemblyPayload(map[string]interface{}{
		"job_id": "creator-legacy-001",
	})
	if err != nil {
		t.Fatalf("missing status should remain accepted for legacy payloads: %v", err)
	}
	if _, present := got["status"]; present {
		t.Fatalf("missing status was synthesized: %#v", got["status"])
	}
}

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
	if got := normalized.WorkerPayload["status"]; got != string(contract.InputAssemblyCompleted) {
		t.Fatalf("WorkerPayload status = %v, want input-assembly wire value %q", got, "completed")
	}
	if normalized.StatusDomains.InputAssembly == nil || *normalized.StatusDomains.InputAssembly != contract.InputAssemblyCompleted {
		t.Fatalf("StatusDomains.InputAssembly = %#v, want %q", normalized.StatusDomains.InputAssembly, contract.InputAssemblyCompleted)
	}
	if normalized.StatusDomains.Job != nil || normalized.StatusDomains.Delivery != nil || normalized.StatusDomains.Publication != nil {
		t.Fatalf("CreatorPush inferred unrelated status domains: %#v", normalized.StatusDomains)
	}
}

func TestNormalizeCreatorPushRequestPreservesRendererRouting(t *testing.T) {
	normalized, err := normalizeCreatorPushRequest(creatorPushRequest{
		Payload: map[string]interface{}{
			"job_id":      "creator-routing-123",
			"status":      "completed",
			"script_text": "A complete script",
			"pipeline_id": "clips.v1",
			"copy_only":   true,
		},
	})
	if err != nil {
		t.Fatalf("normalizeCreatorPushRequest() error = %v", err)
	}
	if normalized.TargetExecutorID != "clips.v1" {
		t.Fatalf("TargetExecutorID = %q, want clips.v1", normalized.TargetExecutorID)
	}
	if got := normalized.WorkerPayload["pipeline_id"]; got != "clips.v1" {
		t.Fatalf("WorkerPayload pipeline_id = %v, want clips.v1", got)
	}
	if got := normalized.WorkerPayload["copy_only"]; got != true {
		t.Fatalf("WorkerPayload copy_only = %v, want true", got)
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
