package pipeline

import (
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-shared/contract/assembly"
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
		Payload: creatorPushV2Payload(t, "creator-job-123"),
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
	if normalized.TargetExecutorID != "video.assemble.copy.v1" {
		t.Fatalf("TargetExecutorID = %q, want video.assemble.copy.v1", normalized.TargetExecutorID)
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

func TestNormalizeCreatorPushRequestCarriesEarlyAssemblyManifest(t *testing.T) {
	const jobID = "creator-prefetch-001"
	manifest := &assembly.EarlyAssemblyManifest{
		ContractVersion: assembly.ContractVersion,
		JobID:           jobID,
		Revision:        1,
		Dispatch:        assembly.NormalizeDispatch(assembly.ExternalDispatch{SendToVelox: true}),
		ExpectedProfile: assembly.CanonicalAssemblyProfileLegacyV1,
		Assets: []assembly.AssetRequirement{{
			AssetID: "clip-0", Kind: assembly.KindSourceClip,
			Availability: assembly.AvailabilityKnown, URL: "https://drive.google.com/clip-0",
			SHA256: strings.Repeat("a", 64), SizeBytes: 123, Required: true, State: assembly.AssetReady,
		}},
		Timeline: []assembly.TimelineItem{{SceneID: "scene-0", AssetID: "clip-0"}},
	}
	manifest.PreparationHash = manifest.ComputePreparationHash()

	normalized, err := normalizeCreatorPushRequest(creatorPushRequest{
		SourceJobID: jobID,
		Assembly:    &assembly.ExternalAssemblyRequest{EarlyManifest: manifest},
		Payload:     creatorPushV2Payload(t, jobID),
	})
	if err != nil {
		t.Fatalf("normalizeCreatorPushRequest() error = %v", err)
	}
	if normalized.Assembly == nil || normalized.Assembly.PreparationHash != manifest.PreparationHash {
		t.Fatalf("assembly manifest was not carried through: %#v", normalized.Assembly)
	}
}

func TestNormalizeCreatorPushRequestRejectsLegacyRendererRouting(t *testing.T) {
	normalized, err := normalizeCreatorPushRequest(creatorPushRequest{
		Payload: func() map[string]interface{} {
			payload := creatorPushV2Payload(t, "creator-routing-123")
			payload["pipeline_id"] = "clips.v1"
			return payload
		}(),
	})
	if err == nil || normalized != nil {
		t.Fatal("legacy renderer routing was accepted")
	}
}

func TestNormalizeCreatorPushRequestEnvelopeOverridesPayloadIdentity(t *testing.T) {
	normalized, err := normalizeCreatorPushRequest(creatorPushRequest{
		SourceProvider:   "creator_pc_2",
		SourceJobID:      "creator-source-456",
		TargetExecutorID: "video.assemble.copy.v1",
		Payload:          creatorPushV2Payload(t, "payload-job-ignored-for-forwarding-identity"),
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
	if normalized.TargetExecutorID != "video.assemble.copy.v1" {
		t.Fatalf("TargetExecutorID = %q, want video.assemble.copy.v1@1", normalized.TargetExecutorID)
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
