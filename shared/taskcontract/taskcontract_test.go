package taskcontract

import (
	"strings"
	"testing"

	"velox-shared/contract/assembly"
)

func TestTaskSpecAssemblyIsValidatedAndHashedOutsideRendererPayload(t *testing.T) {
	spec := &TaskSpec{
		Version: 1, JobID: "job-1", ExecutorID: "scene.composite.v1",
		Payload: map[string]interface{}{"job_id": "job-1"},
		Assembly: &assembly.AssemblyJobV1{
			ContractVersion: assembly.ContractVersion, JobID: "job-1", TimelineRevision: 1,
			TimelineHash: "sha256:timeline", Dispatch: assembly.NormalizeDispatch(assembly.ExternalDispatch{SendToVelox: true}),
			Output: assembly.Output{ProfileID: "velox-h264-copy-v1"},
			Assets: []assembly.AssetRequirement{{AssetID: "clip", Kind: assembly.KindSourceClip, Availability: assembly.AvailabilityKnown, URL: "https://example.test/clip", SHA256: strings.Repeat("a", 64), Required: true, State: assembly.AssetReady}},
		},
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	hash, err := spec.SpecHash()
	if err != nil || hash == "" {
		t.Fatalf("SpecHash() = %q, %v", hash, err)
	}
	if _, leaked := spec.Payload["assembly"]; leaked {
		t.Fatal("assembly metadata leaked into renderer payload")
	}
}
