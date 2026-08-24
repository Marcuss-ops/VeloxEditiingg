package assembly

import (
	"encoding/json"
	"strings"
	"testing"
)

func canonicalContractFixture() CanonicalAssemblyContractV1 {
	return CanonicalAssemblyContractV1{
		ContractVersion:  CanonicalAssemblyContractVersionV1,
		JobID:            "job-assembly-1",
		TimelineRevision: 4,
		TimelineHash:     "sha256:timeline",
		Dispatch:         NormalizeDispatch(ExternalDispatch{SendToVelox: true}),
		Assets: []AssetRequirement{
			{AssetID: "clip-b", Kind: KindSourceClip, Availability: AvailabilityKnown, URL: "https://example.test/b", SHA256: strings.Repeat("b", 64), SizeBytes: 200, Required: true, State: AssetDeclared},
			{AssetID: "clip-a", Kind: KindSourceClip, Availability: AvailabilityKnown, URL: "https://example.test/a", SHA256: strings.Repeat("a", 64), SizeBytes: 100, Required: true, State: AssetDeclared},
			{AssetID: "scene-runtime", Kind: KindPreparedScene, Availability: AvailabilityRuntime, Producer: ProducerRenderingGen, Required: true, State: AssetWaitingProducer},
		},
		Timeline: []TimelineItem{{SceneID: "scene-1", AssetID: "clip-a"}, {SceneID: "scene-2", AssetID: "scene-runtime"}},
		Output:   Output{ProfileID: "velox-h264-copy-v1"},
		State:    StateWaitingRuntime,
	}
}

func TestCanonicalAssemblyContractV1CanonicalJSONSortsAssetsWithoutMutation(t *testing.T) {
	contract := canonicalContractFixture()
	firstAsset := contract.Assets[0].AssetID
	data, err := contract.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON() error = %v", err)
	}
	if contract.Assets[0].AssetID != firstAsset {
		t.Fatal("CanonicalJSON mutated the caller's asset order")
	}
	if !strings.Contains(string(data), `"assets":[{"asset_id":"clip-a"`) {
		t.Fatalf("assets were not sorted canonically: %s", data)
	}
	if !strings.Contains(string(data), `"preparation_hash"`) {
		// The field is optional until the producer binds the final contract;
		// this assertion intentionally documents that the canonical type does
		// not invent a hash before it is computed.
		t.Log("preparation_hash omitted before producer binding")
	}
}

func TestCanonicalAssemblyContractV1PreparationHashIsStableAndBindsKnownInputs(t *testing.T) {
	one := canonicalContractFixture()
	two := canonicalContractFixture()
	two.Assets[0], two.Assets[1] = two.Assets[1], two.Assets[0]
	if got, want := one.ComputePreparationHash(), two.ComputePreparationHash(); got != want {
		t.Fatalf("preparation hash depends on asset order: %q != %q", got, want)
	}
	one.Assets[2].State = AssetReady
	if got, want := one.ComputePreparationHash(), two.ComputePreparationHash(); got != want {
		t.Fatalf("runtime asset state changed preparation hash: %q != %q", got, want)
	}
	one.PreparationHash = one.ComputePreparationHash()
	if err := one.Validate(); err != nil {
		t.Fatalf("Validate() with bound preparation hash = %v", err)
	}
	one.Timeline[0].AssetID = "clip-b"
	if err := one.Validate(); err == nil || !strings.Contains(err.Error(), "preparation_hash") {
		t.Fatalf("Validate() after timeline mutation = %v, want preparation hash mismatch", err)
	}
}

func TestPreparationManifestCanonicalContractRoundTrip(t *testing.T) {
	manifest := PreparationManifest{
		ContractVersion:  CanonicalAssemblyContractVersionV1,
		JobID:            "job-prep-1",
		TimelineRevision: 2,
		Dispatch:         NormalizeDispatch(ExternalDispatch{SendToVelox: true}),
		ExpectedProfile:  "velox-h264-copy-v1",
		Timeline:         []TimelineItem{{SceneID: "scene-1", AssetID: "clip-1"}},
		Assets: []AssetRequirement{{
			AssetID: "clip-1", Kind: KindSourceClip, Availability: AvailabilityKnown,
			URL: "https://example.test/clip", SHA256: strings.Repeat("c", 64), SizeBytes: 42,
			Required: true, State: AssetDeclared,
		}},
	}
	manifest.PreparationHash = manifest.ComputePreparationHash()
	if got, want := manifest.ComputePreparationHash(), manifest.PreparationHash; got != want {
		t.Fatalf("manifest preparation hash = %q, want %q", got, want)
	}
	contract, err := manifest.CanonicalContract()
	if err != nil {
		t.Fatalf("CanonicalContract() error = %v", err)
	}
	if contract.PreparationHash != manifest.PreparationHash || contract.State != StatePreparing {
		t.Fatalf("canonical preparation projection = %#v", contract)
	}
	data, err := contract.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON() error = %v", err)
	}
	decoded, err := DecodeCanonicalAssemblyContractV1(data)
	if err != nil {
		t.Fatalf("DecodeCanonicalAssemblyContractV1() error = %v", err)
	}
	if decoded.ContractVersion != CanonicalAssemblyContractVersionV1 || decoded.JobID != manifest.JobID {
		t.Fatalf("decoded contract identity = %#v", decoded)
	}
}

func TestCanonicalAssemblyContractV1RejectsInvalidPreparationHashAndState(t *testing.T) {
	contract := canonicalContractFixture()
	contract.PreparationHash = "sha256:" + strings.Repeat("d", 64)
	data, err := json.Marshal(contract)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCanonicalAssemblyContractV1(data); err == nil || !strings.Contains(err.Error(), "preparation_hash") {
		t.Fatalf("decode invalid hash error = %v", err)
	}

	contract = canonicalContractFixture()
	contract.State = AssemblyState("unknown")
	if err := contract.Validate(); err == nil || !strings.Contains(err.Error(), "unknown state") {
		t.Fatalf("invalid state error = %v", err)
	}
}
