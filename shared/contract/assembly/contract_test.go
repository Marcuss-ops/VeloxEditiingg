package assembly

import (
	"strings"
	"testing"
)

func TestNormalizeDispatch(t *testing.T) {
	if got := NormalizeDispatch(ExternalDispatch{}); got.Mode != ModeNever || got.StartPrepare {
		t.Fatalf("disabled dispatch = %#v", got)
	}
	if got := NormalizeDispatch(ExternalDispatch{SendToVelox: true}); got.Mode != ModeEager || !got.StartPrepare {
		t.Fatalf("enabled dispatch = %#v", got)
	}
}

func TestAssemblyJobV1ValidateAndDeriveWaitingState(t *testing.T) {
	job := AssemblyJobV1{
		ContractVersion: ContractVersion, JobID: "job-123", TimelineRevision: 1,
		TimelineHash: "sha256:timeline", Dispatch: NormalizeDispatch(ExternalDispatch{SendToVelox: true}),
		Output: Output{ProfileID: "velox-h264-copy-v1"},
		Assets: []AssetRequirement{
			{AssetID: "clip-1", Kind: KindSourceClip, Availability: AvailabilityKnown, URL: "https://example.test/clip.mp4", SHA256: strings.Repeat("a", 64), Required: true, State: AssetReady},
			{AssetID: "scene-7", Kind: KindPreparedScene, Availability: AvailabilityRuntime, Producer: ProducerRenderingGen, Required: true, State: AssetWaitingProducer},
		},
		Timeline: []TimelineItem{{SceneID: "scene-1", AssetID: "clip-1"}, {SceneID: "scene-7", AssetID: "scene-7"}},
	}
	if err := job.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if got := job.DeriveState(); got != StateWaitingRuntime {
		t.Fatalf("DeriveState() = %q, want %q", got, StateWaitingRuntime)
	}
	job.Assets[1].State = AssetReady
	if got := job.DeriveState(); got != StateReadyToFinalize {
		t.Fatalf("ready DeriveState() = %q, want %q", got, StateReadyToFinalize)
	}
}

func TestAssemblyJobV1RejectsUnknownVersionAndIncompleteKnownAsset(t *testing.T) {
	base := AssemblyJobV1{ContractVersion: ContractVersion, JobID: "job", TimelineRevision: 1, TimelineHash: "hash", Dispatch: NormalizeDispatch(ExternalDispatch{}), Output: Output{ProfileID: "profile"}}
	base.Assets = []AssetRequirement{{AssetID: "clip", Kind: KindSourceClip, Availability: AvailabilityKnown, Required: true, State: AssetDeclared}}
	if err := base.Validate(); err == nil {
		t.Fatal("expected incomplete known asset to be rejected")
	}
	base.Assets[0].URL = "https://example.test/clip"
	base.Assets[0].SHA256 = strings.Repeat("z", 64)
	if err := base.Validate(); err == nil {
		t.Fatal("expected malformed sha256 to be rejected")
	}
	base.Assets[0].SHA256 = strings.Repeat("b", 64)
	base.ContractVersion = "velox.assembly.v2"
	if err := base.Validate(); err == nil {
		t.Fatal("expected unknown contract version to be rejected")
	}
}

func TestPreparationManifestHashIsOrderIndependent(t *testing.T) {
	assets := []AssetRequirement{
		{AssetID: "b", Kind: KindSourceClip, Availability: AvailabilityKnown, URL: "https://example.test/b", SHA256: strings.Repeat("b", 64), Required: true, State: AssetDeclared},
		{AssetID: "a", Kind: KindSourceClip, Availability: AvailabilityKnown, URL: "https://example.test/a", SHA256: strings.Repeat("a", 64), Required: true, State: AssetDeclared},
	}
	one := PreparationManifest{ContractVersion: ContractVersion, JobID: "job", TimelineRevision: 1, Dispatch: NormalizeDispatch(ExternalDispatch{SendToVelox: true}), ExpectedProfile: "profile", Assets: assets}
	two := one
	two.Assets = []AssetRequirement{assets[1], assets[0]}
	if one.ComputePreparationHash() != two.ComputePreparationHash() {
		t.Fatal("preparation hash depends on asset ordering")
	}
	one.PreparationHash = one.ComputePreparationHash()
	if err := one.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestLeaseKindsSeparatePreparationFromExecution(t *testing.T) {
	prep := PreparationLease{LeaseID: "p", JobID: "job", WorkerID: "worker", PreparationHash: "sha256:hash", Kind: LeasePreparation, ExpiresAt: "2026-08-23T00:00:00Z"}
	if err := prep.Validate(); err != nil {
		t.Fatalf("preparation lease Validate() error = %v", err)
	}
	exec := ExecutionLease{LeaseID: "e", JobID: "job", WorkerID: "worker", Kind: LeaseExecution}
	if err := exec.Validate(); err != nil {
		t.Fatalf("execution lease Validate() error = %v", err)
	}
}

func TestSelectPreferredWorkerIsCacheAwareAndStable(t *testing.T) {
	request := PlacementRequest{RequiredCapabilities: []string{"assembly.v1"}, AssetSHA256: []string{"a", "b", "c"}}
	decision, err := SelectPreferredWorker([]WorkerPlacementSnapshot{
		{WorkerID: "worker-c", Available: true, CapacityAuthoritative: true, ActiveExecutionSlots: 0, MaxExecutionSlots: 1, FreeDiskBytes: 100, Capabilities: []string{"assembly.v1"}, CachedSHA256: []string{"a", "b"}},
		{WorkerID: "worker-b", Available: true, CapacityAuthoritative: true, ActiveExecutionSlots: 0, MaxExecutionSlots: 1, FreeDiskBytes: 100, Capabilities: []string{"assembly.v1"}, CachedSHA256: []string{"a", "b", "c"}},
		{WorkerID: "worker-a", Available: true, CapacityAuthoritative: false, MaxExecutionSlots: 1, Capabilities: []string{"assembly.v1"}, CachedSHA256: []string{"a", "b", "c"}},
	}, request)
	if err != nil {
		t.Fatalf("SelectPreferredWorker() error = %v", err)
	}
	if decision.WorkerID != "worker-b" || decision.CachedAssets != 3 || decision.MissingAssets != 0 {
		t.Fatalf("decision = %#v, want cache-complete worker-b", decision)
	}
}
