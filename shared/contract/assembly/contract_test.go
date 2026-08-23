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
