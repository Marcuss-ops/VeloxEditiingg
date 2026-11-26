package taskcontract

import (
	"fmt"
	"strings"
	"testing"

	"velox-shared/contract"
)

func validWindowRequest() contract.VideoPrepareWindowV1 {
	return contract.VideoPrepareWindowV1{
		Version:               contract.VideoPrepareWindowVersion,
		Source:                contract.AssetRefV2{AssetID: "drive-window", AssetKey: "clip-window", Kind: "video", SHA256: strings.Repeat("a", 64), SizeBytes: 4096},
		SourceStartFrame:      0,
		SourceFrameCount:      10,
		SourceFPSNum:          30,
		SourceFPSDen:          1,
		OutputFrameCount:      10,
		TimelineRevision:      1,
		TimelineSHA256:        strings.Repeat("b", 64),
		TimelineStartFrame:    0,
		OutputProfileID:       "stock-window-h264",
		OutputProfileRevision: "video-window-preparer.v1:" + strings.Repeat("c", 64),
	}
}

func windowSpec(t *testing.T, request contract.VideoPrepareWindowV1) TaskInput {
	t.Helper()
	return TaskInput{
		ExecutorID:      contract.VideoPrepareWindowExecutorID,
		ExecutorVersion: 1,
		Payload: map[string]interface{}{
			contract.VideoPrepareWindowPayloadKey: request,
		},
	}
}

func TestPrepareWindowDeclaresOnlyItsOwnSource(t *testing.T) {
	spec := windowSpec(t, validWindowRequest())
	// Simulate the r67f shape: the job payload references 246 other assets.
	siblings := make([]map[string]interface{}, 0, 246)
	for i := 0; i < 246; i++ {
		siblings = append(siblings, map[string]interface{}{
			"asset_key":  fmt.Sprintf("sibling-%03d", i),
			"asset_id":   fmt.Sprintf("drive-sibling-%03d", i),
			"sha256":     strings.Repeat("d", 64),
			"size_bytes": 1024,
		})
	}
	spec.Payload["scenes"] = siblings
	spec.Payload["runtime_assets"] = siblings

	taskContract, ok := Default().Lookup(contract.VideoPrepareWindowExecutorID, 1)
	if !ok {
		t.Fatal("window contract is not registered")
	}
	assets, err := taskContract.RequiredAssets(spec)
	if err != nil {
		t.Fatalf("RequiredAssets: %v", err)
	}
	if len(assets) != 1 {
		t.Fatalf("RequiredAssets returned %d assets, want exactly 1 (%v)", len(assets), assetKeys(assets))
	}
	if assets[0].Key != "clip-window" || assets[0].Role != "video_prepare_source" {
		t.Fatalf("asset = %+v, want the declared window source", assets[0])
	}
	if taskContract.RequiresPrefetch(spec) {
		t.Fatal("window preparation must not be blocked by the job-wide prefetch gate")
	}
	if got := taskContract.CompletionKind(); got != CompletionPreparedSegment {
		t.Fatalf("completion kind = %q, want %q", got, CompletionPreparedSegment)
	}
	if err := taskContract.Validate(spec); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestSourceCertificationHasExplicitArtifactFreeCompletion(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	revision, err := contract.VideoWindowPreparationRevision(profile, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	request := contract.VideoSourceCertificationV1{
		Version:         contract.VideoSourceCertificationVersion,
		Source:          contract.AssetRefV2{AssetID: "source", SourceKind: "drive", DriveFileID: "drive-1", Kind: "video", SizeBytes: 4096},
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	spec := TaskInput{ExecutorID: contract.VideoSourceCertificationExecutorID, ExecutorVersion: 1, Payload: map[string]interface{}{contract.VideoSourceCertificationPayloadKey: request}}
	taskContract, ok := Default().Lookup(contract.VideoSourceCertificationExecutorID, 1)
	if !ok {
		t.Fatal("source certification contract is not registered")
	}
	if err := taskContract.Validate(spec); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	assets, err := taskContract.RequiredAssets(spec)
	if err != nil || len(assets) != 1 || assets[0].Role != "video_source_certification" || assets[0].SHA256 != "" {
		t.Fatalf("source inputs = %+v err=%v", assets, err)
	}
	if taskContract.CompletionKind() != CompletionSourceCertificate || taskContract.RequiresPrefetch(spec) {
		t.Fatalf("source certificate completion kind/prefetch = %q/%v", taskContract.CompletionKind(), taskContract.RequiresPrefetch(spec))
	}
}

func TestAssemblyDeclaresPlanAndRuntimeInputs(t *testing.T) {
	plan := contract.CompiledRenderPlanV2{
		PlanVersion:      contract.CompiledPlanVersionV2,
		TimelineRevision: 1,
		TimelineSHA256:   strings.Repeat("e", 64),
		DurationUS:       10_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "h264", Width: 1920, Height: 1080,
			FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p", BFrames: 0,
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio-final",
			SHA256: strings.Repeat("f", 64), SizeBytes: 2048, Codec: "aac",
			SampleRateHz: 48_000, Channels: 2, DurationUS: 10_000_000,
			TimelineRevision: 1, TimelineSHA256: strings.Repeat("e", 64),
		},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "seg-a", AssetID: "clip-a", SHA256: strings.Repeat("2", 64),
			TimelineStartFrame: 0, FrameCount: 300, SourceInUS: 0, SourceDurationUS: 10_000_000,
		}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "clip-b", AssetKey: "clip-b", Kind: "video", MIME: "video/mp4", SHA256: strings.Repeat("1", 64), SizeBytes: 100, DurationUS: 5_000_000, Width: 1920, Height: 1080},
			{AssetID: "clip-a", AssetKey: "clip-a", Kind: "video", MIME: "video/mp4", SHA256: strings.Repeat("2", 64), SizeBytes: 200, DurationUS: 10_000_000, Width: 1920, Height: 1080},
			{AssetID: "audio-final", Kind: "final_audio", MIME: "audio/mp4", SHA256: strings.Repeat("f", 64), SizeBytes: 2048, DurationUS: 10_000_000},
		},
	}
	encoded, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical plan JSON: %v", err)
	}
	spec := TaskInput{
		ExecutorID:      contract.VideoAssemblyCopyExecutorID,
		ExecutorVersion: 1,
		Payload: map[string]interface{}{
			contract.PayloadKeyCompiledRenderPlanJSON: string(encoded),
			"runtime_assets": []map[string]interface{}{
				{"asset_key": "overlay-z", "asset_id": "overlay-z", "sha256": strings.Repeat("3", 64), "size_bytes": 50, "role": "overlay"},
				{"asset_key": "clip-a", "asset_id": "clip-a", "sha256": strings.Repeat("2", 64), "size_bytes": 200, "role": "video"},
			},
		},
	}
	assets, err := videoAssemblyContract{executorID: contract.VideoAssemblyCopyExecutorID}.RequiredAssets(spec)
	if err != nil {
		t.Fatalf("RequiredAssets: %v", err)
	}
	got := assetKeys(assets)
	want := []string{"audio-final", "clip-a", "clip-b", "overlay-z"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("asset keys = %v, want %v", got, want)
	}
	if Default().RequiresPrefetch(spec) {
		t.Fatal("packet-copy assembly fetches content-addressed inputs on demand")
	}
	sliceSpec := spec
	sliceSpec.ExecutorID = contract.VideoAssemblySliceExecutorID
	if Default().RequiresPrefetch(sliceSpec) {
		t.Fatal("legacy slice adapter must share copy assembly on-demand fetching")
	}
	sliceContract, ok := Default().Lookup(sliceSpec.ExecutorID, sliceSpec.ExecutorVersion)
	if !ok {
		t.Fatal("legacy slice assembly contract is not registered")
	}
	if err := sliceContract.Validate(sliceSpec); err != nil {
		t.Fatalf("persisted slice task was not kept executable during routing migration: %v", err)
	}
}

func TestUnregisteredExecutorIsConservative(t *testing.T) {
	spec := TaskInput{ExecutorID: "scene.composite.v1", ExecutorVersion: 1, Payload: map[string]interface{}{}}
	if !Default().RequiresPrefetch(spec) {
		t.Fatal("an unregistered executor must not silently bypass the prefetch gate")
	}
	assets, authoritative, err := Default().RequiredAssets(spec)
	if err != nil {
		t.Fatalf("RequiredAssets: %v", err)
	}
	if authoritative || assets != nil {
		t.Fatalf("unregistered executor must defer to the legacy path, got authoritative=%t assets=%v", authoritative, assets)
	}
}

func TestRegistryRejectsAmbiguousRegistrations(t *testing.T) {
	registry := NewRegistry()
	contractImpl := videoPrepareWindowContract{}
	if err := registry.Register("x", 1, contractImpl); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := registry.Register("x", 1, contractImpl); err == nil {
		t.Fatal("duplicate registration must fail")
	}
	if err := registry.Register("", 1, contractImpl); err == nil {
		t.Fatal("empty executor id must fail")
	}
	if err := registry.Register("y", 0, contractImpl); err == nil {
		t.Fatal("zero version must fail")
	}
	if err := registry.Register("z", 1, nil); err == nil {
		t.Fatal("nil contract must fail")
	}
}

func TestPrepareWindowValidationRejectsMalformedPayload(t *testing.T) {
	taskContract, ok := Default().Lookup(contract.VideoPrepareWindowExecutorID, 1)
	if !ok {
		t.Fatal("window contract is not registered")
	}
	spec := TaskInput{
		ExecutorID:      contract.VideoPrepareWindowExecutorID,
		ExecutorVersion: 1,
		Payload:         map[string]interface{}{contract.VideoPrepareWindowPayloadKey: map[string]interface{}{"version": 1}},
	}
	if err := taskContract.Validate(spec); err == nil {
		t.Fatal("malformed window request must fail validation")
	}
	if _, err := taskContract.RequiredAssets(spec); err == nil {
		t.Fatal("malformed window request must not declare assets")
	}
}

func assetKeys(assets []RequiredAsset) []string {
	out := make([]string, 0, len(assets))
	for _, asset := range assets {
		out = append(out, asset.Key)
	}
	return out
}
