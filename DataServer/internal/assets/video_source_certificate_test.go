package assets

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"velox-server/internal/platform/clock"
	"velox-shared/contract"
)

func TestSourceCertificateRegistrationRemovesFuturePreparationTasks(t *testing.T) {
	p := contract.CanonicalVideoProfileV1Default
	revision, err := contract.VideoWindowPreparationRevision(p, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	level, _ := strconv.Atoi(strings.ReplaceAll(p.CodecLevel, ".", ""))
	source := &contract.VideoSourceCertificateV1{Version: 1, SHA256: strings.Repeat("a", 64), SizeBytes: 1000, ProfileID: p.ProfileID, PreparationRevision: revision, DurationUS: 4000000, KeyframesUS: []int64{0, 1000000, 2000000, 3000000}, Evidence: contract.VideoWindowOutputEvidenceV1{Codec: p.Codec, Profile: strings.ToLower(p.CodecProfile), Level: level, Width: p.Width, Height: p.Height, PixelFormat: p.PixelFormat, FPSNum: p.FPSNum, FPSDen: p.FPSDen, TimeBaseNum: 1, TimeBaseDen: 90000, FrameCount: 96, FirstFrameKeyframe: true, ExtradataSHA256: contract.CanonicalVideoExtradataSHA256}}
	repo := &segmentTestRepository{bySHA: map[string]*AssetRecord{"fragment": {AssetID: "fragment", Status: AssetStatusReady}}}
	service := &AssetService{repo: repo, clock: clock.System{}, videoWindowVerifier: newVideoWindowVerifier()}
	if _, err := service.registerSourceCertificate(context.Background(), &Asset{AssetID: "fragment"}, source); err != nil {
		t.Fatal(err)
	}
	plan := contract.CompiledRenderPlanV2{PlanVersion: 2, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64), DurationUS: 1000000, Output: contract.OutputContractV2{Container: "mp4", VideoCodec: "h264", Width: p.Width, Height: p.Height, FPSNum: p.FPSNum, FPSDen: p.FPSDen, PixelFormat: p.PixelFormat, ProfileID: p.ProfileID}, FinalAudio: contract.FinalAudioV2{Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: strings.Repeat("c", 64), SizeBytes: 100, Codec: "aac", SampleRateHz: 48000, Channels: 2, DurationUS: 1000000, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64)}, VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "clip", AssetID: "drive-source", FrameCount: 24, SourceInUS: 1000000, SourceDurationUS: 1000000}}}}, Assets: []contract.AssetRefV2{{AssetID: "drive-source", Kind: "video", SourceKind: "drive", DriveFileID: "drive-1", SHA256: source.SHA256, SizeBytes: 1000}, {AssetID: "audio", Kind: "final_audio", SHA256: strings.Repeat("c", 64), SizeBytes: 100, DurationUS: 1000000}}}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	bindings := []map[string]interface{}{{"asset_id": "drive-source", "drive_file_id": "drive-1", "source_sha256": source.SHA256, "size_bytes": int64(1000)}}
	before, err := service.PlanDriveStockWindowTasks(string(raw), bindings)
	if err != nil || len(before) != 0 {
		t.Fatalf("known immutable source should use direct assembly without waiting for source proof: tasks=%v err=%v", before, err)
	}
	enriched, err := service.EnrichPlanWithSourceCertificates(context.Background(), string(raw), bindings)
	if err != nil {
		t.Fatal(err)
	}
	var certifiedPlan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(enriched), &certifiedPlan); err != nil {
		t.Fatal(err)
	}
	var certifiedSource *contract.VideoSourceCertificateV1
	for _, asset := range certifiedPlan.Assets {
		if asset.AssetID == "drive-source" {
			certifiedSource = asset.SourceCertificate
			break
		}
	}
	if certifiedSource == nil {
		t.Fatalf("registered source proof was not attached to drive-source asset: %+v", certifiedPlan.Assets)
	}
	after, err := service.PlanDriveStockWindowTasks(enriched, bindings)
	if err != nil || len(after) != 0 {
		t.Fatalf("source proof still creates fragments: tasks=%v err=%v", after, err)
	}
	// A changed source size cannot borrow this evidence.
	plan.Assets[0].SizeBytes++
	changed, _ := json.Marshal(plan)
	miss, err := service.EnrichPlanWithSourceCertificates(context.Background(), string(changed), bindings)
	if err == nil || miss != "" {
		t.Fatalf("source size drift was not rejected: plan=%q err=%v", miss, err)
	}
	// A known immutable identity without reusable profile evidence still goes
	// straight to packet assembly. FINALIZE binds runtime SHA/size into the plan;
	// the worker verifies the complete bytes and native assembly checks IDR and
	// stream compatibility before publishing output.
	uncertifiedSHA := strings.Repeat("f", 64)
	plan.Assets[0].SizeBytes = 1000
	plan.Assets[0].SHA256 = ""
	plan.VideoTracks[0].Segments[0].SHA256 = ""
	uncertified, _ := json.Marshal(plan)
	directBindings := []map[string]interface{}{{
		"asset_id": "drive-source", "drive_file_id": "drive-1", "source_sha256": uncertifiedSHA, "size_bytes": int64(1000),
	}}
	directPlanJSON, err := service.EnrichPlanWithSourceCertificates(context.Background(), string(uncertified), directBindings)
	if err != nil {
		t.Fatalf("bind known source identity without certificate: %v", err)
	}
	var directPlan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(directPlanJSON), &directPlan); err != nil {
		t.Fatal(err)
	}
	var directSource *contract.AssetRefV2
	for i := range directPlan.Assets {
		if directPlan.Assets[i].AssetID == "drive-source" {
			directSource = &directPlan.Assets[i]
			break
		}
	}
	if directSource == nil || directSource.SHA256 != uncertifiedSHA || directPlan.VideoTracks[0].Segments[0].SHA256 != uncertifiedSHA {
		t.Fatalf("runtime immutable source identity was not bound into plan: asset=%+v segment=%+v", directPlan.Assets, directPlan.VideoTracks[0].Segments[0])
	}
	directTasks, err := service.PlanDriveStockWindowTasks(directPlanJSON, directBindings)
	if err != nil || len(directTasks) != 0 {
		t.Fatalf("known uncertified source still schedules a fragment: tasks=%v err=%v", directTasks, err)
	}
}

func TestSourceCertificateRegistrationUpgradesMissingBlockProof(t *testing.T) {
	p := contract.CanonicalVideoProfileV1Default
	revision, err := contract.VideoWindowPreparationRevision(p, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	level, _ := strconv.Atoi(strings.ReplaceAll(p.CodecLevel, ".", ""))
	proof := &contract.VideoSourceCertificateV1{
		Version: 1, SHA256: strings.Repeat("a", 64), SizeBytes: 1000,
		ProfileID: p.ProfileID, PreparationRevision: revision, DurationUS: 4000000,
		KeyframesUS: []int64{0, 1000000, 2000000, 3000000},
		Evidence:    contract.VideoWindowOutputEvidenceV1{Codec: p.Codec, Profile: strings.ToLower(p.CodecProfile), Level: level, Width: p.Width, Height: p.Height, PixelFormat: p.PixelFormat, FPSNum: p.FPSNum, FPSDen: p.FPSDen, TimeBaseNum: 1, TimeBaseDen: 90000, FrameCount: 96, FirstFrameKeyframe: true, ExtradataSHA256: contract.CanonicalVideoExtradataSHA256},
	}
	repo := &segmentTestRepository{bySHA: map[string]*AssetRecord{"fragment": {AssetID: "fragment", Status: AssetStatusReady}}}
	service := &AssetService{repo: repo, clock: clock.System{}}
	asset := &Asset{AssetID: "fragment"}
	if _, err := service.registerSourceCertificate(context.Background(), asset, proof); err != nil {
		t.Fatal(err)
	}
	proofWithChunks := *proof
	proofWithChunks.Chunks = &contract.VideoSourceChunksV1{ChunkSize: contract.VideoSourceChunkSize, SHA256: []string{strings.Repeat("b", 64)}}
	if _, err := service.registerSourceCertificate(context.Background(), asset, &proofWithChunks); err != nil {
		t.Fatalf("upgrade source proof with authenticated blocks: %v", err)
	}
	if len(repo.sources) != 2 {
		t.Fatalf("source evidence rows = %d, want legacy proof plus one upgrade", len(repo.sources))
	}
	var saved contract.VideoSourceCertificateV1
	if err := json.Unmarshal([]byte(repo.sources[1].MetadataJSON), &saved); err != nil || saved.Chunks == nil || saved.Validate() != nil {
		t.Fatalf("upgraded source proof = %+v err=%v", saved, err)
	}
	conflicting := proofWithChunks
	conflicting.Chunks = &contract.VideoSourceChunksV1{ChunkSize: contract.VideoSourceChunkSize, SHA256: []string{strings.Repeat("c", 64)}}
	if _, err := service.registerSourceCertificate(context.Background(), asset, &conflicting); err == nil {
		t.Fatal("different block evidence for the same immutable source was accepted")
	}
}
