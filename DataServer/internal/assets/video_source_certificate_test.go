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
	if err != nil || len(before) != 1 {
		t.Fatalf("before source proof: tasks=%v err=%v", before, err)
	}
	enriched, err := service.EnrichPlanWithSourceCertificates(context.Background(), string(raw), bindings)
	if err != nil {
		t.Fatal(err)
	}
	after, err := service.PlanDriveStockWindowTasks(enriched, bindings)
	if err != nil || len(after) != 0 {
		t.Fatalf("source proof still creates fragments: tasks=%v err=%v", after, err)
	}
	// A changed source size cannot borrow this evidence.
	plan.Assets[0].SizeBytes++
	changed, _ := json.Marshal(plan)
	miss, err := service.EnrichPlanWithSourceCertificates(context.Background(), string(changed), bindings)
	if err != nil || miss != string(changed) {
		t.Fatalf("source size drift reused evidence: err=%v", err)
	}
}
