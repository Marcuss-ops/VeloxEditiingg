package assets

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestLookupPreparedWindowAssetRequiresCanonicalAVCC(t *testing.T) {
	for _, tc := range []struct {
		name      string
		extradata string
		wantHit   bool
	}{
		{name: "avcC alone is not a certificate", extradata: contract.CanonicalVideoExtradataSHA256},
		{name: "other valid hash is a miss", extradata: strings.Repeat("e", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity := strings.Repeat("a", 64)
			outputSHA := strings.Repeat("b", 64)
			metadata, err := json.Marshal(map[string]interface{}{
				"window_certificate": map[string]interface{}{
					"output_evidence": map[string]string{"extradata_sha256": tc.extradata},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			record := AssetRecord{
				AssetID: outputSHA, Kind: "video_segment", Status: AssetStatusReady,
				SHA256: outputSHA, MimeType: "video/mp4", SizeBytes: 4096,
				MetadataJSON: string(metadata),
			}
			repo := &segmentTestRepository{
				bySHA:   map[string]*AssetRecord{outputSHA: &record},
				sources: []AssetSourceRecord{{AssetID: record.AssetID, SourceReference: "prepared-window:" + identity}},
			}
			service := &AssetService{repo: repo}
			binding, err := service.LookupPreparedWindowAsset(context.Background(), identity)
			if err != nil {
				t.Fatalf("LookupPreparedWindowAsset: %v", err)
			}
			if (binding != nil) != tc.wantHit {
				t.Fatalf("binding = %+v, want hit=%v", binding, tc.wantHit)
			}
			if binding != nil && binding.ExtradataSHA256 != contract.CanonicalVideoExtradataSHA256 {
				t.Fatalf("binding avcC = %q", binding.ExtradataSHA256)
			}
		})
	}
}

func TestPreparedWindowLookupValidatesEntireCertificate(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	revision, err := contract.VideoWindowPreparationRevision(profile, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	request := contract.VideoPrepareWindowV1{Version: 1, Source: contract.AssetRefV2{AssetID: "source", SHA256: strings.Repeat("a", 64), SizeBytes: 100, Kind: "video"}, SourceFrameCount: 48, SourceFPSNum: 24, SourceFPSDen: 1, OutputFrameCount: 48, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64), OutputProfileID: profile.ProfileID, OutputProfileRevision: revision}
	identity, err := contract.VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		t.Fatal(err)
	}
	certificate := contract.VideoPrepareWindowCertificateV1{Version: 1, SourceSHA256: request.Source.SHA256, SourceAssetID: "source", SourceSizeBytes: 100, SourceFrameCount: 48, SourceFPSNum: 24, SourceFPSDen: 1, OutputFrameCount: 48, OutputProfileID: profile.ProfileID, OutputProfileRevision: revision, TimelineRevision: 1, TimelineSHA256: request.TimelineSHA256, EncoderVersion: "ffmpeg", EncoderBuildSHA256: strings.Repeat("c", 64), OutputSHA256: strings.Repeat("d", 64), OutputSizeBytes: 200, ClosedGOPAttested: true, PacketCopyUsed: true, OutputEvidence: contract.VideoWindowOutputEvidenceV1{Codec: "h264", FrameCount: 48, FirstFrameKeyframe: true, ExtradataSHA256: contract.CanonicalVideoExtradataSHA256, Width: profile.Width, Height: profile.Height, FPSNum: 24, FPSDen: 1, TimeBaseNum: 1, TimeBaseDen: 90000}}
	if err := certificate.ValidateFor(request); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*contract.VideoPrepareWindowCertificateV1)
		hit    bool
	}{
		{"complete", func(c *contract.VideoPrepareWindowCertificateV1) {}, true},
		{"different bytes", func(c *contract.VideoPrepareWindowCertificateV1) { c.OutputSHA256 = strings.Repeat("e", 64) }, false},
		{"wrong window", func(c *contract.VideoPrepareWindowCertificateV1) { c.SourceStartFrame = 24 }, false},
		{"missing packet copy", func(c *contract.VideoPrepareWindowCertificateV1) { c.PacketCopyUsed = false }, false},
		{"legacy revision", func(c *contract.VideoPrepareWindowCertificateV1) {
			c.OutputProfileRevision = "video-window-preparer.v1:" + strings.Repeat("f", 64)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := certificate
			tc.mutate(&c)
			data, err := json.Marshal(map[string]interface{}{"window_certificate": c})
			if err != nil {
				t.Fatal(err)
			}
			asset := &Asset{AssetID: "fragment", Status: AssetStatusReady, MimeType: "video/mp4", SHA256: certificate.OutputSHA256, SizeBytes: 200, MetadataJSON: string(data)}
			hit, err := preparedWindowBindingFromAsset(identity, asset)
			if err != nil || (hit != nil) != tc.hit {
				t.Fatalf("hit=%+v err=%v, want hit=%v", hit, err, tc.hit)
			}
		})
	}
}
