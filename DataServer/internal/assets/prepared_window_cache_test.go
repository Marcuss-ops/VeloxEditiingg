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
		{name: "canonical evidence", extradata: contract.CanonicalVideoExtradataSHA256, wantHit: true},
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
