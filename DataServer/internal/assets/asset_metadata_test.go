package assets

import (
	"context"
	"testing"
)

type metadataTestRepo struct {
	records map[string]*AssetRecord
}

func (r *metadataTestRepo) Insert(context.Context, AssetRecord) error { return nil }
func (r *metadataTestRepo) GetByID(_ context.Context, id string) (*AssetRecord, error) {
	return r.records[id], nil
}
func (r *metadataTestRepo) GetBySHA256(context.Context, string) (*AssetRecord, error) {
	return nil, nil
}
func (r *metadataTestRepo) UpdateStatus(context.Context, string, string, string) error { return nil }
func (r *metadataTestRepo) InsertSource(context.Context, AssetSourceRecord) error      { return nil }
func (r *metadataTestRepo) LinkToJob(context.Context, string, string, string, int, bool) error {
	return nil
}

func TestAttachAssetMetadataPublishesVerifiedIntegrity(t *testing.T) {
	assetID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	repo := &metadataTestRepo{records: map[string]*AssetRecord{
		assetID: {
			AssetID: assetID, Kind: RoleSceneImage, Status: AssetStatusReady,
			SHA256: assetID, SizeBytes: 123, MimeType: "image/png",
		},
	}}
	service := &AssetService{repo: repo}
	payload := map[string]interface{}{
		"scenes": []interface{}{map[string]interface{}{
			"image_link": VeloxAssetScheme + "://" + assetID,
		}},
	}

	if err := service.attachAssetMetadata(context.Background(), payload); err != nil {
		t.Fatalf("attachAssetMetadata: %v", err)
	}
	assets, ok := payload["assets"].([]interface{})
	if !ok || len(assets) != 1 {
		t.Fatalf("assets = %#v, want one declaration", payload["assets"])
	}
	declaration, ok := assets[0].(map[string]interface{})
	if !ok || declaration["sha256"] != assetID || declaration["size_bytes"] != int64(123) {
		t.Fatalf("asset declaration = %#v, want verified hash and size", assets[0])
	}
}
