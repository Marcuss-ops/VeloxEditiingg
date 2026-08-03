package assets

import (
	"context"
	"strings"
	"testing"
)

type rewriteAssetRepository struct {
	assets map[string]*AssetRecord
}

func (r *rewriteAssetRepository) Insert(context.Context, AssetRecord) error { return nil }
func (r *rewriteAssetRepository) GetByID(_ context.Context, assetID string) (*AssetRecord, error) {
	if asset := r.assets[assetID]; asset != nil {
		copy := *asset
		return &copy, nil
	}
	return nil, nil
}
func (r *rewriteAssetRepository) GetBySHA256(context.Context, string) (*AssetRecord, error) {
	return nil, nil
}
func (r *rewriteAssetRepository) UpdateStatus(context.Context, string, string, string) error {
	return nil
}
func (r *rewriteAssetRepository) InsertSource(context.Context, AssetSourceRecord) error { return nil }
func (r *rewriteAssetRepository) LinkToJob(context.Context, string, string, string, int, bool) error {
	return nil
}

func TestRewriteRemoteInputPayloadRewritesCanonicalAssetsInsideScenesJSON(t *testing.T) {
	payload := map[string]interface{}{
		"scenes_json": `[{"clip":{"asset_id":"clip-1","url":"velox-asset://clip-1","duration_ms":7000},"stock":[{"asset_id":"stock-1","url":"velox-asset://stock-1","duration_ms":5000}],"voiceover":{"asset_id":"voice-1","url":"velox-asset://voice-1","duration_ms":12000}}]`,
	}
	service := &AssetService{repo: &rewriteAssetRepository{assets: map[string]*AssetRecord{
		"clip-1":  {AssetID: "clip-1"},
		"stock-1": {AssetID: "stock-1"},
		"voice-1": {AssetID: "voice-1"},
	}}}
	if err := service.RewriteRemoteInputPayload(context.Background(), payload); err != nil {
		t.Fatalf("RewriteRemoteInputPayload: %v", err)
	}
	encoded, ok := payload["scenes_json"].(string)
	if !ok || !strings.Contains(encoded, `"asset_id":"clip-1"`) || !strings.Contains(encoded, `"duration_ms":12000`) {
		t.Fatalf("scenes_json lost canonical nested assets: %s", encoded)
	}
}

func TestRewriteRemoteInputPayloadRejectsIncompleteCanonicalNestedAsset(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"clip": map[string]interface{}{
					"asset_id": "clip-1",
				},
			},
		},
	}
	service := &AssetService{repo: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}}
	err := service.RewriteRemoteInputPayload(context.Background(), payload)
	if err == nil || !strings.Contains(err.Error(), "canonical asset url is required") {
		t.Fatalf("want incomplete canonical asset error, got %v", err)
	}
}

func TestRewriteRemoteInputPayloadRejectsUnregisteredCanonicalAsset(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"clip": map[string]interface{}{
					"asset_id": "missing",
					"url":      "velox-asset://missing",
				},
			},
		},
	}
	service := &AssetService{repo: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}}
	err := service.RewriteRemoteInputPayload(context.Background(), payload)
	if err == nil || !strings.Contains(err.Error(), "is not registered") {
		t.Fatalf("want unregistered canonical asset error, got %v", err)
	}
}
