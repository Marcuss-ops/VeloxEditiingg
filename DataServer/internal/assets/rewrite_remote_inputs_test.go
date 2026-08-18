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
func (r *rewriteAssetRepository) UpsertMediaMetadata(context.Context, string, MediaMetadataRecord) error {
	return nil
}
func (r *rewriteAssetRepository) GetMediaMetadata(context.Context, string) (*MediaMetadataRecord, error) {
	return nil, nil
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

func TestRewriteRemoteInputPayloadDefersRawDriveURLWithoutEagerRegistration(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{map[string]interface{}{
			"clip": map[string]interface{}{
				"url": "https://drive.google.com/file/d/ABC123/view",
			},
		}},
	}
	service := &AssetService{repo: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}}
	if err := service.RewriteRemoteInputPayload(context.Background(), payload); err != nil {
		t.Fatalf("RewriteRemoteInputPayload: %v", err)
	}
	clip := payload["scenes"].([]interface{})[0].(map[string]interface{})["clip"].(map[string]interface{})
	// Self-sufficient deferred wire: the scheme carries the kind.
	if got := clip["url"]; got != "velox-drive://ABC123" {
		t.Fatalf("url = %v, want deferred velox-drive wire URL", got)
	}
	if got := clip["asset_id"]; got != "ABC123" {
		t.Fatalf("asset_id = %v, want ABC123", got)
	}
	if _, present := clip["asset_ref_kind"]; present {
		t.Fatalf("legacy asset_ref_kind must not be written: %#v", clip)
	}
}

func TestRewriteRemoteInputPayloadPassesExplicitDeferredDriveScheme(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"clip": map[string]interface{}{
					"asset_id": "drive-file-123456",
					"url":      "velox-drive://drive-file-123456",
				},
			},
		},
	}
	service := &AssetService{repo: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}}
	if err := service.RewriteRemoteInputPayload(context.Background(), payload); err != nil {
		t.Fatalf("RewriteRemoteInputPayload: %v", err)
	}
	clip := payload["scenes"].([]interface{})[0].(map[string]interface{})["clip"].(map[string]interface{})
	if got := clip["url"]; got != "velox-drive://drive-file-123456" {
		t.Fatalf("url = %v, want velox-drive wire value", got)
	}
	if got := clip["asset_id"]; got != "drive-file-123456" {
		t.Fatalf("asset_id = %v, want drive-file-123456", got)
	}
	if _, present := clip["asset_ref_kind"]; present {
		t.Fatalf("legacy asset_ref_kind must not be written: %#v", clip)
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

func TestRewriteRemoteInputPayloadDeclaresTransitionSoundEffects(t *testing.T) {
	payload := map[string]interface{}{
		"transition_sound_effects": map[string]interface{}{
			"enabled": true,
			"sources": []interface{}{"velox-asset://sfx-1"},
		},
	}
	service := &AssetService{repo: &rewriteAssetRepository{assets: map[string]*AssetRecord{
		"sfx-1": {AssetID: "sfx-1", Kind: "sfx", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 1234},
	}}}
	if err := service.RewriteRemoteInputPayload(context.Background(), payload); err != nil {
		t.Fatalf("RewriteRemoteInputPayload: %v", err)
	}
	// The SFX integrity is attached to the transition_sound_effects config,
	// not written to a top-level assets declaration.
	if _, present := payload["assets"]; present {
		t.Fatalf("top-level assets must not be written: %#v", payload["assets"])
	}
	config := payload["transition_sound_effects"].(map[string]interface{})
	declarations, ok := config["assets"].([]map[string]interface{})
	if !ok || len(declarations) != 1 {
		t.Fatalf("transition_sound_effects.assets = %#v, want one declaration", config["assets"])
	}
	if declarations[0]["asset_id"] != "sfx-1" || declarations[0]["uri"] != "velox-asset://sfx-1" || declarations[0]["kind"] != "sfx" || declarations[0]["size_bytes"] != int64(1234) {
		t.Fatalf("unexpected SFX declaration: %#v", declarations[0])
	}
}
