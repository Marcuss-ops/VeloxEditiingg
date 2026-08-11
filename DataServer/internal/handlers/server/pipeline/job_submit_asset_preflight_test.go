package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	voiceoverassets "velox-server/internal/assets"

	"github.com/gin-gonic/gin"
)

// preflightTestRepo is a minimal voiceoverassets.AssetRepository for the
// admission-gate tests: it stores asset rows + optional verified media
// metadata rows and never probes (the media gate's registry-hit path).
type preflightTestRepo struct {
	assets   map[string]*voiceoverassets.AssetRecord
	metadata map[string]*voiceoverassets.MediaMetadataRecord
}

func (r *preflightTestRepo) Insert(context.Context, voiceoverassets.AssetRecord) error { return nil }
func (r *preflightTestRepo) GetByID(_ context.Context, assetID string) (*voiceoverassets.AssetRecord, error) {
	if a := r.assets[assetID]; a != nil {
		copied := *a
		return &copied, nil
	}
	return nil, nil
}
func (r *preflightTestRepo) GetBySHA256(context.Context, string) (*voiceoverassets.AssetRecord, error) {
	return nil, nil
}
func (r *preflightTestRepo) UpdateStatus(context.Context, string, string, string) error { return nil }
func (r *preflightTestRepo) InsertSource(context.Context, voiceoverassets.AssetSourceRecord) error {
	return nil
}
func (r *preflightTestRepo) LinkToJob(context.Context, string, string, string, int, bool) error {
	return nil
}
func (r *preflightTestRepo) UpsertMediaMetadata(context.Context, string, voiceoverassets.MediaMetadataRecord) error {
	return nil
}
func (r *preflightTestRepo) GetMediaMetadata(_ context.Context, assetID string) (*voiceoverassets.MediaMetadataRecord, error) {
	if m := r.metadata[assetID]; m != nil {
		copied := *m
		return &copied, nil
	}
	return nil, nil
}

// preflightTestBlobStore is a minimal BlobStore with the final-blob reader
// needed by the preflight's integrity check.
type preflightTestBlobStore struct {
	root string
}

func (b preflightTestBlobStore) StagingPath(string, string, string) (string, error) { return "", nil }
func (b preflightTestBlobStore) PromoteToFinal(string, string) (string, error)      { return "", nil }
func (b preflightTestBlobStore) RemoveStaging(string) error                         { return nil }
func (b preflightTestBlobStore) FinalPath(_, _, _ string) string                    { return "" }
func (b preflightTestBlobStore) FinalDir() string                                   { return b.root }
func (b preflightTestBlobStore) ReadFinal(storageKey string) (*os.File, error) {
	return os.Open(filepath.Join(b.root, storageKey))
}

// newPreflightGateService builds the asset service used by the admission
// gate tests: a real AssetService (canonical MediaMetadataResolver) over a
// fake repo + blob store, so the media gate runs the REAL ffprobe path — a
// missing/renamed binary or a non-media blob fails closed deterministically.
func newPreflightGateService(t *testing.T, asset *voiceoverassets.AssetRecord, metadata *voiceoverassets.MediaMetadataRecord) *voiceoverassets.AssetService {
	t.Helper()
	root := t.TempDir()
	content := []byte("preflight gate blob bytes")
	if err := os.WriteFile(filepath.Join(root, asset.StorageKey), content, 0o644); err != nil {
		t.Fatal(err)
	}
	if asset.SHA256 == "" {
		asset.SHA256 = fmt.Sprintf("%x", sha256.Sum256(content))
	}
	if asset.SizeBytes == 0 {
		asset.SizeBytes = int64(len(content))
	}
	repo := &preflightTestRepo{assets: map[string]*voiceoverassets.AssetRecord{asset.AssetID: asset}}
	if metadata != nil {
		repo.metadata = map[string]*voiceoverassets.MediaMetadataRecord{asset.AssetID: metadata}
	}
	return voiceoverassets.NewAssetService(repo, preflightTestBlobStore{root: root}, nil, nil)
}

// runAssetPreflight executes checkAssetPreflight against the service and
// returns whether the handler wrote a response (true = rejected).
func runAssetPreflight(t *testing.T, svc *voiceoverassets.AssetService, sourceURL string) (bool, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(recorder)
	// gin.CreateTestContext does not populate c.Request; checkAssetPreflight
	// calls c.Request.Context(), so install one explicitly.
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/jobs", nil)
	req := SubmitJobRequest{
		Scenes: []SubmitScene{{Text: "scene text", DurationSeconds: 2, Clip: &SubmitClip{AssetID: "asset-1", URL: sourceURL}}},
	}
	h := &Handlers{assetService: svc}
	handled := checkAssetPreflight(c, h, req)
	return handled, recorder
}

// TestCheckAssetPreflight_RejectsUnverifiableMediaAsset pins the Fase C2
// admission gate: a local media asset whose media metadata cannot be verified
// (real ffprobe rejects the non-media blob) fails closed with 422 and
// media_metadata=false in the details.
func TestCheckAssetPreflight_RejectsUnverifiableMediaAsset(t *testing.T) {
	svc := newPreflightGateService(t, &voiceoverassets.AssetRecord{
		AssetID: "asset-1", Status: voiceoverassets.AssetStatusReady, MimeType: "video/mp4", StorageKey: "asset.mp4",
	}, nil)

	handled, recorder := runAssetPreflight(t, svc, "velox-asset://asset-1")
	if !handled {
		t.Fatal("checkAssetPreflight must reject an unverifiable media asset")
	}
	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", recorder.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["error"] != "asset_preflight_failed" {
		t.Fatalf("error = %v, want asset_preflight_failed", body["error"])
	}
	details, ok := body["details"].([]interface{})
	if !ok || len(details) != 1 {
		t.Fatalf("details = %#v, want one item", body["details"])
	}
	first, ok := details[0].(map[string]interface{})
	if !ok || first["media_metadata"] != false {
		t.Fatalf("details[0] = %#v, want media_metadata=false", details[0])
	}
	if first["issue"] != "media_metadata_unavailable" {
		t.Fatalf("details[0] issue = %v, want media_metadata_unavailable", first["issue"])
	}
}

// TestCheckAssetPreflight_AcceptsVerifiedMediaAsset pins the registry-hit
// path at the gate: a media asset with a verified asset_media_metadata row
// passes the admission gate (no response written).
func TestCheckAssetPreflight_AcceptsVerifiedMediaAsset(t *testing.T) {
	svc := newPreflightGateService(t, &voiceoverassets.AssetRecord{
		AssetID: "asset-1", Status: voiceoverassets.AssetStatusReady, MimeType: "video/mp4", StorageKey: "asset.mp4",
	}, &voiceoverassets.MediaMetadataRecord{
		AssetID: "asset-1", Container: "mp4", DurationMs: 5000, VideoCodec: "h264",
		MetadataVerifiedAt: "2026-08-11T00:00:00Z", MetadataSchemaVersion: 1,
	})

	handled, _ := runAssetPreflight(t, svc, "velox-asset://asset-1")
	if handled {
		t.Fatal("checkAssetPreflight must accept a media asset with verified registry metadata")
	}
}

// TestCheckAssetPreflight_AcceptsNonMediaAsset pins the N/A path: non-media
// assets never trigger the media gate.
func TestCheckAssetPreflight_AcceptsNonMediaAsset(t *testing.T) {
	svc := newPreflightGateService(t, &voiceoverassets.AssetRecord{
		AssetID: "asset-1", Status: voiceoverassets.AssetStatusReady, MimeType: "font/ttf", StorageKey: "font.ttf",
	}, nil)

	handled, _ := runAssetPreflight(t, svc, "velox-asset://asset-1")
	if handled {
		t.Fatal("checkAssetPreflight must accept a non-media asset (N/A media gate)")
	}
}

func TestCollectAssetPreflightRequirementsDeduplicatesLocalAndSkipsDeferredDrive(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"clip": map[string]interface{}{
					"asset_id": "asset-1", "url": "velox-asset://asset-1",
					"sha256":     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"size_bytes": float64(123),
				},
			},
			map[string]interface{}{
				"clip": map[string]interface{}{"url": "velox-asset://asset-1"},
			},
			map[string]interface{}{
				"clip": map[string]interface{}{"url": "velox-drive://drive-1"},
			},
		},
	}

	requirements := collectAssetPreflightRequirements(payload)
	if len(requirements) != 1 {
		t.Fatalf("requirements = %#v, want one local asset", requirements)
	}
	if requirements[0].AssetID != "asset-1" || requirements[0].SizeBytes != 123 || requirements[0].SHA256 == "" {
		t.Fatalf("requirement = %#v, want merged metadata", requirements[0])
	}
}
