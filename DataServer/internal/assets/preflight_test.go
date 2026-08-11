package assets

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type preflightBlobStore struct {
	root string
}

func (b preflightBlobStore) StagingPath(string, string, string) (string, error) { return "", nil }
func (b preflightBlobStore) PromoteToFinal(string, string) (string, error)      { return "", nil }
func (b preflightBlobStore) RemoveStaging(string) error                         { return nil }
func (b preflightBlobStore) FinalPath(_, _, _ string) string                    { return "" }
func (b preflightBlobStore) FinalDir() string                                   { return b.root }
func (b preflightBlobStore) ReadFinal(storageKey string) (*os.File, error) {
	return os.Open(filepath.Join(b.root, storageKey))
}

func TestAssetServicePreflightVerifiesMetadataAndFinalBlob(t *testing.T) {
	content := []byte("verified asset bytes")
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "asset.bin"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	service := NewAssetService(&rewriteAssetRepository{assets: map[string]*AssetRecord{
		"asset-1": {
			AssetID: "asset-1", Status: AssetStatusReady, SHA256: digest,
			SizeBytes: int64(len(content)), StorageKey: "asset.bin",
		},
	}}, preflightBlobStore{root: root}, nil, nil)

	report, err := service.Preflight(context.Background(), []AssetPreflightRequirement{{
		AssetID: "asset-1", SHA256: digest, SizeBytes: int64(len(content)),
	}})
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if report.Requested != 1 || report.MetadataAvailable != 1 || report.BlobResolvable != 1 || report.SHA256Valid != 1 || report.SizeValid != 1 {
		t.Fatalf("report = %#v, want one fully verified asset", report)
	}
	if report.Items[0].Issue != "" {
		t.Fatalf("item issue = %q, want empty", report.Items[0].Issue)
	}
}

func TestAssetServicePreflightRejectsMissingAndCorruptBlobs(t *testing.T) {
	content := []byte("verified asset bytes")
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "corrupt.bin"), []byte("wrong"), 0o644); err != nil {
		t.Fatal(err)
	}
	service := NewAssetService(&rewriteAssetRepository{assets: map[string]*AssetRecord{
		"corrupt": {AssetID: "corrupt", Status: AssetStatusReady, SHA256: digest, SizeBytes: int64(len(content)), StorageKey: "corrupt.bin"},
		"missing": {AssetID: "missing", Status: AssetStatusReady, SHA256: digest, SizeBytes: int64(len(content)), StorageKey: "missing.bin"},
	}}, preflightBlobStore{root: root}, nil, nil)

	report, err := service.Preflight(context.Background(), []AssetPreflightRequirement{{AssetID: "corrupt"}, {AssetID: "missing"}})
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if report.BlobResolvable != 0 {
		t.Fatalf("BlobResolvable = %d, want 0", report.BlobResolvable)
	}
	if report.Items[0].Issue != "blob_size_mismatch" && report.Items[0].Issue != "blob_sha256_mismatch" {
		t.Fatalf("corrupt issue = %q", report.Items[0].Issue)
	}
	if report.Items[1].Issue != "blob_unresolvable" {
		t.Fatalf("missing issue = %q, want blob_unresolvable", report.Items[1].Issue)
	}
}
