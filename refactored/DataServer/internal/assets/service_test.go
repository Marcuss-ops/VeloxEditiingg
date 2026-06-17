package assets

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	driveapi "velox-server/internal/integrations/drive"
)

type fakeDriveDownloader struct {
	mu            sync.Mutex
	metaCalls     int
	downloadCalls int
	metadata      *driveapi.File
	content       []byte
	metaErr       error
	downloadErr   error
}

func (f *fakeDriveDownloader) GetFileMetadata(ctx context.Context, fileID string) (*driveapi.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metaCalls++
	return f.metadata, f.metaErr
}

func (f *fakeDriveDownloader) DownloadFile(ctx context.Context, fileID string, destPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downloadCalls++
	if f.downloadErr != nil {
		return f.downloadErr
	}
	return os.WriteFile(destPath, f.content, 0o600)
}

func TestServiceResolvesHTTPAssets(t *testing.T) {
	tempDir := t.TempDir()
	svc := NewService(tempDir, []string{tempDir}, 1024*1024, nil)

	audioBytes := []byte("ID3\x03\x00\x00\x00\x00\x00\x21fake-mp3-bytes")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/audio.mp3":
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(audioBytes)
		case "/redirect":
			http.Redirect(w, r, "/audio.mp3", http.StatusFound)
		case "/404":
			http.NotFound(w, r)
		case "/400":
			http.Error(w, "bad request", http.StatusBadRequest)
		case "/html":
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "<html><body>login</body></html>")
		case "/empty":
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
		case "/oversize":
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes.Repeat([]byte("a"), 128))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Run("imports valid HTTP audio asset", func(t *testing.T) {
		asset, err := svc.Resolve(context.Background(), srv.URL+"/audio.mp3")
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if asset.SourceType != "http" {
			t.Fatalf("want http source type, got %q", asset.SourceType)
		}
		if asset.AssetID == "" || asset.LocalPath == "" {
			t.Fatalf("expected populated asset record: %#v", asset)
		}
		if !strings.HasPrefix(asset.Reference, VeloxAssetScheme+"://") {
			t.Fatalf("want velox reference, got %q", asset.Reference)
		}
		if _, err := os.Stat(asset.LocalPath); err != nil {
			t.Fatalf("staged asset missing: %v", err)
		}
	})

	t.Run("follows redirect", func(t *testing.T) {
		asset, err := svc.Resolve(context.Background(), srv.URL+"/redirect")
		if err != nil {
			t.Fatalf("resolve redirect: %v", err)
		}
		if asset.SourceType != "http" {
			t.Fatalf("want http source type, got %q", asset.SourceType)
		}
	})

	t.Run("rejects 404", func(t *testing.T) {
		_, err := svc.Resolve(context.Background(), srv.URL+"/404")
		if err == nil {
			t.Fatal("want error")
		}
		if assetErr, ok := AsAcquisitionError(err); !ok || assetErr.SourceType != "http" {
			t.Fatalf("want structured http asset error, got %#v", err)
		}
	})

	t.Run("rejects 400", func(t *testing.T) {
		_, err := svc.Resolve(context.Background(), srv.URL+"/400")
		if err == nil {
			t.Fatal("want error")
		}
		if assetErr, ok := AsAcquisitionError(err); !ok || assetErr.SourceType != "http" {
			t.Fatalf("want structured http asset error, got %#v", err)
		}
	})

	t.Run("rejects html login page", func(t *testing.T) {
		_, err := svc.Resolve(context.Background(), srv.URL+"/html")
		if err == nil {
			t.Fatal("want error")
		}
		if assetErr, ok := AsAcquisitionError(err); !ok || assetErr.SourceType != "http" {
			t.Fatalf("want structured html rejection, got %#v", err)
		}
	})

	t.Run("rejects empty input", func(t *testing.T) {
		_, err := svc.Resolve(context.Background(), srv.URL+"/empty")
		if err == nil {
			t.Fatal("want error")
		}
	})

	t.Run("rejects oversized input", func(t *testing.T) {
		smallSvc := NewService(tempDir, []string{tempDir}, 16, nil)
		_, err := smallSvc.Resolve(context.Background(), srv.URL+"/oversize")
		if err == nil {
			t.Fatal("want error")
		}
		if assetErr, ok := AsAcquisitionError(err); !ok || assetErr.SourceType != "http" {
			t.Fatalf("want structured oversize error, got %#v", err)
		}
	})

	t.Run("deduplicates identical audio", func(t *testing.T) {
		dedupeDir := t.TempDir()
		dedupeSvc := NewService(dedupeDir, []string{dedupeDir}, 1024*1024, nil)
		before := func() int {
			entries, _ := os.ReadDir(filepath.Join(dedupeDir, "worker_downloads", "assets", "audio"))
			count := 0
			for _, entry := range entries {
				if entry.Type().IsRegular() {
					count++
				}
			}
			return count
		}()
		first, err := dedupeSvc.Resolve(context.Background(), srv.URL+"/audio.mp3")
		if err != nil {
			t.Fatalf("first resolve: %v", err)
		}
		second, err := dedupeSvc.Resolve(context.Background(), srv.URL+"/audio.mp3")
		if err != nil {
			t.Fatalf("second resolve: %v", err)
		}
		if first.AssetID != second.AssetID {
			t.Fatalf("want same asset id, got %q and %q", first.AssetID, second.AssetID)
		}
		after := func() int {
			entries, _ := os.ReadDir(filepath.Join(dedupeDir, "worker_downloads", "assets", "audio"))
			count := 0
			for _, entry := range entries {
				if entry.Type().IsRegular() {
					count++
				}
			}
			return count
		}()
		if after != before+1 {
			t.Fatalf("expected one stored asset, got before=%d after=%d", before, after)
		}
	})
}

func TestServiceRejectsPathTraversal(t *testing.T) {
	tempDir := t.TempDir()
	outsideDir := t.TempDir()
	outsidePath := filepath.Join(outsideDir, "escape.mp3")
	if err := os.WriteFile(outsidePath, []byte("fake"), 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}

	svc := NewService(tempDir, []string{tempDir}, 1024*1024, nil)
	_, err := svc.Resolve(context.Background(), outsidePath)
	if err == nil {
		t.Fatal("want error")
	}
	if assetErr, ok := AsAcquisitionError(err); !ok || assetErr.SourceType != "local_file" {
		t.Fatalf("want structured local file error, got %#v", err)
	}
}

func TestServiceResolvesDriveURLsThroughExistingDriveIntegration(t *testing.T) {
	tempDir := t.TempDir()
	content := []byte("ID3fake-drive-audio")
	driveSvc := &fakeDriveDownloader{
		metadata: &driveapi.File{
			ID:       "drive-file-123",
			Name:     "voiceover.mp3",
			MimeType: "audio/mpeg",
			Size:     int64(len(content)),
		},
		content: content,
	}

	svc := NewService(tempDir, []string{tempDir}, 1024*1024, driveSvc)
	asset, err := svc.Resolve(context.Background(), "https://drive.google.com/file/d/drive-file-123/view?usp=sharing")
	if err != nil {
		t.Fatalf("resolve drive url: %v", err)
	}
	if asset.SourceType != "drive" {
		t.Fatalf("want drive source type, got %q", asset.SourceType)
	}
	if !strings.HasSuffix(asset.LocalPath, ".mp3") {
		t.Fatalf("want mp3 staging path, got %q", asset.LocalPath)
	}
	if driveSvc.metaCalls != 1 || driveSvc.downloadCalls != 1 {
		t.Fatalf("unexpected drive call counts: meta=%d download=%d", driveSvc.metaCalls, driveSvc.downloadCalls)
	}
}

func TestServiceReturnsStructuredErrorWhenDriveAuthUnavailable(t *testing.T) {
	tempDir := t.TempDir()
	svc := NewService(tempDir, []string{tempDir}, 1024*1024, nil)
	_, err := svc.Resolve(context.Background(), "https://drive.google.com/file/d/drive-file-123/view?usp=sharing")
	if err == nil {
		t.Fatal("want error")
	}
	assetErr, ok := AsAcquisitionError(err)
	if !ok {
		t.Fatalf("want structured acquisition error, got %#v", err)
	}
	if assetErr.SourceType != "drive" {
		t.Fatalf("want source type drive, got %q", assetErr.SourceType)
	}
	if assetErr.Code != VoiceoverAssetUnavailableCode {
		t.Fatalf("want code %q, got %q", VoiceoverAssetUnavailableCode, assetErr.Code)
	}
}
