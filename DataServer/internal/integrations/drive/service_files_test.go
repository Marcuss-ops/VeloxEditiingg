package drive

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func driveTestService(transport roundTripFunc) *Service {
	return &Service{
		httpClient: &http.Client{Transport: transport},
		currentToken: &Token{
			AccessToken: "test-access-token",
			Expiry:      time.Now().Add(time.Hour),
		},
	}
}

func driveResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func TestUploadFile_ReusesExistingDelivery(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "comic.mp4")
	if err := os.WriteFile(filePath, []byte("video"), 0o600); err != nil {
		t.Fatal(err)
	}
	var calls []string
	service := driveTestService(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.Method+" "+req.URL.Path)
		if req.Method != http.MethodGet {
			t.Fatalf("unexpected upload after existing delivery lookup: %s %s", req.Method, req.URL)
		}
		return driveResponse(http.StatusOK, `{"files":[{"id":"drive-existing","webViewLink":"https://drive.google.com/file/d/drive-existing"}]}`), nil
	})

	result, err := service.UploadFile(context.Background(), filePath, "comic-folder", "delivery-comic-1")
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if !result.Success || result.FileID != "drive-existing" {
		t.Fatalf("result = %#v, want existing successful upload", result)
	}
	if len(calls) != 1 || calls[0] != "GET /drive/v3/files" {
		t.Fatalf("Drive calls = %v, want one lookup", calls)
	}
}

func TestUploadFile_MultipartRecordsNetworkAndLocalBufferSplit(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "small.mp4")
	if err := os.WriteFile(filePath, []byte("video"), 0o600); err != nil {
		t.Fatal(err)
	}
	service := driveTestService(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet {
			return driveResponse(http.StatusOK, `{"files":[]}`), nil
		}
		time.Sleep(10 * time.Millisecond)
		return driveResponse(http.StatusOK, `{"id":"drive-net","webViewLink":"https://drive.google.com/file/d/drive-net"}`), nil
	})

	result, err := service.UploadFile(context.Background(), filePath, "folder", "delivery-net")
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if !result.Success {
		t.Fatalf("want success, got %#v", result)
	}
	if result.NetworkMS <= 0 {
		t.Fatalf("NetworkMS = %d, want > 0 (network round-trip should be measured)", result.NetworkMS)
	}
	if result.LocalBufferMS < 0 {
		t.Fatalf("LocalBufferMS = %d, want >= 0", result.LocalBufferMS)
	}
}

func TestUploadFile_StoresDeliveryMarkerAndParent(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "comic.mp4")
	if err := os.WriteFile(filePath, []byte("video"), 0o600); err != nil {
		t.Fatal(err)
	}
	var uploadBody string
	service := driveTestService(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet {
			return driveResponse(http.StatusOK, `{"files":[]}`), nil
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read upload body: %v", err)
		}
		uploadBody = string(body)
		return driveResponse(http.StatusOK, `{"id":"drive-new","webViewLink":"https://drive.google.com/file/d/drive-new"}`), nil
	})

	result, err := service.UploadFile(context.Background(), filePath, "comic-folder", "delivery-comic-2")
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if !result.Success || result.FileID != "drive-new" {
		t.Fatalf("result = %#v, want new successful upload", result)
	}
	for _, fragment := range []string{`"velox_delivery_id":"delivery-comic-2"`, `"parents":["comic-folder"]`} {
		if !strings.Contains(uploadBody, fragment) {
			t.Fatalf("upload metadata missing %s: %s", fragment, uploadBody)
		}
	}
}
