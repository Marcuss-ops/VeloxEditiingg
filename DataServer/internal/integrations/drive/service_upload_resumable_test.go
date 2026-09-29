package drive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// parseContentRange parses a Drive upload "bytes <start>-<end>/<total>" or
// "bytes */<total>" header. The star form is not valid input here (the
// status-query branch is handled separately), so it fails the test on it.
func parseContentRange(t *testing.T, s string) (start, end, total int64) {
	t.Helper()
	body := strings.TrimPrefix(s, "bytes ")
	rangeAndTotal := strings.SplitN(body, "/", 2)
	if len(rangeAndTotal) != 2 {
		t.Fatalf("bad Content-Range %q", s)
	}
	total, err := strconv.ParseInt(rangeAndTotal[1], 10, 64)
	if err != nil {
		t.Fatalf("parse total in %q: %v", s, err)
	}
	rangeParts := strings.SplitN(rangeAndTotal[0], "-", 2)
	if len(rangeParts) != 2 {
		t.Fatalf("bad Content-Range %q", s)
	}
	start, err = strconv.ParseInt(rangeParts[0], 10, 64)
	if err != nil {
		t.Fatalf("parse start in %q: %v", s, err)
	}
	end, err = strconv.ParseInt(rangeParts[1], 10, 64)
	if err != nil {
		t.Fatalf("parse end in %q: %v", s, err)
	}
	return start, end, total
}

func resumableTestFile(t *testing.T, size int64) string {
	t.Helper()
	filePath := filepath.Join(t.TempDir(), "big.mp4")
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 251)
	}
	if err := os.WriteFile(filePath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return filePath
}

func TestInitiateResumableSessionCreatesSessionWithoutUploadingBytes(t *testing.T) {
	const sessionURI = "https://upload.example/session/prewarmed"
	var initCount, putCount int
	service := driveTestService(func(req *http.Request) (*http.Response, error) {
		switch req.Method {
		case http.MethodPost:
			initCount++
			if got := req.Header.Get("X-Upload-Content-Length"); got != "6291456" {
				t.Errorf("X-Upload-Content-Length = %q, want 6291456", got)
			}
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("read metadata body: %v", err)
			}
			if !strings.Contains(string(body), `"velox_delivery_id":"delivery-prewarm"`) {
				t.Errorf("metadata missing delivery marker: %s", body)
			}
			h := make(http.Header)
			h.Set("Location", sessionURI)
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: h}, nil
		case http.MethodPut:
			putCount++
			return driveResponse(http.StatusOK, `{}`), nil
		default:
			t.Fatalf("unexpected request: %s", req.Method)
			return nil, nil
		}
	})

	got, err := service.InitiateResumableSession(context.Background(), "render.mp4", "folder", "delivery-prewarm", 6<<20)
	if err != nil {
		t.Fatalf("InitiateResumableSession: %v", err)
	}
	if got != sessionURI || initCount != 1 || putCount != 0 {
		t.Fatalf("session=%q init=%d PUT=%d, want session URI, one init, zero PUTs", got, initCount, putCount)
	}
}

func TestInitiateRelaySessionUsesUnknownLengthAndIdentityProperties(t *testing.T) {
	const sessionURI = "https://upload.example/session/relay"
	service := driveTestService(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Fatalf("request method = %s, want POST", req.Method)
		}
		if got := req.Header.Get("X-Upload-Content-Length"); got != "" {
			t.Fatalf("unknown-size session sent X-Upload-Content-Length=%q", got)
		}
		var metadata map[string]interface{}
		if err := json.NewDecoder(req.Body).Decode(&metadata); err != nil {
			t.Fatalf("decode metadata: %v", err)
		}
		props, _ := metadata["properties"].(map[string]interface{})
		if props["velox_artifact_id"] != "artifact-1" || props["velox_destination_id"] != "dest-1" || props["velox_publication_id"] != "pub-1" {
			t.Fatalf("relay properties = %#v", props)
		}
		h := make(http.Header)
		h.Set("Location", sessionURI)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: h}, nil
	})

	got, err := service.InitiateRelaySession(context.Background(), "render.mp4", "folder-1", "artifact-1", "dest-1", "pub-1")
	if err != nil || got != sessionURI {
		t.Fatalf("InitiateRelaySession() = %q, %v", got, err)
	}
}

func TestUploadResumablePartUsesUnknownThenFinalTotalSequentially(t *testing.T) {
	var calls int
	service := driveTestService(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPut {
			t.Fatalf("request method = %s, want PUT", req.Method)
		}
		calls++
		switch calls {
		case 1:
			if got := req.Header.Get("Content-Range"); got != "bytes 0-2/*" {
				t.Fatalf("first Content-Range = %q", got)
			}
			return &http.Response{StatusCode: http.StatusPermanentRedirect, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{"Range": []string{"bytes=0-2"}}}, nil
		case 2:
			if got := req.Header.Get("Content-Range"); got != "bytes 3-4/5" {
				t.Fatalf("final Content-Range = %q", got)
			}
			return driveResponse(http.StatusCreated, `{"id":"relay-file","webViewLink":"https://drive.google.com/file/d/relay-file"}`), nil
		default:
			t.Fatalf("unexpected PUT %d", calls)
			return nil, nil
		}
	})

	next, completed, err := service.UploadResumablePart(context.Background(), "https://upload.example/session/relay", 0, 0, []byte("abc"))
	if err != nil || next != 3 || completed != nil {
		t.Fatalf("unknown-size part = next %d, completed %#v, err %v", next, completed, err)
	}
	next, completed, err = service.UploadResumablePart(context.Background(), "https://upload.example/session/relay", 3, 5, []byte("de"))
	if err != nil || next != 5 || completed == nil || !completed.Success || completed.FileID != "relay-file" {
		t.Fatalf("final part = next %d, completed %#v, err %v", next, completed, err)
	}
}

func TestUploadFile_ResumableChunkedUpload(t *testing.T) {
	oldChunk := resumableChunkSize
	resumableChunkSize = 1 << 20 // 1 MiB, a multiple of 256 KiB
	t.Cleanup(func() { resumableChunkSize = oldChunk })

	const fileSize int64 = 6 << 20 // 6 MiB > 5 MiB threshold
	filePath := resumableTestFile(t, fileSize)

	const sessionURI = "https://upload.example/session/abc"
	var initContentLength string
	var chunkStarts []int64
	service := driveTestService(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet:
			return driveResponse(http.StatusOK, `{"files":[]}`), nil
		case req.Method == http.MethodPost:
			initContentLength = req.Header.Get("X-Upload-Content-Length")
			h := make(http.Header)
			h.Set("Location", sessionURI)
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: h}, nil
		case req.Method == http.MethodPut:
			start, end, total := parseContentRange(t, req.Header.Get("Content-Range"))
			chunkStarts = append(chunkStarts, start)
			if end == total-1 {
				return driveResponse(http.StatusOK, `{"id":"drive-resumable","webViewLink":"https://drive.google.com/file/d/drive-resumable"}`), nil
			}
			return &http.Response{StatusCode: http.StatusPermanentRedirect, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL)
			return nil, nil
		}
	})

	result, err := service.UploadFile(context.Background(), filePath, "folder", "delivery-big")
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if !result.Success || result.FileID != "drive-resumable" {
		t.Fatalf("result = %#v, want successful resumable upload", result)
	}
	if initContentLength != strconv.FormatInt(fileSize, 10) {
		t.Fatalf("X-Upload-Content-Length = %q, want %d", initContentLength, fileSize)
	}
	if len(chunkStarts) == 0 {
		t.Fatalf("no chunk PUTs observed")
	}
	if chunkStarts[0] != 0 {
		t.Fatalf("first chunk start = %d, want 0", chunkStarts[0])
	}
	for i := 1; i < len(chunkStarts); i++ {
		if chunkStarts[i] <= chunkStarts[i-1] {
			t.Fatalf("chunk starts not strictly increasing: %v", chunkStarts)
		}
	}
}

func TestUploadFile_ResumableResumesAfterTransientFailure(t *testing.T) {
	oldChunk := resumableChunkSize
	resumableChunkSize = 1 << 20
	t.Cleanup(func() { resumableChunkSize = oldChunk })

	const fileSize int64 = 6 << 20
	filePath := resumableTestFile(t, fileSize)

	const (
		sessionURI       = "https://upload.example/session/resume"
		committedThrough = int64(524287) // server stored the first 512 KiB
	)
	var (
		failedOnce bool
		resumedAt  int64 = -1
	)
	service := driveTestService(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet:
			return driveResponse(http.StatusOK, `{"files":[]}`), nil
		case req.Method == http.MethodPost:
			h := make(http.Header)
			h.Set("Location", sessionURI)
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: h}, nil
		case req.Method == http.MethodPut:
			cr := req.Header.Get("Content-Range")
			if strings.HasPrefix(cr, "bytes */") {
				h := make(http.Header)
				h.Set("Range", fmt.Sprintf("bytes=0-%d", committedThrough))
				return &http.Response{StatusCode: http.StatusPermanentRedirect, Body: io.NopCloser(strings.NewReader("")), Header: h}, nil
			}
			start, end, total := parseContentRange(t, cr)
			if !failedOnce {
				failedOnce = true
				return driveResponse(http.StatusInternalServerError, `{"error":"transient"}`), nil
			}
			if resumedAt == -1 {
				resumedAt = start
			}
			if end == total-1 {
				return driveResponse(http.StatusOK, `{"id":"drive-resumed","webViewLink":"https://drive.google.com/file/d/drive-resumed"}`), nil
			}
			return &http.Response{StatusCode: http.StatusPermanentRedirect, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL)
			return nil, nil
		}
	})

	result, err := service.UploadFile(context.Background(), filePath, "folder", "delivery-resume")
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if !result.Success || result.FileID != "drive-resumed" {
		t.Fatalf("result = %#v, want successful resumed upload", result)
	}
	if !failedOnce {
		t.Fatalf("expected a transient chunk failure")
	}
	if resumedAt != committedThrough+1 {
		t.Fatalf("resume started at %d, want %d", resumedAt, committedThrough+1)
	}
}

func TestUploadFile_ResumablePermanentFailureNoRetry(t *testing.T) {
	oldChunk := resumableChunkSize
	resumableChunkSize = 1 << 20
	t.Cleanup(func() { resumableChunkSize = oldChunk })

	const fileSize int64 = 6 << 20
	filePath := resumableTestFile(t, fileSize)

	const sessionURI = "https://upload.example/session/perm"
	var statusQueries int
	service := driveTestService(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet:
			return driveResponse(http.StatusOK, `{"files":[]}`), nil
		case req.Method == http.MethodPost:
			h := make(http.Header)
			h.Set("Location", sessionURI)
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: h}, nil
		case req.Method == http.MethodPut:
			if strings.HasPrefix(req.Header.Get("Content-Range"), "bytes */") {
				statusQueries++
			}
			// A permanent 403 on the chunk must abort without a status query.
			return driveResponse(http.StatusForbidden, `{"error":"quota"}`), nil
		default:
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL)
			return nil, nil
		}
	})

	_, err := service.UploadFile(context.Background(), filePath, "folder", "delivery-perm")
	if err == nil {
		t.Fatalf("expected a permanent chunk failure to return an error")
	}
	if statusQueries != 0 {
		t.Fatalf("permanent failure triggered %d status queries, want 0", statusQueries)
	}
}
