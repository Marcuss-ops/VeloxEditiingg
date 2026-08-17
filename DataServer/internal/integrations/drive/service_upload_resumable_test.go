package drive

import (
	"context"
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
