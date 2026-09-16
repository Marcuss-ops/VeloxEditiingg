package publisher

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMasterStreamProgressiveSessionConvertsPartNumberToChunkIndex(t *testing.T) {
	var gotPath string
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	transport := &MasterStreamTransport{HTTPClient: server.Client()}
	session, err := transport.BeginProgressive(context.Background(), ProgressiveUploadRequest{
		Target: UploadTarget{
			UploadURL: server.URL + "/master-stream/upload-1",
			UploadID:  "upload-1",
		},
		CommitToken: "commit-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.UploadPart(context.Background(), 1, strings.NewReader("abc"), 3); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/master-stream/upload-1/0" {
		t.Fatalf("request path = %q; want zero-indexed chunk 0", gotPath)
	}
	if gotBody != "abc" {
		t.Fatalf("request body = %q; want abc", gotBody)
	}
}
