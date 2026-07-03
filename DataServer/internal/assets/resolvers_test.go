package assets

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"
	"testing"
)

type rewriteGoogleDriveTransport struct {
	target *neturl.URL
	base   http.RoundTripper
}

func (t *rewriteGoogleDriveTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	if strings.EqualFold(cloned.URL.Host, "drive.usercontent.google.com") {
		cloned.URL.Scheme = t.target.Scheme
		cloned.URL.Host = t.target.Host
	}
	return t.base.RoundTrip(cloned)
}

func TestPublicDriveDownloadURL_FileLink(t *testing.T) {
	got, fileID, ok := publicDriveDownloadURL("https://drive.google.com/file/d/19m3s1-_guIYqEZE2Ywy77s_mJZMR7686/view?usp=drive_link")
	if !ok {
		t.Fatal("expected public drive download URL to be derived")
	}
	if fileID != "19m3s1-_guIYqEZE2Ywy77s_mJZMR7686" {
		t.Fatalf("fileID = %q", fileID)
	}
	want := "https://drive.usercontent.google.com/download?id=19m3s1-_guIYqEZE2Ywy77s_mJZMR7686&export=download&confirm=t"
	if got != want {
		t.Fatalf("download URL = %q, want %q", got, want)
	}
}

func TestDriveResolver_Open_PublicLinkWithoutAuthUsesHTTPFallback(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/download" {
			t.Fatalf("path = %q, want /download", r.URL.Path)
		}
		if r.URL.Query().Get("id") != "public-file-id" {
			t.Fatalf("id = %q, want public-file-id", r.URL.Query().Get("id"))
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write([]byte("fake-mp3-bytes"))
	}))
	defer srv.Close()

	targetURL, err := neturl.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}

	client := &http.Client{
		Transport: &rewriteGoogleDriveTransport{
			target: targetURL,
			base:   http.DefaultTransport,
		},
	}

	store := NewStore(t.TempDir(), 1024*1024, nil)
	resolver := &driveResolver{
		store: store,
		http:  client,
	}

	src, err := resolver.Open(context.Background(), "https://drive.google.com/file/d/public-file-id/view?usp=drive_link")
	if err != nil {
		t.Fatalf("Open public drive file: %v", err)
	}
	defer src.Reader.Close()

	body, err := io.ReadAll(src.Reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "fake-mp3-bytes" {
		t.Fatalf("body = %q", string(body))
	}
	if src.SourceType != "drive_public" {
		t.Fatalf("SourceType = %q, want drive_public", src.SourceType)
	}
	if src.MIMEType != "audio/mpeg" {
		t.Fatalf("MIMEType = %q, want audio/mpeg", src.MIMEType)
	}
	if src.Metadata["original_reference"] == "" {
		t.Fatal("original_reference metadata missing")
	}
}
