package assets

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"
	"testing"

	driveapi "velox-server/internal/integrations/drive"
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

// mockDriveDownloader implements DriveDownloader for tests.
type mockDriveDownloader struct {
	getFileMetadata func(ctx context.Context, fileID string) (*driveapi.File, error)
	downloadFile    func(ctx context.Context, fileID string, destPath string) error
}

func (m *mockDriveDownloader) GetFileMetadata(ctx context.Context, fileID string) (*driveapi.File, error) {
	if m.getFileMetadata != nil {
		return m.getFileMetadata(ctx, fileID)
	}
	return &driveapi.File{Name: "mock-file", MimeType: "audio/mpeg", Size: 100}, nil
}

func (m *mockDriveDownloader) DownloadFile(ctx context.Context, fileID string, destPath string) error {
	if m.downloadFile != nil {
		return m.downloadFile(ctx, fileID, destPath)
	}
	return nil
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

func TestDriveResolver_Open_PublicDrive200OK(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write([]byte("fake-mp3-bytes"))
	}))
	defer srv.Close()

	targetURL, _ := neturl.Parse(srv.URL)
	client := &http.Client{
		Transport: &rewriteGoogleDriveTransport{target: targetURL, base: http.DefaultTransport},
	}
	store := NewStore(t.TempDir(), 1024*1024, nil)
	resolver := &driveResolver{store: store, http: client}

	src, err := resolver.Open(context.Background(), "https://drive.google.com/file/d/public-file-id/view?usp=drive_link")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer src.Reader.Close()

	body, _ := io.ReadAll(src.Reader)
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

func TestDriveResolver_Open_PublicDrive403WithoutAuth(t *testing.T) {
	t.Parallel()

	// Server returns 403 for the Drive download URL.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("Forbidden"))
	}))
	defer srv.Close()

	targetURL, _ := neturl.Parse(srv.URL)
	client := &http.Client{
		Transport: &rewriteGoogleDriveTransport{target: targetURL, base: http.DefaultTransport},
	}
	store := NewStore(t.TempDir(), 1024*1024, nil)
	resolver := &driveResolver{store: store, http: client}

	_, err := resolver.Open(context.Background(), "https://drive.google.com/file/d/public-file-id/view?usp=drive_link")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var ae *AcquisitionError
	if !errors.As(err, &ae) {
		t.Fatalf("expected AcquisitionError, got %T", err)
	}
	if ae.SourceType != "drive_public" {
		t.Fatalf("SourceType = %q, want drive_public", ae.SourceType)
	}
	if ae.Cause == nil {
		t.Fatal("expected Cause to be non-nil (the public download error should be preserved)")
	}
}

func TestDriveResolver_Open_PublicDriveHTMLWithoutAuth(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>Google Drive consent page</body></html>"))
	}))
	defer srv.Close()

	targetURL, _ := neturl.Parse(srv.URL)
	client := &http.Client{
		Transport: &rewriteGoogleDriveTransport{target: targetURL, base: http.DefaultTransport},
	}
	store := NewStore(t.TempDir(), 1024*1024, nil)
	resolver := &driveResolver{store: store, http: client}

	_, err := resolver.Open(context.Background(), "https://drive.google.com/file/d/public-file-id/view?usp=drive_link")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var ae *AcquisitionError
	if !errors.As(err, &ae) {
		t.Fatalf("expected AcquisitionError, got %T", err)
	}
	if ae.SourceType != "drive_public" {
		t.Fatalf("SourceType = %q, want drive_public", ae.SourceType)
	}
	if ae.Cause == nil {
		t.Fatal("expected Cause to be non-nil")
	}
	if !strings.Contains(ae.Cause.Error(), "unexpected HTML response") {
		t.Fatalf("cause = %q, want 'unexpected HTML response'", ae.Cause.Error())
	}
}

func TestDriveResolver_Open_PublicDriveFailsWithAuthAvailable(t *testing.T) {
	t.Parallel()

	// Public download fails with 403.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("Forbidden"))
	}))
	defer srv.Close()

	targetURL, _ := neturl.Parse(srv.URL)
	client := &http.Client{
		Transport: &rewriteGoogleDriveTransport{target: targetURL, base: http.DefaultTransport},
	}
	store := NewStore(t.TempDir(), 1024*1024, nil)

	drive := &mockDriveDownloader{
		getFileMetadata: func(ctx context.Context, fileID string) (*driveapi.File, error) {
			return &driveapi.File{Name: "auth-file", MimeType: "audio/mpeg", Size: 200}, nil
		},
		downloadFile: func(ctx context.Context, fileID string, destPath string) error {
			return nil
		},
	}

	resolver := &driveResolver{store: store, drive: drive, http: client}

	// When authenticated Drive is available, the resolver should fall back to it
	// after the public download fails.
	src, err := resolver.Open(context.Background(), "https://drive.google.com/file/d/public-file-id/view?usp=drive_link")
	if err != nil {
		t.Fatalf("Open: expected fallback to authenticated Drive, got error: %v", err)
	}
	defer src.Reader.Close()

	if src.SourceType != "drive" {
		t.Fatalf("SourceType = %q, want drive (authenticated fallback)", src.SourceType)
	}
}

func TestDriveResolver_Open_DriveFolderWithoutAuth(t *testing.T) {
	t.Parallel()

	// A Drive folder link — publicDriveDownloadURL returns false so it should
	// NOT be treated as a public file and should go straight to the auth check.
	store := NewStore(t.TempDir(), 1024*1024, nil)
	resolver := &driveResolver{store: store, http: http.DefaultClient}

	_, err := resolver.Open(context.Background(), "https://drive.google.com/drive/folders/abcd1234")
	if err == nil {
		t.Fatal("expected error for folder without auth, got nil")
	}

	var ae *AcquisitionError
	if !errors.As(err, &ae) {
		t.Fatalf("expected AcquisitionError, got %T", err)
	}
	if ae.SourceType != "drive" {
		t.Fatalf("SourceType = %q, want drive (folder link, not drive_public)", ae.SourceType)
	}
	if ae.Cause != nil {
		t.Fatal("expected Cause to be nil (no public error attempted)")
	}
}
