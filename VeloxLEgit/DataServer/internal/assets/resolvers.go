package assets

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	driveapi "velox-server/internal/integrations/drive"

	"velox-shared/paths"
)

// DriveDownloader is the minimal authenticated Drive surface required by resolvers.
type DriveDownloader interface {
	GetFileMetadata(ctx context.Context, fileID string) (*driveapi.File, error)
	DownloadFile(ctx context.Context, fileID string, destPath string) error
}

// ── New-style resolvers (Scheme + Open + ServerOnly) ──
// Used by AssetService.ResolveAndRegister.

type veloxAssetResolver struct {
	store *Store
}

func (r *veloxAssetResolver) Scheme() string   { return "velox-asset" }
func (r *veloxAssetResolver) ServerOnly() bool { return false }

func (r *veloxAssetResolver) Open(ctx context.Context, reference string) (*Source, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	assetID := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(reference), VeloxAssetScheme+"://"))
	if assetID == "" {
		return nil, fmt.Errorf("missing asset id")
	}
	resolved, err := r.store.Lookup(assetID)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(resolved.LocalPath)
	if err != nil {
		return nil, fmt.Errorf("open local asset: %w", err)
	}
	return &Source{
		Reader:        f,
		SuggestedName: assetID,
		MIMEType:      resolved.MediaType,
		ExpectedSize:  resolved.ByteSize,
		SourceType:    "velox_asset",
	}, nil
}

type localFileResolver struct {
	store *Store
}

func (r *localFileResolver) Scheme() string   { return "file" }
func (r *localFileResolver) ServerOnly() bool { return true }

func (r *localFileResolver) Open(ctx context.Context, reference string) (*Source, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	source := strings.TrimSpace(reference)
	if source == "" {
		return nil, fmt.Errorf("empty source")
	}
	if strings.HasPrefix(strings.ToLower(source), "file://") {
		u, err := neturl.Parse(source)
		if err != nil {
			return nil, err
		}
		source = u.Path
	}
	if !r.store.allowedLocalPath(source) {
		return nil, newAcquisitionError("reference", "file", "local source is outside allowed Velox data directories", nil)
	}
	if info, err := os.Stat(source); err != nil {
		return nil, err
	} else if info.IsDir() {
		return nil, fmt.Errorf("source is a directory")
	}
	f, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	return &Source{
		Reader:        f,
		SuggestedName: filepath.Base(source),
		MIMEType:      detectMediaType(source, filepath.Ext(source)),
		SourceType:    "file",
	}, nil
}

type httpResolver struct {
	store *Store
	http  *http.Client
}

func (r *httpResolver) Scheme() string   { return "https" }
func (r *httpResolver) ServerOnly() bool { return false }

func (r *httpResolver) Open(ctx context.Context, reference string) (*Source, error) {
	if r == nil {
		return nil, fmt.Errorf("http resolver unavailable")
	}
	client := r.http
	if client == nil {
		client = &http.Client{
			Timeout: 90 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return http.ErrUseLastResponse
				}
				return nil
			},
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reference, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, newAcquisitionError("reference", "http", "download failed", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, newAcquisitionError("reference", "http", fmt.Sprintf("download failed with status %d", resp.StatusCode), nil)
	}
	mediaType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if idx := strings.Index(mediaType, ";"); idx >= 0 {
		mediaType = strings.TrimSpace(mediaType[:idx])
	}
	if mediaType != "" && isHTMLMediaType(mediaType) {
		_ = resp.Body.Close()
		return nil, newAcquisitionError("reference", "http", "unexpected HTML response", nil)
	}
	reader := bufio.NewReader(resp.Body)
	peek, _ := reader.Peek(512)
	if isHTMLPayload(peek) {
		_ = resp.Body.Close()
		return nil, newAcquisitionError("reference", "http", "unexpected HTML response", nil)
	}
	if mediaType == "" {
		mediaType = http.DetectContentType(peek)
	}
	return &Source{
		Reader:        &readCloser{reader},
		SuggestedName: suggestedFilenameFromURL(reference),
		MIMEType:      mediaType,
		ExpectedSize:  resp.ContentLength,
		SourceType:    "https",
	}, nil
}

type httpSchemeResolver struct {
	*httpResolver
}

func (r *httpSchemeResolver) Scheme() string { return "http" }

type driveResolver struct {
	store *Store
	drive DriveDownloader
	http  *http.Client
}

func (r *driveResolver) Scheme() string   { return "drive" }
func (r *driveResolver) ServerOnly() bool { return false }

func (r *driveResolver) Open(ctx context.Context, reference string) (*Source, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}

	var publicErr error

	if _, _, ok := publicDriveDownloadURL(reference); ok {
		src, err := r.openPublicDriveFile(ctx, reference)
		if err == nil && src != nil {
			return src, nil
		}
		publicErr = err
	}

	if r.drive == nil {
		if publicErr != nil {
			return nil, newAcquisitionError(
				"reference",
				"drive_public",
				"public Drive download failed and authenticated Drive is unavailable",
				publicErr,
			)
		}
		return nil, newAcquisitionError("reference", "drive", "Drive authentication is unavailable", nil)
	}

	fileID := paths.ExtractDriveID(reference)
	if fileID == "" {
		return nil, newAcquisitionError("reference", "drive", "unable to extract Drive file id", nil)
	}
	meta, err := r.drive.GetFileMetadata(ctx, fileID)
	if err != nil {
		return nil, newAcquisitionError("reference", "drive", "Drive file metadata unavailable", err)
	}
	if meta == nil {
		return nil, newAcquisitionError("reference", "drive", "Drive file metadata unavailable", nil)
	}
	if err := os.MkdirAll(r.store.tmpDir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(r.store.tmpDir, "drive-asset-*")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	if err := r.drive.DownloadFile(ctx, fileID, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, newAcquisitionError("reference", "drive", "Drive download failed", err)
	}
	f, err := os.Open(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	return &Source{
		Reader:        &cleanupCloser{f, tmpPath},
		SuggestedName: meta.Name,
		MIMEType:      meta.MimeType,
		ExpectedSize:  meta.Size,
		SourceType:    "drive",
	}, nil
}

func (r *driveResolver) openPublicDriveFile(ctx context.Context, reference string) (*Source, error) {
	downloadURL, fileID, ok := publicDriveDownloadURL(reference)
	if !ok {
		return nil, fmt.Errorf("not a public drive file reference")
	}
	httpSrc, err := (&httpResolver{store: r.store, http: r.http}).Open(ctx, downloadURL)
	if err != nil {
		return nil, err
	}
	if httpSrc.SuggestedName == "" || httpSrc.SuggestedName == "download" {
		httpSrc.SuggestedName = "drive-" + fileID
	}
	httpSrc.SourceType = "drive_public"
	if httpSrc.Metadata == nil {
		httpSrc.Metadata = map[string]string{}
	}
	httpSrc.Metadata["original_reference"] = strings.TrimSpace(reference)
	return httpSrc, nil
}

// cleanupCloser wraps an os.File and removes the underlying file on Close.
type cleanupCloser struct {
	*os.File
	path string
}

func (c *cleanupCloser) Close() error {
	err := c.File.Close()
	_ = os.Remove(c.path)
	return err
}

// readCloser wraps an io.Reader as io.ReadCloser (no-op Close).
type readCloser struct {
	io.Reader
}

func (r *readCloser) Close() error { return nil }

// ── Shared helpers ──

func suggestedFilenameFromURL(reference string) string {
	u, err := neturl.Parse(strings.TrimSpace(reference))
	if err != nil {
		return ""
	}
	base := filepath.Base(u.Path)
	if base == "." || base == string(filepath.Separator) {
		return ""
	}
	return base
}

func looksLikeDriveURL(reference string) bool {
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return false
	}
	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, "drive.usercontent.google.com") {
		return false
	}
	if strings.Contains(lower, "drive.google.com") {
		return true
	}
	return paths.ExtractDriveID(trimmed) != ""
}

func publicDriveDownloadURL(reference string) (string, string, bool) {
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return "", "", false
	}
	lower := strings.ToLower(trimmed)
	if !strings.Contains(lower, "drive.google.com") || strings.Contains(lower, "/folders/") {
		return "", "", false
	}
	fileID := strings.TrimSpace(paths.ExtractDriveID(trimmed))
	if fileID == "" {
		return "", "", false
	}
	return "https://drive.usercontent.google.com/download?id=" + neturl.QueryEscape(fileID) + "&export=download&confirm=t", fileID, true
}

func isHTMLMediaType(mediaType string) bool {
	lower := strings.ToLower(strings.TrimSpace(mediaType))
	return strings.HasPrefix(lower, "text/html") || strings.HasPrefix(lower, "application/xhtml+xml")
}

func isHTMLPayload(buf []byte) bool {
	detected := strings.ToLower(http.DetectContentType(buf))
	return strings.HasPrefix(detected, "text/html") || strings.HasPrefix(detected, "application/xhtml+xml")
}

// NewTypedResolversFromStore creates the 4 standard typed resolvers from a Store.
// Used by the new AssetService.
func NewTypedResolversFromStore(store *Store, drive DriveDownloader, httpClient *http.Client) []Resolver {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 90 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return http.ErrUseLastResponse
				}
				return nil
			},
		}
	}
	return []Resolver{
		&veloxAssetResolver{store: store},
		&driveResolver{store: store, drive: drive, http: httpClient},
		&localFileResolver{store: store},
		&httpResolver{store: store, http: httpClient},
		&httpSchemeResolver{&httpResolver{store: store, http: httpClient}},
	}
}
