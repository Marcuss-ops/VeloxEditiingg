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
}

func (r *driveResolver) Scheme() string   { return "drive" }
func (r *driveResolver) ServerOnly() bool { return false }

func (r *driveResolver) Open(ctx context.Context, reference string) (*Source, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	if r.drive == nil {
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
	if strings.Contains(lower, "drive.google.com") {
		return true
	}
	return paths.ExtractDriveID(trimmed) != ""
}

func isHTMLMediaType(mediaType string) bool {
	lower := strings.ToLower(strings.TrimSpace(mediaType))
	return strings.HasPrefix(lower, "text/html") || strings.HasPrefix(lower, "application/xhtml+xml")
}

func isHTMLPayload(buf []byte) bool {
	detected := strings.ToLower(http.DetectContentType(buf))
	return strings.HasPrefix(detected, "text/html") || strings.HasPrefix(detected, "application/xhtml+xml")
}

func isSupportedVoiceoverMediaType(mediaType string) bool {
	lower := strings.ToLower(strings.TrimSpace(mediaType))
	if lower == "" {
		return false
	}
	switch {
	case strings.HasPrefix(lower, "audio/"):
		return true
	case strings.HasPrefix(lower, "video/"):
		return true
	case lower == "application/octet-stream", lower == "binary/octet-stream":
		return true
	case isHTMLMediaType(lower):
		return false
	case strings.HasPrefix(lower, "text/"):
		return false
	default:
		return false
	}
}

// ── Legacy voiceover resolvers (LegacyResolver interface) ──

type legacyVeloxAssetResolver struct {
	store *Store
}

func (r *legacyVeloxAssetResolver) Supports(reference string) bool {
	return strings.HasPrefix(strings.TrimSpace(reference), VeloxAssetScheme+"://")
}

func (r *legacyVeloxAssetResolver) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	assetID := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(reference), VeloxAssetScheme+"://"))
	if assetID == "" {
		return nil, fmt.Errorf("missing asset id")
	}
	return r.store.Lookup(assetID)
}

type legacyLocalFileResolver struct {
	store *Store
}

func (r *legacyLocalFileResolver) Supports(reference string) bool {
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(strings.ToLower(trimmed), "http://") || strings.HasPrefix(strings.ToLower(trimmed), "https://") {
		return false
	}
	if strings.HasPrefix(trimmed, VeloxAssetScheme+"://") {
		return false
	}
	return true
}

func (r *legacyLocalFileResolver) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
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
		return nil, newAcquisitionError("voiceover_path", "local_file", "local source is outside allowed Velox data directories", nil)
	}
	if info, err := os.Stat(source); err != nil {
		return nil, err
	} else if info.IsDir() {
		return nil, fmt.Errorf("source is a directory")
	}
	suggestedName := filepath.Base(source)
	mediaType := detectMediaType(source, filepath.Ext(source))
	if !isSupportedVoiceoverMediaType(mediaType) {
		return nil, newAcquisitionError("voiceover_path", "local_file", assetErrorMessage("unsupported content type", mediaType), nil)
	}
	return r.store.ingestFile(source, "local_file", source, mediaType, suggestedName)
}

type legacyHttpResolver struct {
	store *Store
	http  *http.Client
}

func (r *legacyHttpResolver) Supports(reference string) bool {
	trimmed := strings.TrimSpace(reference)
	return strings.HasPrefix(strings.ToLower(trimmed), "http://") || strings.HasPrefix(strings.ToLower(trimmed), "https://")
}

func (r *legacyHttpResolver) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	client := r.http
	if client == nil {
		client = &http.Client{
			Timeout: 90 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return fmt.Errorf("too many redirects")
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
		return nil, newAcquisitionError("voiceover_path", "http", "download failed", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, newAcquisitionError("voiceover_path", "http", fmt.Sprintf("download failed with status %d", resp.StatusCode), nil)
	}
	mediaType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if idx := strings.Index(mediaType, ";"); idx >= 0 {
		mediaType = strings.TrimSpace(mediaType[:idx])
	}
	if mediaType != "" && isHTMLMediaType(mediaType) {
		return nil, newAcquisitionError("voiceover_path", "http", "unexpected HTML response", nil)
	}
	reader := bufio.NewReader(resp.Body)
	peek, _ := reader.Peek(512)
	if isHTMLPayload(peek) {
		return nil, newAcquisitionError("voiceover_path", "http", "unexpected HTML response", nil)
	}
	if mediaType == "" {
		mediaType = http.DetectContentType(peek)
	}
	if !isSupportedVoiceoverMediaType(mediaType) {
		return nil, newAcquisitionError("voiceover_path", "http", assetErrorMessage("unsupported content type", mediaType), nil)
	}
	suggestedName := suggestedFilenameFromURL(reference)
	return r.store.ingestReader(reader, "http", reference, mediaType, suggestedName)
}

type legacyDriveResolver struct {
	store *Store
	drive DriveDownloader
}

func (r *legacyDriveResolver) Supports(reference string) bool {
	return looksLikeDriveURL(reference)
}

func (r *legacyDriveResolver) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	if r.drive == nil {
		return nil, newAcquisitionError("voiceover_path", "drive", "Drive authentication is unavailable", nil)
	}
	fileID := paths.ExtractDriveID(reference)
	if fileID == "" {
		return nil, newAcquisitionError("voiceover_path", "drive", "unable to extract Drive file id", nil)
	}
	meta, err := r.drive.GetFileMetadata(ctx, fileID)
	if err != nil {
		return nil, newAcquisitionError("voiceover_path", "drive", "Drive file metadata unavailable", err)
	}
	if meta == nil {
		return nil, newAcquisitionError("voiceover_path", "drive", "Drive file metadata unavailable", nil)
	}
	if r.store.maxBytes > 0 && meta.Size > 0 && meta.Size > r.store.maxBytes {
		return nil, newAcquisitionError("voiceover_path", "drive", fmt.Sprintf("voiceover exceeds maximum size of %d bytes", r.store.maxBytes), nil)
	}
	if !isSupportedVoiceoverMediaType(meta.MimeType) {
		return nil, newAcquisitionError("voiceover_path", "drive", assetErrorMessage("unsupported content type", meta.MimeType), nil)
	}
	if err := os.MkdirAll(r.store.tmpDir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(r.store.tmpDir, "drive-voiceover-*")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := r.drive.DownloadFile(ctx, fileID, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, newAcquisitionError("voiceover_path", "drive", "Drive download failed", err)
	}
	shaHex, size, err := hashFile(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return nil, newAcquisitionError("voiceover_path", "drive", "Drive download validation failed", err)
	}
	return r.store.commitTempFile(tmpPath, shaHex, size, "drive", reference, meta.MimeType, meta.Name)
}

// NewResolversFromStore creates the 4 standard resolvers from an existing voiceover Store.
// Used by the voiceover bridge for backward compatibility.
func NewResolversFromStore(store *Store, drive DriveDownloader, httpClient *http.Client) []LegacyResolver {
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
	return []LegacyResolver{
		&legacyVeloxAssetResolver{store: store},
		&legacyDriveResolver{store: store, drive: drive},
		&legacyLocalFileResolver{store: store},
		&legacyHttpResolver{store: store, http: httpClient},
	}
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
		&driveResolver{store: store, drive: drive},
		&localFileResolver{store: store},
		&httpResolver{store: store, http: httpClient},
		&httpSchemeResolver{&httpResolver{store: store, http: httpClient}},
	}
}
