package assets

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	driveapi "velox-server/internal/integrations/drive"

	"velox-shared/paths"
)

// DriveDownloader is the minimal authenticated Drive surface required by the bridge.
type DriveDownloader interface {
	GetFileMetadata(ctx context.Context, fileID string) (*driveapi.File, error)
	DownloadFile(ctx context.Context, fileID string, destPath string) error
}

type veloxAssetResolver struct {
	store *Store
}

func (r *veloxAssetResolver) Supports(reference string) bool {
	return strings.HasPrefix(strings.TrimSpace(reference), VeloxAssetScheme+"://")
}

func (r *veloxAssetResolver) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	assetID := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(reference), VeloxAssetScheme+"://"))
	if assetID == "" {
		return nil, fmt.Errorf("missing asset id")
	}
	return r.store.Lookup(assetID)
}

type localFileResolver struct {
	store *Store
}

func (r *localFileResolver) Supports(reference string) bool {
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

func (r *localFileResolver) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
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

type httpResolver struct {
	store *Store
	http  *http.Client
}

func (r *httpResolver) Supports(reference string) bool {
	trimmed := strings.TrimSpace(reference)
	return strings.HasPrefix(strings.ToLower(trimmed), "http://") || strings.HasPrefix(strings.ToLower(trimmed), "https://")
}

func (r *httpResolver) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
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

type driveResolver struct {
	store *Store
	drive DriveDownloader
}

func (r *driveResolver) Supports(reference string) bool {
	return looksLikeDriveURL(reference)
}

func (r *driveResolver) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
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
	case lower == "application/octet-stream":
		return true
	case lower == "binary/octet-stream":
		return true
	case isHTMLMediaType(lower):
		return false
	case strings.HasPrefix(lower, "text/"):
		return false
	default:
		return false
	}
}
