package assets

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const VeloxAssetScheme = "velox-asset"

// ResolvedAsset is the canonical asset record returned by the bridge.
type ResolvedAsset struct {
	AssetID        string `json:"asset_id"`
	SHA256         string `json:"sha256"`
	LocalPath      string `json:"local_path"`
	MediaType      string `json:"media_type"`
	ByteSize       int64  `json:"byte_size"`
	SourceType     string `json:"source_type"`
	OriginalSource string `json:"original_source,omitempty"`
	Reference      string `json:"reference"`
}

// VeloxReference returns the canonical velox-asset URI.
func (a *ResolvedAsset) VeloxReference() string {
	if a == nil {
		return ""
	}
	if strings.TrimSpace(a.AssetID) == "" {
		return ""
	}
	return VeloxAssetScheme + "://" + strings.TrimSpace(a.AssetID)
}

// Store manages the canonical local asset directory.
type Store struct {
	dataDir      string
	assetDir     string
	tmpDir       string
	maxBytes     int64
	allowedRoots []string
}

// NewStore creates a content-addressed store under <dataDir>/worker_downloads/assets/audio.
func NewStore(dataDir string, maxBytes int64, allowedRoots []string) *Store {
	trimmed := strings.TrimSpace(dataDir)
	if maxBytes <= 0 {
		maxBytes = 256 * 1024 * 1024
	}
	roots := normalizeAllowedRoots(append(allowedRoots, trimmed)...)
	return &Store{
		dataDir:      trimmed,
		assetDir:     filepath.Join(trimmed, "worker_downloads", "assets", "audio"),
		tmpDir:       filepath.Join(trimmed, "worker_downloads", "assets", "audio", ".tmp"),
		maxBytes:     maxBytes,
		allowedRoots: roots,
	}
}

func (s *Store) AssetDir() string {
	if s == nil {
		return ""
	}
	return s.assetDir
}

func (s *Store) allowedLocalPath(source string) bool {
	if s == nil {
		return false
	}
	trimmed := strings.TrimSpace(source)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(strings.ToLower(trimmed), "file://") {
		trimmed = strings.TrimPrefix(trimmed, "file://")
	}
	absSource, err := filepath.Abs(trimmed)
	if err != nil {
		return false
	}
	for _, root := range s.allowedRoots {
		absRoot, err := filepath.Abs(root)
		if err != nil || absRoot == "" {
			continue
		}
		rel, err := filepath.Rel(absRoot, absSource)
		if err != nil {
			continue
		}
		if rel == "." || rel == "" || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "..") {
			return true
		}
	}
	return false
}

func (s *Store) Lookup(assetID string) (*ResolvedAsset, error) {
	if s == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	assetID = strings.TrimSpace(assetID)
	if assetID == "" {
		return nil, fmt.Errorf("asset id required")
	}
	if err := os.MkdirAll(s.assetDir, 0o755); err != nil {
		return nil, err
	}

	candidates := []string{}
	if matches, err := filepath.Glob(filepath.Join(s.assetDir, assetID+".*")); err == nil {
		candidates = append(candidates, matches...)
	}
	candidates = append(candidates, filepath.Join(s.assetDir, assetID))
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() {
			continue
		}
		mediaType := detectMediaType(candidate, filepath.Ext(candidate))
		return &ResolvedAsset{
			AssetID:    assetID,
			SHA256:     assetID,
			LocalPath:  candidate,
			MediaType:  mediaType,
			ByteSize:   info.Size(),
			SourceType: "velox_asset",
			Reference:  VeloxAssetScheme + "://" + assetID,
		}, nil
	}
	return nil, fmt.Errorf("asset not found")
}

func (s *Store) ingestReader(reader io.Reader, sourceType, originalSource, mediaType, suggestedName string) (*ResolvedAsset, error) {
	if s == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	if reader == nil {
		return nil, fmt.Errorf("empty input")
	}
	if err := os.MkdirAll(s.tmpDir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(s.tmpDir, "voiceover-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tmp.Close()
	}()

	hasher := sha256.New()
	written, err := copyWithLimit(io.MultiWriter(tmp, hasher), reader, s.maxBytes)
	if err != nil {
		_ = os.Remove(tmp.Name())
		return nil, err
	}
	if written <= 0 {
		_ = os.Remove(tmp.Name())
		return nil, fmt.Errorf("empty input")
	}
	if err := tmp.Sync(); err != nil {
		_ = os.Remove(tmp.Name())
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return nil, err
	}

	return s.commitTempFile(tmp.Name(), hex.EncodeToString(hasher.Sum(nil)), written, sourceType, originalSource, mediaType, suggestedName)
}

func (s *Store) ingestFile(path, sourceType, originalSource, mediaType, suggestedName string) (*ResolvedAsset, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return s.ingestReader(f, sourceType, originalSource, mediaType, suggestedName)
}

func (s *Store) commitTempFile(tempPath, shaHex string, size int64, sourceType, originalSource, mediaType, suggestedName string) (*ResolvedAsset, error) {
	if s == nil {
		return nil, fmt.Errorf("asset store unavailable")
	}
	if strings.TrimSpace(shaHex) == "" {
		return nil, fmt.Errorf("missing sha256")
	}
	if size <= 0 {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("empty input")
	}
	if err := os.MkdirAll(s.assetDir, 0o755); err != nil {
		_ = os.Remove(tempPath)
		return nil, err
	}

	ext := safeAudioExtension(suggestedName, mediaType)
	finalPath := filepath.Join(s.assetDir, shaHex+ext)
	if info, err := os.Stat(finalPath); err == nil && !info.IsDir() {
		_ = os.Remove(tempPath)
		assetMediaType := mediaType
		if assetMediaType == "" {
			assetMediaType = detectMediaType(finalPath, ext)
		}
		return &ResolvedAsset{
			AssetID:        shaHex,
			SHA256:         shaHex,
			LocalPath:      finalPath,
			MediaType:      assetMediaType,
			ByteSize:       info.Size(),
			SourceType:     sourceType,
			OriginalSource: originalSource,
			Reference:      VeloxAssetScheme + "://" + shaHex,
		}, nil
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		if info, statErr := os.Stat(finalPath); statErr == nil && !info.IsDir() {
			_ = os.Remove(tempPath)
			assetMediaType := mediaType
			if assetMediaType == "" {
				assetMediaType = detectMediaType(finalPath, ext)
			}
			return &ResolvedAsset{
				AssetID:        shaHex,
				SHA256:         shaHex,
				LocalPath:      finalPath,
				MediaType:      assetMediaType,
				ByteSize:       info.Size(),
				SourceType:     sourceType,
				OriginalSource: originalSource,
				Reference:      VeloxAssetScheme + "://" + shaHex,
			}, nil
		}
		_ = os.Remove(tempPath)
		return nil, err
	}

	info, err := os.Stat(finalPath)
	if err != nil {
		_ = os.Remove(tempPath)
		return nil, err
	}
	assetMediaType := mediaType
	if assetMediaType == "" {
		assetMediaType = detectMediaType(finalPath, ext)
	}
	return &ResolvedAsset{
		AssetID:        shaHex,
		SHA256:         shaHex,
		LocalPath:      finalPath,
		MediaType:      assetMediaType,
		ByteSize:       info.Size(),
		SourceType:     sourceType,
		OriginalSource: originalSource,
		Reference:      VeloxAssetScheme + "://" + shaHex,
	}, nil
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	hasher := sha256.New()
	size, err := io.Copy(hasher, f)
	if err != nil {
		return "", 0, err
	}
	if size <= 0 {
		return "", 0, fmt.Errorf("empty input")
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, nil
}

func detectMediaType(path, ext string) string {
	if trimmed := strings.TrimSpace(ext); trimmed != "" {
		if !strings.HasPrefix(trimmed, ".") {
			trimmed = "." + trimmed
		}
		if mt := mime.TypeByExtension(trimmed); mt != "" {
			return mt
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return "application/octet-stream"
	}
	defer f.Close()

	buf := make([]byte, 512)
	n, _ := f.Read(buf)
	if n > 0 {
		return http.DetectContentType(buf[:n])
	}
	return "application/octet-stream"
}

func safeAudioExtension(suggestedName, mediaType string) string {
	name := strings.TrimSpace(suggestedName)
	if name != "" {
		ext := strings.ToLower(filepath.Ext(name))
		if ext != "" && isLikelyAudioExtension(ext) {
			return ext
		}
	}
	if mt := strings.TrimSpace(mediaType); mt != "" {
		if exts, err := mime.ExtensionsByType(mt); err == nil && len(exts) > 0 {
			for _, ext := range exts {
				if isLikelyAudioExtension(ext) {
					return ext
				}
			}
			return exts[0]
		}
	}
	return ".audio"
}

func isLikelyAudioExtension(ext string) bool {
	switch strings.ToLower(strings.TrimSpace(ext)) {
	case ".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac", ".opus", ".oga", ".wma", ".webm", ".mp4":
		return true
	default:
		return false
	}
}

func normalizeAllowedRoots(roots ...string) []string {
	out := make([]string, 0, len(roots))
	seen := map[string]struct{}{}
	for _, root := range roots {
		trimmed := strings.TrimSpace(root)
		if trimmed == "" {
			continue
		}
		abs, err := filepath.Abs(trimmed)
		if err != nil {
			continue
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		seen[abs] = struct{}{}
		out = append(out, abs)
	}
	return out
}

type limitedWriter struct {
	w        io.Writer
	maxBytes int64
	written  int64
}

func (lw *limitedWriter) Write(p []byte) (int, error) {
	if lw.maxBytes > 0 && lw.written+int64(len(p)) > lw.maxBytes {
		return 0, fmt.Errorf("voiceover exceeds maximum size of %d bytes", lw.maxBytes)
	}
	n, err := lw.w.Write(p)
	lw.written += int64(n)
	return n, err
}

func copyWithLimit(dst io.Writer, src io.Reader, maxBytes int64) (int64, error) {
	lw := &limitedWriter{w: dst, maxBytes: maxBytes}
	buf := make([]byte, 32*1024)
	return io.CopyBuffer(lw, src, buf)
}
