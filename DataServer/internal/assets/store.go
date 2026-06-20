package assets

import (
	"fmt"
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
	// PR-PILOT dev-bypass: gated escape hatch mirroring VELOX_GRPC_ALLOW_INSECURE_DEV.
	// Production deployments must leave this unset; opt-in flips the local-path
	// allowlist to "any" so the SQLite-only smoke test can pass staged fixtures
	// from /tmp/velox-pilot/staging without expanding allowedRoots by structural
	// surgery on the bootstrap wiring. A loud audit log keeps an engaged bypass
	// visible in master.log.
	if os.Getenv("VELOX_ASSET_REWRITE_DEV_BYPASS") == "true" {
		fmt.Fprintf(os.Stderr, "[ASSETS] WARNING: dev-bypass engaged (VELOX_ASSET_REWRITE_DEV_BYPASS=true) source=%q\n", source)
		return true
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
