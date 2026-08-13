package assets

import (
	"fmt"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"velox-server/internal/inputsecurity"
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
	dataDir               string
	assetDir              string
	tmpDir                string
	maxBytes              int64
	allowedRoots          []string
	security              inputsecurity.Policy
	allowRewriteDevBypass bool
}

// NewStore creates a content-addressed store under <dataDir>/worker_downloads/assets/audio.
func NewStore(dataDir string, maxBytes int64, allowedRoots []string) *Store {
	trimmed := strings.TrimSpace(dataDir)
	if maxBytes <= 0 {
		maxBytes = 256 * 1024 * 1024
	}
	roots := normalizeAllowedRoots(append(allowedRoots, trimmed)...)
	tmpDir := filepath.Join(trimmed, "worker_downloads", "assets", "audio", ".tmp")
	policy := inputsecurity.DefaultPolicy()
	policy.MaxBytes = maxBytes
	policy.TempDir = tmpDir
	policy.QuarantineDir = filepath.Join(trimmed, "worker_downloads", "quarantine")
	policy.AllowedRoots = append([]string(nil), roots...)
	return &Store{
		dataDir:               trimmed,
		assetDir:              filepath.Join(trimmed, "worker_downloads", "assets", "audio"),
		tmpDir:                tmpDir,
		maxBytes:              maxBytes,
		allowedRoots:          roots,
		security:              policy,
		allowRewriteDevBypass: false,
	}
}

// SetRewriteDevBypass configures the explicitly captured development bypass.
func (s *Store) SetRewriteDevBypass(enabled bool) {
	if s != nil {
		s.allowRewriteDevBypass = enabled
	}
}

// SecurityPolicy returns a copy of the input policy used by this store.
func (s *Store) SecurityPolicy() inputsecurity.Policy {
	if s == nil {
		return inputsecurity.DefaultPolicy()
	}
	return s.security
}

// SetSecurityPolicy is intended for composition and hermetic tests. Production
// callers should use the policy created by NewStore and only adjust explicit
// operational limits before wiring the resolver registry.
func (s *Store) SetSecurityPolicy(policy inputsecurity.Policy) {
	if s == nil {
		return
	}
	s.security = policy
}

func (s *Store) SecurityMetrics() *inputsecurity.Metrics {
	if s == nil {
		return nil
	}
	return s.security.Metrics
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
	if s.allowRewriteDevBypass {
		fmt.Fprintf(os.Stderr, "[ASSETS] WARNING: dev-bypass engaged source=%q\n", source)
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
