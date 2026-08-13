package worker

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"velox-worker-agent/internal/telemetry"
)

// assetCacheDir returns the directory where downloaded audio assets are
// cached. Returns the canonical assets/audio subdirectory.
func (w *Worker) assetCacheDir() string {
	if w != nil && w.config != nil {
		if trimmed := strings.TrimSpace(w.config.AssetCacheDir); trimmed != "" {
			return filepath.Join(trimmed, "assets", "audio")
		}
		if trimmed := strings.TrimSpace(w.config.StateDir); trimmed != "" {
			return filepath.Join(trimmed, "asset-cache", "assets", "audio")
		}
		if trimmed := strings.TrimSpace(w.config.WorkDir); trimmed != "" {
			return filepath.Join(trimmed, "asset-cache", "assets", "audio")
		}
	}
	return filepath.Join(os.TempDir(), "velox-worker", "assets", "audio")
}

// cacheKeyPrefix builds the filesystem-safe cache key from assetID and an
// optional SHA-256 prefix. When sha256Prefix is non-empty, the first 12
// characters are embedded in the filename so different versions of the same
// asset do not collide. Format: <assetID>_<sha12> (when sha256 is set) or
// just <assetID> (legacy, no integrity check possible).
func cacheKeyPrefix(assetID string, sha256Prefix string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, assetID)
	if sha256Prefix != "" {
		short := sha256Prefix
		if len(short) > 12 {
			short = short[:12]
		}
		return safe + "_" + short
	}
	return safe
}

// cachedAssetPath returns a previously cached asset path when present.
// A hit is valid only after every supplied integrity constraint passes:
// expectedSizeBytes (when positive) and expectedSHA256 (when non-empty).
// Any invalid entry is removed individually and reported as a miss so the
// caller re-downloads it from the Master.
func cachedAssetPath(cacheDir, assetID string, expectedSHA256 string, expectedSizeBytes int64) (string, error) {
	path, _, err := cachedAssetPathTimedWithContext(context.Background(), cacheDir, assetID, expectedSHA256, expectedSizeBytes)
	return path, err
}

func cachedAssetPathTimedWithContext(ctx context.Context, cacheDir, assetID string, expectedSHA256 string, expectedSizeBytes int64) (string, time.Duration, error) {
	// Folder-backed assets may have no expected hash/size in the job payload.
	// In that mode reuse the asset-ID cache entry after a basic regular-file
	// check; the downloader computed the SHA while creating the file.
	if expectedSHA256 == "" || expectedSizeBytes <= 0 {
		prefix := cacheKeyPrefix(assetID, "")
		matches, err := filepath.Glob(filepath.Join(cacheDir, prefix+".*"))
		if err != nil || len(matches) == 0 {
			return "", 0, err
		}
		for _, candidate := range matches {
			info, statErr := os.Stat(candidate)
			if statErr == nil && info.Mode().IsRegular() && info.Size() > 0 {
				return candidate, 0, nil
			}
		}
		return "", 0, nil
	}
	prefix := cacheKeyPrefix(assetID, expectedSHA256)
	matches, err := filepath.Glob(filepath.Join(cacheDir, prefix+".*"))
	if err != nil || len(matches) == 0 {
		// Fall back to legacy cache key (assetID without SHA-256 suffix)
		// when the new key yields no results.
		if expectedSHA256 != "" {
			legacyPrefix := cacheKeyPrefix(assetID, "")
			legacyMatches, legacyErr := filepath.Glob(filepath.Join(cacheDir, legacyPrefix+".*"))
			if legacyErr == nil && len(legacyMatches) > 0 {
				// Legacy cache entry exists but has no SHA-256 guarantee.
				// Treat as cache miss so we re-download with integrity.
				return "", 0, nil
			}
		}
		return "", 0, err
	}
	cachedPath := matches[0]

	info, err := os.Stat(cachedPath)
	if err != nil || !info.Mode().IsRegular() {
		// Remove only this invalid entry; the rest of the cache remains
		// untouched and the caller re-downloads the asset.
		recordCacheProjectionEvent(ctx, "eviction", 0, telemetry.StatusOK, "invalid", 0)
		_ = os.Remove(cachedPath)
		return "", 0, nil
	}
	verifyDuration := time.Duration(0)
	if expectedSizeBytes > 0 && info.Size() != expectedSizeBytes {
		recordCacheProjectionEvent(ctx, "eviction", 0, telemetry.StatusOK, "invalid", 0)
		_ = os.Remove(cachedPath)
		return "", 0, nil // size mismatch → re-download
	}
	if expectedSHA256 != "" {
		verifyStarted := time.Now()
		actual, err := sha256File(cachedPath)
		verifyDuration = time.Since(verifyStarted)
		verifyStatus := telemetry.StatusOK
		if err != nil || actual != expectedSHA256 {
			verifyStatus = telemetry.StatusFailed
		}
		recordCacheProjectionEvent(ctx, "hash_verify", verifyDuration, verifyStatus, "", 0)
		if err != nil || actual != expectedSHA256 {
			// A cache hit is valid only after the digest matches. Remove
			// this corrupt entry atomically from the cache namespace before
			// reacquiring it; never clear the entire cache.
			recordCacheProjectionEvent(ctx, "eviction", 0, telemetry.StatusOK, "invalid", 0)
			_ = os.Remove(cachedPath)
			return "", verifyDuration, nil // hash mismatch → re-download
		}
	}
	return cachedPath, verifyDuration, nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// sha256File computes the lowercase hex SHA-256 of a file.
func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

var (
	ErrAssetVerification = errors.New("worker asset cache: verification failed")
	ErrAssetIncomplete   = errors.New("worker asset cache: partial asset incomplete")
)

var activeAssetPartials sync.Map

// assetPartialPath is stable across retry attempts and worker restarts. The
// partial namespace is deliberately separate from final cache files so an
// incomplete response can never be mistaken for a ready asset.
func assetPartialPath(cacheDir, assetID, expectedSHA256 string) string {
	return filepath.Join(cacheDir, "partial", cacheKeyPrefix(assetID, expectedSHA256)+".part")
}

func assetPartialSize(cacheDir, assetID, expectedSHA256 string) int64 {
	info, err := os.Stat(assetPartialPath(cacheDir, assetID, expectedSHA256))
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return 0
	}
	return info.Size()
}

func removeAssetPartial(cacheDir, assetID, expectedSHA256 string) {
	path := assetPartialPath(cacheDir, assetID, expectedSHA256)
	_ = os.Remove(path)
}

func markAssetPartialActive(path string) func() {
	activeAssetPartials.Store(path, struct{}{})
	return func() { activeAssetPartials.Delete(path) }
}

// cleanupOrphanedAssetPartials removes stale partials left by a worker that
// stopped without completing a transfer. Recent partials are retained so a
// restarted worker can resume them with HTTP Range.
// CleanupAssetPartials removes stale partial files for a worker cache. It is
// safe to call during worker initialization and before individual transfers.
func CleanupAssetPartials(cacheDir string, maxAge time.Duration) (int, error) {
	return cleanupOrphanedAssetPartials(cacheDir, maxAge)
}

func cleanupOrphanedAssetPartials(cacheDir string, maxAge time.Duration) (int, error) {
	if maxAge <= 0 {
		maxAge = 24 * time.Hour
	}
	partialDir := filepath.Join(cacheDir, "partial")
	entries, err := os.ReadDir(partialDir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().Add(-maxAge)
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".part") {
			continue
		}
		info, statErr := entry.Info()
		if statErr != nil {
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		path := filepath.Join(partialDir, entry.Name())
		if _, active := activeAssetPartials.Load(path); active {
			continue
		}
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return removed, removeErr
		}
		removed++
	}
	return removed, nil
}

func syncAssetDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// writeVeloxAssetToCacheAtOffset appends a 206 response to an existing
// partial, or truncates/restarts the partial when offset is zero. It never
// promotes a file until the complete partial has passed size and SHA checks.
// syncDir is the directory-durability primitive: production passes
// syncAssetDirectory, tests pass a deterministic stand-in. It is an explicit
// parameter rather than a package-level variable so the shared state stays
// immutable and the seam is visible at the call site.
func writeVeloxAssetToCacheAtOffset(cacheDir, assetID string, expectedSHA256 string, expectedSizeBytes int64, resp *http.Response, offset int64, syncDir func(string) error) (string, int64, string, time.Duration, error) {
	if offset < 0 {
		return "", 0, "", 0, fmt.Errorf("%w: negative resume offset", ErrAssetVerification)
	}
	if err := os.MkdirAll(filepath.Join(cacheDir, "partial"), 0o755); err != nil {
		return "", 0, "", 0, err
	}
	mediaType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if idx := strings.Index(mediaType, ";"); idx >= 0 {
		mediaType = strings.TrimSpace(mediaType[:idx])
	}
	if isHTMLMediaType(mediaType) {
		return "", 0, "", 0, fmt.Errorf("unexpected HTML response while downloading asset")
	}

	reader := bufio.NewReader(resp.Body)
	peek, _ := reader.Peek(512)
	if isHTMLPayload(peek) {
		return "", 0, "", 0, fmt.Errorf("unexpected HTML response while downloading asset")
	}
	if mediaType == "" {
		mediaType = http.DetectContentType(peek)
	}

	ext := extensionForMediaType(mediaType)
	if ext == "" {
		ext = ".audio"
	}

	prefix := cacheKeyPrefix(assetID, expectedSHA256)
	partPath := assetPartialPath(cacheDir, assetID, expectedSHA256)
	deactivatePartial := markAssetPartialActive(partPath)
	defer deactivatePartial()

	effectiveExpectedSize := expectedSizeBytes
	if resp.StatusCode == http.StatusPartialContent {
		contentRange := strings.TrimSpace(resp.Header.Get("Content-Range"))
		start, end, total, rangeErr := parseAssetContentRange(contentRange)
		if rangeErr != nil || start != offset {
			return "", 0, "", 0, fmt.Errorf("%w: invalid Content-Range %q", ErrAssetVerification, contentRange)
		}
		if resp.ContentLength > 0 && end-start+1 != resp.ContentLength {
			return "", 0, "", 0, fmt.Errorf("%w: Content-Range length does not match Content-Length", ErrAssetVerification)
		}
		if total > 0 {
			if effectiveExpectedSize > 0 && total != effectiveExpectedSize {
				return "", 0, "", 0, fmt.Errorf("%w: Content-Range total %d does not match expected size %d", ErrAssetVerification, total, effectiveExpectedSize)
			}
			if effectiveExpectedSize <= 0 {
				effectiveExpectedSize = total
			}
		}
	}
	if effectiveExpectedSize <= 0 && resp.ContentLength <= 0 {
		return "", 0, "", 0, fmt.Errorf("%w: response has no verifiable total size", ErrAssetIncomplete)
	}
	if effectiveExpectedSize <= 0 {
		effectiveExpectedSize = resp.ContentLength
	}

	flags := os.O_CREATE | os.O_WRONLY
	if offset > 0 {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	partFile, err := os.OpenFile(partPath, flags, 0o644)
	if err != nil {
		return "", 0, "", 0, err
	}

	// Hash the complete partial after each append. This avoids treating the
	// suffix hash from a resumed response as the asset hash.
	writtenNow, err := io.Copy(partFile, reader)
	if err != nil {
		_ = partFile.Close()
		// Preserve the partial on stream errors: a later retry/restart can
		// request the remaining suffix instead of starting over.
		return "", 0, "", 0, err
	}
	if err := partFile.Sync(); err != nil {
		_ = partFile.Close()
		return "", 0, "", 0, err
	}
	if err := partFile.Close(); err != nil {
		return "", 0, "", 0, err
	}

	if resp.ContentLength > 0 && writtenNow != resp.ContentLength {
		return "", 0, "", 0, fmt.Errorf("%w: response body truncated (got %d, want %d)", ErrAssetIncomplete, writtenNow, resp.ContentLength)
	}
	info, err := os.Stat(partPath)
	if err != nil {
		return "", 0, "", 0, err
	}
	written := info.Size()
	if written <= 0 {
		return "", 0, "", 0, fmt.Errorf("%w: downloaded asset is empty", ErrAssetVerification)
	}

	// Verify all supplied integrity metadata against the complete partial
	// before promoting it. Verification errors delete the partial because
	// resuming a corrupt byte sequence cannot produce a valid asset.
	verifyStarted := time.Now()
	if effectiveExpectedSize > 0 && written != effectiveExpectedSize {
		// A short partial is a retryable interrupted transfer. Preserve it
		// so the next HTTP attempt can request the missing suffix.
		return "", written, "", time.Since(verifyStarted), fmt.Errorf("%w: downloaded asset size mismatch (got %d, want %d)", ErrAssetIncomplete, written, effectiveExpectedSize)
	}
	actualSHA256, err := sha256File(partPath)
	if err != nil {
		_ = os.Remove(partPath)
		return "", 0, "", time.Since(verifyStarted), fmt.Errorf("%w: hash partial: %v", ErrAssetVerification, err)
	}
	if expectedSHA256 != "" && actualSHA256 != expectedSHA256 {
		_ = os.Remove(partPath)
		return "", 0, "", time.Since(verifyStarted), fmt.Errorf("%w: downloaded asset SHA-256 mismatch", ErrAssetVerification)
	}

	// When no expected digest was supplied, embed the computed digest in the
	// final filename anyway so a later remembered-integrity access can find
	// the entry through the content-addressed key (primo → MISS, successivi
	// → HIT). Files already on disk under the bare asset-ID name from older
	// worker builds are simply re-downloaded once into the suffixed form.
	if expectedSHA256 == "" && actualSHA256 != "" {
		prefix = cacheKeyPrefix(assetID, actualSHA256)
	}

	finalPath := filepath.Join(cacheDir, prefix+ext)
	// Preserve an existing valid destination until both directory fsyncs have
	// succeeded. A plain rename-overwrite followed by Remove(finalPath) on
	// fsync failure would otherwise destroy the last known-good copy.
	backupPath := ""
	if _, statErr := os.Stat(finalPath); statErr == nil {
		backupPath = fmt.Sprintf("%s.previous-%d", finalPath, time.Now().UnixNano())
		if err := os.Rename(finalPath, backupPath); err != nil {
			return "", 0, "", time.Since(verifyStarted), err
		}
	}
	restorePrevious := func() {
		if backupPath == "" {
			return
		}
		_ = os.Remove(finalPath)
		_ = os.Rename(backupPath, finalPath)
	}
	// Rename is atomic within the cache directory. Keep the previous final
	// available for rollback until durability is confirmed.
	if err := os.Rename(partPath, finalPath); err != nil {
		restorePrevious()
		return "", 0, "", time.Since(verifyStarted), err
	}
	// Persist both directory entries after the atomic promotion. If fsync is
	// unavailable on a platform, restore the previous final rather than
	// claiming the new promotion succeeded.
	if err := syncDir(filepath.Join(cacheDir, "partial")); err != nil {
		restorePrevious()
		return "", 0, "", time.Since(verifyStarted), err
	}
	if err := syncDir(cacheDir); err != nil {
		restorePrevious()
		return "", 0, "", time.Since(verifyStarted), err
	}
	if backupPath != "" {
		_ = os.Remove(backupPath)
		// The new final is already durable; failure to persist deletion of the
		// rollback copy is harmless and will be cleaned on a later cache pass.
		_ = syncDir(cacheDir)
	}
	return finalPath, written, actualSHA256, time.Since(verifyStarted), nil
}

// isHTMLMediaType reports whether a Content-Type looks like HTML.
func isHTMLMediaType(mediaType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(mediaType))
	return normalized == "text/html" || strings.HasPrefix(normalized, "text/html;")
}

// isHTMLPayload inspects the very first bytes of a response for HTML markers
// even when the upstream lies about Content-Type.
func isHTMLPayload(data []byte) bool {
	trimmed := strings.ToLower(strings.TrimSpace(string(data)))
	return strings.HasPrefix(trimmed, "<!doctype html") ||
		strings.HasPrefix(trimmed, "<html") ||
		strings.Contains(trimmed, "<body") ||
		strings.Contains(trimmed, "login")
}

// extensionForMediaType maps a Content-Type to the file extension used on
// disk. Falls back to .mp3/.mp4 for audio/video MIME families.
func extensionForMediaType(mediaType string) string {
	mediaType = strings.TrimSpace(mediaType)
	if mediaType == "" {
		return ""
	}
	if exts, err := mime.ExtensionsByType(mediaType); err == nil && len(exts) > 0 {
		return exts[0]
	}
	switch {
	case strings.HasPrefix(strings.ToLower(mediaType), "audio/"):
		return ".mp3"
	case strings.HasPrefix(strings.ToLower(mediaType), "video/"):
		return ".mp4"
	default:
		return ""
	}
}
