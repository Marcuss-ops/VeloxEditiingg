package worker

import (
	"bufio"
	"context"
	"crypto/sha256"
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

// assetBlobPath returns the content-addressed blob location for a verified
// SHA-256 digest: <cacheDir>/<sha[:2]>/<sha><ext>. The blob is identified by
// its bytes, never the asset ID, so distinct assets with identical bytes share
// one physical file (dedup).
func assetBlobPath(cacheDir, sha256Hex, ext string) string {
	if len(sha256Hex) < 2 {
		return filepath.Join(cacheDir, sha256Hex+ext)
	}
	return filepath.Join(cacheDir, sha256Hex[:2], sha256Hex+ext)
}

// assetBlobGlob returns a glob matching a content-addressed blob for any
// extension: the bytes are the identity, the extension is only a hint.
func assetBlobGlob(cacheDir, sha256Hex string) string {
	if len(sha256Hex) < 2 {
		return filepath.Join(cacheDir, sha256Hex+".*")
	}
	return filepath.Join(cacheDir, sha256Hex[:2], sha256Hex+".*")
}

// assetPartialKey builds the filesystem-safe partial-namespace key from
// assetID and an optional SHA-256 prefix. It is used ONLY for the resumable
// staging file (<cacheDir>/partial/<assetID>_<sha12>.part): the partial is
// keyed by asset so a restarted worker can resume the same transfer. Final
// blobs are content-addressed via assetBlobPath and never use this key.
func assetPartialKey(assetID string, sha256Prefix string) string {
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
func cachedAssetPath(cacheDir, expectedSHA256 string, expectedSizeBytes int64) (string, error) {
	path, _, err := cachedAssetPathTimedWithContext(context.Background(), cacheDir, expectedSHA256, expectedSizeBytes)
	return path, err
}

func cachedAssetPathTimedWithContext(ctx context.Context, cacheDir, expectedSHA256 string, expectedSizeBytes int64) (string, time.Duration, error) {
	// A content-addressed blob hit requires the full integrity contract: the
	// SHA locates the blob and the size gates the stat check. Partial metadata
	// (SHA-only or size-only) cannot address a verified blob and is reported
	// as a miss; the resolver upgrades partial metadata through the remembered
	// self-verified digest before reaching this probe.
	if expectedSHA256 == "" || expectedSizeBytes <= 0 {
		return "", 0, nil
	}
	matches, err := filepath.Glob(assetBlobGlob(cacheDir, expectedSHA256))
	if err != nil || len(matches) == 0 {
		return "", 0, nil
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
	if info.Size() != expectedSizeBytes {
		recordCacheProjectionEvent(ctx, "eviction", 0, telemetry.StatusOK, "invalid", 0)
		_ = os.Remove(cachedPath)
		return "", 0, nil // size mismatch → re-download
	}
	verifyStarted := time.Now()
	actual, err := sha256File(cachedPath)
	verifyDuration := time.Since(verifyStarted)
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
	return cachedPath, verifyDuration, nil
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
	return filepath.Join(cacheDir, "partial", assetPartialKey(assetID, expectedSHA256)+".part")
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

// writeVeloxAssetStreamToCacheAtOffset streams body into the asset partial at
// offset (appending a resumed suffix or truncating a fresh download), then
// verifies size and SHA-256 before atomic promotion. It is the generic byte
// sink behind the downloader.AssetSource seam: it takes a plain reader plus
// source metadata instead of an *http.Response, so the HTTP coupling lives
// only in httpAssetSource.Open. The offset/total contract is validated by the
// caller (Open validates Content-Range start; the chunked pipeline validates
// its own bounded ranges); the final size+SHA check here is the last gate.
// syncDir is the directory-durability primitive: production passes
// syncAssetDirectory, tests pass a deterministic stand-in. It is an explicit
// parameter rather than a package-level variable so the shared state stays
// immutable and the seam is visible at the call site.
func writeVeloxAssetStreamToCacheAtOffset(cacheDir, assetID string, expectedSHA256 string, expectedSizeBytes int64, body io.Reader, offset int64, mediaType string, totalSizeBytes int64, syncDir func(string) error) (string, int64, string, time.Duration, error) {
	if offset < 0 {
		return "", 0, "", 0, fmt.Errorf("%w: negative resume offset", ErrAssetVerification)
	}
	if err := os.MkdirAll(filepath.Join(cacheDir, "partial"), 0o755); err != nil {
		return "", 0, "", 0, err
	}
	mediaType = strings.TrimSpace(mediaType)
	if idx := strings.Index(mediaType, ";"); idx >= 0 {
		mediaType = strings.TrimSpace(mediaType[:idx])
	}
	if isHTMLMediaType(mediaType) {
		return "", 0, "", 0, fmt.Errorf("unexpected HTML response while downloading asset")
	}

	reader := bufio.NewReader(body)
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

	partPath := assetPartialPath(cacheDir, assetID, expectedSHA256)
	deactivatePartial := markAssetPartialActive(partPath)
	defer deactivatePartial()

	effectiveExpectedSize := expectedSizeBytes
	if effectiveExpectedSize <= 0 {
		effectiveExpectedSize = totalSizeBytes
	}
	if effectiveExpectedSize <= 0 {
		return "", 0, "", 0, fmt.Errorf("%w: response has no verifiable total size", ErrAssetIncomplete)
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
	if _, err := io.Copy(partFile, reader); err != nil {
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

	return verifyAndPromoteVeloxAsset(cacheDir, expectedSHA256, effectiveExpectedSize, partPath, ext, syncDir)
}

// verifyAndPromoteVeloxAsset is the shared finalize step for both the
// single-stream and chunked byte pipelines: it verifies the fully-written
// partial against the integrity contract (size + SHA-256) and atomically
// promotes it to the final cache path. A size mismatch returns
// ErrAssetIncomplete and PRESERVES the partial (a short write is retryable);
// a hash mismatch deletes it (resuming corrupt bytes can never produce a
// valid asset).
func verifyAndPromoteVeloxAsset(cacheDir, expectedSHA256 string, effectiveExpectedSize int64, partPath, ext string, syncDir func(string) error) (string, int64, string, time.Duration, error) {
	info, err := os.Stat(partPath)
	if err != nil {
		return "", 0, "", 0, err
	}
	written := info.Size()
	if written <= 0 {
		return "", 0, "", 0, fmt.Errorf("%w: downloaded asset is empty", ErrAssetVerification)
	}

	verifyStarted := time.Now()
	if effectiveExpectedSize > 0 && written != effectiveExpectedSize {
		// A short partial is a retryable interrupted transfer. Preserve it
		// so a later attempt can request the missing suffix.
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

	// The final blob is content-addressed: the identity is the verified
	// digest, never the asset ID, so two assets with the same bytes share one
	// physical file. When no expected digest was supplied, the digest computed
	// during verification becomes the identity anyway, so a later
	// remembered-integrity access finds the entry through the same key.
	blobSHA := expectedSHA256
	if blobSHA == "" {
		blobSHA = actualSHA256
	}
	finalPath := assetBlobPath(cacheDir, blobSHA, ext)
	blobDir := filepath.Dir(finalPath)
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		return "", 0, "", time.Since(verifyStarted), err
	}
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
	if blobDir != cacheDir {
		if err := syncDir(blobDir); err != nil {
			restorePrevious()
			return "", 0, "", time.Since(verifyStarted), err
		}
	}
	if err := syncDir(cacheDir); err != nil {
		restorePrevious()
		return "", 0, "", time.Since(verifyStarted), err
	}
	// The promoted blob is now immutable from the normal worker: read-only
	// (0444) so an accidental write can never corrupt a verified
	// content-addressed file. A later re-download still replaces it
	// atomically (rename needs only directory write permission) and eviction
	// still unlinks it (a directory operation), so the read-only mode does
	// not restrict the cache lifecycle.
	if err := os.Chmod(finalPath, 0o444); err != nil {
		restorePrevious()
		return "", 0, "", time.Since(verifyStarted), fmt.Errorf("chmod promoted blob read-only: %w", err)
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

// sniffAssetExtension detects a completed asset's MIME type from its first
// bytes and maps it to the cache filename extension, falling back to ".audio"
// (mirrors the single-stream fallback in writeVeloxAssetStreamToCacheAtOffset). The
// chunked pipeline uses it because no single response Content-Type header is
// authoritative across parallel range requests.
func sniffAssetExtension(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ".audio"
	}
	defer f.Close()
	head := make([]byte, 512)
	n, _ := io.ReadFull(f, head)
	// io.ReadFull returns ErrUnexpectedEOF when the file is smaller than 512;
	// n still carries the bytes read, which is all sniffing needs.
	ext := extensionForMediaType(http.DetectContentType(head[:n]))
	if ext == "" {
		return ".audio"
	}
	return ext
}
