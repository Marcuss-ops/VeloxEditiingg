package workercache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/assetref"
)

// ValidateBlobForRead is the shared physical-read gate for cache callers.
// It checks metadata plus stat/size; full SHA verification remains at
// promotion and in the background integrity scrubber.
func (c *Cache) ValidateBlobForRead(_ context.Context, blob Blob) (bool, error) {
	if !validStoredContentHash(string(blob.ContentHash)) || strings.HasPrefix(string(blob.ContentHash), legacyBlobKeyPrefix) {
		return false, nil
	}
	return c.validatePhysicalPath(blob.LocalPath, blob.SizeBytes)
}

func validStoredContentHash(stored string) bool {
	if strings.HasPrefix(stored, legacyBlobKeyPrefix) {
		return true
	}
	_, err := assetref.ParseContentHash(stored)
	return err == nil
}

func (c *Cache) validatePhysicalPath(path string, expectedSize int64) (bool, error) {
	if path == "" || expectedSize < 0 {
		return false, nil
	}
	// Open (without a root) is retained for metadata-only tools and legacy
	// fixtures that intentionally do not materialize files. Production workers
	// use OpenWithRoot and therefore always take the physical validation path.
	if c.root == "" {
		return true, nil
	}
	if c.root != "" && !pathWithinRoot(c.root, path) {
		return false, nil
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		// Do not invalidate on transient permission/I/O errors.
		return false, err
	}
	return info.Mode().IsRegular() && info.Size() == expectedSize, nil
}

func pathWithinRoot(root, path string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(absPath))
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}
