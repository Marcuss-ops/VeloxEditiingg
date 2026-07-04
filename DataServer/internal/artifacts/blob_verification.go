// Package artifacts / blob_verification.go
//
// Blob-handling helpers shared by Receive (write path) and Finalize
// (read + promote path):
//
//   - stagingTempKey: temp-key layout under blobStore.StagingDir().
//   - mimeToExt: stable file-extension mapping for the storage_key so
//     one sha256 → one canonical storage_key across Receive, Finalize,
//     and ReconcilerCleanup.
//   - countingWriter: pipe-through byte counter for io.MultiWriter.
//   - verifyStagedBlob: post-write end-to-end re-hash (trust boundary).
//   - detectMIME: content sniff of the staged blob (first 512 bytes
//     via http.DetectContentType); falls back to "" on read error.
//     Called by Finalize to derive the canonical storage_key
//     extension after BeginUpload-declared mime resolution.
//
// Centralized so the same hash + size semantics apply to staging,
// Receive, Finalize, and ReconcilerCleanup.
package artifacts

import (
	"crypto/sha256"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"

	"velox-server/internal/store"
)

// stagingTempKey returns the staging blob path used during Receive().
// Lives under blobStore.StagingDir() so removal is trivial on hash /
// size mismatch (just call blobStore.RemoveStaging).
func stagingTempKey(bl store.BlobStore, uploadID string) string {
	return filepath.Join(bl.StagingDir(), "upload-"+uploadID+".tmp")
}

// mimeToExt maps a master-detected MIME to a stable file extension.
// Fallback: ".bin" so the SHA-derived storage_key is still valid for
// unknown mime types. The spec mandates the extension in the
// storage_key; mime alone is not enough (text/plain → .txt,
// application/json → .json, etc.).
//
// The result MUST be applied identically across Finalize,
// ReconcilerCleanup, and any pre-create path so a single sha256 maps
// to a single canonical storage_key. Centralizing this here prevents
// drift across the 3+ call sites.
func mimeToExt(mimeType string) string {
	if mimeType == "" {
		return ".bin"
	}
	exts, err := mime.ExtensionsByType(mimeType)
	if err == nil && len(exts) > 0 && exts[0] != "" {
		ext := exts[0]
		if ext[0] != '.' {
			ext = "." + ext
		}
		return ext
	}
	return ".bin"
}

// countingWriter is the io.Writer side of io.MultiWriter — it counts
// bytes while piping them through to the blob on disk. The spec example
// (writer = io.MultiWriter(temporaryBlobWriter, hasher, counter))
// requires a counter implementation that does not buffer.
type countingWriter struct{ n int64 }

// Write records the byte count from a MultiWriter pipe-through.
// Always succeeds; never returns a downstream error so the io.Copy
// budget stays determinate.
func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// verifyStagedBlob reads the staged temp file end-to-end and returns
// (sha256 hex, byte count). Used AFTER io.Copy completes in Receive()
// to catch io.MultiWriter partial-write hazards where a downstream
// error would leave the disk with bytes that were hashed + counted but
// never actually durably written. The cost is one extra fs read but
// it is correctness-critical for the trust boundary.
func verifyStagedBlob(path string) (string, int64, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return "", 0, fmt.Errorf("verifyStagedBlob open: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("verifyStagedBlob read: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), n, nil
}

// detectMIME sniffs the first 512 bytes of path and returns the
// canonical MIME type via http.DetectContentType. Falls back to ""
// when the file cannot be read (caller treats "" as "no signal,
// use BeginUpload-declared mime or application/octet-stream").
//
// Co-located with mimeToExt because the two form the MIME pipeline:
// detectMIME produces the type, mimeToExt produces the canonical
// file extension. Keeping them in the same file prevents drift
// between detection and extension mapping.
func detectMIME(path string) string {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return ""
	}
	defer f.Close()
	var sniff [512]byte
	n, _ := io.ReadFull(f, sniff[:])
	return http.DetectContentType(sniff[:n])
}
