// Package store / blobstore.go
//
// BlobStore abstracts artifact storage with a staging area, canonical
// storage_key generation, and atomic move-to-final on verification.
//
// The implementation uses local filesystem paths:
//
//	staging/  ← worker upload lands here (STAGING status)
//	final/    ← atomic move after VERIFYING → READY (storage_key points here)
//
// Future: swap the implementation for S3/MinIO/R2 without changing callers.
package store

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BlobStore is the storage abstraction for artifact blobs.
type BlobStore interface {
	// StagingPath returns a unique path in the staging area. The caller
	// writes the upload bytes to this path, then calls PromoteToFinal.
	StagingPath(jobID, artifactID, extension string) (string, error)

	// FinalPath returns the canonical storage_key for a verified artifact.
	// The key is deterministic from the artifact's identity so retries
	// produce the same path (idempotent move).
	FinalPath(jobID, artifactID, extension string) string

	// PromoteToFinal moves a staged file to its final canonical location.
	// Returns the storage_key (relative path) on success.
	PromoteToFinal(stagingPath, finalPath string) (string, error)

	// RemoveStaging cleanup a staged file on failure.
	RemoveStaging(path string) error

	// ReadFinal opens the final file for reading (providers use this).
	ReadFinal(storageKey string) (*os.File, error)

	// StagingDir returns the staging root path (for reconciliation).
	StagingDir() string

	// FinalDir returns the final storage root path (for reconciliation).
	FinalDir() string
}

// FilesystemBlobStore implements BlobStore on the local filesystem.
type FilesystemBlobStore struct {
	stagingDir string // e.g. /data/staging/
	finalDir   string // e.g. /data/final/
}

// NewFilesystemBlobStore creates a FilesystemBlobStore, ensuring both directories exist.
func NewFilesystemBlobStore(stagingDir, finalDir string) (*FilesystemBlobStore, error) {
	for _, d := range []string{stagingDir, finalDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return nil, fmt.Errorf("blobstore: create %s: %w", d, err)
		}
	}
	return &FilesystemBlobStore{
		stagingDir: stagingDir,
		finalDir:   finalDir,
	}, nil
}

// StagingPath generates a unique staging path. The path includes a random
// suffix to avoid collisions when the same job produces multiple artifacts.
func (b *FilesystemBlobStore) StagingPath(jobID, artifactID, extension string) (string, error) {
	randBytes := make([]byte, 8)
	if _, err := rand.Read(randBytes); err != nil {
		return "", fmt.Errorf("blobstore: rand: %w", err)
	}
	suffix := hex.EncodeToString(randBytes)
	filename := fmt.Sprintf("%s_%s_%s%s", jobID, artifactID, suffix, extension)
	path := filepath.Join(b.stagingDir, filename)
	return filepath.Clean(path), nil
}

// FinalPath returns the canonical final path for a verified artifact.
// Format: final/<jobID>/<artifactID>/<timestamp>_<sha256_prefix>.ext
func (b *FilesystemBlobStore) FinalPath(jobID, artifactID, extension string) string {
	ts := time.Now().UnixMilli()
	rel := filepath.Join(jobID, fmt.Sprintf("%s_%d%s", artifactID, ts, extension))
	return filepath.Join(b.finalDir, rel)
}

// PromoteToFinal atomically moves a staged file to its final location.
// The parent directory is created if necessary. Returns the storage_key
// (absolute path to final location).
func (b *FilesystemBlobStore) PromoteToFinal(stagingPath, finalPath string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return "", fmt.Errorf("blobstore: mkdir final: %w", err)
	}
	if err := os.Rename(stagingPath, finalPath); err != nil {
		return "", fmt.Errorf("blobstore: rename %s → %s: %w", stagingPath, finalPath, err)
	}
	return finalPath, nil
}

// RemoveStaging removes the staged file (called on verification failure).
func (b *FilesystemBlobStore) RemoveStaging(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("blobstore: remove staging %s: %w", path, err)
	}
	return nil
}

// ReadFinal opens the final file for reading. If storageKey is a relative
// path it is resolved against finalDir; if it is already absolute (legacy
// behaviour from PromoteToFinal returning absolute paths) it is used as-is.
// Relative keys that would escape finalDir via ".." are rejected to prevent
// path traversal outside the final directory.
func (b *FilesystemBlobStore) ReadFinal(storageKey string) (*os.File, error) {
	cleaned := filepath.Clean(storageKey)
	if !filepath.IsAbs(cleaned) {
		resolved := filepath.Join(b.finalDir, cleaned)
		rel, err := filepath.Rel(filepath.Clean(b.finalDir), resolved)
		if err != nil || strings.HasPrefix(filepath.ToSlash(rel), "../") || rel == ".." {
			return nil, fmt.Errorf("blobstore: reject traversal in storage_key %q", storageKey)
		}
		cleaned = resolved
	}
	f, err := os.Open(cleaned)
	if err != nil {
		return nil, fmt.Errorf("blobstore: open %s: %w", storageKey, err)
	}
	return f, nil
}

// StagingDir returns the staging root.
func (b *FilesystemBlobStore) StagingDir() string { return b.stagingDir }

// FinalDir returns the final root.
func (b *FilesystemBlobStore) FinalDir() string { return b.finalDir }

// NopBlobStore is a pass-through blob store for tests that write directly
// to the final directory (preserves legacy behavior).
type NopBlobStore struct {
	baseDir string
}

// NewNopBlobStore creates a NopBlobStore.
func NewNopBlobStore(baseDir string) *NopBlobStore {
	return &NopBlobStore{baseDir: baseDir}
}

func (n *NopBlobStore) StagingPath(_, _, ext string) (string, error) {
	f, err := os.CreateTemp(n.baseDir, "staging-*"+ext)
	if err != nil {
		return "", err
	}
	f.Close()
	return f.Name(), nil
}

func (n *NopBlobStore) FinalPath(_, _, ext string) string {
	f, _ := os.CreateTemp(n.baseDir, "final-*"+ext)
	f.Close()
	return f.Name()
}

func (n *NopBlobStore) PromoteToFinal(staging, _ string) (string, error) {
	return staging, nil // no-op: already final
}

func (n *NopBlobStore) RemoveStaging(path string) error {
	return os.Remove(path)
}

func (n *NopBlobStore) ReadFinal(path string) (*os.File, error) {
	return os.Open(filepath.Clean(path))
}

func (n *NopBlobStore) StagingDir() string { return n.baseDir }
func (n *NopBlobStore) FinalDir() string   { return n.baseDir }

// Ensure BlobStore interface is satisfied at compile time.
var _ BlobStore = (*FilesystemBlobStore)(nil)
var _ BlobStore = (*NopBlobStore)(nil)

// Extension returns the file extension from a filename string.
func Extension(filename string) string {
	ext := filepath.Ext(filename)
	if ext == "" {
		return ".bin"
	}
	return ext
}

// CopyFile copies a file from src to dst (for provider read operations).
func CopyFile(dst, src string) error {
	in, err := os.Open(filepath.Clean(src))
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}
