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
// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Remove after: 2026-09-30
// Read-only:    yes

package store

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/repository"
)

// ErrPromoteDurableFailed marks the atomic rename / staging-removal
// steps of PromoteDurable (the "promotion" half of the operation, as
// opposed to the copy/fsync preparation steps). Callers map it onto
// their own promotion-failed sentinel for log classification without
// importing artifacts' sentinels into the store layer.
var ErrPromoteDurableFailed = errors.New("blobstore: durable promotion failed")

// BlobStore is re-exported from the repository leaf package; the canonical
// declaration lives in internal/repository so consumers do not inherit the
// store dependency graph.
type BlobStore = repository.BlobStore

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
//
// Callers fsync the staged file's CONTENT before calling this, so the bytes
// are durable; the rename itself needs a directory fsync of both the source
// (entry removed) and destination (entry created) directories so a crash
// cannot lose the new final name while a DB row already references it.
// Both are best-effort (POSIX; no-op where directory fsync is unsupported).
func (b *FilesystemBlobStore) PromoteToFinal(stagingPath, finalPath string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return "", fmt.Errorf("blobstore: mkdir final: %w", err)
	}
	if err := os.Rename(stagingPath, finalPath); err != nil {
		return "", fmt.Errorf("blobstore: rename %s → %s: %w", stagingPath, finalPath, err)
	}
	syncDirBestEffort(filepath.Dir(stagingPath))
	syncDirBestEffort(filepath.Dir(finalPath))
	return finalPath, nil
}

// syncDirBestEffort fsyncs a directory entry so a rename/link/copy that
// precedes a DB commit survives a crash (POSIX best-effort; no-op elsewhere).
func syncDirBestEffort(path string) {
	if dir, err := os.Open(path); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
}

// PromoteDurable streams a staged blob to finalPath with the durability
// guarantees the artifact spec requires, then removes the staging file.
// Steps: MkdirAll parent → open staging (tolerating an already-promoted
// identical blob) → CreateTemp in the SAME directory → io.Copy → fsync →
// close → atomic rename → remove staging → best-effort directory fsync.
func (b *FilesystemBlobStore) PromoteDurable(stagingPath, finalPath string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return "", fmt.Errorf("blobstore: promote mkdir: %w", err)
	}

	src, err := os.Open(filepath.Clean(stagingPath))
	if err != nil {
		// Concurrent-finalize tolerance: a peer finalizer may have already
		// promoted the SAME content-addressed key and removed the shared
		// staging file before this caller opened it. The final path is
		// deterministic from (sha256, ext) — if it already exists, the
		// promotion happened and this call is an idempotent no-op.
		if os.IsNotExist(err) {
			if _, statErr := os.Stat(finalPath); statErr == nil {
				return finalPath, nil
			}
		}
		return "", fmt.Errorf("blobstore: promote open staging: %w", err)
	}
	defer src.Close()

	// Temp file in the SAME directory as final target so rename(2) is
	// atomic (POSIX). CreateTemp supplies a unique name: concurrent
	// finalizers for the same content must never share a staging file.
	dst, err := os.CreateTemp(filepath.Dir(finalPath), filepath.Base(finalPath)+".tmp.*")
	if err != nil {
		return "", fmt.Errorf("blobstore: promote create temp: %w", err)
	}
	tempPath := dst.Name()

	cleanupTemp := func() { _ = os.Remove(tempPath) }

	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		cleanupTemp()
		return "", fmt.Errorf("blobstore: promote copy: %w", err)
	}

	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		cleanupTemp()
		return "", fmt.Errorf("blobstore: promote fsync: %w", err)
	}
	if err := dst.Close(); err != nil {
		cleanupTemp()
		return "", fmt.Errorf("blobstore: promote close: %w", err)
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		cleanupTemp()
		return "", fmt.Errorf("%w: rename %s → %s: %v", ErrPromoteDurableFailed, tempPath, finalPath, err)
	}

	// The canonical copy is durable now; remove the upload staging file so
	// successful finalization cannot leave a second, non-addressable copy
	// of the artifact behind. If cleanup fails, surface it rather than
	// silently claiming the staging area is clean.
	if err := os.Remove(filepath.Clean(stagingPath)); err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("%w: remove staging %s: %v", ErrPromoteDurableFailed, stagingPath, err)
	}

	// fsync the directory entry (POSIX best-effort).
	syncDirBestEffort(filepath.Dir(finalPath))

	return finalPath, nil
}

// OpenStagedWrite creates (or truncates) a staged file for writing,
// creating parent directories first.
func (b *FilesystemBlobStore) OpenStagedWrite(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("blobstore: mkdir staged: %w", err)
	}
	f, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("blobstore: open staged write: %w", err)
	}
	return f, nil
}

// OpenStagedRead opens a staged file for reading.
func (b *FilesystemBlobStore) OpenStagedRead(path string) (*os.File, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("blobstore: open staged read: %w", err)
	}
	return f, nil
}

// RemoveStaging removes the staged file (called on verification failure).
func (b *FilesystemBlobStore) RemoveStaging(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("blobstore: remove staging %s: %w", path, err)
	}
	return nil
}

// RemoveFinal removes a promoted final blob after the database transaction
// that should reference it fails. Callers use this only for compensating
// cleanup of an otherwise unreferenced blob.
func (b *FilesystemBlobStore) RemoveFinal(storageKey string) error {
	cleaned := filepath.Clean(storageKey)
	if !filepath.IsAbs(cleaned) {
		cleaned = filepath.Join(b.finalDir, cleaned)
		rel, err := filepath.Rel(filepath.Clean(b.finalDir), cleaned)
		if err != nil || strings.HasPrefix(filepath.ToSlash(rel), "../") || rel == ".." {
			return fmt.Errorf("blobstore: reject traversal in final storage_key %q", storageKey)
		}
	}
	if err := os.Remove(cleaned); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("blobstore: remove final %s: %w", storageKey, err)
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

func (n *NopBlobStore) PromoteDurable(_ string, finalPath string) (string, error) {
	return finalPath, nil // no-op: already final
}

func (n *NopBlobStore) OpenStagedWrite(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
}

func (n *NopBlobStore) OpenStagedRead(path string) (*os.File, error) {
	return os.Open(filepath.Clean(path))
}

func (n *NopBlobStore) RemoveStaging(path string) error {
	return os.Remove(path)
}

func (n *NopBlobStore) RemoveFinal(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (n *NopBlobStore) ReadFinal(path string) (*os.File, error) {
	return os.Open(filepath.Clean(path))
}

func (n *NopBlobStore) StagingDir() string { return n.baseDir }
func (n *NopBlobStore) FinalDir() string   { return n.baseDir }

// Ensure BlobStore interface is satisfied at compile time.
var _ BlobStore = (*FilesystemBlobStore)(nil)
var _ BlobStore = (*NopBlobStore)(nil)

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
