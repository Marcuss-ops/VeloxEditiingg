package artifacts

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"velox-server/internal/store"
)

// STORAGE KEY FORMAT
//
// The canonical storage_key is content-addressable, derived from the
// master-computed SHA-256 and a worker-declared extension:
//
//	artifacts/sha256/<primi-2>/<sha256>.<ext>
//
//	e.g. artifacts/sha256/ab/abcdef123456.mp4
//
// The worker NEVER provides a path. (sha256, ext) is computed in Receive()
// and passed via FinalizeArtifactAndCompleteJobCommand. A retry of
// FINALIZE produces the same storage_key — so a crashed mid-promote
// retry is naturally idempotent at the FS layer, complementing the
// INSERT...ON CONFLICT DO NOTHING idempotency that the spec mandates
// on job_deliveries at the SQL layer.

// CanonicalStorageKey returns the relative storage_key for an artifact.
// Exposed so callers can pre-compute the path for backup / audit. The
// "." prefix on extension is enforced by mimeToExt (see service.go).
func CanonicalStorageKey(sha256Hex, extension string) (string, error) {
	if len(sha256Hex) < 2 {
		return "", fmt.Errorf("%w: sha256 hex too short", ErrStorageKeyInvalid)
	}
	ext := strings.TrimSpace(extension)
	if ext == "" {
		ext = ".bin"
	} else if ext[0] != '.' {
		ext = "." + ext
	}
	return filepath.ToSlash(filepath.Join(
		"artifacts",
		"sha256",
		sha256Hex[:2],
		sha256Hex+ext,
	)), nil
}

// FinalStorageKey is a higher-level wrapper: takes a BlobStore + sha +
// mime and returns both the relative canonical key AND the absolute
// filesystem path the Promotion will land on.
func FinalStorageKey(blobStore store.BlobStore, sha256Hex, extension string) (relKey, absPath string, err error) {
	relKey, err = CanonicalStorageKey(sha256Hex, extension)
	if err != nil {
		return "", "", err
	}
	absPath = filepath.Join(blobStore.FinalDir(), relKey)
	return relKey, absPath, nil
}

// PromoteToCanonical streams the staged blob to its final content-
// addressable location with the durability guarantees the spec requires
// (Fase 3, "Per FilesystemBlobStore: flush; fsync; close; rename atomico
// dalla staging alla destinazione; fsync directory, quando supportato.").
//
// Steps:
//  1. compute the canonical storage_key from sha + extension
//  2. ensure the parent directory exists (MkdirAll)
//  3. open the staging file for reading
//  4. create a temp file IN THE SAME DIRECTORY as finalPath so the
//     rename(2) is atomic on POSIX (same filesystem)
//  5. copy bytes from staging to temp via io.Copy
//  6. flush -> fsync -> close the temp handle
//  7. rename the temp file to the final canonical path
//  8. fsync the parent directory (best-effort; Windows may no-op)
//
// Returns the relative canonical storage_key. On any failure, the
// temp file is best-effort cleaned up before the error is returned so
// no half-written final blob can be mistaken for verified bytes.
//
// This is a SAFETY-CRITICAL function: the FS state is what makes the
// SQL "no blob promoted without matching SQL row in READY" promise
// real. An orphaned blob on disk must be (and is) cleaned up by the
// reconciler in chunk 5.
func PromoteToCanonical(blobStore store.BlobStore, stagingPath, sha256Hex, extension string) (string, error) {
	if blobStore == nil {
		return "", fmt.Errorf("artifacts: PromoteToCanonical: nil blob store")
	}
	if stagingPath == "" {
		return "", fmt.Errorf("artifacts: PromoteToCanonical: missing staging_path")
	}
	if len(sha256Hex) < 2 {
		return "", fmt.Errorf("%w: sha256 hex too short", ErrStorageKeyInvalid)
	}

	relKey, finalPath, err := FinalStorageKey(blobStore, sha256Hex, extension)
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return "", fmt.Errorf("artifacts: PromoteToCanonical mkdir: %w", err)
	}

	src, err := os.Open(filepath.Clean(stagingPath))
	if err != nil {
		return "", fmt.Errorf("artifacts: PromoteToCanonical open staging: %w", err)
	}
	defer src.Close()

	// Temp file in the SAME directory as final target so rename(2)
	// is atomic (POSIX). The suffix is keyed on the first 8 chars of
	// sha256 so two crashed retries cannot collide on the temp file.
	tempPath := finalPath + ".tmp." + sha256Hex[:8]
	dst, err := os.OpenFile(filepath.Clean(tempPath),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return "", fmt.Errorf("artifacts: PromoteToCanonical create temp: %w", err)
	}

	cleanupTemp := func() { _ = os.Remove(tempPath) }

	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		cleanupTemp()
		return "", fmt.Errorf("artifacts: PromoteToCanonical copy: %w", err)
	}

	// 6. flush + fsync + close the temp handle.
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		cleanupTemp()
		return "", fmt.Errorf("artifacts: PromoteToCanonical fsync: %w", err)
	}
	if err := dst.Close(); err != nil {
		cleanupTemp()
		return "", fmt.Errorf("artifacts: PromoteToCanonical close: %w", err)
	}

	// 7. rename atomically. On POSIX, rename is atomic when source and
	//    target are on the same filesystem; OS-specific semantics
	//    apply on Windows (MoveFileEx with REPLACE_EXISTING).
	if err := os.Rename(tempPath, finalPath); err != nil {
		cleanupTemp()
		return "", fmt.Errorf("%w: rename %s -> %s: %v",
			ErrBlobPromoteFailed, tempPath, finalPath, err)
	}

	// 8. fsync the directory entry (POSIX best-effort).
	if dir, derr := os.Open(filepath.Dir(finalPath)); derr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	return relKey, nil
}

// RemoveStaging best-effort removes the staging blob. Called on
// Receive() failures (ErrHashMismatch, ErrSizeMismatch, write error)
// so we don't leave orphan temp files.
func RemoveStaging(blobStore store.BlobStore, stagingPath string) {
	if stagingPath == "" || blobStore == nil {
		return
	}
	_ = blobStore.RemoveStaging(stagingPath)
}
