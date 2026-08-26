package artifacts

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"velox-server/internal/repository"
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
// uniqueness-conflict idempotency that the spec mandates
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
func FinalStorageKey(blobStore repository.BlobStore, sha256Hex, extension string) (relKey, absPath string, err error) {
	relKey, err = CanonicalStorageKey(sha256Hex, extension)
	if err != nil {
		return "", "", err
	}
	absPath = filepath.Join(blobStore.FinalDir(), relKey)
	return relKey, absPath, nil
}

// PromoteToCanonical promotes the staged blob to its final content-
// addressable location with the durability guarantees the spec requires
// (Fase 3: flush; fsync; close; rename atomico dalla staging alla
// destinazione; fsync directory, quando supportato).
//
// The durable filesystem I/O is delegated to repository.BlobStore.PromoteDurable
// so this package's business logic (content-addressable key derivation +
// finalization semantics) never touches the filesystem driver directly.
//
// Returns the relative canonical storage_key. On any failure, the
// temp file is best-effort cleaned up before the error is returned so
// no half-written final blob can be mistaken for verified bytes.
//
// This is a SAFETY-CRITICAL function: the FS state is what makes the
// SQL "no blob promoted without matching SQL row in READY" promise
// real. An orphaned blob on disk must be (and is) cleaned up by the
// reconciler in chunk 5.
func PromoteToCanonical(blobStore repository.BlobStore, stagingPath, sha256Hex, extension string) (string, error) {
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

	if _, err := blobStore.PromoteDurable(stagingPath, finalPath); err != nil {
		if errors.Is(err, store.ErrPromoteDurableFailed) {
			return "", fmt.Errorf("%w: %v", ErrBlobPromoteFailed, err)
		}
		return "", fmt.Errorf("artifacts: PromoteToCanonical: %w", err)
	}

	return relKey, nil
}

// RemoveStaging best-effort removes the staging blob. Called on
// Receive() failures (ErrHashMismatch, ErrSizeMismatch, write error)
// so we don't leave orphan temp files.
func RemoveStaging(blobStore repository.BlobStore, stagingPath string) {
	if stagingPath == "" || blobStore == nil {
		return
	}
	_ = blobStore.RemoveStaging(stagingPath)
}
