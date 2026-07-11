package artifacts

// service_duplicate_blob.go — fallback path for the storage_key
// unique-constraint conflict surfaced by the atomic
// FinalizeVerified SQL tx.
//
// The Finalize pipeline promotes a blob to a content-addressable
// canonical storage_key BEFORE the SQL tx (so a tx rollback reclaims
// the orphan via the reconciler). On the rare path where another
// artifact row already owns the same (storage_provider, storage_key)
// tuple — for example a retry against a partially-cleaned orphan —
// the SQL tx fails on UNIQUE constraint. Without this fallback, the
// master would return ErrTransitionConflict and the worker would
// hang on the failed upload.
//
// This file owns the three private helpers that encapsulate the
// retry-with-alternate-key strategy. Extracting them out of
// service_finalize.go keeps the main orchestrator linear and makes
// the duplicate-blob mechanic individually-testable.

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// isArtifactStorageKeyConflict classifies a SQLite error as the
// (storage_provider, storage_key) UNIQUE-constraint conflict raised
// by FinalizeVerified's INSERT/UPDATE on the artifacts table. Used
// as the discriminator by finalizeWithDuplicateStorageFallback to
// decide whether to retry with an alt storage_key.
//
// Pattern-based matching: SQLite's UNIQUE-constraint error string is
// `UNIQUE constraint failed: artifacts.storage_provider, artifacts.storage_key`.
// Stable across SQLite driver versions (we pin go-sqlite3) but
// brittle if the schema or column names ever change. If that
// happens, this classifier MUST move to a SQL-driver-aware error-type
// check (errors.As) — NOT a substring match — to avoid silent
// misclassification.
func isArtifactStorageKeyConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed: artifacts.storage_provider, artifacts.storage_key")
}

// makeDuplicateStorageKey derives an alternate canonical storage_key
// for the duplicate-blob fallback. The original key is
// content-addressable so an alternate MUST preserve content identity
// (the sha256 prefix is the base) while disambiguating by artifact
// id to dodge the (storage_provider, storage_key) UNIQUE tuple:
//
//	<base>.dup-<artifact-id><ext>
//
// Returns an error when either input is empty (caller guards input
// rather than silently producing a bogus key).
func makeDuplicateStorageKey(storageKey, artifactID string) (string, error) {
	storageKey = strings.TrimSpace(storageKey)
	artifactID = strings.TrimSpace(artifactID)
	if storageKey == "" {
		return "", fmt.Errorf("artifacts: duplicate storage key fallback requires storage key")
	}
	if artifactID == "" {
		return "", fmt.Errorf("artifacts: duplicate storage key fallback requires artifact id")
	}
	ext := filepath.Ext(storageKey)
	base := strings.TrimSuffix(storageKey, ext)
	return base + ".dup-" + artifactID + ext, nil
}

// materializeDuplicateFinalBlob hardlinks (or copies) the original
// canonical blob to the alt storage_key on disk. The hardlink path
// is preferred because it is atomic on the same filesystem and
// preserves the underlying inode's read-only guarantees. Falls back
// to a streaming copy when the filesystem rejects the link (e.g.
// cross-device on the upgrade path where FinalDir() lay on a separate
// mount than the new storage volume).
//
// Idempotent: if the target already exists, returns nil without
// modifying anything. This handles the rare race where two Finalize
// callers collide on the same (artifact_id, alt_key) tuple.
func (s *Service) materializeDuplicateFinalBlob(sourceStorageKey, targetStorageKey string) error {
	if s == nil || s.blobStore == nil {
		return fmt.Errorf("blob store unavailable")
	}
	sourcePath := filepath.Join(s.blobStore.FinalDir(), filepath.FromSlash(sourceStorageKey))
	targetPath := filepath.Join(s.blobStore.FinalDir(), filepath.FromSlash(targetStorageKey))
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(targetPath); err == nil {
		return nil
	}
	if err := os.Link(sourcePath, targetPath); err == nil {
		return nil
	}
	src, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, src); err != nil {
		_ = os.Remove(targetPath)
		return err
	}
	if err := dst.Sync(); err != nil {
		_ = os.Remove(targetPath)
		return err
	}
	return nil
}
