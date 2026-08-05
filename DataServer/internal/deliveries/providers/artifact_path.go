package providers

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"velox-server/internal/deliveries"
	"velox-server/internal/store"
)

// resolveArtifactFilePath is the single path contract shared by file-based
// delivery providers. StorageKey is canonical and must be resolved through the
// BlobStore; a delivery never falls back to a worker-local artifact path. A
// successful result is always an absolute, existing regular file path suitable
// for SDK upload APIs.
func resolveArtifactFilePath(blobStore store.BlobStore, artifact *store.Artifact) (string, error) {
	if artifact == nil {
		return "", permanentArtifactPathError("nil artifact", nil)
	}

	if artifact.StorageKey == "" {
		return "", permanentArtifactPathError(
			fmt.Sprintf("artifact %s has no canonical storage_key", artifact.ID),
			nil,
		)
	}
	if blobStore == nil {
		if filepath.IsAbs(artifact.StorageKey) {
			if path, err := existingAbsoluteFile(artifact.StorageKey); err == nil {
				return path, nil
			} else {
				return "", permanentArtifactPathError(
					fmt.Sprintf("canonical storage_key is unreadable for artifact %s", artifact.ID),
					err,
				)
			}
		}
		return "", permanentArtifactPathError(
			fmt.Sprintf("artifact %s has a relative storage_key but no blob store", artifact.ID),
			fmt.Errorf("relative storage_key requires a blob store"),
		)
	}
	file, err := blobStore.ReadFinal(artifact.StorageKey)
	if err != nil {
		return "", permanentArtifactPathError(
			fmt.Sprintf("canonical storage_key is unreadable for artifact %s", artifact.ID),
			err,
		)
	}
	name := file.Name()
	_ = file.Close()
	return existingAbsoluteFile(name)
}

func existingAbsoluteFile(path string) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("not a regular file: %s", absolute)
	}
	return absolute, nil
}

func permanentArtifactPathError(message string, cause error) error {
	if cause == nil {
		cause = deliveries.ErrProviderPermanent
	} else {
		cause = errors.Join(deliveries.ErrProviderPermanent, cause)
	}
	return &deliveries.ProviderError{
		Class:   deliveries.ErrorClassPermanent,
		Code:    "artifact_path_invalid",
		Message: message,
		Cause:   cause,
	}
}
