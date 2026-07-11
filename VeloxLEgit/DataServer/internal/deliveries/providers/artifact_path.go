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
// delivery providers. StorageKey is canonical and is resolved through the
// BlobStore; LocalPath is an explicit legacy fallback. A successful result is
// always an absolute, existing regular file path suitable for SDK upload APIs.
func resolveArtifactFilePath(blobStore store.BlobStore, artifact *store.Artifact) (string, error) {
	if artifact == nil {
		return "", permanentArtifactPathError("nil artifact", nil)
	}

	var canonicalErr error
	if artifact.StorageKey != "" {
		if blobStore != nil {
			file, err := blobStore.ReadFinal(artifact.StorageKey)
			if err == nil {
				name := file.Name()
				_ = file.Close()
				return existingAbsoluteFile(name)
			}
			canonicalErr = err
		} else if filepath.IsAbs(artifact.StorageKey) {
			path, err := existingAbsoluteFile(artifact.StorageKey)
			if err == nil {
				return path, nil
			}
			canonicalErr = err
		} else {
			canonicalErr = fmt.Errorf("relative storage_key requires a blob store")
		}
	}

	if artifact.LocalPath != "" {
		path, err := existingAbsoluteFile(artifact.LocalPath)
		if err == nil {
			return path, nil
		}
		if canonicalErr != nil {
			return "", permanentArtifactPathError(
				fmt.Sprintf("storage_key and legacy local_path are unreadable for artifact %s", artifact.ID),
				errors.Join(canonicalErr, err),
			)
		}
		return "", permanentArtifactPathError(
			fmt.Sprintf("legacy local_path is unreadable for artifact %s", artifact.ID),
			err,
		)
	}

	if canonicalErr != nil {
		return "", permanentArtifactPathError(
			fmt.Sprintf("storage_key is unreadable for artifact %s", artifact.ID),
			canonicalErr,
		)
	}
	return "", permanentArtifactPathError(
		fmt.Sprintf("artifact %s has no storage_key or local_path", artifact.ID),
		nil,
	)
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
