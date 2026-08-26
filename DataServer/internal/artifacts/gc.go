package artifacts

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/artifactsstore"
	"velox-server/internal/repository"
)

// RunArtifactGC leases durable candidates, deletes only local paths within
// the configured final/staging roots, and acknowledges the DB row afterwards.
// A missing file is success; an unsafe path or unsupported provider remains
// eligible for an operator-visible retry.
func RunArtifactGC(ctx context.Context, db *artifactsstore.ArtifactGCStore, blobStore repository.BlobStore, owner string, now time.Time, lease time.Duration, limit int) (deleted, failed int, err error) {
	if db == nil || blobStore == nil || owner == "" {
		return 0, 0, fmt.Errorf("artifact gc: db, blob store and owner are required")
	}
	candidates, err := db.LeaseArtifactGCCandidates(ctx, owner, now, lease, limit)
	if err != nil {
		return 0, 0, err
	}
	for _, candidate := range candidates {
		path, pathErr := gcPath(candidate, blobStore)
		if pathErr == nil {
			removeErr := os.Remove(path)
			if removeErr == nil || os.IsNotExist(removeErr) {
				if err := db.CompleteArtifactGC(ctx, candidate.ArtifactID, owner, true, ""); err != nil {
					return deleted, failed, err
				}
				deleted++
				continue
			}
			pathErr = removeErr
		}
		failed++
		if err := db.CompleteArtifactGC(ctx, candidate.ArtifactID, owner, false, pathErr.Error()); err != nil {
			return deleted, failed, err
		}
	}
	return deleted, failed, nil
}

func gcPath(candidate artifactsstore.ArtifactGCCandidate, blobStore repository.BlobStore) (string, error) {
	if candidate.StorageProvider != "" && candidate.StorageProvider != "local" {
		return "", fmt.Errorf("unsupported storage provider %q", candidate.StorageProvider)
	}
	path := candidate.LocalPath
	if path == "" && candidate.StorageKey == "" {
		return "", fmt.Errorf("artifact has no deletable storage path")
	}
	if path == "" {
		path = filepath.Join(blobStore.FinalDir(), filepath.FromSlash(candidate.StorageKey))
	}
	cleaned := filepath.Clean(path)
	for _, root := range []string{filepath.Clean(blobStore.FinalDir()), filepath.Clean(blobStore.StagingDir())} {
		rel, err := filepath.Rel(root, cleaned)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel) {
			return cleaned, nil
		}
	}
	return "", fmt.Errorf("unsafe artifact path %q", path)
}
