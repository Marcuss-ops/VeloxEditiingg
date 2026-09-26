package artifacts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/artifactsstore"
	"velox-server/internal/repository"
)

var errArtifactNoDeletablePath = errors.New("artifact has no deletable storage path")

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
		// A durable candidate can outlive its artifact row (for example, a
		// failed staging record removed by a repair). There is no object or
		// filesystem path left to delete, so retire only the candidate row.
		if candidate.ArtifactStatus == "" {
			if err := db.CompleteArtifactGCNoObject(ctx, candidate.ArtifactID, owner); err != nil {
				return deleted, failed, err
			}
			deleted++
			continue
		}
		if candidate.StorageProvider == "" || candidate.StorageProvider == "local" {
			if candidate.StorageKey == "" && candidate.LocalPath == "" {
				if candidate.ArtifactStatus == "FAILED" || candidate.ArtifactStatus == "QUARANTINED" || candidate.ArtifactStatus == "DELETED" {
					if err := db.CompleteArtifactGC(ctx, candidate.ArtifactID, owner, true, ""); err != nil {
						return deleted, failed, err
					}
					deleted++
					continue
				}
				pathErr := errArtifactNoDeletablePath
				failed++
				if err := db.CompleteArtifactGCAt(ctx, candidate.ArtifactID, owner, false, pathErr.Error(), now.Add(artifactGCRetryDelay(candidate.DeleteAttempts))); err != nil {
					return deleted, failed, err
				}
				continue
			}
		}
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
		if err := db.CompleteArtifactGCAt(ctx, candidate.ArtifactID, owner, false, pathErr.Error(), now.Add(artifactGCRetryDelay(candidate.DeleteAttempts))); err != nil {
			return deleted, failed, err
		}
	}
	return deleted, failed, nil
}

func artifactGCRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 10 {
		attempt = 10
	}
	delay := time.Minute * time.Duration(1<<attempt)
	if delay > 24*time.Hour {
		return 24 * time.Hour
	}
	return delay
}

func gcPath(candidate artifactsstore.ArtifactGCCandidate, blobStore repository.BlobStore) (string, error) {
	if candidate.StorageProvider != "" && candidate.StorageProvider != "local" {
		return "", fmt.Errorf("unsupported storage provider %q", candidate.StorageProvider)
	}
	path := candidate.LocalPath
	if path == "" && candidate.StorageKey == "" {
		return "", errArtifactNoDeletablePath
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
