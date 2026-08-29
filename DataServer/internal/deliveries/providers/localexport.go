// Package deliveries/providers: LocalExport skeleton.
//
// LocalExportProvider writes a copy of the artifact into a destination's
// configured output directory. The run remains local (no external upload),
// but it IS a real provider: it stamps remote_url with the new local
// path so the JobViewAssembler can surface it via Drive/social URLs.
//
// Useful for "gray-box" CI environments where external provider credentials
// are unavailable but end-to-end delivery flow still needs to be exercised.
package providers

import (
	"context"
	"os"
	"path/filepath"

	"velox-server/internal/deliveries"
	"velox-server/internal/repository"
	"velox-server/internal/store"
)

// LocalExportProvider handles local-only export deliveries.
type LocalExportProvider struct {
	outputRoot string
	blobStore  repository.BlobStore
}

// NewLocalExportProvider constructs a local-export provider. outputRoot
// is the directory where deliveries will be written. Empty outputRoot
// causes Deliver to return ErrProviderNotConfigured.
func NewLocalExportProvider(outputRoot string, blobStores ...repository.BlobStore) *LocalExportProvider {
	var blobStore repository.BlobStore
	if len(blobStores) > 0 {
		blobStore = blobStores[0]
	}
	return &LocalExportProvider{outputRoot: outputRoot, blobStore: blobStore}
}

// Name returns "local_export".
func (l *LocalExportProvider) Name() string { return "local_export" }

// Deliver copies the artifact's local file into <outputRoot>/<artifact_id>
// and returns its absolute path on Result.RemoteURL.
func (l *LocalExportProvider) Deliver(_ context.Context, artifact *repository.Artifact, destination *deliveries.Destination, _, _ string) (*deliveries.Result, error) {
	if l == nil || l.outputRoot == "" {
		return nil, deliveries.ErrProviderNotConfigured
	}
	source, err := resolveArtifactFilePath(l.blobStore, artifact)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(l.outputRoot, 0o755); err != nil {
		return nil, err
	}
	target := filepath.Join(l.outputRoot, filepath.Base(source))
	if err := store.CopyFile(target, source); err != nil {
		return nil, err
	}
	return &deliveries.Result{
		Success:   true,
		RemoteID:  filepath.Base(target),
		RemoteURL: target,
	}, nil
}
