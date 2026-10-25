package executors

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"

	"velox-shared/iopool"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/storage"
)

func storageResolverFromExecutionContext(execCtx executor.ExecutionContext) *storage.Resolver {
	if provider, ok := execCtx.(interface{ StorageResolver() *storage.Resolver }); ok && provider != nil {
		return provider.StorageResolver()
	}
	return nil
}

const (
	outputEstimateConservativeBps = 4_200_000
	outputEstimateMargin          = 1.20
)

func estimateOutputBytesFromDuration(durationUS int64) int64 {
	if durationUS <= 0 {
		return -1
	}
	return int64(float64(durationUS) / 1_000_000 * outputEstimateConservativeBps / 8 * outputEstimateMargin)
}

func artifactFromFile(kind, path string) (executor.ArtifactRef, error) {
	info, err := os.Stat(path)
	if err != nil {
		return executor.ArtifactRef{}, err
	}
	if info.Size() <= 0 {
		return executor.ArtifactRef{}, errors.New("artifact is empty")
	}
	f, err := os.Open(path)
	if err != nil {
		return executor.ArtifactRef{}, err
	}
	defer f.Close()
	hash := sha256.New()
	pooled := iopool.Get()
	defer iopool.Put(pooled)
	if _, err := io.CopyBuffer(hash, f, pooled[:]); err != nil {
		return executor.ArtifactRef{}, err
	}
	return executor.ArtifactRef{Type: kind, Hash: hex.EncodeToString(hash.Sum(nil)), URI: path, SizeBytes: info.Size()}, nil
}
