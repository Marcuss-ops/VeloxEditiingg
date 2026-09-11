package executors

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
)

var (
	errPreparedAssetBindings  = errors.New("video.assemble.copy.v1: prepared asset bindings are required")
	errPreparedAssetIntegrity = errors.New("video.assemble.copy.v1: prepared asset binding integrity mismatch")
)

func decodeCompiledPlanV2(spec executor.TaskSpec) (*contract.CompiledRenderPlanV2, error) {
	plan, err := contract.DecodeCompiledRenderPlanV2Payload(spec.Payload)
	if err != nil {
		return nil, fmt.Errorf("video.assemble.copy.v1: decode CompiledRenderPlanV2: %w", err)
	}
	if plan == nil {
		return nil, errors.New("video.assemble.copy.v1: compiled plan JSON must be present")
	}
	return plan, nil
}

func validateBindings(plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings) error {
	if plan == nil || bindings == nil {
		return errPreparedAssetBindings
	}
	assetByID := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assetByID[asset.AssetID] = asset
		if err := validateBinding(asset.AssetID, asset.SHA256, asset.SizeBytes, bindings); err != nil {
			return err
		}
	}
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			asset, ok := assetByID[segment.AssetID]
			if !ok {
				return fmt.Errorf("%w: segment asset_id=%q is not declared", errPreparedAssetIntegrity, segment.AssetID)
			}
			if err := validateBinding(segment.AssetID, asset.SHA256, asset.SizeBytes, bindings); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateBinding(assetID, wantSHA string, wantSize int64, bindings runtimeassets.Bindings) error {
	binding, ok := bindings[assetID]
	if !ok || strings.TrimSpace(binding.Path) == "" {
		return fmt.Errorf("%w: asset_id=%q", errPreparedAssetBindings, assetID)
	}
	if strings.TrimSpace(binding.SHA256) == "" || binding.SHA256 != wantSHA || wantSize <= 0 || binding.Size != wantSize {
		return fmt.Errorf("%w: asset_id=%q declared metadata does not match plan", errPreparedAssetIntegrity, assetID)
	}
	info, err := os.Stat(binding.Path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		if err == nil {
			err = errors.New("file is empty or not regular")
		}
		return fmt.Errorf("%w: asset_id=%q path: %v", errPreparedAssetIntegrity, assetID, err)
	}
	if info.Size() != wantSize {
		return fmt.Errorf("%w: asset_id=%q actual size=%d want=%d", errPreparedAssetIntegrity, assetID, info.Size(), wantSize)
	}
	if binding.Verified {
		return nil
	}
	file, err := os.Open(binding.Path)
	if err != nil {
		return fmt.Errorf("%w: asset_id=%q open: %v", errPreparedAssetIntegrity, assetID, err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("%w: asset_id=%q hash: %v", errPreparedAssetIntegrity, assetID, err)
	}
	actualSHA := hex.EncodeToString(hash.Sum(nil))
	if actualSHA != wantSHA || actualSHA != binding.SHA256 {
		return fmt.Errorf("%w: asset_id=%q actual sha256=%s want=%s", errPreparedAssetIntegrity, assetID, actualSHA, wantSHA)
	}
	return nil
}

func safeOutputJobID(jobID string) (string, error) {
	if strings.TrimSpace(jobID) == "" || jobID == "." || jobID == ".." || filepath.IsAbs(jobID) || strings.ContainsAny(jobID, "/\\\x00") || filepath.Base(jobID) != jobID {
		return "", errors.New("job_id must be a non-empty path-free identifier")
	}
	return jobID, nil
}
