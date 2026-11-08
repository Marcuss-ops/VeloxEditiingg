package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// LookupPreparedWindowAsset returns the certified fragment registered by any
// worker for a content-preparation identity. FINALIZE consults this before
// publishing a preparation task, so a window prepared for one job is reused
// by another job and by a different assembly worker without re-encoding,
// re-probing or transferring the source again.
//
// The identity is a hash over the canonical source window, output profile
// revision and preparation revision, so a hit already implies matching
// evidence; READY status plus a certified SHA-256/size still fail closed.
func (s *AssetService) LookupPreparedWindowAsset(ctx context.Context, identity string) (*PreparedWindowAssetBinding, error) {
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("prepared-window cache lookup is unavailable")
	}
	identity = strings.TrimSpace(identity)
	if len(identity) != 64 {
		return nil, fmt.Errorf("prepared-window cache identity must be a SHA-256")
	}
	asset, err := s.GetBySourceReference(ctx, "prepared-window:"+identity)
	if err != nil {
		return nil, fmt.Errorf("prepared-window cache lookup: %w", err)
	}
	if asset == nil {
		return nil, nil
	}
	if asset.Status != AssetStatusReady || !validStockSourceSHA256(asset.SHA256) || asset.SizeBytes <= 0 {
		return nil, fmt.Errorf("prepared-window cache asset %q is not a certified READY fragment", identity)
	}
	var metadata struct {
		WindowCertificate struct {
			OutputEvidence struct {
				ExtradataSHA256 string `json:"extradata_sha256"`
			} `json:"output_evidence"`
		} `json:"window_certificate"`
	}
	if err := json.Unmarshal([]byte(asset.MetadataJSON), &metadata); err != nil {
		return nil, nil
	}
	extradata := strings.ToLower(strings.TrimSpace(metadata.WindowCertificate.OutputEvidence.ExtradataSHA256))
	if !validStockSourceSHA256(extradata) {
		return nil, nil
	}
	return &PreparedWindowAssetBinding{AssetID: asset.AssetID, SHA256: asset.SHA256, SizeBytes: asset.SizeBytes, ExtradataSHA256: extradata}, nil
}
